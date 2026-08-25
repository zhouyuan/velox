/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/dwio/common/BitPackDecoder.h"

#include <cstring>

namespace facebook::velox::dwio::common {

using int128_t = __int128_t;

#if XSIMD_WITH_AVX2

typedef int32_t __m256si __attribute__((__vector_size__(32), __may_alias__));

typedef int32_t __m256si_u
    __attribute__((__vector_size__(32), __may_alias__, __aligned__(1)));

namespace {

template <int8_t i>
auto as4x64(__m256i x) {
  return _mm256_cvtepu32_epi64(_mm256_extracti128_si256(x, i));
}

template <typename T>
void store8Ints(__m256i eightInts, int32_t i, T* result) {
  if (sizeof(T) == 4) {
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result + i), eightInts);
  } else {
    _mm256_storeu_si256(
        reinterpret_cast<__m256i*>(result + i), as4x64<0>(eightInts));
    _mm256_storeu_si256(
        reinterpret_cast<__m256i*>(result + i + 4), as4x64<1>(eightInts));
  }
}

// Safely load a uint64_t from a potentially unaligned address computed as
// 'pointer' + 'byteOffset' bytes.
inline uint64_t loadUnaligned64(const uint64_t* pointer, int32_t byteOffset) {
  uint64_t value;
  std::memcpy(
      &value,
      reinterpret_cast<const char*>(pointer) + byteOffset,
      sizeof(value));
  return value;
}

template <typename T>
inline __m256i as256i(T x) {
  return reinterpret_cast<__m256i>(x);
}

template <typename T>
inline __m256si as8x32(T x) {
  return reinterpret_cast<__m256si>(x);
}

template <uint8_t width, typename T>
FOLLY_ALWAYS_INLINE __m256i gather8Sparse(
    const uint64_t* bits,
    int32_t bitOffset,
    const int32_t* rows,
    int32_t i,
    __m256si masks,
    T* result) {
  constexpr __m256si kMultipliers = {256, 128, 64, 32, 16, 8, 4, 2};
  // workaround for:
  // https://github.com/llvm/llvm-project/issues/64819#issuecomment-1684943890
  constexpr __m256si kWidthSplat = {
      width, width, width, width, width, width, width, width};

  // When 'width' is a multiple of 8 the intra-byte shift below is skipped, so
  // every field must be byte aligned. That holds for the only caller: a
  // Parquet bit-packed run starts byte aligned and a group of 8 fields spans
  // whole bytes, so 'bitOffset' stays 0 for these widths.
  VELOX_DCHECK(width % 8 != 0 || bitOffset == 0);
  auto indices =
      *reinterpret_cast<const __m256si_u*>(rows + i) * kWidthSplat + bitOffset;
  __m256si multipliers;
  if (width % 8 != 0) {
    multipliers = (__m256si)_mm256_permutevar8x32_epi32(
        as256i(kMultipliers), as256i(indices & 7));
  }
  auto byteIndices = indices >> 3;
  auto data = as8x32(_mm256_i32gather_epi32(
      reinterpret_cast<const int*>(bits), as256i(byteIndices), 1));
  if (width % 8 != 0) {
    data = (data * multipliers) >> 8;
  }
  return as256i(data & masks);
}

// Byte-shuffle indices and per-lane shifts that place 8 consecutive
// bit-packed fields into 8 32-bit lanes. A group of 8 fields spans exactly
// 'width' whole bytes, so these tables depend only on the bit offset of the
// group's first field within its first byte.
struct ContiguousTables {
  // Byte selectors for _mm256_shuffle_epi8, four per lane.
  __m256i shuffle;
  // Right shift that moves each field to the bottom of its lane.
  __m256i shift;
  // Offset of the 128-bit load feeding lanes 4 through 7, relative to the
  // first byte of the group.
  int32_t highHalfByte;
};

template <uint8_t width>
ContiguousTables makeContiguousTables(int32_t startShift) {
  alignas(32) int8_t shuffle[32];
  alignas(32) int32_t shifts[8];
  const int32_t highHalfByte = (startShift + 4 * width) >> 3;
  for (int32_t lane = 0; lane < 4; ++lane) {
    const int32_t lowBit = startShift + lane * width;
    const int32_t highBit = startShift + (lane + 4) * width;
    shifts[lane] = lowBit & 7;
    shifts[lane + 4] = highBit & 7;
    // _mm256_shuffle_epi8 selects within each 128-bit half, so the high
    // lanes index relative to their own load.
    for (int32_t byte = 0; byte < 4; ++byte) {
      shuffle[lane * 4 + byte] = static_cast<int8_t>((lowBit >> 3) + byte);
      shuffle[16 + lane * 4 + byte] =
          static_cast<int8_t>(((highBit >> 3) - highHalfByte) + byte);
    }
  }
  return {
      _mm256_load_si256(reinterpret_cast<const __m256i*>(shuffle)),
      _mm256_load_si256(reinterpret_cast<const __m256i*>(shifts)),
      highHalfByte,
  };
}

// Decodes 8 consecutive fields using neither pdep nor gather: each 32-bit
// lane collects the bytes holding its field with a byte shuffle, then a
// per-lane variable shift and mask.
//
// Requires 17 <= width <= 25. The lower bound keeps the two 128-bit loads
// within the 8-byte redzone that unpack() already reserves past the last
// field; narrower widths would read further past the buffer. The upper bound
// keeps a field plus its up-to-7-bit start offset inside 32 bits.
template <uint8_t width>
FOLLY_ALWAYS_INLINE __m256i decode8Contiguous(
    const uint64_t* bits,
    int64_t startBit,
    const ContiguousTables& tables) {
  static_assert(width >= 17 && width <= 25);
  const char* base = reinterpret_cast<const char*>(bits) + (startBit >> 3);
  const __m256i source = _mm256_set_m128i(
      _mm_loadu_si128(
          reinterpret_cast<const __m128i*>(base + tables.highHalfByte)),
      _mm_loadu_si128(reinterpret_cast<const __m128i*>(base)));
  return _mm256_and_si256(
      _mm256_srlv_epi32(
          _mm256_shuffle_epi8(source, tables.shuffle), tables.shift),
      _mm256_set1_epi32(bits::lowMask(width)));
}

template <uint8_t width, typename T>
int32_t decode1To24(
    const uint64_t* bits,
    int32_t bitOffset,
    const int* rows,
    int32_t numRows,
    T* result) {
  constexpr uint64_t kMask = bits::lowMask(width);
  constexpr uint64_t kMask2 = kMask | (kMask << 8);
  constexpr uint64_t kMask4 = kMask2 | (kMask2 << 16);
  constexpr uint64_t kDepMask8 = kMask4 | (kMask4 << 32);
  constexpr uint64_t kMask16 = kMask | (kMask << 16);
  constexpr uint64_t kDepMask16 = kMask16 | (kMask16 << 32);
  int32_t i = 0;
  const auto masks = as8x32(_mm256_set1_epi32(kMask));
  // Tables for the pdep-free contiguous kernel. Consecutive groups in one
  // contiguous run share a start bit offset, because 8 fields span 'width'
  // whole bytes, so this is rebuilt at most once per run.
  [[maybe_unused]] int32_t tableShift = -1;
  [[maybe_unused]] ContiguousTables tables{};
  for (; i + 8 <= numRows; i += 8) {
    auto row = rows[i];
    auto endRow = rows[i + 7];
    __m256i eightInts;
    if constexpr (width >= 17) {
      if (endRow - row == 7) {
        // Widths 17 and up have no pdep kernel, so they would otherwise
        // gather. When the fields are adjacent a byte shuffle reaches all of
        // them from two loads, at a fraction of a gather's throughput cost.
        const int64_t startBit =
            static_cast<int64_t>(bitOffset) + static_cast<int64_t>(row) * width;
        const int32_t startShift = static_cast<int32_t>(startBit & 7);
        if (startShift != tableShift) {
          tableShift = startShift;
          tables = makeContiguousTables<width>(startShift);
        }
        eightInts = decode8Contiguous<width>(bits, startBit, tables);
      } else {
        eightInts =
            gather8Sparse<width>(bits, bitOffset, rows, i, masks, result);
      }
    } else if (endRow - row == 7) {
      // Special cases for 8 contiguous values with <= 16 bits.
      if (width <= 8) {
        uint64_t eightBytes;
        if (width == 8) {
          if (!bitOffset) {
            eightBytes = loadUnaligned64(bits, row);
          } else {
            eightBytes =
                bits::detail::loadBits<uint64_t>(bits, bitOffset + 8 * row, 64);
          }
        } else {
          auto bit = row * width + bitOffset;
          auto byte = bit >> 3;
          auto shift = bit & 7;
          uint64_t word = loadUnaligned64(bits, byte) >> shift;
          eightBytes = _pdep_u64(word, kDepMask8);
        }
        eightInts = _mm256_cvtepu8_epi32(
            _mm_loadl_epi64(reinterpret_cast<const __m128i*>(&eightBytes)));
      } else {
        // Use pdep to shift 2 words of bit packed data with width
        // 9-16. For widts <= 14 four bit packed fields can always be
        // loaded with a single uint64_t load. For 15 and 16 bits this
        // depends on the start bit position. For either case we fill
        // an array of 2x64 bits and widen that to a 8x32 word.
        uint64_t words[2];
        if (width <= 14) {
          auto bit = row * width + bitOffset;
          auto byte = bit >> 3;
          auto shift = bit & 7;
          uint64_t word = loadUnaligned64(bits, byte) >> shift;
          words[0] = _pdep_u64(word, kDepMask16);
          bit += 4 * width;
          byte = bit >> 3;
          shift = bit & 7;
          word = loadUnaligned64(bits, byte) >> shift;
          words[1] = _pdep_u64(word, kDepMask16);
        } else {
          words[0] = bits::detail::loadBits<uint64_t>(
              bits, bitOffset + width * row, 64);
          words[1] = bits::detail::loadBits<uint64_t>(
              bits, bitOffset + width * (row + 4), 64);
          if (width == 15) {
            words[0] = _pdep_u64(words[0], kDepMask16);
            words[1] = _pdep_u64(words[1], kDepMask16);
          }
        }
        eightInts = _mm256_cvtepu16_epi32(
            _mm_load_si128(reinterpret_cast<const __m128i*>(&words)));
      }
    } else {
      eightInts = gather8Sparse<width>(bits, bitOffset, rows, i, masks, result);
    }
    store8Ints(eightInts, i, result);
  }
  return i;
}

#define WIDTH_CASE(width)                                                      \
  case width:                                                                  \
    i = decode1To24<width>(bits, bitOffset, rows.data(), numSafeRows, result); \
    break;

} // namespace

#endif

template <typename T>
void unpack(
    const uint64_t* bits,
    int32_t bitOffset,
    RowSet rows,
    int32_t rowBias,
    uint8_t bitWidth,
    const char* bufferEnd,
    T* result) {
  uint64_t mask = bits::lowMask(bitWidth);

  if (bitWidth == 0) {
    // A column of dictionary indices can be 0 bits wide if all indices are 0.
    memset(result, 0, rows.size() * sizeof(T));
    return;
  }
  VELOX_DCHECK_NOT_NULL(bits);

  // We subtract rowBias * bitWidth bits from the starting position.
  bitOffset -= rowBias * bitWidth;
  if (bitOffset < 0) {
    // Decrement the pointer by enough bytes to have a non-negative bitOffset.
    auto bytes = bits::divRoundUp(-bitOffset, 8);
    bitOffset += bytes * 8;
    bits = reinterpret_cast<const uint64_t*>(
        reinterpret_cast<const char*>(bits) - bytes);
  }
  auto numRows = rows.size();
  if (bitWidth > 56) {
    for (auto i = 0; i < numRows; ++i) {
      auto bit = bitOffset + (rows[i]) * bitWidth;
      result[i] = bits::detail::loadBits<T>(bits, bit, bitWidth) & mask;
    }
    return;
  }
  int32_t numSafeRows = numRows;
  bool anyUnsafe = false;
  if (bufferEnd) {
    const char* endByte = reinterpret_cast<const char*>(bits) +
        bits::divRoundUp(bitOffset + (rows.back() + 1) * bitWidth, 8);
    // redzone is the number of bytes at the end of the accessed range that
    // could overflow the buffer if accessed 64 its wide.
    int64_t redZone =
        sizeof(uint64_t) - static_cast<int64_t>(bufferEnd - endByte);
    if (redZone > 0) {
      anyUnsafe = true;
      auto numRed = (redZone + 1) * 8 / bitWidth;
      int32_t lastSafeIndex = rows.back() - numRed;
      --numSafeRows;
      for (; numSafeRows >= 1; --numSafeRows) {
        if (rows[numSafeRows - 1] < lastSafeIndex) {
          break;
        }
      }
    }
  }
  int32_t i = 0;

#if XSIMD_WITH_AVX2
  // Use AVX2 for specific widths.
  switch (bitWidth) {
    WIDTH_CASE(1);
    WIDTH_CASE(2);
    WIDTH_CASE(3);
    WIDTH_CASE(4);
    WIDTH_CASE(5);
    WIDTH_CASE(6);
    WIDTH_CASE(7);
    WIDTH_CASE(8);
    WIDTH_CASE(9);
    WIDTH_CASE(10);
    WIDTH_CASE(11);
    WIDTH_CASE(12);
    WIDTH_CASE(13);
    WIDTH_CASE(14);
    WIDTH_CASE(15);
    WIDTH_CASE(16);
    WIDTH_CASE(17);
    WIDTH_CASE(18);
    WIDTH_CASE(19);
    WIDTH_CASE(20);
    WIDTH_CASE(21);
    WIDTH_CASE(22);
    WIDTH_CASE(23);
    WIDTH_CASE(24);
    default:
      break;
  }
#endif

  for (; i < numSafeRows; ++i) {
    auto bit = bitOffset + (rows[i]) * bitWidth;
    auto byte = bit / 8;
    auto shift = bit & 7;
    uint64_t word;
    std::memcpy(
        &word, reinterpret_cast<const char*>(bits) + byte, sizeof(word));
    result[i] = (word >> shift) & mask;
  }
  if (anyUnsafe) {
    auto lastSafeWord = bufferEnd - sizeof(uint64_t);
    VELOX_DCHECK(lastSafeWord);
    for (auto i_2 = numSafeRows; i_2 < numRows; ++i_2) {
      auto bit = bitOffset + (rows[i_2]) * bitWidth;
      auto byte = bit / 8;
      auto shift = bit & 7;
      result[i_2] = safeLoadBits(
                        reinterpret_cast<const char*>(bits) + byte,
                        shift,
                        bitWidth,
                        lastSafeWord) &
          mask;
    }
  }
}

template void unpack(
    const uint64_t* bits,
    int32_t bitOffset,
    RowSet rows,
    int32_t rowBias,
    uint8_t bitWidth,
    const char* bufferEnd,
    int32_t* result);

template void unpack(
    const uint64_t* bits,
    int32_t bitOffset,
    RowSet rows,
    int32_t rowBias,
    uint8_t bitWidth,
    const char* bufferEnd,
    int64_t* result);

template void unpack(
    const uint64_t* bits,
    int32_t bitOffset,
    RowSet rows,
    int32_t rowBias,
    uint8_t bitWidth,
    const char* bufferEnd,
    int128_t* result);

template void unpack(
    const uint64_t* bits,
    int32_t bitOffset,
    RowSet rows,
    int32_t rowBias,
    uint8_t bitWidth,
    const char* bufferEnd,
    int16_t* result);

} // namespace facebook::velox::dwio::common
