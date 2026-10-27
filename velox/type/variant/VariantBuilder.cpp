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
#include "velox/type/variant/VariantBuilder.h"

#include <algorithm>
#include <cstring>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox {
namespace {

// Basic types in the low 2 bits of a value header.
constexpr uint8_t kObject{2};
constexpr uint8_t kArray{3};

// Primitive type ids. See VariantEncoding.md.
constexpr uint8_t kNullTypeId{0};
constexpr uint8_t kTrueTypeId{1};
constexpr uint8_t kFalseTypeId{2};
constexpr uint8_t kInt8TypeId{3};
constexpr uint8_t kInt16TypeId{4};
constexpr uint8_t kInt32TypeId{5};
constexpr uint8_t kInt64TypeId{6};
constexpr uint8_t kDoubleTypeId{7};
constexpr uint8_t kDecimal4TypeId{8};
constexpr uint8_t kDecimal8TypeId{9};
constexpr uint8_t kDecimal16TypeId{10};
constexpr uint8_t kDateTypeId{11};
constexpr uint8_t kTimestampTypeId{12};
constexpr uint8_t kTimestampNtzTypeId{13};
constexpr uint8_t kFloatTypeId{14};
constexpr uint8_t kBinaryTypeId{15};
constexpr uint8_t kStringTypeId{16};
constexpr uint8_t kUuidTypeId{20};

constexpr uint8_t kMaxShortStringSize{63};
constexpr uint8_t kMaxDecimal4Precision{9};
constexpr uint8_t kMaxDecimal8Precision{18};
constexpr uint8_t kMaxDecimal16Precision{38};
constexpr uint8_t kUuidSize{16};
constexpr uint8_t kMetadataVersion{1};

// Returns the smallest number of bytes, 1 to 4, that holds 'value'.
uint8_t integerSize(uint64_t value) {
  if (value <= 0xFF) {
    return 1;
  }
  if (value <= 0xFFFF) {
    return 2;
  }
  if (value <= 0xFF'FFFF) {
    return 3;
  }
  return 4;
}

void appendLittleEndianTo(std::string& out, uint64_t value, uint8_t size) {
  for (uint8_t i = 0; i < size; ++i) {
    out.push_back(static_cast<char>((value >> (8 * i)) & 0xFF));
  }
}

uint8_t numDigits(int128_t value) {
  auto magnitude = value < 0 ? -static_cast<uint128_t>(value)
                             : static_cast<uint128_t>(value);
  uint8_t digits{1};
  while (magnitude >= 10) {
    magnitude /= 10;
    ++digits;
  }
  return digits;
}

// Returns the size of the encoded value at the start of 'data'.
uint64_t valueSize(std::string_view data) {
  // Computing the size of a value does not read the metadata.
  static const std::string kEmptyMetadataBytes("\x01\x00\x00", 3);
  static const VariantMetadata kEmptyMetadata(kEmptyMetadataBytes);
  return VariantValue(kEmptyMetadata, data).data().size();
}

} // namespace

VariantBuilder::VariantBuilder(bool allowDuplicateKeys)
    : allowDuplicateKeys_{allowDuplicateKeys} {}

void VariantBuilder::checkSize(uint64_t size) const {
  if (size > kSizeLimit) {
    VELOX_USER_FAIL("Cannot build variant bigger than {} bytes.", kSizeLimit);
  }
}

void VariantBuilder::appendHeader(uint8_t typeId) {
  value_.push_back(static_cast<char>(typeId << 2));
}

void VariantBuilder::appendLittleEndian(uint64_t value, uint8_t size) {
  appendLittleEndianTo(value_, value, size);
}

void VariantBuilder::appendNull() {
  appendHeader(kNullTypeId);
}

void VariantBuilder::appendBoolean(bool value) {
  appendHeader(value ? kTrueTypeId : kFalseTypeId);
}

void VariantBuilder::appendLong(int64_t value) {
  if (value == static_cast<int8_t>(value)) {
    appendHeader(kInt8TypeId);
    appendLittleEndian(value, 1);
  } else if (value == static_cast<int16_t>(value)) {
    appendHeader(kInt16TypeId);
    appendLittleEndian(value, 2);
  } else if (value == static_cast<int32_t>(value)) {
    appendHeader(kInt32TypeId);
    appendLittleEndian(value, 4);
  } else {
    appendHeader(kInt64TypeId);
    appendLittleEndian(value, 8);
  }
  checkSize(value_.size());
}

void VariantBuilder::appendFloat(float value) {
  uint32_t bits;
  std::memcpy(&bits, &value, sizeof(bits));
  appendHeader(kFloatTypeId);
  appendLittleEndian(bits, 4);
  checkSize(value_.size());
}

void VariantBuilder::appendDouble(double value) {
  uint64_t bits;
  std::memcpy(&bits, &value, sizeof(bits));
  appendHeader(kDoubleTypeId);
  appendLittleEndian(bits, 8);
  checkSize(value_.size());
}

void VariantBuilder::appendDecimal(int128_t unscaledValue, uint8_t scale) {
  const auto precision = std::max(numDigits(unscaledValue), scale);
  if (precision <= kMaxDecimal4Precision) {
    appendHeader(kDecimal4TypeId);
    value_.push_back(static_cast<char>(scale));
    appendLittleEndian(static_cast<uint64_t>(unscaledValue), 4);
  } else if (precision <= kMaxDecimal8Precision) {
    appendHeader(kDecimal8TypeId);
    value_.push_back(static_cast<char>(scale));
    appendLittleEndian(static_cast<uint64_t>(unscaledValue), 8);
  } else {
    VELOX_USER_CHECK_LE(
        precision,
        kMaxDecimal16Precision,
        "Variant decimal exceeds the maximum precision.");
    appendHeader(kDecimal16TypeId);
    value_.push_back(static_cast<char>(scale));
    value_.append(
        reinterpret_cast<const char*>(&unscaledValue), sizeof(unscaledValue));
  }
  checkSize(value_.size());
}

void VariantBuilder::appendDate(int32_t days) {
  appendHeader(kDateTypeId);
  appendLittleEndian(static_cast<uint32_t>(days), 4);
  checkSize(value_.size());
}

void VariantBuilder::appendTimestamp(int64_t micros) {
  appendHeader(kTimestampTypeId);
  appendLittleEndian(micros, 8);
  checkSize(value_.size());
}

void VariantBuilder::appendTimestampNtz(int64_t micros) {
  appendHeader(kTimestampNtzTypeId);
  appendLittleEndian(micros, 8);
  checkSize(value_.size());
}

void VariantBuilder::appendString(std::string_view value) {
  checkSize(value_.size() + 1 + sizeof(uint32_t) + value.size());
  if (value.size() <= kMaxShortStringSize) {
    value_.push_back(static_cast<char>((value.size() << 2) | 1));
  } else {
    appendHeader(kStringTypeId);
    appendLittleEndian(value.size(), 4);
  }
  value_.append(value);
}

void VariantBuilder::appendBinary(std::string_view value) {
  checkSize(value_.size() + 1 + sizeof(uint32_t) + value.size());
  appendHeader(kBinaryTypeId);
  appendLittleEndian(value.size(), 4);
  value_.append(value);
}

void VariantBuilder::appendUuid(std::string_view bytes) {
  VELOX_USER_CHECK_EQ(bytes.size(), kUuidSize, "UUID must have 16 bytes.");
  appendHeader(kUuidTypeId);
  value_.append(bytes);
  checkSize(value_.size());
}

void VariantBuilder::appendVariant(const VariantValue& value) {
  switch (value.kind()) {
    case VariantKind::kObject: {
      const auto start = writePosition();
      std::vector<Field> fields;
      fields.reserve(value.numElements());
      for (uint32_t i = 0; i < value.numElements(); ++i) {
        fields.push_back(
            {addKey(value.fieldNameAt(i)), writePosition() - start});
        appendVariant(value.fieldValueAt(i));
      }
      finishObject(start, fields);
      break;
    }
    case VariantKind::kArray: {
      const auto start = writePosition();
      std::vector<uint64_t> offsets;
      offsets.reserve(value.numElements());
      for (uint32_t i = 0; i < value.numElements(); ++i) {
        offsets.push_back(writePosition() - start);
        appendVariant(value.elementAt(i));
      }
      finishArray(start, offsets);
      break;
    }
    default:
      checkSize(value_.size() + value.data().size());
      value_.append(value.data());
      break;
  }
}

uint32_t VariantBuilder::addKey(std::string_view key) {
  auto it = keyIds_.find(key);
  if (it != keyIds_.end()) {
    return it->second;
  }
  const auto id = static_cast<uint32_t>(keys_.size());
  keys_.emplace_back(key);
  keyIds_.emplace(key, id);
  return id;
}

void VariantBuilder::removeDuplicates(
    uint64_t start,
    std::vector<Field>& fields) {
  // Fields with equal names are adjacent. Keep the one appended last, which
  // has the largest offset.
  size_t numDistinct{0};
  for (size_t i = 0; i < fields.size(); ++i) {
    if (numDistinct > 0 && fields[numDistinct - 1].id == fields[i].id) {
      fields[numDistinct - 1].offset =
          std::max(fields[numDistinct - 1].offset, fields[i].offset);
    } else {
      fields[numDistinct++] = fields[i];
    }
  }
  if (numDistinct == fields.size()) {
    return;
  }
  fields.resize(numDistinct);

  // Move the kept values to the front, in offset order so that no value is
  // overwritten before it moves.
  std::sort(fields.begin(), fields.end(), [](const auto& a, const auto& b) {
    return a.offset < b.offset;
  });
  uint64_t currentOffset{0};
  for (auto& field : fields) {
    const auto size =
        valueSize(std::string_view(value_).substr(start + field.offset));
    std::memmove(
        value_.data() + start + currentOffset,
        value_.data() + start + field.offset,
        size);
    field.offset = currentOffset;
    currentOffset += size;
  }
  value_.resize(start + currentOffset);
  std::sort(fields.begin(), fields.end(), [&](const auto& a, const auto& b) {
    return keys_[a.id] < keys_[b.id];
  });
}

void VariantBuilder::finishObject(uint64_t start, std::vector<Field>& fields) {
  std::stable_sort(
      fields.begin(), fields.end(), [&](const auto& a, const auto& b) {
        return keys_[a.id] < keys_[b.id];
      });
  if (allowDuplicateKeys_) {
    removeDuplicates(start, fields);
  } else {
    for (size_t i = 1; i < fields.size(); ++i) {
      if (fields[i].id == fields[i - 1].id) {
        VELOX_USER_FAIL(
            "Failed to build variant because of a duplicate object key: {}",
            keys_[fields[i].id]);
      }
    }
  }

  uint32_t maxId{0};
  for (const auto& field : fields) {
    maxId = std::max(maxId, field.id);
  }
  const uint64_t dataSize = value_.size() - start;
  const uint64_t numFields = fields.size();
  const bool isLarge = numFields > 0xFF;
  const uint8_t idSize = integerSize(maxId);
  const uint8_t offsetSize = integerSize(dataSize);

  std::string header;
  header.push_back(
      static_cast<char>(
          ((((isLarge ? 1 : 0) << 4) | ((idSize - 1) << 2) | (offsetSize - 1))
           << 2) |
          kObject));
  appendLittleEndianTo(header, numFields, isLarge ? 4 : 1);
  for (const auto& field : fields) {
    appendLittleEndianTo(header, field.id, idSize);
  }
  for (const auto& field : fields) {
    appendLittleEndianTo(header, field.offset, offsetSize);
  }
  appendLittleEndianTo(header, dataSize, offsetSize);
  checkSize(value_.size() + header.size());
  value_.insert(start, header);
}

void VariantBuilder::finishArray(
    uint64_t start,
    const std::vector<uint64_t>& offsets) {
  const uint64_t dataSize = value_.size() - start;
  const uint64_t numElements = offsets.size();
  const bool isLarge = numElements > 0xFF;
  const uint8_t offsetSize = integerSize(dataSize);

  std::string header;
  header.push_back(
      static_cast<char>(
          ((((isLarge ? 1 : 0) << 2) | (offsetSize - 1)) << 2) | kArray));
  appendLittleEndianTo(header, numElements, isLarge ? 4 : 1);
  for (auto offset : offsets) {
    appendLittleEndianTo(header, offset, offsetSize);
  }
  appendLittleEndianTo(header, dataSize, offsetSize);
  checkSize(value_.size() + header.size());
  value_.insert(start, header);
}

std::string VariantBuilder::metadata() const {
  uint64_t stringsSize{0};
  for (const auto& key : keys_) {
    stringsSize += key.size();
  }
  const uint64_t numKeys = keys_.size();
  const uint8_t offsetSize = integerSize(std::max(stringsSize, numKeys));
  checkSize(1 + (numKeys + 2) * offsetSize + stringsSize);

  std::string out;
  out.reserve(1 + (numKeys + 2) * offsetSize + stringsSize);
  out.push_back(static_cast<char>(kMetadataVersion | ((offsetSize - 1) << 6)));
  appendLittleEndianTo(out, numKeys, offsetSize);
  uint64_t offset{0};
  for (const auto& key : keys_) {
    appendLittleEndianTo(out, offset, offsetSize);
    offset += key.size();
  }
  appendLittleEndianTo(out, offset, offsetSize);
  for (const auto& key : keys_) {
    out.append(key);
  }
  return out;
}

} // namespace facebook::velox
