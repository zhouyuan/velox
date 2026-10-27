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
#include "velox/type/variant/VariantEncoding.h"

#include <cstring>

#include <folly/Portability.h>
#include <folly/container/F14Map.h>

#include "velox/common/EnumDefine.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox {

// The encoding is little-endian. Reads below copy bytes directly into native
// integers.
static_assert(folly::kIsLittleEndian);

namespace {

const auto& variantKindNames() {
  static const folly::F14FastMap<VariantKind, std::string_view> kNames = {
      {VariantKind::kNull, "NULL"},
      {VariantKind::kBoolean, "BOOLEAN"},
      {VariantKind::kInt8, "INT8"},
      {VariantKind::kInt16, "INT16"},
      {VariantKind::kInt32, "INT32"},
      {VariantKind::kInt64, "INT64"},
      {VariantKind::kFloat, "FLOAT"},
      {VariantKind::kDouble, "DOUBLE"},
      {VariantKind::kDecimal4, "DECIMAL4"},
      {VariantKind::kDecimal8, "DECIMAL8"},
      {VariantKind::kDecimal16, "DECIMAL16"},
      {VariantKind::kDate, "DATE"},
      {VariantKind::kTime, "TIME"},
      {VariantKind::kTimestamp, "TIMESTAMP"},
      {VariantKind::kTimestampNtz, "TIMESTAMP_NTZ"},
      {VariantKind::kTimestampNanos, "TIMESTAMP_NANOS"},
      {VariantKind::kTimestampNtzNanos, "TIMESTAMP_NTZ_NANOS"},
      {VariantKind::kBinary, "BINARY"},
      {VariantKind::kString, "STRING"},
      {VariantKind::kUuid, "UUID"},
      {VariantKind::kObject, "OBJECT"},
      {VariantKind::kArray, "ARRAY"},
  };
  return kNames;
}

// Values of the 2-bit basic type in the low bits of a value header.
constexpr uint8_t kPrimitive{0};
constexpr uint8_t kShortString{1};
constexpr uint8_t kObject{2};
constexpr uint8_t kArray{3};

// Primitive type id of a 'true' boolean. Booleans carry their value in the
// type id and have no payload.
constexpr uint8_t kTrueTypeId{1};

constexpr uint8_t kMetadataVersion{1};

// Objects with fewer fields than this are searched linearly.
constexpr uint32_t kBinarySearchThreshold{32};

// Reads a little-endian unsigned integer of 'size' bytes, 1 to 4, at 'pos'.
// The caller must have checked that the bytes are in range.
uint32_t readUnsigned(std::string_view data, uint64_t pos, uint8_t size) {
  uint32_t result{0};
  std::memcpy(&result, data.data() + pos, size);
  return result;
}

template <typename T>
T readValue(std::string_view data, uint64_t pos) {
  T result;
  std::memcpy(&result, data.data() + pos, sizeof(T));
  return result;
}

// Reads a decimal payload: a 1-byte scale followed by the unscaled value.
template <typename T>
VariantDecimal readDecimal(std::string_view data, uint64_t pos) {
  return {readValue<T>(data, pos + 1), static_cast<uint8_t>(data[pos])};
}

void checkInBounds(uint64_t end, std::string_view data) {
  VELOX_USER_CHECK_LE(end, data.size(), "Variant bytes are truncated.");
}

struct PrimitiveInfo {
  VariantKind kind;
  // Size of the fixed-width payload. Strings and binaries have a 4-byte
  // length followed by a variable-size payload.
  uint8_t fixedSize;
};

std::optional<PrimitiveInfo> primitiveInfo(uint8_t typeId) {
  switch (typeId) {
    case 0:
      return PrimitiveInfo{VariantKind::kNull, 0};
    case 1:
    case 2:
      return PrimitiveInfo{VariantKind::kBoolean, 0};
    case 3:
      return PrimitiveInfo{VariantKind::kInt8, 1};
    case 4:
      return PrimitiveInfo{VariantKind::kInt16, 2};
    case 5:
      return PrimitiveInfo{VariantKind::kInt32, 4};
    case 6:
      return PrimitiveInfo{VariantKind::kInt64, 8};
    case 7:
      return PrimitiveInfo{VariantKind::kDouble, 8};
    case 8:
      return PrimitiveInfo{VariantKind::kDecimal4, 5};
    case 9:
      return PrimitiveInfo{VariantKind::kDecimal8, 9};
    case 10:
      return PrimitiveInfo{VariantKind::kDecimal16, 17};
    case 11:
      return PrimitiveInfo{VariantKind::kDate, 4};
    case 12:
      return PrimitiveInfo{VariantKind::kTimestamp, 8};
    case 13:
      return PrimitiveInfo{VariantKind::kTimestampNtz, 8};
    case 14:
      return PrimitiveInfo{VariantKind::kFloat, 4};
    case 15:
      return PrimitiveInfo{VariantKind::kBinary, 4};
    case 16:
      return PrimitiveInfo{VariantKind::kString, 4};
    case 17:
      return PrimitiveInfo{VariantKind::kTime, 8};
    case 18:
      return PrimitiveInfo{VariantKind::kTimestampNanos, 8};
    case 19:
      return PrimitiveInfo{VariantKind::kTimestampNtzNanos, 8};
    case 20:
      return PrimitiveInfo{VariantKind::kUuid, 16};
    default:
      return std::nullopt;
  }
}

} // namespace

VELOX_DEFINE_ENUM_NAME(VariantKind, variantKindNames);

VariantMetadata::VariantMetadata(std::string_view data) : data_{data} {
  VELOX_USER_CHECK(!data_.empty(), "Variant metadata must not be empty.");
  const auto header = static_cast<uint8_t>(data_[0]);
  VELOX_USER_CHECK_EQ(
      header & 0x0F, kMetadataVersion, "Unsupported Variant metadata version.");
  sortedStrings_ = (header >> 4) & 1;
  offsetSize_ = ((header >> 6) & 0x03) + 1;

  checkInBounds(1 + offsetSize_, data_);
  dictionarySize_ = readUnsigned(data_, 1, offsetSize_);
  offsetsStart_ = 1 + offsetSize_;
  stringsStart_ = offsetsStart_ +
      (static_cast<uint64_t>(dictionarySize_) + 1) * offsetSize_;
  checkInBounds(stringsStart_, data_);
}

std::string_view VariantMetadata::keyAt(uint32_t id) const {
  VELOX_USER_CHECK_LT(id, dictionarySize_, "Variant field id is out of range.");
  const auto begin = readUnsigned(
      data_,
      offsetsStart_ + static_cast<uint64_t>(id) * offsetSize_,
      offsetSize_);
  const auto end = readUnsigned(
      data_,
      offsetsStart_ + (static_cast<uint64_t>(id) + 1) * offsetSize_,
      offsetSize_);
  VELOX_USER_CHECK_LE(begin, end, "Variant metadata offsets are not ordered.");
  checkInBounds(stringsStart_ + end, data_);
  return data_.substr(stringsStart_ + begin, end - begin);
}

VariantValue::VariantValue(
    const VariantMetadata& metadata,
    std::string_view data)
    : metadata_{&metadata}, data_{data} {
  VELOX_USER_CHECK(!data_.empty(), "Variant value must not be empty.");
  const auto header = static_cast<uint8_t>(data_[0]);
  const uint8_t valueHeader = header >> 2;
  switch (header & 0x03) {
    case kPrimitive:
      initPrimitive(valueHeader);
      break;
    case kShortString:
      kind_ = VariantKind::kString;
      payloadSize_ = valueHeader;
      size_ = payloadStart_ + payloadSize_;
      checkInBounds(size_, data_);
      break;
    case kObject:
      initObject(valueHeader);
      break;
    case kArray:
      initArray(valueHeader);
      break;
    default:
      VELOX_UNREACHABLE();
  }
}

void VariantValue::initPrimitive(uint8_t typeId) {
  const auto info = primitiveInfo(typeId);
  VELOX_USER_CHECK(
      info.has_value(), "Unsupported Variant primitive type: {}", typeId);
  kind_ = info->kind;
  size_ = payloadStart_ + info->fixedSize;
  checkInBounds(size_, data_);
  switch (kind_) {
    case VariantKind::kString:
    case VariantKind::kBinary:
      payloadSize_ = readValue<uint32_t>(data_, payloadStart_);
      payloadStart_ += sizeof(uint32_t);
      size_ = payloadStart_ + payloadSize_;
      checkInBounds(size_, data_);
      break;
    case VariantKind::kUuid:
      payloadSize_ = info->fixedSize;
      break;
    default:
      break;
  }
}

void VariantValue::initObject(uint8_t valueHeader) {
  kind_ = VariantKind::kObject;
  offsetSize_ = (valueHeader & 0x03) + 1;
  fieldIdSize_ = ((valueHeader >> 2) & 0x03) + 1;
  const bool isLarge = (valueHeader >> 4) & 1;
  const uint8_t numElementsSize = isLarge ? 4 : 1;

  checkInBounds(1 + numElementsSize, data_);
  numElements_ = readUnsigned(data_, 1, numElementsSize);
  fieldIdsStart_ = 1 + numElementsSize;
  offsetsStart_ =
      fieldIdsStart_ + static_cast<uint64_t>(numElements_) * fieldIdSize_;
  valuesStart_ =
      offsetsStart_ + (static_cast<uint64_t>(numElements_) + 1) * offsetSize_;
  checkInBounds(valuesStart_, data_);
  valuesSize_ = readUnsigned(
      data_,
      offsetsStart_ + static_cast<uint64_t>(numElements_) * offsetSize_,
      offsetSize_);
  size_ = valuesStart_ + valuesSize_;
  checkInBounds(size_, data_);
}

void VariantValue::initArray(uint8_t valueHeader) {
  kind_ = VariantKind::kArray;
  offsetSize_ = (valueHeader & 0x03) + 1;
  const bool isLarge = (valueHeader >> 2) & 1;
  const uint8_t numElementsSize = isLarge ? 4 : 1;

  checkInBounds(1 + numElementsSize, data_);
  numElements_ = readUnsigned(data_, 1, numElementsSize);
  offsetsStart_ = 1 + numElementsSize;
  valuesStart_ =
      offsetsStart_ + (static_cast<uint64_t>(numElements_) + 1) * offsetSize_;
  checkInBounds(valuesStart_, data_);
  valuesSize_ = readUnsigned(
      data_,
      offsetsStart_ + static_cast<uint64_t>(numElements_) * offsetSize_,
      offsetSize_);
  size_ = valuesStart_ + valuesSize_;
  checkInBounds(size_, data_);
}

void VariantValue::checkKind(VariantKind expected) const {
  VELOX_USER_CHECK_EQ(kind_, expected, "Unexpected Variant value kind.");
}

void VariantValue::checkContainer() const {
  VELOX_USER_CHECK(
      kind_ == VariantKind::kObject || kind_ == VariantKind::kArray,
      "Variant value is not an object or array: {}",
      kind_);
}

bool VariantValue::asBoolean() const {
  checkKind(VariantKind::kBoolean);
  return (static_cast<uint8_t>(data_[0]) >> 2) == kTrueTypeId;
}

int64_t VariantValue::asInteger() const {
  switch (kind_) {
    case VariantKind::kInt8:
      return readValue<int8_t>(data_, payloadStart_);
    case VariantKind::kInt16:
      return readValue<int16_t>(data_, payloadStart_);
    case VariantKind::kInt32:
      return readValue<int32_t>(data_, payloadStart_);
    case VariantKind::kInt64:
      return readValue<int64_t>(data_, payloadStart_);
    default:
      VELOX_USER_FAIL("Variant value is not an integer: {}", kind_);
  }
}

float VariantValue::asFloat() const {
  checkKind(VariantKind::kFloat);
  return readValue<float>(data_, payloadStart_);
}

double VariantValue::asDouble() const {
  checkKind(VariantKind::kDouble);
  return readValue<double>(data_, payloadStart_);
}

VariantDecimal VariantValue::asDecimal() const {
  switch (kind_) {
    case VariantKind::kDecimal4:
      return readDecimal<int32_t>(data_, payloadStart_);
    case VariantKind::kDecimal8:
      return readDecimal<int64_t>(data_, payloadStart_);
    case VariantKind::kDecimal16:
      return readDecimal<int128_t>(data_, payloadStart_);
    default:
      VELOX_USER_FAIL("Variant value is not a decimal: {}", kind_);
  }
}

int32_t VariantValue::asDate() const {
  checkKind(VariantKind::kDate);
  return readValue<int32_t>(data_, payloadStart_);
}

int64_t VariantValue::asTimestamp() const {
  switch (kind_) {
    case VariantKind::kTime:
    case VariantKind::kTimestamp:
    case VariantKind::kTimestampNtz:
    case VariantKind::kTimestampNanos:
    case VariantKind::kTimestampNtzNanos:
      return readValue<int64_t>(data_, payloadStart_);
    default:
      VELOX_USER_FAIL("Variant value is not a time or timestamp: {}", kind_);
  }
}

std::string_view VariantValue::payload() const {
  return data_.substr(payloadStart_, payloadSize_);
}

std::string_view VariantValue::asString() const {
  checkKind(VariantKind::kString);
  return payload();
}

std::string_view VariantValue::asBinary() const {
  checkKind(VariantKind::kBinary);
  return payload();
}

std::string_view VariantValue::asUuid() const {
  checkKind(VariantKind::kUuid);
  return payload();
}

uint32_t VariantValue::numElements() const {
  checkContainer();
  return numElements_;
}

uint32_t VariantValue::fieldIdAt(uint32_t index) const {
  return readUnsigned(
      data_,
      fieldIdsStart_ + static_cast<uint64_t>(index) * fieldIdSize_,
      fieldIdSize_);
}

VariantValue VariantValue::childAt(uint32_t index) const {
  const auto offset = readUnsigned(
      data_,
      offsetsStart_ + static_cast<uint64_t>(index) * offsetSize_,
      offsetSize_);
  VELOX_USER_CHECK_LT(
      offset, valuesSize_, "Variant child offset is out of range.");
  return VariantValue(
      *metadata_, data_.substr(valuesStart_ + offset, valuesSize_ - offset));
}

std::string_view VariantValue::fieldNameAt(uint32_t index) const {
  checkKind(VariantKind::kObject);
  VELOX_USER_CHECK_LT(index, numElements_);
  return metadata_->keyAt(fieldIdAt(index));
}

VariantValue VariantValue::fieldValueAt(uint32_t index) const {
  checkKind(VariantKind::kObject);
  VELOX_USER_CHECK_LT(index, numElements_);
  return childAt(index);
}

std::optional<VariantValue> VariantValue::field(std::string_view name) const {
  checkKind(VariantKind::kObject);
  // Spark sorts field names by UTF-16 code units rather than UTF-8 bytes. The
  // orders differ only for some non-BMP names. A linear scan of small objects
  // finds those names, and is as fast as binary search at this size.
  if (numElements_ < kBinarySearchThreshold) {
    for (uint32_t i = 0; i < numElements_; ++i) {
      if (metadata_->keyAt(fieldIdAt(i)) == name) {
        return childAt(i);
      }
    }
    return std::nullopt;
  }

  // The encoding orders fields by name, so binary search applies even when
  // the metadata dictionary is not sorted.
  uint32_t low{0};
  uint32_t high{numElements_};
  while (low < high) {
    const uint32_t middle = low + (high - low) / 2;
    const auto middleName = metadata_->keyAt(fieldIdAt(middle));
    if (middleName == name) {
      return childAt(middle);
    }
    if (middleName < name) {
      low = middle + 1;
    } else {
      high = middle;
    }
  }
  return std::nullopt;
}

VariantValue VariantValue::elementAt(uint32_t index) const {
  checkKind(VariantKind::kArray);
  VELOX_USER_CHECK_LT(index, numElements_);
  return childAt(index);
}

} // namespace facebook::velox
