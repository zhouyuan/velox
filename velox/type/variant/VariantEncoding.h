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
#pragma once

#include <cstdint>
#include <optional>
#include <string_view>

#include <fmt/format.h>

#include "velox/common/EnumDeclare.h"
#include "velox/type/HugeInt.h"

/// Read-only views over the Parquet Variant binary encoding.
///
/// A Variant value is a pair of byte strings: 'metadata', which holds a
/// dictionary of field names, and 'value', which holds a recursively encoded
/// value whose object fields refer to the dictionary by id. The views below
/// navigate these bytes in place, without allocating or copying.
///
/// See https://github.com/apache/parquet-format/blob/master/VariantEncoding.md.
namespace facebook::velox {

/// Kind of a value in the Variant encoding. Short strings and long strings
/// both map to kString.
enum class VariantKind : uint8_t {
  kNull,
  kBoolean,
  kInt8,
  kInt16,
  kInt32,
  kInt64,
  kFloat,
  kDouble,
  kDecimal4,
  kDecimal8,
  kDecimal16,
  kDate,
  kTime,
  kTimestamp,
  kTimestampNtz,
  kTimestampNanos,
  kTimestampNtzNanos,
  kBinary,
  kString,
  kUuid,
  kObject,
  kArray,
};

VELOX_DECLARE_ENUM_NAME(VariantKind);

/// Decimal value stored as an unscaled integer and a scale.
struct VariantDecimal {
  int128_t unscaledValue;
  uint8_t scale;
};

/// View over the metadata bytes of a Variant. Does not own the bytes; they
/// must outlive the view.
class VariantMetadata {
 public:
  /// Validates the header and the dictionary bounds. Throws a user error if
  /// 'data' is malformed or uses an unsupported version.
  explicit VariantMetadata(std::string_view data);

  /// Returns the number of field names in the dictionary.
  uint32_t dictionarySize() const {
    return dictionarySize_;
  }

  /// Returns the metadata bytes.
  std::string_view data() const {
    return data_;
  }

  /// Returns true if the dictionary is sorted and has no duplicate names.
  bool sortedStrings() const {
    return sortedStrings_;
  }

  /// Returns the field name with the given dictionary id. Throws a user error
  /// if 'id' is out of range or the name lies outside the metadata bytes.
  std::string_view keyAt(uint32_t id) const;

 private:
  std::string_view data_;
  uint8_t offsetSize_;
  bool sortedStrings_;
  uint32_t dictionarySize_;
  // Position in 'data_' of the first entry of the offset list.
  uint64_t offsetsStart_;
  // Position in 'data_' of the first byte of the first field name.
  uint64_t stringsStart_;
};

/// View over an encoded Variant value. Does not own the bytes; the bytes and
/// 'metadata' must outlive the view and any views derived from it.
///
/// Accessors check the kind of the value and throw a user error on mismatch.
/// Callers that need to convert between kinds (e.g. INT8 to BIGINT) should
/// switch on kind() and implement the conversion rules of their engine.
class VariantValue {
 public:
  /// @param metadata Dictionary that object field ids refer to.
  /// @param data Bytes starting at the value header. May extend past the end
  /// of the value, e.g. when 'data' is a child inside an object or array.
  /// Throws a user error if the header is malformed or the value does not fit
  /// in 'data'.
  VariantValue(const VariantMetadata& metadata, std::string_view data);

  VariantKind kind() const {
    return kind_;
  }

  const VariantMetadata& metadata() const {
    return *metadata_;
  }

  /// Returns the bytes of this value, excluding any bytes after it.
  std::string_view data() const {
    return data_.substr(0, size_);
  }

  /// Returns true if the value is a Variant null. A Variant null is distinct
  /// from a SQL NULL, which is represented by a null row in the vector.
  bool isNull() const {
    return kind_ == VariantKind::kNull;
  }

  bool asBoolean() const;

  /// Returns the value of a kInt8, kInt16, kInt32 or kInt64 widened to 64
  /// bits.
  int64_t asInteger() const;

  float asFloat() const;

  double asDouble() const;

  /// Returns the value of a kDecimal4, kDecimal8 or kDecimal16.
  VariantDecimal asDecimal() const;

  /// Returns the number of days since the Unix epoch.
  int32_t asDate() const;

  /// Returns the raw 64-bit value of a time or timestamp. The unit depends on
  /// kind(): microseconds since midnight for kTime, microseconds since the
  /// Unix epoch for kTimestamp and kTimestampNtz, and nanoseconds since the
  /// Unix epoch for kTimestampNanos and kTimestampNtzNanos.
  int64_t asTimestamp() const;

  /// Returns the UTF-8 bytes of a short or long string.
  std::string_view asString() const;

  std::string_view asBinary() const;

  /// Returns the 16 bytes of a UUID in big-endian order.
  std::string_view asUuid() const;

  /// Returns the number of fields of an object or elements of an array.
  uint32_t numElements() const;

  /// Returns the name of the object field at 'index'. Fields are ordered by
  /// name.
  std::string_view fieldNameAt(uint32_t index) const;

  /// Returns the value of the object field at 'index'.
  VariantValue fieldValueAt(uint32_t index) const;

  /// Returns the value of the object field named 'name', or std::nullopt if
  /// the object has no such field. Uses binary search over the field names of
  /// large objects.
  std::optional<VariantValue> field(std::string_view name) const;

  /// Returns the array element at 'index'.
  VariantValue elementAt(uint32_t index) const;

 private:
  void initPrimitive(uint8_t typeId);

  void initObject(uint8_t valueHeader);

  void initArray(uint8_t valueHeader);

  void checkKind(VariantKind expected) const;

  void checkContainer() const;

  uint32_t fieldIdAt(uint32_t index) const;

  // Returns the value of the field or element at 'index' without checking the
  // kind.
  VariantValue childAt(uint32_t index) const;

  // Returns the payload bytes of a string, binary or UUID value.
  std::string_view payload() const;

  const VariantMetadata* metadata_;
  std::string_view data_;
  VariantKind kind_;
  // Size in bytes of the value, including the header.
  uint64_t size_{1};

  // Position in 'data_' of the first payload byte after the value header. For
  // long strings and binaries this skips the 4-byte length.
  uint64_t payloadStart_{1};
  // Size in bytes of a string, binary or UUID payload.
  uint32_t payloadSize_{0};

  // Layout of objects and arrays. Arrays have no field id list.
  uint32_t numElements_{0};
  uint8_t fieldIdSize_{0};
  uint8_t offsetSize_{0};
  uint64_t fieldIdsStart_{0};
  uint64_t offsetsStart_{0};
  uint64_t valuesStart_{0};
  // Size of the region holding the field or element values.
  uint64_t valuesSize_{0};
};

} // namespace facebook::velox

template <>
struct fmt::formatter<facebook::velox::VariantKind>
    : fmt::formatter<string_view> {
  template <typename FormatContext>
  auto format(facebook::velox::VariantKind kind, FormatContext& ctx) const {
    return formatter<string_view>::format(
        facebook::velox::VariantKindName::toName(kind), ctx);
  }
};
