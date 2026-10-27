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
#include <string>
#include <string_view>
#include <vector>

#include <folly/container/F14Map.h>

#include "velox/type/HugeInt.h"
#include "velox/type/variant/VariantEncoding.h"

namespace facebook::velox {

/// Builds one Variant value in the Parquet Variant binary encoding.
///
/// Scalars are appended directly. Objects and arrays are written in two steps:
/// record writePosition(), append the fields or elements while recording their
/// offsets relative to that position, then call finishObject() or
/// finishArray(), which insert the header in front of the data.
///
/// The output matches what Spark's VariantBuilder produces, except that object
/// fields are sorted by UTF-8 bytes rather than UTF-16 code units. The orders
/// differ only for some non-BMP field names.
class VariantBuilder {
 public:
  /// Maximum size in bytes of the metadata or the value. Matches Spark.
  static constexpr uint64_t kSizeLimit{128 * 1024 * 1024};

  /// @param allowDuplicateKeys If true, an object with duplicate keys keeps
  /// the last value for each key. If false, finishObject() throws a user error.
  explicit VariantBuilder(bool allowDuplicateKeys);

  void appendNull();

  void appendBoolean(bool value);

  /// Appends an integer using the smallest of INT8, INT16, INT32 and INT64
  /// that holds it.
  void appendLong(int64_t value);

  void appendFloat(float value);

  void appendDouble(double value);

  /// Appends a decimal using the smallest of DECIMAL4, DECIMAL8 and DECIMAL16
  /// whose maximum precision covers both the number of digits in
  /// 'unscaledValue' and 'scale'. Throws if neither fits in 38 digits.
  void appendDecimal(int128_t unscaledValue, uint8_t scale);

  /// @param days Days since the Unix epoch.
  void appendDate(int32_t days);

  /// @param micros Microseconds since the Unix epoch, in UTC.
  void appendTimestamp(int64_t micros);

  /// @param micros Microseconds since the Unix epoch, without a time zone.
  void appendTimestampNtz(int64_t micros);

  /// Appends a short string if 'value' has at most 63 bytes, otherwise a long
  /// string.
  void appendString(std::string_view value);

  void appendBinary(std::string_view value);

  /// @param bytes The 16 bytes of the UUID in big-endian order.
  void appendUuid(std::string_view bytes);

  /// Appends a copy of 'value'. Field names are re-encoded against this
  /// builder's dictionary.
  void appendVariant(const VariantValue& value);

  /// Returns the id of 'key' in the dictionary, adding it if needed.
  uint32_t addKey(std::string_view key);

  /// Returns the number of value bytes written so far.
  uint64_t writePosition() const {
    return value_.size();
  }

  /// A field of an object being written.
  struct Field {
    /// Dictionary id returned by addKey().
    uint32_t id;
    /// Position of the field value relative to the start of the object.
    uint64_t offset;
  };

  /// Finishes an object whose field values were appended after 'start'.
  /// Sorts 'fields' by name and handles duplicate keys as configured.
  void finishObject(uint64_t start, std::vector<Field>& fields);

  /// Finishes an array whose elements were appended after 'start'.
  /// @param offsets Position of each element relative to 'start'.
  void finishArray(uint64_t start, const std::vector<uint64_t>& offsets);

  /// Returns the metadata bytes for the keys added so far.
  std::string metadata() const;

  /// Returns the value bytes. Valid once every object and array is finished.
  const std::string& value() const {
    return value_;
  }

 private:
  void appendHeader(uint8_t typeId);

  void appendLittleEndian(uint64_t value, uint8_t size);

  void checkSize(uint64_t size) const;

  // Removes all but the last value of each duplicate key. 'fields' must be
  // sorted by name.
  void removeDuplicates(uint64_t start, std::vector<Field>& fields);

  const bool allowDuplicateKeys_;
  std::string value_;
  // Keys in id order.
  std::vector<std::string> keys_;
  // Maps each key to its id.
  folly::F14FastMap<std::string, uint32_t> keyIds_;
};

} // namespace facebook::velox
