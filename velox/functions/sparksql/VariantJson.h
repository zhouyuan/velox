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

#include <string>
#include <string_view>

#include "velox/type/tz/TimeZoneMap.h"
#include "velox/type/variant/VariantBuilder.h"
#include "velox/type/variant/VariantEncoding.h"

namespace facebook::velox::functions::sparksql {

/// Converts between JSON text and Variant values following Spark 4.0.
class VariantJson {
 public:
  /// Parses 'json' like Spark parse_json and appends the value to 'builder'.
  ///
  /// Follows the Jackson parser Spark uses: only the first JSON value is read
  /// and the rest of the input is ignored, except that a top-level number must
  /// be followed by whitespace or the end. Integers that fit in 64 bits become
  /// integers. Other numbers without an exponent become decimals if they fit
  /// in 38 digits, and doubles otherwise.
  ///
  /// Throws a user error if 'json' is not valid JSON, has duplicate keys and
  /// 'builder' rejects them, or exceeds the Variant size limit.
  static void parse(std::string_view json, VariantBuilder& builder);

  /// Appends 'value' to 'out' as JSON in the format of Spark's
  /// Variant.toJson. Timestamps are printed in 'timeZone', or UTC if null.
  /// Throws a user error for kinds that Spark 4.0 does not support, such as
  /// TIME and nanosecond timestamps.
  static void toJson(
      const VariantValue& value,
      const tz::TimeZone* timeZone,
      std::string& out);

  /// Appends a decimal with trailing fractional zeros removed, e.g. 1.50 as
  /// "1.5" and 100 as "100".
  static void appendDecimal(const VariantDecimal& decimal, std::string& out);

  /// Appends a timestamp as "yyyy-MM-dd HH:mm:ss[.ffffff]" followed by the
  /// "+HH:MM" offset of 'timeZone'. Omits the offset if 'timeZone' is null.
  static void appendTimestamp(
      int64_t micros,
      const tz::TimeZone* timeZone,
      std::string& out);

  /// Appends a UUID as 8-4-4-4-12 lowercase hex digits.
  static void appendUuid(std::string_view bytes, std::string& out);
};

} // namespace facebook::velox::functions::sparksql
