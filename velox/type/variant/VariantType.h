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

#include "velox/type/Type.h"

namespace facebook::velox {

/// Represents semi-structured data in the Parquet Variant binary encoding as
/// ROW(metadata VARBINARY, value VARBINARY). Use VariantMetadata and
/// VariantValue in VariantEncoding.h to navigate the bytes.
///
/// Shredded columns never appear in this type. Readers that find shredded
/// columns in a file either reassemble them into 'value' or, when the query
/// accesses specific paths, return those paths as separate typed columns.
///
/// Variant values have no defined equality or order, so the type cannot be
/// used as a grouping, join or sorting key.
class VariantType final : public RowType {
  VariantType()
      : RowType(
            {std::string(kMetadataName), std::string(kValueName)},
            {VARBINARY(), VARBINARY()}) {}

 public:
  static constexpr std::string_view kMetadataName{"metadata"};
  static constexpr std::string_view kValueName{"value"};
  static constexpr column_index_t kMetadataIndex{0};
  static constexpr column_index_t kValueIndex{1};

  static std::shared_ptr<const VariantType> get() {
    static const VariantType kInstance;
    return {std::shared_ptr<const VariantType>{}, &kInstance};
  }

  bool equivalent(const Type& other) const override {
    // Pointer comparison works since this type is a singleton.
    return this == &other;
  }

  const char* name() const override {
    return "VARIANT";
  }

  std::string toString() const override {
    return name();
  }

  bool isOrderable() const override {
    return false;
  }

  bool isComparable() const override {
    return false;
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "Type";
    obj["type"] = name();
    return obj;
  }

  std::span<const TypeParameter> parameters() const override {
    return {};
  }
};

FOLLY_ALWAYS_INLINE bool isVariantType(const TypePtr& type) {
  // Pointer comparison works since this type is a singleton.
  return VariantType::get() == type;
}

FOLLY_ALWAYS_INLINE std::shared_ptr<const VariantType> VARIANT() {
  return VariantType::get();
}

} // namespace facebook::velox
