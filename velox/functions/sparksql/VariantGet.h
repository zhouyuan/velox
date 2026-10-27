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

#include "velox/expression/FunctionCallToSpecialForm.h"

namespace facebook::velox::functions::sparksql {

/// variant_get(variant, path) and try_variant_get(variant, path).
///
/// Extracts the value at a JSON-like 'path' such as '$.a[0].b' and converts
/// it to the result type of the call, which the caller chooses. The result
/// type is VARIANT for a sub-variant, or any type Spark allows: numeric,
/// boolean, varchar, varbinary, date, timestamp, and arrays, maps with varchar
/// keys, and rows of those. Missing paths and Variant nulls return NULL.
///
/// A value that cannot be converted fails variant_get and makes
/// try_variant_get return NULL. Scalar conversions follow Spark's try_cast.
class VariantGetCallToSpecialForm : public exec::FunctionCallToSpecialForm {
 public:
  explicit VariantGetCallToSpecialForm(bool failOnError)
      : failOnError_{failOnError} {}

  /// Throws: the result type must come from the caller.
  TypePtr resolveType(const std::vector<TypePtr>& argTypes) override;

  exec::ExprPtr constructSpecialForm(
      const TypePtr& type,
      std::vector<exec::ExprPtr>&& args,
      bool trackCpuUsage,
      const core::QueryConfig& config) override;

  static constexpr const char* kVariantGet = "variant_get";
  static constexpr const char* kTryVariantGet = "try_variant_get";

 private:
  const bool failOnError_;
};

} // namespace facebook::velox::functions::sparksql
