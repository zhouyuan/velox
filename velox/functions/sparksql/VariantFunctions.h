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

namespace facebook::velox::functions::sparksql {

/// Registers the VARIANT type and the Spark Variant functions parse_json,
/// try_parse_json, is_variant_null, schema_of_variant and to_variant_object.
/// variant_get and try_variant_get are special forms registered separately.
void registerVariantFunctions(const std::string& prefix);

} // namespace facebook::velox::functions::sparksql
