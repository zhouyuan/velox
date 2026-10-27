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
#include "velox/functions/sparksql/VariantVectors.h"

#include "velox/type/variant/VariantType.h"

namespace facebook::velox::functions::sparksql {

VariantReader::VariantReader(
    const BaseVector& vector,
    const SelectivityVector& rows)
    : decoded_(vector, rows) {
  const auto* base = decoded_.base()->asChecked<RowVector>();
  const SelectivityVector allRows(base->size());
  metadata_.decode(*base->childAt(VariantType::kMetadataIndex), allRows);
  value_.decode(*base->childAt(VariantType::kValueIndex), allRows);
}

StringView VariantReader::metadata(vector_size_t row) const {
  const auto index = decoded_.index(row);
  VELOX_USER_CHECK(
      !metadata_.isNullAt(index),
      "Variant binary is malformed: null metadata.");
  return metadata_.valueAt<StringView>(index);
}

StringView VariantReader::value(vector_size_t row) const {
  const auto index = decoded_.index(row);
  VELOX_USER_CHECK(
      !value_.isNullAt(index), "Variant binary is malformed: null value.");
  return value_.valueAt<StringView>(index);
}

VariantWriter::VariantWriter(vector_size_t size, memory::MemoryPool* pool)
    : result_{BaseVector::create<RowVector>(VARIANT(), size, pool)},
      metadata_{result_->childAt(VariantType::kMetadataIndex)
                    ->asFlatVector<StringView>()},
      value_{result_->childAt(VariantType::kValueIndex)
                 ->asFlatVector<StringView>()} {}

void VariantWriter::set(
    vector_size_t row,
    std::string_view metadata,
    std::string_view value) {
  result_->setNull(row, false);
  metadata_->set(row, StringView(metadata));
  value_->set(row, StringView(value));
}

void VariantWriter::setNull(vector_size_t row) {
  result_->setNull(row, true);
  // Children of null rows must still hold valid strings.
  metadata_->set(row, StringView());
  value_->set(row, StringView());
}

} // namespace facebook::velox::functions::sparksql
