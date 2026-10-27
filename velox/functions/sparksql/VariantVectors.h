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

#include <string_view>

#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::functions::sparksql {

/// Reads the metadata and value bytes of a VARIANT vector of any encoding.
class VariantReader {
 public:
  VariantReader(const BaseVector& vector, const SelectivityVector& rows);

  bool isNullAt(vector_size_t row) const {
    return decoded_.isNullAt(row);
  }

  /// Returns the metadata bytes of a non-null row. Throws a user error if
  /// the metadata is null. Short strings are stored inside the returned
  /// StringView, so it must outlive any view of its bytes.
  StringView metadata(vector_size_t row) const;

  /// Returns the value bytes of a non-null row. Throws a user error if the
  /// value is null. See metadata() about the lifetime.
  StringView value(vector_size_t row) const;

 private:
  DecodedVector decoded_;
  DecodedVector metadata_;
  DecodedVector value_;
};

/// Writes rows of a new flat VARIANT vector.
class VariantWriter {
 public:
  VariantWriter(vector_size_t size, memory::MemoryPool* pool);

  void
  set(vector_size_t row, std::string_view metadata, std::string_view value);

  void setNull(vector_size_t row);

  const RowVectorPtr& result() const {
    return result_;
  }

 private:
  RowVectorPtr result_;
  FlatVector<StringView>* metadata_;
  FlatVector<StringView>* value_;
};

} // namespace facebook::velox::functions::sparksql
