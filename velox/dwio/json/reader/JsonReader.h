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

#include <array>
#include <limits>
#include <string>

#include "folly/CppAttributes.h"
#include "folly/json.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/dwio/common/compression/Compression.h"

namespace facebook::velox::json {

using common::CompressionKind;
using common::ScanSpec;
using dwio::common::BufferedInput;
using dwio::common::ColumnSelector;
using dwio::common::ColumnStatistics;
using dwio::common::Mutation;
using dwio::common::ReaderOptions;
using dwio::common::RowReaderOptions;
using dwio::common::TypeWithId;
using memory::MemoryPool;

// Shared state for a file between JsonReader and JsonRowReader
struct JsonFileContents {
  JsonFileContents(MemoryPool& pool, const std::shared_ptr<const RowType>& t);

  const size_t COLUMN_POSITION_INVALID = std::numeric_limits<size_t>::max();
  const std::shared_ptr<const RowType> schema;

  std::unique_ptr<BufferedInput> input;
  std::unique_ptr<dwio::common::SeekableInputStream> inputStream;
  std::unique_ptr<dwio::common::SeekableInputStream> decompressedInputStream;
  MemoryPool& pool;
  uint64_t fileLength;
  CompressionKind compression;
  dwio::common::compression::CompressionOptions compressionOptions;
};

class JsonReader : public dwio::common::Reader {
 public:
  JsonReader(
      const ReaderOptions& options,
      std::unique_ptr<BufferedInput> input);

  std::optional<uint64_t> numberOfRows() const override;

  std::unique_ptr<ColumnStatistics> columnStatistics(
      uint32_t index) const override;

  const RowTypePtr& rowType() const override;

  CompressionKind getCompression() const;

  const std::shared_ptr<const TypeWithId>& typeWithId() const override;

  std::unique_ptr<dwio::common::RowReader> createRowReader(
      const RowReaderOptions& options) const override;

  uint64_t getFileLength() const;

 private:
  ReaderOptions options_;
  mutable std::shared_ptr<const TypeWithId> typeWithId_;
  std::shared_ptr<JsonFileContents> contents_;
  std::shared_ptr<const TypeWithId> schemaWithId_;
  std::shared_ptr<const RowType> internalSchema_;
};

class JsonRowReader : public dwio::common::RowReader {
 public:
  JsonRowReader(
      std::shared_ptr<JsonFileContents> fileContents,
      const RowReaderOptions& options);

  uint64_t next(
      uint64_t size,
      VectorPtr& result,
      const Mutation* mutation = nullptr) override;

  int64_t nextRowNumber() override;

  int64_t nextReadSize(uint64_t size) override;

  void updateRuntimeStats(
      dwio::common::RuntimeStatistics& stats) const override;

  void resetFilterCaches() override;

  std::optional<size_t> estimatedRowSize() const override;

  const ColumnSelector& getColumnSelector() const;

  std::shared_ptr<const TypeWithId> getSelectedType() const;

  uint64_t getRowNumber() const;

  uint64_t seekToRow(uint64_t rowNumber);

 private:
  const RowReaderOptions& getDefaultOpts();

  const std::shared_ptr<const RowType>& getType() const;

  bool isSelectedField(const std::shared_ptr<const TypeWithId>& t);

  const char* getStreamNameData() const;

  uint64_t getLength();

  uint64_t getStreamLength() const;

  void setEOF();

  bool readLine(std::string& line);

  void parseJsonObject(
      const folly::dynamic& obj,
      const std::shared_ptr<const Type>& type,
      const std::shared_ptr<const Type>& reqType,
      BaseVector* data,
      vector_size_t row);

  void parseJsonValue(
      const folly::dynamic& value,
      const std::shared_ptr<const Type>& type,
      const std::shared_ptr<const Type>& reqType,
      BaseVector* data,
      vector_size_t row);

  template <typename T>
  void setScalarValue(
      const folly::dynamic& value,
      BaseVector* data,
      vector_size_t row);

  const std::shared_ptr<JsonFileContents> contents_;
  const std::shared_ptr<const TypeWithId> schemaWithId_;
  const std::shared_ptr<velox::common::ScanSpec>& scanSpec_;

  mutable std::shared_ptr<const TypeWithId> selectedSchema_;

  RowReaderOptions options_;
  ColumnSelector columnSelector_;
  uint64_t currentRow_;
  uint64_t pos_;
  bool atEOF_;
  bool atPhysicalEOF_;
  std::string lineBuffer_;
  uint64_t limit_; // lowest offset not in the range
  uint64_t fileLength_;
};

} // namespace facebook::velox::json

// Made with Bob
