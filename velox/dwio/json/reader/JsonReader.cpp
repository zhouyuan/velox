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

#include "velox/dwio/json/reader/JsonReader.h"

#include <boost/algorithm/string/predicate.hpp>
#include <string>

#include "velox/dwio/common/exception/Exceptions.h"

namespace facebook::velox::json {
namespace {

using common::CompressionKind;

using dwio::common::EOFError;
using dwio::common::RowReader;
using dwio::common::verify;

static constexpr std::string_view kJsonCompressionExtensionGzip{".gz"};
static constexpr std::string_view kJsonCompressionExtensionZst{".zst"};

constexpr const int32_t kDecompressionBufferFactor = 3;

void resizeVector(
    BaseVector* FOLLY_NULLABLE data,
    const vector_size_t insertionIdx) {
  if (data == nullptr) {
    return;
  }

  auto dataSize = data->size();
  if (dataSize == 0) {
    data->resize(10);
  } else if (dataSize <= insertionIdx) {
    if (data->type()->kind() == TypeKind::ARRAY) {
      auto oldSize = dataSize;
      auto newSize = dataSize * 2;
      data->resize(newSize);

      auto arrayVector = data->asChecked<ArrayVector>();
      auto rawOffsets = arrayVector->offsets()->asMutable<vector_size_t>();
      auto rawSizes = arrayVector->sizes()->asMutable<vector_size_t>();

      auto lastOffset = oldSize > 0 ? rawOffsets[oldSize - 1] : 0;
      auto lastSize = oldSize > 0 ? rawSizes[oldSize - 1] : 0;
      auto newOffset = oldSize > 0 ? lastOffset + lastSize : 0;

      for (auto i = oldSize; i < newSize; ++i) {
        rawSizes[i] = 0;
        rawOffsets[i] = newOffset;
      }
    } else if (data->type()->kind() == TypeKind::MAP) {
      auto oldSize = dataSize;
      auto newSize = dataSize * 2;
      data->resize(newSize);

      auto mapVector = data->asChecked<MapVector>();
      auto rawOffsets = mapVector->offsets()->asMutable<vector_size_t>();
      auto rawSizes = mapVector->sizes()->asMutable<vector_size_t>();

      auto lastOffset = oldSize > 0 ? rawOffsets[oldSize - 1] : 0;
      auto lastSize = oldSize > 0 ? rawSizes[oldSize - 1] : 0;
      auto newOffset = oldSize > 0 ? lastOffset + lastSize : 0;

      for (auto i = oldSize; i < newSize; ++i) {
        rawSizes[i] = 0;
        rawOffsets[i] = newOffset;
      }
    } else {
      data->resize(dataSize * 2);
    }
  }
}

void setCompressionSettings(
    const std::string& filename,
    CompressionKind& kind,
    dwio::common::compression::CompressionOptions& compressionOptions) {
  if (filename.ends_with(kJsonCompressionExtensionGzip)) {
    kind = CompressionKind::CompressionKind_GZIP;
    compressionOptions.format.zlib.windowBits = 15;
  } else if (filename.ends_with(kJsonCompressionExtensionZst)) {
    kind = CompressionKind::CompressionKind_ZSTD;
  } else {
    kind = CompressionKind::CompressionKind_NONE;
  }
}

} // namespace

JsonFileContents::JsonFileContents(
    MemoryPool& pool,
    const std::shared_ptr<const RowType>& t)
    : schema{t},
      input{nullptr},
      pool{pool},
      fileLength{0},
      compression{CompressionKind::CompressionKind_NONE},
      compressionOptions{} {}

JsonReader::JsonReader(
    const ReaderOptions& options,
    std::unique_ptr<BufferedInput> input)
    : options_{options} {
  auto& fileType = options_.fileSchema();
  VELOX_CHECK_NOT_NULL(
      fileType, "JsonReader requires a file schema in ReaderOptions");

  internalSchema_ = std::dynamic_pointer_cast<const RowType>(fileType);
  VELOX_CHECK_NOT_NULL(
      internalSchema_, "JsonReader file schema must be a ROW type");

  contents_ = std::make_shared<JsonFileContents>(
      options_.memoryPool(), internalSchema_);
  contents_->input = std::move(input);
  contents_->fileLength = contents_->input->getReadFile()->size();

  setCompressionSettings(
      contents_->input->getReadFile()->getName(),
      contents_->compression,
      contents_->compressionOptions);

  schemaWithId_ = TypeWithId::create(internalSchema_);
}

std::optional<uint64_t> JsonReader::numberOfRows() const {
  return std::nullopt;
}

std::unique_ptr<ColumnStatistics> JsonReader::columnStatistics(
    uint32_t index) const {
  return nullptr;
}

const RowTypePtr& JsonReader::rowType() const {
  return internalSchema_;
}

CompressionKind JsonReader::getCompression() const {
  return contents_->compression;
}

const std::shared_ptr<const TypeWithId>& JsonReader::typeWithId() const {
  if (!typeWithId_) {
    typeWithId_ = schemaWithId_;
  }
  return typeWithId_;
}

std::unique_ptr<dwio::common::RowReader> JsonReader::createRowReader(
    const RowReaderOptions& options) const {
  return std::make_unique<JsonRowReader>(contents_, options);
}

uint64_t JsonReader::getFileLength() const {
  return contents_->fileLength;
}

JsonRowReader::JsonRowReader(
    std::shared_ptr<JsonFileContents> fileContents,
    const RowReaderOptions& opts)
    : RowReader(),
      contents_{fileContents},
      schemaWithId_{TypeWithId::create(fileContents->schema)},
      scanSpec_{opts.scanSpec()},
      selectedSchema_{nullptr},
      options_{opts},
      columnSelector_{
          ColumnSelector::apply(opts.selector(), contents_->schema)},
      currentRow_{0},
      pos_{opts.offset()},
      atEOF_{false},
      atPhysicalEOF_{false},
      limit_{opts.limit()},
      fileLength_{getStreamLength()} {
  // Seek to first line at or after the specified region.
  if (contents_->compression == CompressionKind::CompressionKind_NONE) {
    const auto streamPosition_ = pos_;

    contents_->inputStream = contents_->input->read(
        streamPosition_,
        contents_->fileLength - streamPosition_,
        dwio::common::LogType::STREAM);

    if (pos_ != 0) {
      lineBuffer_.clear();
      std::string dummy;
      (void)readLine(dummy); // Skip partial line at start
    }
    if (opts.skipRows() > 0) {
      (void)seekToRow(opts.skipRows());
    }
  } else {
    // compressed JSON files, the first split reads the whole file, rest read 0
    if (pos_ != 0) {
      atEOF_ = true;
    }
    limit_ = std::numeric_limits<uint64_t>::max();

    contents_->inputStream = contents_->input->loadCompleteFile();
    auto name = contents_->inputStream->getName();
    contents_->decompressedInputStream = createDecompressor(
        contents_->compression,
        std::move(contents_->inputStream),
        kDecompressionBufferFactor * contents_->fileLength,
        contents_->pool,
        contents_->compressionOptions,
        fmt::format("JSON Reader: Stream {}", name),
        nullptr,
        true,
        contents_->fileLength);

    if (opts.skipRows() > 0) {
      (void)seekToRow(opts.skipRows());
    }
  }
}

bool JsonRowReader::readLine(std::string& line) {
  line.clear();
  
  auto* stream = contents_->compression == CompressionKind::CompressionKind_NONE
      ? contents_->inputStream.get()
      : contents_->decompressedInputStream.get();

  if (!stream) {
    atPhysicalEOF_ = true;
    return false;
  }

  char ch;
  while (true) {
    const void* buffer;
    int32_t size;
    
    if (!stream->Next(&buffer, &size)) {
      atPhysicalEOF_ = true;
      if (!line.empty()) {
        pos_ += line.size();
        return true;
      }
      return false;
    }

    const char* data = static_cast<const char*>(buffer);
    for (int32_t i = 0; i < size; ++i) {
      ch = data[i];
      if (ch == '\n') {
        pos_ += line.size() + 1;
        return true;
      }
      line += ch;
    }
  }
}

uint64_t JsonRowReader::next(
    uint64_t rows,
    VectorPtr& result,
    const Mutation* mutation) {
  if (atEOF_) {
    return 0;
  }

  auto& t = schemaWithId_;
  verify(
      t->type()->isRow(),
      "Top-level TypeKind of schema is not Row for JSON file");

  auto projectSelectedType = options_.projectSelectedType();
  auto reqT =
      (projectSelectedType ? getSelectedType() : TypeWithId::create(getType()));
  verify(
      reqT->type()->isRow(),
      "Top-level TypeKind of schema is not Row for JSON file");

  // create top level RowVector
  auto rowVecPtr = BaseVector::create<RowVector>(
      reqT->type(), (vector_size_t)rows, &contents_->pool);

  vector_size_t rowsRead = 0;
  const auto initialPos = pos_;
  
  while (!atEOF_ && rowsRead < rows) {
    std::string line;
    if (!readLine(line)) {
      setEOF();
      break;
    }

    // Skip empty lines
    if (line.empty()) {
      continue;
    }

    try {
      // Parse JSON line
      auto jsonObj = folly::parseJson(line);
      
      if (!jsonObj.isObject()) {
        VELOX_FAIL("JSON line is not an object at row {}", currentRow_);
      }

      // Parse each field
      for (vector_size_t i = 0; i < t->size(); i++) {
        const auto& ct = t->childAt(i);
        const auto& rct = reqT->childAt(i);
        BaseVector* childVector = nullptr;

        if (isSelectedField(ct)) {
          childVector = rowVecPtr->childAt(i).get();
        } else if (!projectSelectedType) {
          rowVecPtr->childAt(i) = nullptr;
        } else {
          rowVecPtr->childAt(i) = nullptr;
        }

        resizeVector(childVector, rowsRead);
        
        if (childVector) {
          auto fieldName = ct->type()->asRow().nameOf(i);
          if (jsonObj.count(fieldName)) {
            parseJsonValue(
                jsonObj[fieldName], ct->type(), rct->type(), childVector, rowsRead);
          } else {
            // Field not present in JSON, set to null
            childVector->setNull(rowsRead, true);
          }
        }
      }

      ++currentRow_;
      ++rowsRead;

    } catch (const std::exception& e) {
      VELOX_FAIL("Failed to parse JSON at row {}: {}", currentRow_, e.what());
    }

    bool eof = false;
    if (contents_->compression == CompressionKind::CompressionKind_NONE) {
      eof = pos_ >= getLength();
    } else if (atPhysicalEOF_) {
      eof = pos_ >= contents_->decompressedInputStream->ByteCount();
    }

    if (eof) {
      setEOF();
    }

    // handle empty file
    if (initialPos == pos_ && atEOF_) {
      currentRow_ = 0;
      rowsRead = 0;
    }
  }

  if (rowsRead > 0) {
    rowVecPtr->resize(rowsRead);
  }

  result = rowVecPtr;
  return rowsRead;
}

void JsonRowReader::parseJsonValue(
    const folly::dynamic& value,
    const std::shared_ptr<const Type>& type,
    const std::shared_ptr<const Type>& reqType,
    BaseVector* data,
    vector_size_t row) {
  if (value.isNull()) {
    data->setNull(row, true);
    return;
  }

  data->setNull(row, false);

  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      setScalarValue<bool>(value, data, row);
      break;
    case TypeKind::TINYINT:
      setScalarValue<int8_t>(value, data, row);
      break;
    case TypeKind::SMALLINT:
      setScalarValue<int16_t>(value, data, row);
      break;
    case TypeKind::INTEGER:
      setScalarValue<int32_t>(value, data, row);
      break;
    case TypeKind::BIGINT:
      setScalarValue<int64_t>(value, data, row);
      break;
    case TypeKind::REAL:
      setScalarValue<float>(value, data, row);
      break;
    case TypeKind::DOUBLE:
      setScalarValue<double>(value, data, row);
      break;
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY: {
      auto flatVector = data->asFlatVector<StringView>();
      if (value.isString()) {
        flatVector->set(row, StringView(std::string_view(value.asString())));
      } else {
	auto tmpJson = folly::toJson(value);
        flatVector->set(row, StringView(tmpJson));
      }
      break;
    }
    case TypeKind::ARRAY: {
      if (!value.isArray()) {
        VELOX_FAIL("Expected array type for JSON array");
      }
      auto arrayVector = data->asChecked<ArrayVector>();
      auto elements = arrayVector->elements();
      auto offsets = arrayVector->offsets()->asMutable<vector_size_t>();
      auto sizes = arrayVector->sizes()->asMutable<vector_size_t>();
      
      auto arrayType = std::dynamic_pointer_cast<const ArrayType>(type);
      auto elementType = arrayType->elementType();
      auto reqArrayType = std::dynamic_pointer_cast<const ArrayType>(reqType);
      auto reqElementType = reqArrayType->elementType();
      
      auto offset = elements->size();
      offsets[row] = offset;
      sizes[row] = value.size();
      
      elements->resize(offset + value.size());
      
      for (size_t i = 0; i < value.size(); ++i) {
        parseJsonValue(value[i], elementType, reqElementType, elements.get(), offset + i);
      }
      break;
    }
    case TypeKind::MAP: {
      if (!value.isObject()) {
        VELOX_FAIL("Expected object type for JSON map");
      }
      auto mapVector = data->asChecked<MapVector>();
      auto keys = mapVector->mapKeys();
      auto values = mapVector->mapValues();
      auto offsets = mapVector->offsets()->asMutable<vector_size_t>();
      auto sizes = mapVector->sizes()->asMutable<vector_size_t>();
      
      auto mapType = std::dynamic_pointer_cast<const MapType>(type);
      auto keyType = mapType->keyType();
      auto valueType = mapType->valueType();
      auto reqMapType = std::dynamic_pointer_cast<const MapType>(reqType);
      auto reqKeyType = reqMapType->keyType();
      auto reqValueType = reqMapType->valueType();
      
      auto offset = keys->size();
      offsets[row] = offset;
      sizes[row] = value.size();
      
      keys->resize(offset + value.size());
      values->resize(offset + value.size());
      
      size_t idx = 0;
      for (const auto& pair : value.items()) {
        // Set key
        auto keyStr = pair.first.asString();
        keys->asFlatVector<StringView>()->set(offset + idx, StringView(keyStr));
        
        // Set value
        parseJsonValue(pair.second, valueType, reqValueType, values.get(), offset + idx);
        ++idx;
      }
      break;
    }
    case TypeKind::ROW: {
      if (!value.isObject()) {
        VELOX_FAIL("Expected object type for JSON row");
      }
      auto rowVector = data->asChecked<RowVector>();
      auto rowType = std::dynamic_pointer_cast<const RowType>(type);
      auto reqRowType = std::dynamic_pointer_cast<const RowType>(reqType);
      
      for (size_t i = 0; i < rowType->size(); ++i) {
        auto fieldName = rowType->nameOf(i);
        auto child = rowVector->childAt(i);
        
        if (value.count(fieldName)) {
          parseJsonValue(
              value[fieldName],
              rowType->childAt(i),
              reqRowType->childAt(i),
              child.get(),
              row);
        } else {
          child->setNull(row, true);
        }
      }
      break;
    }
    default:
      VELOX_FAIL("Unsupported type for JSON parsing: {}", type->toString());
  }
}

template <typename T>
void JsonRowReader::setScalarValue(
    const folly::dynamic& value,
    BaseVector* data,
    vector_size_t row) {
  auto flatVector = data->asFlatVector<T>();
  
  if constexpr (std::is_same_v<T, bool>) {
    flatVector->set(row, value.asBool());
  } else if constexpr (std::is_integral_v<T>) {
    flatVector->set(row, static_cast<T>(value.asInt()));
  } else if constexpr (std::is_floating_point_v<T>) {
    flatVector->set(row, static_cast<T>(value.asDouble()));
  }
}

int64_t JsonRowReader::nextRowNumber() {
  return atEOF_ ? RowReader::kAtEnd : currentRow_;
}

int64_t JsonRowReader::nextReadSize(uint64_t size) {
  return atEOF_ ? RowReader::kAtEnd : size;
}

void JsonRowReader::updateRuntimeStats(
    dwio::common::RuntimeStatistics& stats) const {
  // No specific stats for JSON reader yet
}

void JsonRowReader::resetFilterCaches() {
  // No filter caches in JSON reader
}

std::optional<size_t> JsonRowReader::estimatedRowSize() const {
  return std::nullopt;
}

const ColumnSelector& JsonRowReader::getColumnSelector() const {
  return columnSelector_;
}

std::shared_ptr<const TypeWithId> JsonRowReader::getSelectedType() const {
  //if (!selectedSchema_) {
  //  selectedSchema_ = TypeWithId::create(
  //      columnSelector_.buildSelected(), schemaWithId_->maxId() + 1);
  //}
  return selectedSchema_;
}

uint64_t JsonRowReader::getRowNumber() const {
  return currentRow_;
}

uint64_t JsonRowReader::seekToRow(uint64_t rowNumber) {
  if (rowNumber < currentRow_) {
    VELOX_FAIL("Cannot seek backwards in JSON file");
  }

  while (currentRow_ < rowNumber && !atEOF_) {
    std::string line;
    if (!readLine(line)) {
      setEOF();
      break;
    }
    if (!line.empty()) {
      ++currentRow_;
    }
  }

  return currentRow_;
}

const RowReaderOptions& JsonRowReader::getDefaultOpts() {
  return options_;
}

const std::shared_ptr<const RowType>& JsonRowReader::getType() const {
  return contents_->schema;
}

bool JsonRowReader::isSelectedField(
    const std::shared_ptr<const TypeWithId>& t) {
  return columnSelector_.shouldReadNode(t->id());
}

const char* JsonRowReader::getStreamNameData() const {
  if (contents_->compression == CompressionKind::CompressionKind_NONE) {
    return contents_->inputStream
        ? contents_->inputStream->getName().data()
        : "";
  }
  return contents_->decompressedInputStream
      ? contents_->decompressedInputStream->getName().data()
      : "";
}

uint64_t JsonRowReader::getLength() {
  return limit_;
}

uint64_t JsonRowReader::getStreamLength() const {
  return contents_->fileLength;
}

void JsonRowReader::setEOF() {
  atEOF_ = true;
}

} // namespace facebook::velox::json

// Made with Bob
