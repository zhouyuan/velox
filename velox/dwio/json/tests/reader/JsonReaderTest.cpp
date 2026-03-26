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
#include "velox/dwio/json/RegisterJsonReader.h"

#include <gtest/gtest.h>
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::json;

class JsonReaderTest : public testing::Test {

 protected:
  std::string getExampleFilePath(const std::string& fileName) {
      return test::getDataFilePath("", 
          "velox/dwio/json/tests/reader/examples/" + fileName);
  }
  void SetUp() override {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    pool_ = memory::memoryManager()->addLeafPool();
    registerJsonReaderFactory();
  }

  void TearDown() override {
    unregisterJsonReaderFactory();
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(JsonReaderTest, SimpleTypes) {
  // Define schema
  auto schema = ROW(
      {{"id", BIGINT()},
       {"name", VARCHAR()},
       {"age", INTEGER()},
       {"salary", DOUBLE()},
       {"active", BOOLEAN()}});

  // Create test file path
  auto testFilePath = getExampleFilePath("simple_types.json");

  // Create reader options
  dwio::common::ReaderOptions readerOpts{pool_.get()};
  readerOpts.setFileSchema(schema);

  // Create buffered input
  auto input = std::make_unique<BufferedInput>(
      std::make_shared<LocalReadFile>(testFilePath), *pool_);

  // Create reader
  auto reader = std::make_unique<JsonReader>(readerOpts, std::move(input));

  // Verify schema
  ASSERT_EQ(reader->rowType()->size(), 5);
  ASSERT_EQ(reader->rowType()->nameOf(0), "id");
  ASSERT_EQ(reader->rowType()->nameOf(1), "name");

  // Create row reader
  dwio::common::RowReaderOptions rowReaderOpts;
  auto rowReader = reader->createRowReader(rowReaderOpts);

  // Read data
  VectorPtr result;
  auto rowsRead = rowReader->next(100, result);

  ASSERT_EQ(rowsRead, 3);
  ASSERT_TRUE(result != nullptr);

  auto rowVector = std::dynamic_pointer_cast<RowVector>(result);
  ASSERT_TRUE(rowVector != nullptr);
  ASSERT_EQ(rowVector->size(), 3);

  // Verify first row
  auto idVector = rowVector->childAt(0)->asFlatVector<int64_t>();
  auto nameVector = rowVector->childAt(1)->asFlatVector<StringView>();
  auto ageVector = rowVector->childAt(2)->asFlatVector<int32_t>();
  auto salaryVector = rowVector->childAt(3)->asFlatVector<double>();
  auto activeVector = rowVector->childAt(4)->asFlatVector<bool>();

  EXPECT_EQ(idVector->valueAt(0), 1);
  EXPECT_EQ(nameVector->valueAt(0).str(), "Alice");
  EXPECT_EQ(ageVector->valueAt(0), 30);
  EXPECT_DOUBLE_EQ(salaryVector->valueAt(0), 50000.5);
  EXPECT_TRUE(activeVector->valueAt(0));

  // Verify second row
  EXPECT_EQ(idVector->valueAt(1), 2);
  EXPECT_EQ(nameVector->valueAt(1).str(), "Bob");
  EXPECT_EQ(ageVector->valueAt(1), 25);
  EXPECT_DOUBLE_EQ(salaryVector->valueAt(1), 45000.75);
  EXPECT_FALSE(activeVector->valueAt(1));
}

TEST_F(JsonReaderTest, NestedTypes) {
  // Define schema with nested types
  auto schema = ROW(
      {{"id", BIGINT()},
       {"name", VARCHAR()},
       {"scores", ARRAY(INTEGER())},
       {"metadata", MAP(VARCHAR(), VARCHAR())}});

  // Create test file path
  auto testFilePath = getExampleFilePath("nested_types.json");

  // Create reader options
  dwio::common::ReaderOptions readerOpts{pool_.get()};
  readerOpts.setFileSchema(schema);

  // Create buffered input
  auto input = std::make_unique<BufferedInput>(
      std::make_shared<LocalReadFile>(testFilePath), *pool_);

  // Create reader
  auto reader = std::make_unique<JsonReader>(readerOpts, std::move(input));

  // Create row reader
  dwio::common::RowReaderOptions rowReaderOpts;
  auto rowReader = reader->createRowReader(rowReaderOpts);

  // Read data
  VectorPtr result;
  auto rowsRead = rowReader->next(100, result);

  ASSERT_EQ(rowsRead, 2);
  ASSERT_TRUE(result != nullptr);

  auto rowVector = std::dynamic_pointer_cast<RowVector>(result);
  ASSERT_TRUE(rowVector != nullptr);
  ASSERT_EQ(rowVector->size(), 2);

  // Verify array field
  auto scoresVector = std::dynamic_pointer_cast<ArrayVector>(rowVector->childAt(2));
  ASSERT_TRUE(scoresVector != nullptr);
  
  auto scoresElements = scoresVector->elements()->asFlatVector<int32_t>();
  EXPECT_EQ(scoresVector->sizeAt(0), 3);
  EXPECT_EQ(scoresElements->valueAt(scoresVector->offsetAt(0)), 85);
  EXPECT_EQ(scoresElements->valueAt(scoresVector->offsetAt(0) + 1), 90);
  EXPECT_EQ(scoresElements->valueAt(scoresVector->offsetAt(0) + 2), 95);

  // Verify map field
  auto metadataVector = std::dynamic_pointer_cast<MapVector>(rowVector->childAt(3));
  ASSERT_TRUE(metadataVector != nullptr);
  EXPECT_EQ(metadataVector->sizeAt(0), 2);
}



// Made with Bob
