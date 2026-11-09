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
#include "velox/experimental/stateful/fuzzer/FlinkQueryRunner.h"

#include <gtest/gtest.h>

#include "velox/core/Expressions.h"
#include "velox/core/QueryConfig.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::stateful::fuzzer {
namespace {

using exec::test::ReferenceQueryErrorCode;

// These tests require a Flink SQL Gateway at FLINK_GATEWAY_URL (default
// http://localhost:8083), see
// velox/docs/develop/testing/flink-query-runner.rst. They are disabled in
// CTest.
class FlinkQueryRunnerTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    const char* url = std::getenv("FLINK_GATEWAY_URL");
    aggregatePool_ = rootPool_->addAggregateChild("flink");
    runner_ = std::make_unique<FlinkQueryRunner>(
        aggregatePool_.get(),
        url ? url : "http://localhost:8083",
        std::unordered_map<std::string, std::string>{
            {core::QueryConfig::kSessionTimezone, "America/Los_Angeles"}});
  }

  core::PlanNodePtr makePlan(
      const RowVectorPtr& input,
      std::vector<core::TypedExprPtr> projections) {
    std::vector<std::string> names;
    for (size_t i = 0; i < projections.size(); ++i) {
      names.push_back(fmt::format("p{}", i));
    }
    return std::make_shared<core::ProjectNode>(
        "project",
        std::move(names),
        std::move(projections),
        std::make_shared<core::ValuesNode>(
            "values", std::vector<RowVectorPtr>{input}));
  }

  static core::TypedExprPtr field(
      const TypePtr& type,
      const std::string& name) {
    return std::make_shared<core::FieldAccessTypedExpr>(type, name);
  }

  static core::TypedExprPtr call(
      const TypePtr& type,
      const std::string& name,
      std::vector<core::TypedExprPtr> inputs) {
    return std::make_shared<core::CallTypedExpr>(type, std::move(inputs), name);
  }

  std::shared_ptr<memory::MemoryPool> aggregatePool_;
  std::unique_ptr<FlinkQueryRunner> runner_;
};

TEST_F(FlinkQueryRunnerTest, sql) {
  auto results = runner_->execute(
      "SELECT a, b, c FROM (VALUES (1, 'x', CAST(1.5 AS DOUBLE)), "
      "(2, CAST(NULL AS STRING), CAST(NULL AS DOUBLE))) AS t(a, b, c)");
  auto expected = makeRowVector({
      makeFlatVector<int32_t>({1, 2}),
      makeNullableFlatVector<std::string>({"x", std::nullopt}),
      makeNullableFlatVector<double>({1.5, std::nullopt}),
  });
  exec::test::assertEqualResults({expected}, results);
}

TEST_F(FlinkQueryRunnerTest, types) {
  auto input = makeRowVector(
      {"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"},
      {
          makeNullableFlatVector<bool>({true, std::nullopt}),
          makeNullableFlatVector<int8_t>({-128, std::nullopt}),
          makeNullableFlatVector<int16_t>({32'767, std::nullopt}),
          makeNullableFlatVector<int32_t>({-7, std::nullopt}),
          makeNullableFlatVector<int64_t>(
              {std::numeric_limits<int64_t>::min(), std::nullopt}),
          makeNullableFlatVector<float>({0.1f, std::nullopt}),
          makeNullableFlatVector<double>({-1e300, std::nullopt}),
          makeNullableFlatVector<std::string>({"it's ü", std::nullopt}),
          makeNullableFlatVector<std::string>(
              {std::string("\x00\xff", 2), std::nullopt}, VARBINARY()),
          makeNullableFlatVector<int32_t>({-719'162, std::nullopt}, DATE()),
          makeNullableFlatVector<Timestamp>(
              {Timestamp(1'577'882'096, 123'456'789), std::nullopt}),
      });
  std::vector<core::TypedExprPtr> projections;
  for (const auto& name : input->type()->asRow().names()) {
    projections.push_back(field(input->type()->asRow().findChild(name), name));
  }
  auto [results, errorCode] =
      runner_->executeAndReturnVector(makePlan(input, projections));
  ASSERT_EQ(errorCode, ReferenceQueryErrorCode::kSuccess);
  exec::test::assertEqualResults({input}, *results);
}

TEST_F(FlinkQueryRunnerTest, functions) {
  auto input = makeRowVector(
      {"s", "b", "row_number"},
      {
          makeNullableFlatVector<std::string>(
              {"  abc  ", "a,b,c", std::nullopt}),
          makeNullableFlatVector<bool>({true, false, std::nullopt}),
          makeFlatVector<int64_t>({0, 1, 2}),
      });
  auto s = field(VARCHAR(), "s");
  auto plan = makePlan(
      input,
      {
          call(VARCHAR(), "trim", {s}),
          call(BOOLEAN(), "is_not_true", {field(BOOLEAN(), "b")}),
          call(VARCHAR(), "md5", {s}),
          field(BIGINT(), "row_number"),
      });
  auto [results, errorCode] = runner_->execute(plan);
  ASSERT_EQ(errorCode, ReferenceQueryErrorCode::kSuccess);

  auto expected = makeRowVector({
      makeNullableFlatVector<std::string>({"abc", "a,b,c", std::nullopt}),
      makeFlatVector<bool>({false, true, true}),
      makeNullableFlatVector<std::string>(
          {"a064c2f1646e31d3c8e7ab5ead8864f5",
           "a44c56c8177e32d3613988f4dba7962e",
           std::nullopt}),
      makeFlatVector<int64_t>({0, 1, 2}),
  });
  exec::test::assertEqualResults(*results, plan->outputType(), {expected});
}

TEST_F(FlinkQueryRunnerTest, errors) {
  auto input = makeRowVector({"s"}, {makeFlatVector<std::string>({"a"})});

  // Flink rejects an unknown function while validating the query.
  auto [unsupported, unsupportedCode] = runner_->execute(makePlan(
      input, {call(VARCHAR(), "no_such_function", {field(VARCHAR(), "s")})}));
  EXPECT_FALSE(unsupported.has_value());
  EXPECT_EQ(
      unsupportedCode, ReferenceQueryErrorCode::kReferenceQueryUnsupported);

  // Plans that cannot be translated are unsupported as well.
  auto [untranslated, untranslatedCode] = runner_->execute(
      makePlan(input, {call(VARCHAR(), "try", {field(VARCHAR(), "s")})}));
  EXPECT_EQ(
      untranslatedCode, ReferenceQueryErrorCode::kReferenceQueryUnsupported);

  // Runtime errors fail the query.
  EXPECT_ANY_THROW(runner_->execute(
      "SELECT CAST(s AS INT) FROM (VALUES ('abc')) AS t(s)", ROW({INTEGER()})));
}

} // namespace
} // namespace facebook::velox::stateful::fuzzer
