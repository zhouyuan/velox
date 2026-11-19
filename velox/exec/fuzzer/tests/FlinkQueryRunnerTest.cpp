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
#include "velox/exec/fuzzer/FlinkQueryRunner.h"

#include <folly/json/json.h>
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/Expressions.h"
#include "velox/core/QueryConfig.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::exec::test {
namespace {

class FlinkQueryRunnerTest : public testing::Test,
                             public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  // Returns a runner for the Flink SQL Gateway at FLINK_GATEWAY_URL (default
  // http://localhost:8083). Only the DISABLED_ tests send requests.
  std::unique_ptr<FlinkQueryRunner> makeRunner() {
    const char* url = std::getenv("FLINK_GATEWAY_URL");
    aggregatePool_ = rootPool_->addAggregateChild("flink");
    return std::make_unique<FlinkQueryRunner>(
        aggregatePool_.get(),
        url ? url : "http://localhost:8083",
        std::unordered_map<std::string, std::string>{
            {core::QueryConfig::kSessionTimezone, "America/Los_Angeles"}},
        std::chrono::seconds(60));
  }

  static folly::dynamic parseJson(const std::string& json) {
    folly::json::serialization_opts options;
    options.parse_numbers_as_strings = true;
    return folly::parseJson(json, options);
  }

  static core::PlanNodePtr makePlan(
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
};

TEST_F(FlinkQueryRunnerTest, toVeloxType) {
  EXPECT_EQ(
      *FlinkQueryRunner::toVeloxType(
          parseJson(R"({"type": "INTEGER", "nullable": true})")),
      *INTEGER());
  EXPECT_EQ(
      *FlinkQueryRunner::toVeloxType(parseJson(
          R"({"type": "VARCHAR", "nullable": true, "length": 2147483647})")),
      *VARCHAR());
  EXPECT_EQ(
      *FlinkQueryRunner::toVeloxType(parseJson(
          R"({"type": "DECIMAL", "nullable": true, "precision": 20, "scale": 3})")),
      *DECIMAL(20, 3));
  EXPECT_EQ(
      *FlinkQueryRunner::toVeloxType(parseJson(
          R"({"type": "TIMESTAMP_WITHOUT_TIME_ZONE", "precision": 9})")),
      *TIMESTAMP());
  VELOX_ASSERT_THROW(
      FlinkQueryRunner::toVeloxType(
          parseJson(R"({"type": "TIMESTAMP_WITH_LOCAL_TIME_ZONE"})")),
      "Unsupported Flink type");
}

TEST_F(FlinkQueryRunnerTest, toRowVector) {
  // Values as serialized by the SQL Gateway: BYTES in Base64, TIMESTAMP(9) in
  // ISO 8601 with nanoseconds, DECIMAL as JSON numbers that may use exponents.
  const auto rows = parseJson(R"([
      [true, 1, 9223372036854775807, 1.5, "abc", "AX8=", "2020-01-01",
       "2020-01-01T12:34:56.123456789", -123.45, 1E-7],
      [null, null, null, null, null, null, null, null, null, null]
  ])");
  const auto type = ROW({
      BOOLEAN(),
      TINYINT(),
      BIGINT(),
      DOUBLE(),
      VARCHAR(),
      VARBINARY(),
      DATE(),
      TIMESTAMP(),
      DECIMAL(10, 2),
      DECIMAL(20, 8),
  });
  auto result = FlinkQueryRunner::toRowVector(
      std::vector<folly::dynamic>(rows.begin(), rows.end()), type, pool());

  auto expected = makeRowVector({
      makeNullableFlatVector<bool>({true, std::nullopt}),
      makeNullableFlatVector<int8_t>({1, std::nullopt}),
      makeNullableFlatVector<int64_t>(
          {std::numeric_limits<int64_t>::max(), std::nullopt}),
      makeNullableFlatVector<double>({1.5, std::nullopt}),
      makeNullableFlatVector<std::string>({"abc", std::nullopt}),
      makeNullableFlatVector<std::string>(
          {std::string("\x01\x7f"), std::nullopt}, VARBINARY()),
      makeNullableFlatVector<int32_t>({18'262, std::nullopt}, DATE()),
      makeNullableFlatVector<Timestamp>(
          {Timestamp(1'577'882'096, 123'456'789), std::nullopt}),
      makeNullableFlatVector<int64_t>({-12'345, std::nullopt}, DECIMAL(10, 2)),
      makeNullableFlatVector<int128_t>({10, std::nullopt}, DECIMAL(20, 8)),
  });
  velox::test::assertEqualVectors(expected, result);
}

// The tests below require a Flink SQL Gateway, see
// velox/docs/develop/testing/flink-query-runner.rst, so disable them by
// default.
TEST_F(FlinkQueryRunnerTest, DISABLED_sql) {
  auto runner = makeRunner();
  auto results = runner->execute(
      "SELECT a, b, c FROM (VALUES (1, 'x', CAST(1.5 AS DOUBLE)), "
      "(2, CAST(NULL AS STRING), CAST(NULL AS DOUBLE))) AS t(a, b, c)");
  auto expected = makeRowVector({
      makeFlatVector<int32_t>({1, 2}),
      makeNullableFlatVector<std::string>({"x", std::nullopt}),
      makeNullableFlatVector<double>({1.5, std::nullopt}),
  });
  assertEqualResults({expected}, results);
}

TEST_F(FlinkQueryRunnerTest, DISABLED_types) {
  auto runner = makeRunner();
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
  const auto& rowType = input->type()->asRow();
  for (column_index_t i = 0; i < rowType.size(); ++i) {
    projections.push_back(field(rowType.childAt(i), rowType.nameOf(i)));
  }
  auto [results, errorCode] =
      runner->executeAndReturnVector(makePlan(input, projections));
  ASSERT_EQ(errorCode, ReferenceQueryErrorCode::kSuccess);
  assertEqualResults({input}, *results);
}

TEST_F(FlinkQueryRunnerTest, DISABLED_prestoFunctions) {
  auto runner = makeRunner();
  auto input = makeRowVector(
      {"s", "n", "row_number"},
      {
          makeNullableFlatVector<std::string>(
              {"  abc  ", "a,b,c", std::nullopt}),
          makeNullableFlatVector<int64_t>({2, 10'000'000'000, std::nullopt}),
          makeFlatVector<int64_t>({0, 1, 2}),
      });
  auto s = field(VARCHAR(), "s");
  auto n = field(BIGINT(), "n");
  auto plan = makePlan(
      input,
      {
          call(VARCHAR(), "trim", {s}),
          call(BIGINT(), "length", {s}),
          call(VARCHAR(), "substr", {s, n}),
          call(BOOLEAN(), "eq", {n, n}),
          field(BIGINT(), "row_number"),
      });
  auto [results, errorCode] = runner->execute(plan);
  ASSERT_EQ(errorCode, ReferenceQueryErrorCode::kSuccess);

  auto expected = makeRowVector({
      makeNullableFlatVector<std::string>({"abc", "a,b,c", std::nullopt}),
      makeNullableFlatVector<int64_t>({7, 5, std::nullopt}),
      makeNullableFlatVector<std::string>({" abc  ", "", std::nullopt}),
      makeNullableFlatVector<bool>({true, true, std::nullopt}),
      makeFlatVector<int64_t>({0, 1, 2}),
  });
  assertEqualResults(*results, plan->outputType(), {expected});
}

TEST_F(FlinkQueryRunnerTest, DISABLED_unsupported) {
  auto runner = makeRunner();
  auto input = makeRowVector(
      {"s"}, {makeFlatVector<std::string>(std::vector<std::string>{"a"})});
  auto s = field(VARCHAR(), "s");

  // Flink rejects an unknown function while validating the query.
  {
    auto [results, errorCode] = runner->execute(
        makePlan(input, {call(VARCHAR(), "no_such_function", {s})}));
    EXPECT_FALSE(results.has_value());
    EXPECT_EQ(errorCode, ReferenceQueryErrorCode::kReferenceQueryUnsupported);
  }

  // Flink MD5 returns STRING while Velox Presto md5 returns VARBINARY.
  {
    auto [results, errorCode] = runner->execute(makePlan(
        input,
        {call(
            VARBINARY(),
            "md5",
            {std::make_shared<core::CastTypedExpr>(VARBINARY(), s, false)})}));
    EXPECT_FALSE(results.has_value());
    EXPECT_EQ(errorCode, ReferenceQueryErrorCode::kReferenceQueryUnsupported);
  }

  // Plans that cannot be translated are unsupported as well.
  {
    auto [results, errorCode] =
        runner->execute(makePlan(input, {call(VARCHAR(), "try", {s})}));
    EXPECT_EQ(errorCode, ReferenceQueryErrorCode::kReferenceQueryUnsupported);
  }

  // Runtime errors fail the query.
  VELOX_ASSERT_THROW(
      runner->execute(
          "SELECT CAST(s AS INT) FROM (VALUES ('abc')) AS t(s)",
          ROW({INTEGER()})),
      "Failed to fetch results");
}

} // namespace
} // namespace facebook::velox::exec::test
