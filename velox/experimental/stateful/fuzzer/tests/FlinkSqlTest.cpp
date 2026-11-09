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
#include "velox/experimental/stateful/fuzzer/FlinkSql.h"

#include <folly/json/json.h>
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/Expressions.h"
#include "velox/experimental/stateful/fuzzer/FlinkQueryRunner.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::stateful::fuzzer {
namespace {

class FlinkSqlTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  static core::TypedExprPtr field(
      const TypePtr& type,
      const std::string& name) {
    return std::make_shared<core::FieldAccessTypedExpr>(type, name);
  }

  static core::TypedExprPtr constant(const TypePtr& type, variant value) {
    return std::make_shared<core::ConstantTypedExpr>(type, std::move(value));
  }

  static core::TypedExprPtr call(
      const TypePtr& type,
      const std::string& name,
      std::vector<core::TypedExprPtr> inputs) {
    return std::make_shared<core::CallTypedExpr>(type, std::move(inputs), name);
  }

  std::optional<std::string> toSql(const core::TypedExprPtr& expr) {
    return toFlinkExprSql(expr, pool());
  }

  std::optional<std::string> literal(const VectorPtr& vector) {
    return toFlinkLiteralSql(*vector, 0);
  }

  static folly::dynamic parseJson(const std::string& json) {
    folly::json::serialization_opts options;
    options.parse_numbers_as_strings = true;
    return folly::parseJson(json, options);
  }
};

TEST_F(FlinkSqlTest, types) {
  EXPECT_EQ(toFlinkTypeSql(BOOLEAN()), "BOOLEAN");
  EXPECT_EQ(toFlinkTypeSql(TINYINT()), "TINYINT");
  EXPECT_EQ(toFlinkTypeSql(SMALLINT()), "SMALLINT");
  EXPECT_EQ(toFlinkTypeSql(INTEGER()), "INT");
  EXPECT_EQ(toFlinkTypeSql(BIGINT()), "BIGINT");
  EXPECT_EQ(toFlinkTypeSql(REAL()), "FLOAT");
  EXPECT_EQ(toFlinkTypeSql(DOUBLE()), "DOUBLE");
  EXPECT_EQ(toFlinkTypeSql(VARCHAR()), "STRING");
  EXPECT_EQ(toFlinkTypeSql(VARBINARY()), "BYTES");
  EXPECT_EQ(toFlinkTypeSql(DATE()), "DATE");
  EXPECT_EQ(toFlinkTypeSql(TIMESTAMP()), "TIMESTAMP(9)");
  EXPECT_EQ(toFlinkTypeSql(DECIMAL(10, 2)), "DECIMAL(10, 2)");
  EXPECT_EQ(toFlinkTypeSql(DECIMAL(38, 5)), "DECIMAL(38, 5)");

  EXPECT_EQ(toFlinkTypeSql(INTERVAL_DAY_TIME()), std::nullopt);
  EXPECT_EQ(toFlinkTypeSql(INTERVAL_YEAR_MONTH()), std::nullopt);
  EXPECT_EQ(toFlinkTypeSql(ARRAY(INTEGER())), std::nullopt);
  EXPECT_EQ(toFlinkTypeSql(MAP(INTEGER(), VARCHAR())), std::nullopt);
}

TEST_F(FlinkSqlTest, literals) {
  EXPECT_EQ(
      literal(makeFlatVector<bool>(std::vector<bool>{true})),
      "CAST(TRUE AS BOOLEAN)");
  EXPECT_EQ(
      literal(makeFlatVector<int8_t>(std::vector<int8_t>{-5})),
      "CAST(-5 AS TINYINT)");
  EXPECT_EQ(
      literal(makeFlatVector<int64_t>(
          std::vector<int64_t>{std::numeric_limits<int64_t>::min()})),
      "CAST(-9223372036854775808 AS BIGINT)");
  EXPECT_EQ(
      literal(makeFlatVector<double>(std::vector<double>{0.1})),
      "CAST(1.0000000000000001e-01 AS DOUBLE)");
  EXPECT_EQ(
      literal(makeFlatVector<float>(std::vector<float>{1.5f})),
      "CAST(1.50000000e+00 AS FLOAT)");
  EXPECT_EQ(
      literal(makeFlatVector<std::string>(std::vector<std::string>{"it's"})),
      "CAST('it''s' AS STRING)");
  EXPECT_EQ(
      literal(makeFlatVector<std::string>(
          std::vector<std::string>{"\x01\x7f"}, VARBINARY())),
      "CAST(X'017f' AS BYTES)");
  EXPECT_EQ(
      literal(makeFlatVector<int32_t>(std::vector<int32_t>{18'262}, DATE())),
      "CAST(DATE '2020-01-01' AS DATE)");
  EXPECT_EQ(
      literal(
          makeFlatVector<Timestamp>(std::vector<Timestamp>{Timestamp(1, 5)})),
      "CAST(TIMESTAMP '1970-01-01 00:00:01.000000005' AS TIMESTAMP(9))");
  EXPECT_EQ(
      literal(makeFlatVector<int64_t>(
          std::vector<int64_t>{-12'345}, DECIMAL(10, 2))),
      "CAST('-123.45' AS DECIMAL(10, 2))");
  EXPECT_EQ(
      literal(makeNullableFlatVector<int32_t>({std::nullopt})),
      "CAST(NULL AS INT)");

  // Values Flink cannot represent.
  EXPECT_EQ(
      literal(makeFlatVector<double>(std::vector<double>{std::nan("")})),
      std::nullopt);
  EXPECT_EQ(
      literal(makeFlatVector<int32_t>(std::vector<int32_t>{-800'000}, DATE())),
      std::nullopt);
  EXPECT_EQ(
      literal(makeFlatVector<Timestamp>(
          std::vector<Timestamp>{Timestamp(253'402'300'800, 0)})),
      std::nullopt);
  EXPECT_EQ(
      literal(makeFlatVector<std::string>(
          std::vector<std::string>{std::string("a\0b", 3)})),
      std::nullopt);
  EXPECT_EQ(
      literal(makeFlatVector<std::string>(std::vector<std::string>{"\xff"})),
      std::nullopt);
}

TEST_F(FlinkSqlTest, operators) {
  auto b = field(BOOLEAN(), "b");
  EXPECT_EQ(toSql(call(BOOLEAN(), "is_true", {b})), "(`b` IS TRUE)");
  EXPECT_EQ(toSql(call(BOOLEAN(), "is_not_false", {b})), "(`b` IS NOT FALSE)");
  EXPECT_EQ(toSql(call(BOOLEAN(), "not", {b})), "(NOT `b`)");
  EXPECT_EQ(toSql(call(BOOLEAN(), "and", {b, b, b})), "(`b` AND `b` AND `b`)");
  EXPECT_EQ(
      toSql(call(INTEGER(), "negate", {field(INTEGER(), "i")})), "(- `i`)");

  auto s = field(VARCHAR(), "s");
  EXPECT_EQ(toSql(call(BOOLEAN(), "like", {s, s})), "(`s` LIKE `s`)");
  EXPECT_EQ(
      toSql(call(BOOLEAN(), "like", {s, s, s})), "(`s` LIKE `s` ESCAPE `s`)");

  auto i = field(INTEGER(), "i");
  EXPECT_EQ(
      toSql(call(INTEGER(), "if", {b, i, i})),
      "CASE WHEN `b` THEN `i` ELSE `i` END");
  EXPECT_EQ(toSql(call(INTEGER(), "if", {b, i})), "CASE WHEN `b` THEN `i` END");
  EXPECT_EQ(
      toSql(call(INTEGER(), "switch", {b, i, b, i, i})),
      "CASE WHEN `b` THEN `i` WHEN `b` THEN `i` ELSE `i` END");
  EXPECT_EQ(toSql(call(INTEGER(), "coalesce", {i, i})), "COALESCE(`i`, `i`)");

  EXPECT_EQ(
      toSql(std::make_shared<core::CastTypedExpr>(BIGINT(), i, false)),
      "CAST(`i` AS BIGINT)");
  EXPECT_EQ(
      toSql(std::make_shared<core::CastTypedExpr>(BIGINT(), i, true)),
      "TRY_CAST(`i` AS BIGINT)");

  // Identifiers are quoted, e.g. the reserved keyword ROW_NUMBER.
  EXPECT_EQ(toSql(field(BIGINT(), "row_number")), "`row_number`");
}

TEST_F(FlinkSqlTest, functions) {
  auto s = field(VARCHAR(), "s");
  auto c = field(VARCHAR(), "c");
  EXPECT_EQ(toSql(call(VARCHAR(), "md5", {s})), "MD5(`s`)");
  EXPECT_EQ(
      toSql(call(DOUBLE(), "ceiling", {field(DOUBLE(), "d")})), "CEIL(`d`)");
  EXPECT_EQ(toSql(call(VARCHAR(), "ifnull", {s, c})), "IFNULL(`s`, `c`)");

  // Trim functions.
  EXPECT_EQ(toSql(call(VARCHAR(), "trim", {s})), "TRIM(`s`)");
  EXPECT_EQ(toSql(call(VARCHAR(), "ltrim", {s})), "LTRIM(`s`)");
  EXPECT_EQ(toSql(call(VARCHAR(), "trim", {s, c})), "TRIM(BOTH `c` FROM `s`)");
  EXPECT_EQ(
      toSql(call(VARCHAR(), "ltrim_runtime", {s, c})),
      "TRIM(LEADING `c` FROM `s`)");
  EXPECT_EQ(
      toSql(call(VARCHAR(), "rtrim", {s, c})), "TRIM(TRAILING `c` FROM `s`)");

  // BIGINT arguments that Flink takes as INT.
  EXPECT_EQ(
      toSql(call(VARCHAR(), "split_index", {s, c, field(BIGINT(), "n")})),
      "SPLIT_INDEX(`s`, `c`, CASE WHEN `n` IS NULL THEN CAST(NULL AS INT) "
      "WHEN `n` BETWEEN -2147483648 AND 2147483647 THEN CAST(`n` AS INT) "
      "ELSE CAST(NULL AS INT) END)");
  EXPECT_EQ(
      toSql(call(VARCHAR(), "sha2", {s, field(BIGINT(), "n")})),
      "SHA2(`s`, CASE WHEN `n` IS NULL THEN CAST(NULL AS INT) "
      "WHEN `n` BETWEEN -2147483648 AND 2147483647 THEN CAST(`n` AS INT) "
      "ELSE -1 END)");
  EXPECT_EQ(
      toSql(call(VARCHAR(), "sha2", {s, field(INTEGER(), "n")})),
      "SHA2(`s`, `n`)");

  // EXTRACT requires a constant unit.
  auto ts = field(TIMESTAMP(), "ts");
  EXPECT_EQ(
      toSql(call(BIGINT(), "extract", {constant(VARCHAR(), "Year"), ts})),
      "EXTRACT(YEAR FROM `ts`)");
  EXPECT_EQ(
      toSql(call(BIGINT(), "extract", {constant(VARCHAR(), "doy"), ts})),
      "EXTRACT(DOY FROM `ts`)");
  EXPECT_EQ(toSql(call(BIGINT(), "extract", {s, ts})), std::nullopt);
  EXPECT_EQ(
      toSql(call(BIGINT(), "extract", {constant(VARCHAR(), "foo"), ts})),
      std::nullopt);

  // Functions without a Flink counterpart.
  EXPECT_EQ(toSql(call(VARCHAR(), "try", {s})), std::nullopt);
  EXPECT_EQ(toSql(call(BIGINT(), "count_char", {s, c})), std::nullopt);
  // Unsupported arguments propagate.
  EXPECT_EQ(
      toSql(call(VARCHAR(), "md5", {call(VARCHAR(), "try", {s})})),
      std::nullopt);
}

TEST_F(FlinkSqlTest, plan) {
  auto input = makeRowVector(
      {"c0", "row_number"},
      {
          makeNullableFlatVector<bool>({true, std::nullopt}),
          makeFlatVector<int64_t>(std::vector<int64_t>{0, 1}),
      });
  auto plan = std::make_shared<core::ProjectNode>(
      "project",
      std::vector<std::string>{"p0", "p1"},
      std::vector<core::TypedExprPtr>{
          call(BOOLEAN(), "is_true", {field(BOOLEAN(), "c0")}),
          field(BIGINT(), "row_number"),
      },
      std::make_shared<core::ValuesNode>(
          "values", std::vector<RowVectorPtr>{input}));

  EXPECT_EQ(
      toFlinkSql(plan, pool()),
      "SELECT (`c0` IS TRUE) AS `p0`, `row_number` AS `p1` FROM (VALUES "
      "(CAST(TRUE AS BOOLEAN), CAST(0 AS BIGINT)), "
      "(CAST(NULL AS BOOLEAN), CAST(1 AS BIGINT))) "
      "AS t(`c0`, `row_number`)");

  // Plans without input rows are not supported.
  auto emptyPlan = std::make_shared<core::ProjectNode>(
      "project",
      std::vector<std::string>{"p0"},
      std::vector<core::TypedExprPtr>{field(BIGINT(), "row_number")},
      std::make_shared<core::ValuesNode>(
          "values",
          std::vector<RowVectorPtr>{makeRowVector(
              {"row_number"},
              {makeFlatVector<int64_t>(std::vector<int64_t>{})})}));
  EXPECT_EQ(toFlinkSql(emptyPlan, pool()), std::nullopt);

  // Other plan shapes are not supported.
  EXPECT_EQ(
      toFlinkSql(
          std::make_shared<core::ValuesNode>(
              "values", std::vector<RowVectorPtr>{input}),
          pool()),
      std::nullopt);
}

TEST_F(FlinkSqlTest, toVeloxType) {
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

TEST_F(FlinkSqlTest, toRowVector) {
  const auto rows = parseJson(R"([
      [true, 1, 9223372036854775807, 1.5, "abc", "AX8=", "2020-01-01",
       "2020-01-01T12:34:56.123456789", "-123.45", "1.0E-7"],
      [null, null, null, null, null, null, null, null, null, null]
  ])");
  const auto type = ROW(
      {BOOLEAN(),
       TINYINT(),
       BIGINT(),
       DOUBLE(),
       VARCHAR(),
       VARBINARY(),
       DATE(),
       TIMESTAMP(),
       DECIMAL(10, 2),
       DECIMAL(20, 8)});
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
  test::assertEqualVectors(expected, result);
}

} // namespace
} // namespace facebook::velox::stateful::fuzzer
