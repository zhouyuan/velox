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
#include "velox/exec/fuzzer/FlinkSql.h"

#include <gtest/gtest.h>

#include "velox/core/Expressions.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::exec::test {
namespace {

class FlinkSqlTest : public testing::Test, public velox::test::VectorTestBase {
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
      literal(
          makeFlatVector<int64_t>(
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
      literal(
          makeFlatVector<std::string>(
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
      literal(
          makeFlatVector<int64_t>(
              std::vector<int64_t>{-12'345}, DECIMAL(10, 2))),
      "CAST('-123.45' AS DECIMAL(10, 2))");
  EXPECT_EQ(
      literal(makeNullableFlatVector<int32_t>({std::nullopt})),
      "CAST(NULL AS INT)");

  // Values that Flink cannot represent.
  EXPECT_EQ(
      literal(makeFlatVector<double>(std::vector<double>{std::nan("")})),
      std::nullopt);
  EXPECT_EQ(
      literal(makeFlatVector<int32_t>(std::vector<int32_t>{-800'000}, DATE())),
      std::nullopt);
  EXPECT_EQ(
      literal(
          makeFlatVector<Timestamp>(
              std::vector<Timestamp>{Timestamp(253'402'300'800, 0)})),
      std::nullopt);
  EXPECT_EQ(
      literal(
          makeFlatVector<std::string>(
              std::vector<std::string>{std::string("a\0b", 3)})),
      std::nullopt);
  EXPECT_EQ(
      literal(makeFlatVector<std::string>(std::vector<std::string>{"\xff"})),
      std::nullopt);
}

TEST_F(FlinkSqlTest, operators) {
  auto i = field(INTEGER(), "i");
  auto j = field(INTEGER(), "j");
  EXPECT_EQ(toSql(call(INTEGER(), "plus", {i, j})), "(`i` + `j`)");
  EXPECT_EQ(toSql(call(INTEGER(), "divide", {i, j})), "(`i` / `j`)");
  EXPECT_EQ(toSql(call(INTEGER(), "mod", {i, j})), "MOD(`i`, `j`)");
  EXPECT_EQ(toSql(call(INTEGER(), "negate", {i})), "(- `i`)");
  EXPECT_EQ(toSql(call(BOOLEAN(), "eq", {i, j})), "(`i` = `j`)");
  EXPECT_EQ(toSql(call(BOOLEAN(), "neq", {i, j})), "(`i` <> `j`)");
  EXPECT_EQ(toSql(call(BOOLEAN(), "lte", {i, j})), "(`i` <= `j`)");
  EXPECT_EQ(
      toSql(call(BOOLEAN(), "distinct_from", {i, j})),
      "(`i` IS DISTINCT FROM `j`)");
  EXPECT_EQ(
      toSql(call(BOOLEAN(), "between", {i, j, j})),
      "(`i` BETWEEN `j` AND `j`)");
  EXPECT_EQ(
      toSql(call(BOOLEAN(), "in", {i, j, constant(INTEGER(), 3)})),
      "(`i` IN (`j`, CAST(3 AS INT)))");
  EXPECT_EQ(toSql(call(BOOLEAN(), "is_null", {i})), "(`i` IS NULL)");
  EXPECT_EQ(toSql(call(BOOLEAN(), "not_null", {i})), "(`i` IS NOT NULL)");

  auto b = field(BOOLEAN(), "b");
  EXPECT_EQ(toSql(call(BOOLEAN(), "not", {b})), "(NOT `b`)");
  EXPECT_EQ(toSql(call(BOOLEAN(), "and", {b, b, b})), "(`b` AND `b` AND `b`)");
  EXPECT_EQ(
      toSql(call(INTEGER(), "if", {b, i, j})),
      "CASE WHEN `b` THEN `i` ELSE `j` END");
  EXPECT_EQ(toSql(call(INTEGER(), "if", {b, i})), "CASE WHEN `b` THEN `i` END");
  EXPECT_EQ(
      toSql(call(INTEGER(), "switch", {b, i, b, j, i})),
      "CASE WHEN `b` THEN `i` WHEN `b` THEN `j` ELSE `i` END");
  EXPECT_EQ(toSql(call(INTEGER(), "coalesce", {i, j})), "COALESCE(`i`, `j`)");

  auto s = field(VARCHAR(), "s");
  EXPECT_EQ(toSql(call(BOOLEAN(), "like", {s, s})), "(`s` LIKE `s`)");
  EXPECT_EQ(
      toSql(call(BOOLEAN(), "like", {s, s, s})), "(`s` LIKE `s` ESCAPE `s`)");

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
  auto t = field(VARCHAR(), "t");
  auto n = field(BIGINT(), "n");
  auto d = field(DOUBLE(), "d");

  // Functions of the same name.
  EXPECT_EQ(toSql(call(VARCHAR(), "upper", {s})), "UPPER(`s`)");
  EXPECT_EQ(toSql(call(DOUBLE(), "sqrt", {d})), "SQRT(`d`)");

  // Renamed functions.
  EXPECT_EQ(toSql(call(BIGINT(), "length", {s})), "CHAR_LENGTH(`s`)");
  EXPECT_EQ(toSql(call(DOUBLE(), "ceiling", {d})), "CEIL(`d`)");
  EXPECT_EQ(toSql(call(DOUBLE(), "pow", {d, d})), "POWER(`d`, `d`)");
  EXPECT_EQ(toSql(call(BOOLEAN(), "regexp_like", {s, t})), "REGEXP(`s`, `t`)");
  auto date = field(DATE(), "dt");
  EXPECT_EQ(toSql(call(BIGINT(), "day", {date})), "DAYOFMONTH(`dt`)");
  EXPECT_EQ(toSql(call(BIGINT(), "dow", {date})), "DAYOFWEEK(`dt`)");

  // Functions with different syntax.
  EXPECT_EQ(toSql(call(VARCHAR(), "trim", {s})), "TRIM(`s`)");
  EXPECT_EQ(
      toSql(call(VARCHAR(), "ltrim", {s, t})), "TRIM(LEADING `t` FROM `s`)");
  EXPECT_EQ(toSql(call(BIGINT(), "strpos", {s, t})), "POSITION(`t` IN `s`)");
  EXPECT_EQ(toSql(call(BIGINT(), "strpos", {s, t, n})), std::nullopt);
  EXPECT_EQ(toSql(call(VARCHAR(), "replace", {s, t})), "REPLACE(`s`, `t`, '')");
  EXPECT_EQ(
      toSql(call(VARCHAR(), "regexp_replace", {s, t})),
      "REGEXP_REPLACE(`s`, `t`, '')");

  // BIGINT arguments that Flink takes as INT are clamped to the INT range.
  EXPECT_EQ(
      toSql(call(VARCHAR(), "substr", {s, n})),
      "SUBSTR(`s`, CASE WHEN `n` > 2147483647 THEN 2147483647 "
      "WHEN `n` < -2147483648 THEN -2147483648 ELSE CAST(`n` AS INT) END)");
  EXPECT_EQ(
      toSql(call(VARCHAR(), "substr", {s, field(INTEGER(), "i")})),
      "SUBSTR(`s`, `i`)");

  // date_trunc requires a constant unit.
  auto ts = field(TIMESTAMP(), "ts");
  EXPECT_EQ(
      toSql(call(TIMESTAMP(), "date_trunc", {constant(VARCHAR(), "Day"), ts})),
      "FLOOR(`ts` TO DAY)");
  EXPECT_EQ(toSql(call(TIMESTAMP(), "date_trunc", {s, ts})), std::nullopt);
  EXPECT_EQ(
      toSql(call(TIMESTAMP(), "date_trunc", {constant(VARCHAR(), "foo"), ts})),
      std::nullopt);

  // try() has no Flink counterpart, also when nested.
  EXPECT_EQ(toSql(call(VARCHAR(), "try", {s})), std::nullopt);
  EXPECT_EQ(
      toSql(call(VARCHAR(), "upper", {call(VARCHAR(), "try", {s})})),
      std::nullopt);
}

TEST_F(FlinkSqlTest, plan) {
  auto input = makeRowVector(
      {"c0", "row_number"},
      {
          makeNullableFlatVector<int32_t>({1, std::nullopt}),
          makeFlatVector<int64_t>({0, 1}),
      });
  auto plan = std::make_shared<core::ProjectNode>(
      "project",
      std::vector<std::string>{"p0", "p1"},
      std::vector<core::TypedExprPtr>{
          call(BOOLEAN(), "is_null", {field(INTEGER(), "c0")}),
          field(BIGINT(), "row_number"),
      },
      std::make_shared<core::ValuesNode>(
          "values", std::vector<RowVectorPtr>{input}));
  EXPECT_EQ(
      toFlinkSql(plan, pool()),
      "SELECT (`c0` IS NULL) AS `p0`, `row_number` AS `p1` FROM (VALUES "
      "(CAST(1 AS INT), CAST(0 AS BIGINT)), "
      "(CAST(NULL AS INT), CAST(1 AS BIGINT))) "
      "AS t(`c0`, `row_number`)");

  // Plans without input rows are not supported.
  {
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
  }

  // Other plan shapes are not supported.
  EXPECT_EQ(
      toFlinkSql(
          std::make_shared<core::ValuesNode>(
              "values", std::vector<RowVectorPtr>{input}),
          pool()),
      std::nullopt);
}

} // namespace
} // namespace facebook::velox::exec::test
