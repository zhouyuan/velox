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
#include <gmock/gmock.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/Expressions.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "velox/type/variant/VariantBuilder.h"
#include "velox/type/variant/VariantType.h"

using namespace facebook::velox::test;

namespace facebook::velox::functions::sparksql::test {
namespace {

// Expected values were produced by Spark 4.0.2 with the session time zone
// America/Los_Angeles.

std::string fromHex(std::string_view hex) {
  std::string bytes;
  for (size_t i = 0; i < hex.size(); i += 2) {
    bytes.push_back(
        static_cast<char>(
            std::stoi(std::string(hex.substr(i, 2)), nullptr, 16)));
  }
  return bytes;
}

std::string toHex(std::string_view bytes) {
  std::string out;
  for (auto byte : bytes) {
    out += fmt::format("{:02x}", static_cast<uint8_t>(byte));
  }
  return out;
}

class VariantFunctionsTest : public SparkFunctionBaseTest {
 protected:
  void SetUp() override {
    SparkFunctionBaseTest::SetUp();
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kSessionTimezone, "America/Los_Angeles"},
        {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
    });
  }

  static core::TypedExprPtr field(
      const TypePtr& type,
      const std::string& name) {
    return std::make_shared<core::FieldAccessTypedExpr>(type, name);
  }

  static core::TypedExprPtr parseJson(core::TypedExprPtr input) {
    return std::make_shared<core::CallTypedExpr>(
        VARIANT(), std::vector<core::TypedExprPtr>{input}, "parse_json");
  }

  static core::TypedExprPtr variantGet(
      bool tryGet,
      const TypePtr& type,
      core::TypedExprPtr variant,
      core::TypedExprPtr path) {
    return std::make_shared<core::CallTypedExpr>(
        type,
        std::vector<core::TypedExprPtr>{variant, path},
        tryGet ? "try_variant_get" : "variant_get");
  }

  // Returns the hex metadata and value of parse_json(json), or std::nullopt
  // if try_parse_json returns NULL.
  std::optional<std::pair<std::string, std::string>> parseToHex(
      const std::string& json) {
    auto result = evaluate(
        "try_parse_json(c0)",
        makeRowVector({makeFlatVector<std::string>({json})}));
    if (result->isNullAt(0)) {
      return std::nullopt;
    }
    auto* row = result->as<RowVector>();
    return std::make_pair(
        toHex(row->childAt(0)->asFlatVector<StringView>()->valueAt(0)),
        toHex(row->childAt(1)->asFlatVector<StringView>()->valueAt(0)));
  }

  std::string evaluateString(
      const std::string& expression,
      const std::string& json) {
    auto result = evaluate(
        expression, makeRowVector({makeFlatVector<std::string>({json})}));
    return result->asFlatVector<StringView>()->valueAt(0).str();
  }

  // Evaluates try_variant_get and variant_get of 'path' in parse_json(json)
  // as 'type'. Expects try_variant_get to return 'expected'. Expects
  // variant_get to fail with 'error' if set, and to return 'expected'
  // otherwise.
  void testGet(
      const std::string& json,
      const std::string& path,
      const VectorPtr& expected,
      const std::optional<std::string>& error = std::nullopt) {
    const auto input = makeRowVector({
        makeFlatVector<std::string>({json}),
        makeFlatVector<std::string>({path}),
    });
    const auto& type = expected->type();
    SCOPED_TRACE(fmt::format("{} {} {}", json, path, type->toString()));
    const auto variant = parseJson(field(VARCHAR(), "c0"));
    const auto pathField = field(VARCHAR(), "c1");

    assertEqualVectors(
        expected, evaluate(variantGet(true, type, variant, pathField), input));
    if (error.has_value()) {
      VELOX_ASSERT_USER_THROW(
          evaluate(variantGet(false, type, variant, pathField), input), *error);
    } else {
      assertEqualVectors(
          expected,
          evaluate(variantGet(false, type, variant, pathField), input));
    }
  }

  VectorPtr nullOf(const TypePtr& type) {
    return BaseVector::createNullConstant(type, 1, pool());
  }
};

struct ParseJsonCase {
  std::string json;
  // Empty if parsing fails.
  std::string metadataHex;
  std::string valueHex;
  std::string toJson;
  std::string schema;
};

const std::vector<ParseJsonCase>& parseJsonCases() {
  static const std::vector<ParseJsonCase> kCases = {
      {"null", "010000", "00", "null", "VOID"},
      {"true", "010000", "04", "true", "BOOLEAN"},
      {"false", "010000", "08", "false", "BOOLEAN"},
      {"0", "010000", "0c00", "0", "BIGINT"},
      {"-1", "010000", "0cff", "-1", "BIGINT"},
      {"128", "010000", "108000", "128", "BIGINT"},
      {"-129", "010000", "107fff", "-129", "BIGINT"},
      {"32768", "010000", "1400800000", "32768", "BIGINT"},
      {"2147483648", "010000", "180000008000000000", "2147483648", "BIGINT"},
      {"-9223372036854775808",
       "010000",
       "180000000000000080",
       "-9223372036854775808",
       "BIGINT"},
      {"9223372036854775808",
       "010000",
       "280000000000000000800000000000000000",
       "9223372036854775808",
       "DECIMAL(19,0)"},
      {"12345678901234567890123456789012345678",
       "010000",
       "28004ef338de509049c4133302f0f6b04909",
       "12345678901234567890123456789012345678",
       "DECIMAL(38,0)"},
      {"123456789012345678901234567890123456789",
       "010000",
       "1c800558693a38d747",
       "1.2345678901234568E38",
       "DOUBLE"},
      {"1.5", "010000", "20010f000000", "1.5", "DECIMAL(2,1)"},
      {"1.50", "010000", "200296000000", "1.5", "DECIMAL(2,1)"},
      {"-0.0", "010000", "200100000000", "0", "DECIMAL(1,0)"},
      {"0.001", "010000", "200301000000", "0.001", "DECIMAL(3,3)"},
      {"100.0", "010000", "2001e8030000", "100", "DECIMAL(3,0)"},
      {"1e2", "010000", "1c0000000000005940", "100.0", "DOUBLE"},
      {"1.5E-3", "010000", "1cfa7e6abc7493583f", "0.0015", "DOUBLE"},
      {"-2.5e10", "010000", "1c000000e8764817c2", "-2.5E10", "DOUBLE"},
      {R"("")", "010000", "01", R"("")", "STRING"},
      {R"("abc")", "010000", "0d616263", R"("abc")", "STRING"},
      {R"("é\n\t\"")",
       "010000",
       "15c3a90a0922",
       "\"\xc3\xa9\\n\\t\\\"\"",
       "STRING"},
      {"[]", "010000", "030000", "[]", "ARRAY<VOID>"},
      {"{}", "010000", "020000", "{}", "OBJECT<>"},
      {R"([1,"a",null,[true]])",
       "010000",
       "0304000204050a0c010561000301000104",
       R"([1,"a",null,[true]])",
       "ARRAY<VARIANT>"},
      {R"({"b":1,"a":2})",
       "01020001026261",
       "020201000200040c010c02",
       R"({"a":2,"b":1})",
       "OBJECT<a: BIGINT, b: BIGINT>"},
      {R"({"a":{"c":[1,2],"b":"x"},"d":3.25})",
       "0104000102030461636264",
       "020200030012180202020109000b03020002040c010c020578200245010000",
       R"({"a":{"b":"x","c":[1,2]},"d":3.25})",
       "OBJECT<a: OBJECT<b: STRING, c: ARRAY<BIGINT>>, d: DECIMAL(3,2)>"},
      {R"( {"a" : 1 } )",
       "0101000161",
       "02010000020c01",
       R"({"a":1})",
       "OBJECT<a: BIGINT>"},
      // Spark ignores content after the first value.
      {"1 2", "010000", "0c01", "1", "BIGINT"},
      {R"({"a":1} xyz)",
       "0101000161",
       "02010000020c01",
       R"({"a":1})",
       "OBJECT<a: BIGINT>"},
      // Invalid inputs.
      {R"({"k":1,"k":2})", "", "", "", ""},
      {"[1,2", "", "", "", ""},
      {"abc", "", "", "", ""},
      {"", "", "", "", ""},
      {"01", "", "", "", ""},
      {"NaN", "", "", "", ""},
      {"'a'", "", "", "", ""},
      {R"({"a":1,})", "", "", "", ""},
  };
  return kCases;
}

TEST_F(VariantFunctionsTest, parseJson) {
  for (const auto& testCase : parseJsonCases()) {
    SCOPED_TRACE(testCase.json);
    const auto result = parseToHex(testCase.json);
    if (testCase.metadataHex.empty()) {
      EXPECT_FALSE(result.has_value());
      VELOX_ASSERT_THROW(
          evaluate(
              "parse_json(c0)",
              makeRowVector({makeFlatVector<std::string>({testCase.json})})),
          "");
      continue;
    }
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->first, testCase.metadataHex);
    EXPECT_EQ(result->second, testCase.valueHex);
  }
}

TEST_F(VariantFunctionsTest, parseJsonNull) {
  auto result = evaluate(
      "parse_json(c0)",
      makeRowVector({makeNullableFlatVector<std::string>({std::nullopt})}));
  EXPECT_TRUE(result->isNullAt(0));
}

TEST_F(VariantFunctionsTest, parseJsonLimits) {
  // Jackson limits nesting to 1000 levels.
  const std::string nested1000 =
      std::string(1'000, '[') + std::string(1'000, ']');
  EXPECT_TRUE(parseToHex(nested1000).has_value());
  const std::string nested1001 =
      std::string(1'001, '[') + std::string(1'001, ']');
  EXPECT_FALSE(parseToHex(nested1001).has_value());

  EXPECT_FALSE(parseToHex("truex").has_value());
  EXPECT_FALSE(parseToHex("1x").has_value());
  EXPECT_FALSE(parseToHex("\"a\nb\"").has_value());
  EXPECT_FALSE(parseToHex(R"("\x")").has_value());
  EXPECT_TRUE(parseToHex("[true,false]").has_value());

  // A surrogate pair becomes one 4-byte character. An unpaired surrogate
  // becomes '?'.
  EXPECT_EQ(parseToHex(R"("\ud83d\ude00")")->second, "11f09f9880");
  EXPECT_EQ(parseToHex(R"("\ud83d")")->second, "053f");
}

TEST_F(VariantFunctionsTest, duplicateKeys) {
  const std::string json = R"({"k":1,"j":[1],"k":"two"})";
  EXPECT_FALSE(parseToHex(json).has_value());
  VELOX_ASSERT_USER_THROW(
      evaluate(
          "parse_json(c0)",
          makeRowVector({makeFlatVector<std::string>({json})})),
      "Failed to build variant because of a duplicate object key: k");

  queryCtx_->testingOverrideConfigUnsafe({
      {SparkQueryConfig::qualify(SparkQueryConfig::kVariantAllowDuplicateKeys),
       "true"},
  });
  const auto result = parseToHex(json);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, "01020001026b6a");
  EXPECT_EQ(result->second, "0202010000060a030100020c010d74776f");
}

TEST_F(VariantFunctionsTest, toJsonAndSchema) {
  for (const auto& testCase : parseJsonCases()) {
    if (testCase.metadataHex.empty()) {
      continue;
    }
    SCOPED_TRACE(testCase.json);
    EXPECT_EQ(
        evaluateString("to_json(parse_json(c0))", testCase.json),
        testCase.toJson);
    EXPECT_EQ(
        evaluateString("schema_of_variant(parse_json(c0))", testCase.json),
        testCase.schema);
  }
}

TEST_F(VariantFunctionsTest, schemaOfVariant) {
  auto schema = [&](const std::string& json) {
    return evaluateString("schema_of_variant(parse_json(c0))", json);
  };
  EXPECT_EQ(schema("[1, 1.5, 2.25]"), "ARRAY<DECIMAL(22,2)>");
  EXPECT_EQ(schema(R"([1, "a"])"), "ARRAY<VARIANT>");
  EXPECT_EQ(
      schema(R"([{"a":1},{"b":"x"},{"a":2.5}])"),
      "ARRAY<OBJECT<a: DECIMAL(21,1), b: STRING>>");
  EXPECT_EQ(
      schema(R"({"a b":1,"`c":null})"), "OBJECT<```c`: VOID, `a b`: BIGINT>");
  EXPECT_EQ(schema("[1, 1e0]"), "ARRAY<DOUBLE>");
  EXPECT_EQ(
      schema("[1.5, 123456789012345678901234567.5]"), "ARRAY<DECIMAL(28,1)>");
  EXPECT_EQ(schema("[]"), "ARRAY<VOID>");
}

TEST_F(VariantFunctionsTest, isVariantNull) {
  auto input = makeRowVector({makeNullableFlatVector<std::string>(
      {"null", "1", std::nullopt, R"("null")"})});
  assertEqualVectors(
      makeFlatVector<bool>({true, false, false, false}),
      evaluate("is_variant_null(parse_json(c0))", input));

  // variant_get keeps a Variant null as a Variant null.
  auto get = variantGet(
      false,
      VARIANT(),
      parseJson(field(VARCHAR(), "c0")),
      field(VARCHAR(), "c1"));
  auto isNull = std::make_shared<core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{get}, "is_variant_null");
  assertEqualVectors(
      makeFlatVector<bool>({true, false}),
      evaluate(
          isNull,
          makeRowVector({
              makeFlatVector<std::string>({R"({"a":null})", R"({"a":null})"}),
              makeFlatVector<std::string>({"$.a", "$.b"}),
          })));
}

TEST_F(VariantFunctionsTest, toVariantObject) {
  // to_variant_object(named_struct('b', 1, 'a', array(1.5, null),
  //     'c', map('x', date'2024-01-02')))
  auto input = makeRowVector({makeRowVector(
      {"b", "a", "c"},
      {
          makeFlatVector<int32_t>({1}),
          makeNullableArrayVector<int64_t>(
              std::vector<std::vector<std::optional<int64_t>>>{
                  {15, std::nullopt}},
              ARRAY(DECIMAL(2, 1))),
          makeMapVector<std::string, int32_t>(
              {{{"x", 19'724}}}, MAP(VARCHAR(), DATE())),
      })});
  auto resultVector = evaluate("to_variant_object(c0)", input);
  auto* result = resultVector->as<RowVector>();
  EXPECT_EQ(
      toHex(result->childAt(0)->asFlatVector<StringView>()->valueAt(0)),
      "0104000102030462616378");
  EXPECT_EQ(
      toHex(result->childAt(1)->asFlatVector<StringView>()->valueAt(0)),
      "020301000202000e180c01030200060720010f0000000002010300052c0c4d0000");

  // to_json(to_variant_object(named_struct('t', timestamp'2024-01-02
  //     03:04:05.123', 'f', cast(1.5 as float), 'bin', X'0102', 'd', 1.5d,
  //     's', 'x')))
  input = makeRowVector({makeRowVector(
      {"t", "f", "bin", "d", "s"},
      {
          makeFlatVector<Timestamp>({Timestamp(1'704'193'445, 123'000'000)}),
          makeFlatVector<float>({1.5f}),
          makeFlatVector<std::string>({std::string("\x01\x02")}, VARBINARY()),
          makeFlatVector<double>({1.5}),
          makeFlatVector<std::string>({"x"}),
      })});
  assertEqualVectors(
      makeFlatVector<std::string>(
          {R"({"bin":"AQI=","d":1.5,"f":1.5,"s":"x","t":"2024-01-02 03:04:05.123-08:00"})"}),
      evaluate("to_json(to_variant_object(c0))", input));

  VELOX_ASSERT_THROW(
      evaluate(
          "to_variant_object(c0)",
          makeRowVector({makeRowVector(
              {"k", "k"},
              {makeFlatVector<int32_t>({1}), makeFlatVector<int32_t>({2})})})),
      "duplicate object key: k");
}

// TODO: Add a small double such as 1e-5. Spark prints "1.0E-5", but Velox's
// double to string conversion prints "1.0000000000000001E-5".
TEST_F(VariantFunctionsTest, toJsonSpecialValues) {
  VariantBuilder builder(false);
  const auto start = builder.writePosition();
  std::vector<VariantBuilder::Field> fields;
  auto add = [&](const std::string& name, auto append) {
    fields.push_back({builder.addKey(name), builder.writePosition() - start});
    append();
  };
  add("d", [&] { builder.appendDate(19'724); });
  add("t", [&] { builder.appendTimestamp(1'704'193'445'000'000); });
  add("ntz", [&] { builder.appendTimestampNtz(1'704'164'645'500'000); });
  add("nan", [&] { builder.appendDouble(std::nan("")); });
  add("inf",
      [&] { builder.appendFloat(-std::numeric_limits<float>::infinity()); });
  add("big", [&] { builder.appendDouble(1e20); });
  add("dec", [&] { builder.appendDecimal(10'000, 2); });
  add("u", [&] { builder.appendString("\xc3\xa9"); });
  builder.finishObject(start, fields);

  auto variant = makeRowVector(
      {"metadata", "value"},
      {
          makeFlatVector<std::string>({builder.metadata()}, VARBINARY()),
          makeFlatVector<std::string>({builder.value()}, VARBINARY()),
      });
  auto input = makeRowVector({std::make_shared<RowVector>(
      pool(), VARIANT(), nullptr, 1, variant->children())});
  assertEqualVectors(
      makeFlatVector<std::string>(
          {"{\"big\":1.0E20,\"d\":\"2024-01-02\",\"dec\":100,\"inf\":\"-Infinity\",\"nan\":\"NaN\",\"ntz\":\"2024-01-02 03:04:05.5\",\"t\":\"2024-01-02 03:04:05-08:00\",\"u\":\"\xc3\xa9\"}"}),
      evaluate("to_json(c0)", input));
}

TEST_F(VariantFunctionsTest, variantGetScalars) {
  const std::string kCastError = "cannot be cast into";
  testGet(R"({"a":1})", "$.a", makeFlatVector<int32_t>({1}));
  testGet(R"({"a":1})", "$.a", makeFlatVector<std::string>({"1"}));
  testGet(R"({"a":1})", "$.a", makeFlatVector<double>({1.0}));
  testGet(R"({"a":1})", "$.a", makeFlatVector<bool>({true}));
  testGet(
      R"({"a":300})",
      "$.a",
      nullOf(TINYINT()),
      "The variant value cannot be cast into TINYINT. Please use try_variant_get instead: 300");
  testGet(R"({"a":1})", "$.a", makeFlatVector<Timestamp>({Timestamp(1, 0)}));
  testGet(R"({"a":1})", "$.a", nullOf(DATE()), kCastError);
  testGet(R"({"a":1})", "$.a", makeFlatVector<int64_t>({100}, DECIMAL(5, 2)));
  testGet(R"({"a":1})", "$.b", nullOf(INTEGER()));
  testGet(R"({"a":1})", "$[0]", nullOf(INTEGER()));
  testGet("[1,2,3]", "$[2]", makeFlatVector<int32_t>({3}));
  testGet("[1,2,3]", "$[3]", nullOf(INTEGER()));
  testGet(R"({"a":"12"})", "$.a", makeFlatVector<int32_t>({12}));
  testGet(R"({"a":" 12 "})", "$.a", makeFlatVector<int32_t>({12}));
  testGet(R"({"a":"1.5"})", "$.a", nullOf(INTEGER()), kCastError);
  testGet(R"({"a":"abc"})", "$.a", nullOf(INTEGER()), kCastError);
  testGet(R"({"a":"true"})", "$.a", makeFlatVector<bool>({true}));
  testGet(
      R"({"a":"2024-01-02"})",
      "$.a",
      makeFlatVector<int32_t>({19'724}, DATE()));
  testGet(
      R"({"a":"2024-01-02 03:04:05"})",
      "$.a",
      makeFlatVector<Timestamp>({Timestamp(1'704'193'445, 0)}));
  testGet(R"({"a":1.25})", "$.a", makeFlatVector<std::string>({"1.25"}));
  testGet(R"({"a":1.25})", "$.a", makeFlatVector<int32_t>({1}));
  testGet(
      R"({"a":1.25})",
      "$.a",
      makeFlatVector<int64_t>({12'500}, DECIMAL(10, 4)));
  testGet(R"({"a":1.25})", "$.a", makeFlatVector<double>({1.25}));
  testGet(R"({"a":1.5e0})", "$.a", makeFlatVector<std::string>({"1.5"}));
  testGet(
      R"({"a":1.5e0})", "$.a", makeFlatVector<int64_t>({150}, DECIMAL(5, 2)));
  testGet(R"({"a":true})", "$.a", makeFlatVector<int32_t>({1}));
  testGet(R"({"a":true})", "$.a", makeFlatVector<std::string>({"true"}));
  testGet(R"({"a":null})", "$.a", nullOf(INTEGER()));
  testGet(
      R"({"a":{"b":[1,"x"]}})",
      "$.a",
      makeFlatVector<std::string>({R"({"b":[1,"x"]})"}));
  testGet(R"({"a":{"b":[1,"x"]}})", "$.a", nullOf(INTEGER()), kCastError);
  testGet(R"({"a b":1})", "$['a b']", makeFlatVector<int32_t>({1}));
  testGet(R"({"a.b":1})", R"($["a.b"])", makeFlatVector<int32_t>({1}));
  testGet(R"({"a":[{"b":7}]})", "$.a[0].b", makeFlatVector<int64_t>({7}));
  testGet(R"({"a":1})", "$", makeFlatVector<std::string>({R"({"a":1})"}));
  testGet(
      R"({"a":"x"})", "$.a", makeFlatVector<std::string>({"x"}, VARBINARY()));
  testGet(R"({"a":12345678901})", "$.a", nullOf(INTEGER()), kCastError);
  testGet(R"({"a":1e400})", "$.a", makeFlatVector<std::string>({"Infinity"}));
  testGet(
      R"({"a":2.5})",
      "$.a",
      makeFlatVector<Timestamp>({Timestamp(2, 500'000'000)}));
  testGet(R"({"a":0.1})", "$.a", makeFlatVector<float>({0.1f}));
  testGet(
      R"({"a":1e20})",
      "$.a",
      nullOf(BIGINT()),
      "The variant value cannot be cast into BIGINT. Please use try_variant_get instead: 1.0E20");
}

TEST_F(VariantFunctionsTest, variantGetComplex) {
  testGet(
      R"({"a":{"b":[1,"x"]}})",
      "$.a.b",
      makeArrayVector<std::string>({{"1", "x"}}));
  testGet(
      R"({"a":{"b":[1,"x"]}})",
      "$.a.b",
      makeNullableArrayVector<int32_t>(
          std::vector<std::vector<std::optional<int32_t>>>{{1, std::nullopt}}),
      "The variant value cannot be cast into INT. Please use try_variant_get instead: \"x\"");
  testGet(
      R"({"a":{"b":1,"c":"x"}})",
      "$.a",
      makeMapVector<std::string, std::string>({{{"b", "1"}, {"c", "x"}}}));
  testGet(
      R"({"a":{"b":1,"c":"x"}})",
      "$.a",
      makeRowVector(
          {"c", "b", "z"},
          {
              makeFlatVector<std::string>({"x"}),
              makeFlatVector<int32_t>({1}),
              makeNullableFlatVector<int32_t>({std::nullopt}),
          }));
  testGet(R"({"a":1})", "$.a", nullOf(ARRAY(INTEGER())), "cannot be cast into");
}

TEST_F(VariantFunctionsTest, variantGetVariant) {
  auto result = evaluate(
      variantGet(
          false,
          VARIANT(),
          parseJson(field(VARCHAR(), "c0")),
          field(VARCHAR(), "c1")),
      makeRowVector({
          makeFlatVector<std::string>({R"({"a":{"z":1,"y":[true]},"b":2})"}),
          makeFlatVector<std::string>({"$.a"}),
      }));
  auto* row = result->as<RowVector>();
  EXPECT_EQ(
      toHex(row->childAt(0)->asFlatVector<StringView>()->valueAt(0)),
      "0102000102797a");
  EXPECT_EQ(
      toHex(row->childAt(1)->asFlatVector<StringView>()->valueAt(0)),
      "0202000100050703010001040c01");
}

TEST_F(VariantFunctionsTest, variantGetInvalidPath) {
  for (const auto& path : {"", "a", "$.", "$[", "$[a]", "$['a]", "$a"}) {
    SCOPED_TRACE(path);
    VELOX_ASSERT_USER_THROW(
        testGet(R"({"a":1})", path, nullOf(INTEGER())),
        "The path is not a valid variant extraction path");
  }
}

TEST_F(VariantFunctionsTest, variantGetUnsupportedType) {
  VELOX_ASSERT_THROW(
      evaluate(
          variantGet(
              false,
              MAP(INTEGER(), INTEGER()),
              parseJson(field(VARCHAR(), "c0")),
              field(VARCHAR(), "c1")),
          makeRowVector({
              makeFlatVector<std::string>({"{}"}),
              makeFlatVector<std::string>({"$"}),
          })),
      "Unsupported result type for variant_get");
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
