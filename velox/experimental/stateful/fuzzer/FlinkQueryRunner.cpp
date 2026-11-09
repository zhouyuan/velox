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

#include <boost/algorithm/string.hpp>
#include <cpr/cpr.h> // @manual
#include <folly/Conv.h>
#include <folly/String.h>
#include <folly/json/json.h>
#include <thread>
#include <unordered_set>

#include "velox/common/encode/Base64.h"
#include "velox/core/QueryConfig.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/experimental/stateful/fuzzer/FlinkSql.h"
#include "velox/type/TimestampConversion.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::stateful::fuzzer {
namespace {

using exec::test::ReferenceQueryErrorCode;

folly::dynamic parseJson(const std::string& text) {
  folly::json::serialization_opts options;
  // Keep numbers as strings to parse BIGINT and DECIMAL values exactly.
  options.parse_numbers_as_strings = true;
  return folly::parseJson(text, options);
}

int64_t toInt(const folly::dynamic& value) {
  return value.isString() ? folly::to<int64_t>(value.getString())
                          : value.asInt();
}

std::string toFlinkSessionTimezone(
    const std::unordered_map<std::string, std::string>& queryConfigs) {
  const core::QueryConfig queryConfig{queryConfigs};
  // Velox uses UTC if the session timezone is not set.
  const auto sessionTimezone = queryConfig.sessionTimezone();
  return sessionTimezone.empty() ? "UTC" : sessionTimezone;
}

// Parses a decimal number such as '-12.5', '1E+3' or '1.0E-7' into an
// unscaled value of 'scale'.
int128_t parseDecimal(const std::string& text, uint8_t scale) {
  std::string mantissa = text;
  int32_t exponent = 0;
  if (auto pos = text.find_first_of("eE"); pos != std::string::npos) {
    mantissa = text.substr(0, pos);
    exponent = folly::to<int32_t>(text.substr(pos + 1));
  }
  bool negative = false;
  if (!mantissa.empty() && (mantissa[0] == '-' || mantissa[0] == '+')) {
    negative = mantissa[0] == '-';
    mantissa = mantissa.substr(1);
  }
  std::string digits;
  int32_t fractionDigits = 0;
  bool afterPoint = false;
  for (char c : mantissa) {
    if (c == '.') {
      afterPoint = true;
      continue;
    }
    VELOX_CHECK(std::isdigit(c), "Invalid decimal: {}", text);
    digits += c;
    if (afterPoint) {
      ++fractionDigits;
    }
  }
  // value = digits * 10^(exponent - fractionDigits). Rescale to 'scale'.
  const int32_t shift = scale + exponent - fractionDigits;
  int128_t unscaled = 0;
  for (char c : digits) {
    unscaled = unscaled * 10 + (c - '0');
  }
  for (int32_t i = 0; i < shift; ++i) {
    unscaled *= 10;
  }
  for (int32_t i = 0; i < -shift; ++i) {
    VELOX_CHECK_EQ(
        unscaled % 10, 0, "Decimal {} does not fit scale {}", text, scale);
    unscaled /= 10;
  }
  return negative ? -unscaled : unscaled;
}

double parseDouble(const folly::dynamic& value) {
  if (value.isDouble()) {
    return value.getDouble();
  }
  if (value.isInt()) {
    return value.getInt();
  }
  const auto& text = value.getString();
  if (text == "NaN") {
    return std::numeric_limits<double>::quiet_NaN();
  }
  if (text == "Infinity") {
    return std::numeric_limits<double>::infinity();
  }
  if (text == "-Infinity") {
    return -std::numeric_limits<double>::infinity();
  }
  return folly::to<double>(text);
}

void setValue(
    BaseVector& vector,
    vector_size_t row,
    const folly::dynamic& value) {
  if (value.isNull()) {
    vector.setNull(row, true);
    return;
  }
  const auto& type = vector.type();
  if (type->isDate()) {
    const auto& text = value.getString();
    auto days = util::fromDateString(
        text.data(), text.size(), util::ParseMode::kStrict);
    VELOX_CHECK(!days.hasError(), "Invalid date: {}", text);
    vector.asFlatVector<int32_t>()->set(row, days.value());
    return;
  }
  if (type->isShortDecimal()) {
    const auto scale = getDecimalPrecisionScale(*type).second;
    vector.asFlatVector<int64_t>()->set(
        row, static_cast<int64_t>(parseDecimal(value.asString(), scale)));
    return;
  }
  if (type->isLongDecimal()) {
    const auto scale = getDecimalPrecisionScale(*type).second;
    vector.asFlatVector<int128_t>()->set(
        row, parseDecimal(value.asString(), scale));
    return;
  }

  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      vector.asFlatVector<bool>()->set(
          row, value.isBool() ? value.getBool() : value.asString() == "true");
      return;
    case TypeKind::TINYINT:
      vector.asFlatVector<int8_t>()->set(row, folly::to<int8_t>(toInt(value)));
      return;
    case TypeKind::SMALLINT:
      vector.asFlatVector<int16_t>()->set(
          row, folly::to<int16_t>(toInt(value)));
      return;
    case TypeKind::INTEGER:
      vector.asFlatVector<int32_t>()->set(
          row, folly::to<int32_t>(toInt(value)));
      return;
    case TypeKind::BIGINT:
      vector.asFlatVector<int64_t>()->set(row, toInt(value));
      return;
    case TypeKind::REAL:
      vector.asFlatVector<float>()->set(
          row, static_cast<float>(parseDouble(value)));
      return;
    case TypeKind::DOUBLE:
      vector.asFlatVector<double>()->set(row, parseDouble(value));
      return;
    case TypeKind::VARCHAR:
      vector.asFlatVector<StringView>()->set(
          row, StringView(value.getString()));
      return;
    case TypeKind::VARBINARY: {
      // The JSON row format encodes binary values in Base64.
      const auto decoded = encoding::Base64::decode(value.getString());
      vector.asFlatVector<StringView>()->set(row, StringView(decoded));
      return;
    }
    case TypeKind::TIMESTAMP: {
      // E.g. 2020-01-01T12:34:56.123456789.
      const auto& text = value.getString();
      auto timestamp = util::fromTimestampString(
          text.data(), text.size(), util::TimestampParseMode::kIso8601);
      VELOX_CHECK(!timestamp.hasError(), "Invalid timestamp: {}", text);
      vector.asFlatVector<Timestamp>()->set(row, timestamp.value());
      return;
    }
    default:
      VELOX_UNSUPPORTED("Unsupported result type: {}", type->toString());
  }
}

// Returns true if every type in 'signature' is supported, treating type
// variables as supported.
bool isSupportedTypeSignature(
    const exec::TypeSignature& signature,
    const std::unordered_set<std::string>& variables) {
  static const std::unordered_set<std::string> kSupportedTypes{
      "boolean",
      "tinyint",
      "smallint",
      "integer",
      "bigint",
      "real",
      "double",
      "varchar",
      "varbinary",
      "timestamp",
      "date",
      "decimal",
  };
  const auto baseName = boost::algorithm::to_lower_copy(signature.baseName());
  if (variables.count(signature.baseName()) > 0) {
    return true;
  }
  if (kSupportedTypes.count(baseName) == 0) {
    return false;
  }
  if (baseName == "decimal") {
    return true;
  }
  for (const auto& parameter : signature.parameters()) {
    if (!isSupportedTypeSignature(parameter, variables)) {
      return false;
    }
  }
  return true;
}

} // namespace

FlinkQueryRunner::FlinkQueryRunner(
    memory::MemoryPool* pool,
    std::string gatewayUrl,
    const std::unordered_map<std::string, std::string>& queryConfigs,
    std::chrono::milliseconds timeout)
    : ReferenceQueryRunner(pool),
      gatewayUrl_(std::move(gatewayUrl)),
      timeout_(timeout),
      sessionProperties_(
          folly::dynamic::object
          // Run queries as bounded batch jobs that return final results only.
          ("execution.runtime-mode", "batch")(
              "table.local-time-zone",
              toFlinkSessionTimezone(queryConfigs))("parallelism.default", "1")(
              "table.exec.resource.default-parallelism",
              "1")),
      pool_(aggregatePool()->addLeafChild("leaf")) {}

FlinkQueryRunner::~FlinkQueryRunner() {
  try {
    closeSession();
  } catch (const std::exception& e) {
    LOG(WARNING) << "Failed to close Flink session: " << e.what();
  }
}

const std::vector<TypePtr>& FlinkQueryRunner::supportedScalarTypes() const {
  static const std::vector<TypePtr> kScalarTypes{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      REAL(),
      DOUBLE(),
      VARCHAR(),
      VARBINARY(),
      TIMESTAMP(),
      DATE(),
  };
  return kScalarTypes;
}

const std::unordered_map<std::string, exec::test::DataSpec>&
FlinkQueryRunner::aggregationFunctionDataSpecs() const {
  static const std::unordered_map<std::string, exec::test::DataSpec>
      kAggregationFunctionDataSpecs{};
  return kAggregationFunctionDataSpecs;
}

std::optional<std::string> FlinkQueryRunner::toSql(
    const core::PlanNodePtr& plan) {
  return toFlinkSql(plan, pool_.get());
}

bool FlinkQueryRunner::isSupported(const exec::FunctionSignature& signature) {
  std::unordered_set<std::string> variables;
  for (const auto& [name, _] : signature.variables()) {
    variables.insert(name);
  }
  if (!isSupportedTypeSignature(signature.returnType(), variables)) {
    return false;
  }
  for (const auto& argument : signature.argumentTypes()) {
    if (!isSupportedTypeSignature(argument, variables)) {
      return false;
    }
  }
  return true;
}

bool FlinkQueryRunner::isConstantExprSupported(const core::TypedExprPtr& expr) {
  return toFlinkExprSql(expr, pool_.get()).has_value();
}

std::pair<
    std::optional<std::multiset<std::vector<velox::variant>>>,
    ReferenceQueryErrorCode>
FlinkQueryRunner::execute(const core::PlanNodePtr& plan) {
  auto [results, errorCode] = executeAndReturnVector(plan);
  if (results) {
    return {exec::test::materialize(*results), errorCode};
  }
  return {std::nullopt, errorCode};
}

std::pair<std::optional<std::vector<RowVectorPtr>>, ReferenceQueryErrorCode>
FlinkQueryRunner::executeAndReturnVector(const core::PlanNodePtr& plan) {
  auto sql = toSql(plan);
  if (!sql) {
    return {std::nullopt, ReferenceQueryErrorCode::kReferenceQueryUnsupported};
  }
  try {
    return {
        execute(*sql, plan->outputType()), ReferenceQueryErrorCode::kSuccess};
  } catch (const FlinkConnectionError&) {
    throw;
  } catch (const FlinkQueryUnsupportedError& e) {
    LOG(INFO) << "Query unsupported in Flink: " << e.what();
    return {std::nullopt, ReferenceQueryErrorCode::kReferenceQueryUnsupported};
  } catch (const std::exception& e) {
    LOG(WARNING) << "Query failed in Flink: " << e.what();
    return {std::nullopt, ReferenceQueryErrorCode::kReferenceQueryFail};
  }
}

std::pair<folly::dynamic, std::vector<folly::dynamic>>
FlinkQueryRunner::runStatement(const std::string& sql) {
  ensureSession();
  VLOG(1) << "Executing Flink SQL: " << sql;

  const auto response = send(
      "POST",
      fmt::format("/v1/sessions/{}/statements", sessionHandle_),
      // Flink 1.19 rejects 'executionTimeout' ("SqlGatewayService doesn't
      // support timeout mechanism now"). 'timeout_' bounds fetching instead.
      folly::toJson(folly::dynamic::object("statement", sql)));
  if (response.statusCode != 200) {
    throwError("submit statement", response);
  }
  const auto operationHandle = response.body["operationHandle"].asString();
  const auto closeOperation = [&]() {
    send(
        "DELETE",
        fmt::format(
            "/v1/sessions/{}/operations/{}/close",
            sessionHandle_,
            operationHandle));
  };

  std::pair<folly::dynamic, std::vector<folly::dynamic>> results;
  try {
    results = fetchResults(operationHandle);
  } catch (const FlinkConnectionError&) {
    throw;
  } catch (...) {
    closeOperation();
    throw;
  }
  closeOperation();
  return results;
}

std::vector<RowVectorPtr> FlinkQueryRunner::execute(
    const std::string& sql,
    const RowTypePtr& resultType) {
  const auto rows = runStatement(sql).second;
  if (rows.empty()) {
    return {};
  }
  return {toRowVector(rows, resultType, pool_.get())};
}

std::vector<RowVectorPtr> FlinkQueryRunner::execute(const std::string& sql) {
  const auto [columns, rows] = runStatement(sql);
  if (columns.isNull() || rows.empty()) {
    return {};
  }
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (const auto& column : columns) {
    names.push_back(column["name"].asString());
    types.push_back(toVeloxType(column["logicalType"]));
  }
  return {
      toRowVector(rows, ROW(std::move(names), std::move(types)), pool_.get())};
}

std::pair<folly::dynamic, std::vector<folly::dynamic>>
FlinkQueryRunner::fetchResults(const std::string& operationHandle) {
  folly::dynamic columns = nullptr;
  std::vector<folly::dynamic> rows;
  std::string path = fmt::format(
      "/v2/sessions/{}/operations/{}/result/0?rowFormat=JSON",
      sessionHandle_,
      operationHandle);
  const auto deadline = std::chrono::steady_clock::now() + timeout_;
  while (true) {
    const auto response = send("GET", path);
    if (response.statusCode != 200) {
      throwError("fetch results", response);
    }
    const auto& body = response.body;
    const auto resultType = body["resultType"].asString();
    if (auto* results = body.get_ptr("results")) {
      if (auto* resultColumns = results->get_ptr("columns");
          resultColumns && !resultColumns->empty()) {
        columns = *resultColumns;
      }
      if (auto* data = results->get_ptr("data")) {
        for (const auto& row : *data) {
          const auto kind = row["kind"].asString();
          VELOX_CHECK_EQ(
              kind, "INSERT", "Unexpected row kind in batch mode: {}", kind);
          rows.push_back(row["fields"]);
        }
      }
    }
    if (resultType == "EOS") {
      break;
    }
    auto* nextResultUri = body.get_ptr("nextResultUri");
    if (nextResultUri == nullptr || nextResultUri->isNull()) {
      break;
    }
    path = nextResultUri->asString();
    if (resultType == "NOT_READY") {
      VELOX_CHECK(
          std::chrono::steady_clock::now() < deadline,
          "Timed out waiting for Flink results after {} ms",
          timeout_.count());
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }
  return {std::move(columns), std::move(rows)};
}

FlinkQueryRunner::Response FlinkQueryRunner::send(
    const std::string& method,
    const std::string& path,
    const std::string& body) {
  const cpr::Url url{gatewayUrl_ + path};
  const cpr::Timeout timeout{timeout_};
  const cpr::Header header{{"Content-Type", "application/json"}};
  cpr::Response response;
  if (method == "GET") {
    response = cpr::Get(url, header, timeout);
  } else if (method == "POST") {
    response = cpr::Post(url, header, cpr::Body{body}, timeout);
  } else if (method == "DELETE") {
    response = cpr::Delete(url, header, timeout);
  } else {
    VELOX_UNREACHABLE("Unsupported HTTP method: {}", method);
  }

  if (response.error) {
    throw FlinkConnectionError(fmt::format(
        "Couldn't connect to Flink SQL Gateway at {}: {}",
        gatewayUrl_,
        response.error.message));
  }
  folly::dynamic json = nullptr;
  if (!response.text.empty()) {
    try {
      json = parseJson(response.text);
    } catch (const std::exception&) {
      json = folly::dynamic::object(
          "errors", folly::dynamic::array(response.text));
    }
  }
  return {response.status_code, std::move(json)};
}

void FlinkQueryRunner::ensureSession() {
  if (!sessionHandle_.empty()) {
    return;
  }
  const auto response = send(
      "POST",
      "/v1/sessions",
      folly::toJson(folly::dynamic::object("sessionName", "velox-fuzzer")(
          "properties", sessionProperties_)));
  if (response.statusCode != 200) {
    throwError("open session", response);
  }
  sessionHandle_ = response.body["sessionHandle"].asString();
  LOG(INFO) << "Opened Flink session " << sessionHandle_ << " with properties "
            << folly::toJson(sessionProperties_);
}

void FlinkQueryRunner::closeSession() {
  if (sessionHandle_.empty()) {
    return;
  }
  send("DELETE", fmt::format("/v1/sessions/{}", sessionHandle_));
  sessionHandle_.clear();
}

void FlinkQueryRunner::throwError(
    const std::string& action,
    const Response& response) const {
  std::vector<std::string> errors;
  if (response.body.isObject()) {
    if (auto* list = response.body.get_ptr("errors")) {
      for (const auto& error : *list) {
        errors.push_back(error.asString());
      }
    }
  }
  const auto errorMessage = fmt::format(
      "Failed to {} (HTTP {}): {}",
      action,
      response.statusCode,
      folly::join("\n", errors));

  // Errors raised before the query runs: the query is not supported by Flink.
  static const std::vector<std::string> kUnsupportedErrors{
      "org.apache.flink.table.api.ValidationException",
      "org.apache.flink.table.api.SqlParserException",
      "org.apache.flink.sql.parser.impl.ParseException",
      "org.apache.calcite.sql.parser.SqlParseException",
      "org.apache.calcite.sql.validate.SqlValidatorException",
      "org.apache.calcite.runtime.CalciteContextException",
  };
  for (const auto& error : kUnsupportedErrors) {
    if (errorMessage.find(error) != std::string::npos) {
      throw FlinkQueryUnsupportedError(errorMessage);
    }
  }
  VELOX_FAIL(errorMessage);
}

TypePtr FlinkQueryRunner::toVeloxType(const folly::dynamic& logicalType) {
  const auto type = logicalType["type"].asString();
  if (type == "BOOLEAN") {
    return BOOLEAN();
  }
  if (type == "TINYINT") {
    return TINYINT();
  }
  if (type == "SMALLINT") {
    return SMALLINT();
  }
  if (type == "INTEGER") {
    return INTEGER();
  }
  if (type == "BIGINT") {
    return BIGINT();
  }
  if (type == "FLOAT") {
    return REAL();
  }
  if (type == "DOUBLE") {
    return DOUBLE();
  }
  if (type == "CHAR" || type == "VARCHAR") {
    return VARCHAR();
  }
  if (type == "BINARY" || type == "VARBINARY") {
    return VARBINARY();
  }
  if (type == "DATE") {
    return DATE();
  }
  if (type == "TIMESTAMP_WITHOUT_TIME_ZONE") {
    return TIMESTAMP();
  }
  if (type == "DECIMAL") {
    return DECIMAL(
        static_cast<uint8_t>(toInt(logicalType["precision"])),
        static_cast<uint8_t>(toInt(logicalType["scale"])));
  }
  VELOX_UNSUPPORTED("Unsupported Flink type: {}", folly::toJson(logicalType));
}

RowVectorPtr FlinkQueryRunner::toRowVector(
    const std::vector<folly::dynamic>& rows,
    const RowTypePtr& type,
    memory::MemoryPool* pool) {
  const auto size = static_cast<vector_size_t>(rows.size());
  auto rowVector = BaseVector::create<RowVector>(type, size, pool);
  for (column_index_t column = 0; column < type->size(); ++column) {
    auto& child = rowVector->childAt(column);
    for (vector_size_t row = 0; row < size; ++row) {
      const auto& fields = rows[row];
      VELOX_CHECK_EQ(
          fields.size(),
          type->size(),
          "Unexpected number of fields: {}",
          folly::toJson(fields));
      setValue(*child, row, fields[column]);
    }
  }
  return rowVector;
}

} // namespace facebook::velox::stateful::fuzzer
