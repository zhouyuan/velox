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

#include <boost/algorithm/string.hpp>
#include <fmt/format.h>
#include <folly/String.h>
#include <cmath>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

#include "velox/core/Expressions.h"
#include "velox/type/DecimalUtil.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/SimpleVector.h"

namespace facebook::velox::stateful::fuzzer {
namespace {

// Flink supports dates and timestamps from 0001-01-01 to 9999-12-31.
constexpr int32_t kMinDays = -719'162; // 0001-01-01
constexpr int32_t kMaxDays = 2'932'896; // 9999-12-31
constexpr int64_t kMinSeconds = static_cast<int64_t>(kMinDays) * 86'400;
constexpr int64_t kMaxSeconds = static_cast<int64_t>(kMaxDays + 1) * 86'400;

std::string quoteIdentifier(const std::string& name) {
  std::string quoted = "`";
  for (char c : name) {
    if (c == '`') {
      quoted += '`';
    }
    quoted += c;
  }
  quoted += '`';
  return quoted;
}

// Returns true if 's' is valid UTF-8 without NUL characters, which Flink SQL
// string literals cannot carry reliably.
bool isSupportedString(std::string_view s) {
  size_t i = 0;
  while (i < s.size()) {
    const auto c = static_cast<unsigned char>(s[i]);
    if (c == 0) {
      return false;
    }
    size_t length;
    if (c < 0x80) {
      length = 1;
    } else if ((c >> 5) == 0x6) {
      length = 2;
    } else if ((c >> 4) == 0xE) {
      length = 3;
    } else if ((c >> 3) == 0x1E) {
      length = 4;
    } else {
      return false;
    }
    if (i + length > s.size()) {
      return false;
    }
    for (size_t j = 1; j < length; ++j) {
      if ((static_cast<unsigned char>(s[i + j]) >> 6) != 0x2) {
        return false;
      }
    }
    i += length;
  }
  return true;
}

std::string quoteString(std::string_view s) {
  std::string quoted = "'";
  for (char c : s) {
    if (c == '\'') {
      quoted += '\'';
    }
    quoted += c;
  }
  quoted += '\'';
  return quoted;
}

template <typename T>
T valueAt(const BaseVector& vector, vector_size_t row) {
  return vector.as<SimpleVector<T>>()->valueAt(row);
}

std::optional<std::string> toLiteralValueSql(
    const BaseVector& vector,
    vector_size_t row) {
  const auto& type = vector.type();
  if (type->isDate()) {
    const auto days = valueAt<int32_t>(vector, row);
    if (days < kMinDays || days > kMaxDays) {
      return std::nullopt;
    }
    TimestampToStringOptions options;
    options.mode = TimestampToStringOptions::Mode::kDateOnly;
    options.zeroPaddingYear = true;
    return fmt::format(
        "DATE '{}'",
        Timestamp(static_cast<int64_t>(days) * 86'400, 0).toString(options));
  }
  if (type->isShortDecimal()) {
    return quoteString(
        DecimalUtil::toString(valueAt<int64_t>(vector, row), type));
  }
  if (type->isLongDecimal()) {
    return quoteString(
        DecimalUtil::toString(valueAt<int128_t>(vector, row), type));
  }

  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      return valueAt<bool>(vector, row) ? "TRUE" : "FALSE";
    case TypeKind::TINYINT:
      return std::to_string(valueAt<int8_t>(vector, row));
    case TypeKind::SMALLINT:
      return std::to_string(valueAt<int16_t>(vector, row));
    case TypeKind::INTEGER:
      return std::to_string(valueAt<int32_t>(vector, row));
    case TypeKind::BIGINT:
      return std::to_string(valueAt<int64_t>(vector, row));
    case TypeKind::REAL: {
      const auto value = valueAt<float>(vector, row);
      if (!std::isfinite(value)) {
        return std::nullopt;
      }
      // Exponent notation makes Flink parse an approximate (DOUBLE) literal
      // instead of an exact DECIMAL one. 9 digits identify a float uniquely.
      return fmt::format("{:.8e}", value);
    }
    case TypeKind::DOUBLE: {
      const auto value = valueAt<double>(vector, row);
      if (!std::isfinite(value)) {
        return std::nullopt;
      }
      // 17 digits identify a double uniquely.
      return fmt::format("{:.16e}", value);
    }
    case TypeKind::VARCHAR: {
      const auto value = valueAt<StringView>(vector, row);
      const std::string_view view(value.data(), value.size());
      if (!isSupportedString(view)) {
        return std::nullopt;
      }
      return quoteString(view);
    }
    case TypeKind::VARBINARY: {
      const auto value = valueAt<StringView>(vector, row);
      return fmt::format(
          "X'{}'",
          folly::hexlify(std::string_view(value.data(), value.size())));
    }
    case TypeKind::TIMESTAMP: {
      const auto value = valueAt<Timestamp>(vector, row);
      if (value.getSeconds() < kMinSeconds ||
          value.getSeconds() >= kMaxSeconds) {
        return std::nullopt;
      }
      TimestampToStringOptions options;
      options.dateTimeSeparator = ' ';
      options.zeroPaddingYear = true;
      return fmt::format("TIMESTAMP '{}'", value.toString(options));
    }
    default:
      return std::nullopt;
  }
}

// Binary operators whose Velox names differ from Flink SQL.
const std::unordered_map<std::string, std::string>& binaryOperators() {
  static const std::unordered_map<std::string, std::string> kOperators{
      {"plus", "+"},
      {"minus", "-"},
      {"multiply", "*"},
      {"interval_lessthan", "<"},
      {"interval_lessthanorequal", "<="},
      {"interval_greaterthan", ">"},
      {"interval_greaterthanorequal", ">="},
      {"interval_equalto", "="},
      {"interval_notequalto", "<>"},
  };
  return kOperators;
}

// Unary postfix predicates, e.g. 'x IS TRUE'.
const std::unordered_map<std::string, std::string>& postfixOperators() {
  static const std::unordered_map<std::string, std::string> kOperators{
      {"is_true", "IS TRUE"},
      {"is_false", "IS FALSE"},
      {"is_not_true", "IS NOT TRUE"},
      {"is_not_false", "IS NOT FALSE"},
      {"is_null", "IS NULL"},
      {"not_null", "IS NOT NULL"},
  };
  return kOperators;
}

// Velox functions whose names differ from their Flink SQL counterparts.
const std::unordered_map<std::string, std::string>& renamedFunctions() {
  static const std::unordered_map<std::string, std::string> kFunctions{
      {"ceiling", "CEIL"},
  };
  return kFunctions;
}

// Arguments of Velox signatures that are BIGINT while Flink takes INT. Keyed
// by function name, valued by argument index and the Flink SQL that replaces
// values out of the INT range, chosen to give the same result as Velox:
// - split_index and regexp_extract: an out-of-range index yields NULL.
// - sha2: an out-of-range length is invalid, which fails in both engines.
struct IntArgument {
  size_t index;
  std::string outOfRangeSql;
};

const std::unordered_map<std::string, IntArgument>& intArguments() {
  static const std::unordered_map<std::string, IntArgument> kArguments{
      {"split_index", {2, "CAST(NULL AS INT)"}},
      {"regexp_extract", {2, "CAST(NULL AS INT)"}},
      {"sha2", {1, "-1"}},
  };
  return kArguments;
}

// Units of the 'extract' UDF and their Flink SQL time interval units.
const std::unordered_map<std::string, std::string>& extractUnits() {
  static const std::unordered_map<std::string, std::string> kUnits{
      {"year", "YEAR"},
      {"month", "MONTH"},
      {"quarter", "QUARTER"},
      {"day", "DAY"},
      {"day_of_month", "DAY"},
      {"hour", "HOUR"},
      {"minute", "MINUTE"},
      {"second", "SECOND"},
      {"day_of_week", "DOW"},
      {"dow", "DOW"},
      {"isodow", "ISODOW"},
      {"day_of_year", "DOY"},
      {"doy", "DOY"},
      {"week", "WEEK"},
      {"week_of_year", "WEEK"},
  };
  return kUnits;
}

// Functions that have no deterministic Flink SQL counterpart taking the same
// arguments.
const std::unordered_set<std::string>& unsupportedFunctions() {
  static const std::unordered_set<std::string> kFunctions{
      // Velox evaluates try() in the retry path of the fuzzer. Flink has no
      // such function.
      "try",
      // The arguments encode RETURNING and ON EMPTY / ON ERROR clauses.
      "json_value",
      // Not a Flink built-in function.
      "count_char",
  };
  return kFunctions;
}

class FlinkExprToSql {
 public:
  explicit FlinkExprToSql(memory::MemoryPool* pool) : pool_(pool) {}

  std::optional<std::string> toSql(const core::TypedExprPtr& expr) const {
    if (auto field =
            std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expr)) {
      if (!field->isInputColumn()) {
        return std::nullopt;
      }
      return quoteIdentifier(field->name());
    }
    if (auto constant =
            std::dynamic_pointer_cast<const core::ConstantTypedExpr>(expr)) {
      auto vector = constant->toConstantVector(pool_);
      return toFlinkLiteralSql(*vector, 0);
    }
    if (auto cast =
            std::dynamic_pointer_cast<const core::CastTypedExpr>(expr)) {
      auto input = toSql(cast->inputs()[0]);
      auto type = toFlinkTypeSql(cast->type());
      if (!input || !type) {
        return std::nullopt;
      }
      return fmt::format(
          "{}({} AS {})",
          cast->nullOnFailure() ? "TRY_CAST" : "CAST",
          *input,
          *type);
    }
    if (auto call =
            std::dynamic_pointer_cast<const core::CallTypedExpr>(expr)) {
      return toCallSql(*call);
    }
    return std::nullopt;
  }

 private:
  std::optional<std::vector<std::string>> toInputsSql(
      const core::CallTypedExpr& call) const {
    std::vector<std::string> inputs;
    for (const auto& input : call.inputs()) {
      auto sql = toSql(input);
      if (!sql) {
        return std::nullopt;
      }
      inputs.push_back(std::move(*sql));
    }
    return inputs;
  }

  // Narrows a BIGINT argument to INT, replacing values out of the INT range
  // with 'outOfRangeSql'. NULL stays NULL.
  static std::string toIntSql(
      const std::string& sql,
      const std::string& outOfRangeSql) {
    return fmt::format(
        "CASE WHEN {0} IS NULL THEN CAST(NULL AS INT) "
        "WHEN {0} BETWEEN -2147483648 AND 2147483647 THEN CAST({0} AS INT) "
        "ELSE {1} END",
        sql,
        outOfRangeSql);
  }

  std::optional<std::string> toCallSql(const core::CallTypedExpr& call) const {
    const auto& name = call.name();
    if (unsupportedFunctions().count(name)) {
      return std::nullopt;
    }
    if (name == "extract") {
      return toExtractSql(call);
    }

    auto maybeInputs = toInputsSql(call);
    if (!maybeInputs) {
      return std::nullopt;
    }
    auto& inputs = *maybeInputs;

    if (auto it = intArguments().find(name); it != intArguments().end()) {
      const auto& [index, outOfRangeSql] = it->second;
      if (index < inputs.size() &&
          call.inputs()[index]->type()->kind() == TypeKind::BIGINT) {
        inputs[index] = toIntSql(inputs[index], outOfRangeSql);
      }
    }

    if (auto it = binaryOperators().find(name); it != binaryOperators().end()) {
      VELOX_CHECK_EQ(inputs.size(), 2, "{} takes two arguments", name);
      return fmt::format("({} {} {})", inputs[0], it->second, inputs[1]);
    }
    if (auto it = postfixOperators().find(name);
        it != postfixOperators().end()) {
      VELOX_CHECK_EQ(inputs.size(), 1, "{} takes one argument", name);
      return fmt::format("({} {})", inputs[0], it->second);
    }
    if (name == "negate") {
      VELOX_CHECK_EQ(inputs.size(), 1);
      return fmt::format("(- {})", inputs[0]);
    }
    if (name == "not") {
      VELOX_CHECK_EQ(inputs.size(), 1);
      return fmt::format("(NOT {})", inputs[0]);
    }
    if (name == "and" || name == "or") {
      return fmt::format(
          "({})", folly::join(name == "and" ? " AND " : " OR ", inputs));
    }
    if (name == "like") {
      VELOX_CHECK(inputs.size() == 2 || inputs.size() == 3);
      return inputs.size() == 2
          ? fmt::format("({} LIKE {})", inputs[0], inputs[1])
          : fmt::format(
                "({} LIKE {} ESCAPE {})", inputs[0], inputs[1], inputs[2]);
    }
    if (name == "if" || name == "switch") {
      std::stringstream sql;
      sql << "CASE";
      size_t i = 0;
      for (; i + 1 < inputs.size(); i += 2) {
        sql << " WHEN " << inputs[i] << " THEN " << inputs[i + 1];
      }
      if (i < inputs.size()) {
        sql << " ELSE " << inputs[i];
      }
      sql << " END";
      return sql.str();
    }
    if (name == "trim" || name == "ltrim" || name == "rtrim" ||
        name == "trim_runtime" || name == "ltrim_runtime" ||
        name == "rtrim_runtime") {
      return toTrimSql(name, inputs);
    }

    std::string flinkName;
    if (auto it = renamedFunctions().find(name);
        it != renamedFunctions().end()) {
      flinkName = it->second;
    } else {
      flinkName = boost::algorithm::to_upper_copy(name);
    }
    return fmt::format("{}({})", flinkName, folly::join(", ", inputs));
  }

  // Velox trim functions take (string[, characters]). Flink SQL spells the
  // two-argument forms as TRIM(BOTH|LEADING|TRAILING characters FROM string).
  static std::optional<std::string> toTrimSql(
      const std::string& name,
      const std::vector<std::string>& inputs) {
    const bool leading = name.rfind("ltrim", 0) == 0;
    const bool trailing = name.rfind("rtrim", 0) == 0;
    if (inputs.size() == 1) {
      return fmt::format(
          "{}({})",
          leading ? "LTRIM" : (trailing ? "RTRIM" : "TRIM"),
          inputs[0]);
    }
    VELOX_CHECK_EQ(inputs.size(), 2);
    return fmt::format(
        "TRIM({} {} FROM {})",
        leading ? "LEADING" : (trailing ? "TRAILING" : "BOTH"),
        inputs[1],
        inputs[0]);
  }

  // The 'extract' UDF takes the unit as a string. Flink SQL requires a unit
  // keyword, so only constant units can be translated.
  std::optional<std::string> toExtractSql(
      const core::CallTypedExpr& call) const {
    VELOX_CHECK_EQ(call.inputs().size(), 2);
    auto unit = std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
        call.inputs()[0]);
    if (!unit || unit->type()->kind() != TypeKind::VARCHAR) {
      return std::nullopt;
    }
    auto unitVector = unit->toConstantVector(pool_);
    if (unitVector->isNullAt(0)) {
      return std::nullopt;
    }
    auto unitName = boost::algorithm::to_lower_copy(
        std::string(valueAt<StringView>(*unitVector, 0)));
    auto it = extractUnits().find(unitName);
    if (it == extractUnits().end()) {
      return std::nullopt;
    }
    auto source = toSql(call.inputs()[1]);
    if (!source) {
      return std::nullopt;
    }
    return fmt::format("EXTRACT({} FROM {})", it->second, *source);
  }

  memory::MemoryPool* const pool_;
};

} // namespace

std::optional<std::string> toFlinkTypeSql(const TypePtr& type) {
  if (type->isDate()) {
    return "DATE";
  }
  if (type->isDecimal()) {
    const auto [precision, scale] = getDecimalPrecisionScale(*type);
    return fmt::format("DECIMAL({}, {})", precision, scale);
  }
  if (type->isIntervalDayTime() || type->isIntervalYearMonth()) {
    // Interval literals and columns are not supported yet.
    return std::nullopt;
  }
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      return "BOOLEAN";
    case TypeKind::TINYINT:
      return "TINYINT";
    case TypeKind::SMALLINT:
      return "SMALLINT";
    case TypeKind::INTEGER:
      return "INT";
    case TypeKind::BIGINT:
      return "BIGINT";
    case TypeKind::REAL:
      return "FLOAT";
    case TypeKind::DOUBLE:
      return "DOUBLE";
    case TypeKind::VARCHAR:
      return "STRING";
    case TypeKind::VARBINARY:
      return "BYTES";
    case TypeKind::TIMESTAMP:
      return "TIMESTAMP(9)";
    default:
      return std::nullopt;
  }
}

std::optional<std::string> toFlinkLiteralSql(
    const BaseVector& vector,
    vector_size_t row) {
  auto type = toFlinkTypeSql(vector.type());
  if (!type) {
    return std::nullopt;
  }
  if (vector.isNullAt(row)) {
    return fmt::format("CAST(NULL AS {})", *type);
  }
  auto value = toLiteralValueSql(vector, row);
  if (!value) {
    return std::nullopt;
  }
  return fmt::format("CAST({} AS {})", *value, *type);
}

std::optional<std::string> toFlinkExprSql(
    const core::TypedExprPtr& expr,
    memory::MemoryPool* pool) {
  return FlinkExprToSql(pool).toSql(expr);
}

std::optional<std::string> toFlinkSql(
    const core::PlanNodePtr& plan,
    memory::MemoryPool* pool) {
  auto project = std::dynamic_pointer_cast<const core::ProjectNode>(plan);
  if (!project) {
    return std::nullopt;
  }
  auto values =
      std::dynamic_pointer_cast<const core::ValuesNode>(project->sources()[0]);
  if (!values || values->repeatTimes() != 1) {
    return std::nullopt;
  }

  // Inline the input rows. Every value is cast to its type so that the
  // column types do not depend on the literals.
  const auto& inputType = values->outputType();
  std::vector<std::string> rows;
  for (const auto& batch : values->values()) {
    for (vector_size_t row = 0; row < batch->size(); ++row) {
      std::vector<std::string> fields;
      for (column_index_t column = 0; column < batch->childrenSize();
           ++column) {
        auto literal = toFlinkLiteralSql(*batch->childAt(column), row);
        if (!literal) {
          return std::nullopt;
        }
        fields.push_back(std::move(*literal));
      }
      if (fields.empty()) {
        // VALUES rows cannot be empty.
        fields.emplace_back("CAST(NULL AS INT)");
      }
      rows.push_back(fmt::format("({})", folly::join(", ", fields)));
    }
  }
  if (rows.empty()) {
    return std::nullopt;
  }

  std::vector<std::string> columnNames;
  for (const auto& name : inputType->names()) {
    columnNames.push_back(quoteIdentifier(name));
  }
  if (columnNames.empty()) {
    columnNames.push_back(quoteIdentifier("__dummy"));
  }

  std::vector<std::string> projections;
  for (size_t i = 0; i < project->projections().size(); ++i) {
    auto sql = toFlinkExprSql(project->projections()[i], pool);
    if (!sql) {
      return std::nullopt;
    }
    projections.push_back(
        fmt::format("{} AS {}", *sql, quoteIdentifier(project->names()[i])));
  }

  return fmt::format(
      "SELECT {} FROM (VALUES {}) AS t({})",
      folly::join(", ", projections),
      folly::join(", ", rows),
      folly::join(", ", columnNames));
}

} // namespace facebook::velox::stateful::fuzzer
