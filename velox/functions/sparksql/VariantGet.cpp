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
#include "velox/functions/sparksql/VariantGet.h"

#include <optional>

#include "velox/common/base/Exceptions.h"
#include "velox/expression/EvalCtx.h"
#include "velox/expression/FieldReference.h"
#include "velox/expression/ScopedVarSetter.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/TimeUtils.h"
#include "velox/functions/sparksql/VariantJson.h"
#include "velox/functions/sparksql/VariantVectors.h"
#include "velox/functions/sparksql/specialforms/SparkCastExpr.h"
#include "velox/functions/sparksql/specialforms/SparkCastHooks.h"
#include "velox/type/variant/VariantBuilder.h"
#include "velox/type/variant/VariantType.h"

namespace facebook::velox::functions::sparksql {
namespace {

// One step of a variant_get path: an object field or an array index.
struct PathSegment {
  std::optional<std::string> key;
  int32_t index{0};
};

// Parses a path like Spark's VariantPathParser: '$' followed by '.name',
// '['name']', '["name"]' or '[123]'. Returns std::nullopt if invalid.
std::optional<std::vector<PathSegment>> parsePath(std::string_view path) {
  if (path.empty() || path[0] != '$') {
    return std::nullopt;
  }
  std::vector<PathSegment> segments;
  size_t pos{1};
  while (pos < path.size()) {
    if (path[pos] == '.') {
      const auto end = path.find_first_of(".[", pos + 1);
      const auto key = path.substr(
          pos + 1, end == std::string_view::npos ? end : end - pos - 1);
      if (key.empty()) {
        return std::nullopt;
      }
      segments.push_back({std::string(key), 0});
      pos += 1 + key.size();
      continue;
    }
    if (path[pos] != '[' || pos + 1 >= path.size()) {
      return std::nullopt;
    }
    const char quote = path[pos + 1];
    if (quote == '\'' || quote == '"') {
      const auto close = path.find(quote, pos + 2);
      if (close == std::string_view::npos || close + 1 >= path.size() ||
          path[close + 1] != ']') {
        return std::nullopt;
      }
      segments.push_back(
          {std::string(path.substr(pos + 2, close - pos - 2)), 0});
      pos = close + 2;
      continue;
    }
    const auto close = path.find(']', pos + 1);
    if (close == std::string_view::npos || close == pos + 1) {
      return std::nullopt;
    }
    int64_t index{0};
    for (auto i = pos + 1; i < close; ++i) {
      if (path[i] < '0' || path[i] > '9') {
        return std::nullopt;
      }
      index = index * 10 + (path[i] - '0');
      if (index > std::numeric_limits<int32_t>::max()) {
        return std::nullopt;
      }
    }
    segments.push_back({std::nullopt, static_cast<int32_t>(index)});
    pos = close + 1;
  }
  return segments;
}

std::optional<VariantValue> navigate(
    VariantValue value,
    const std::vector<PathSegment>& path) {
  for (const auto& segment : path) {
    if (segment.key.has_value()) {
      if (value.kind() != VariantKind::kObject) {
        return std::nullopt;
      }
      auto field = value.field(*segment.key);
      if (!field.has_value()) {
        return std::nullopt;
      }
      value = *field;
    } else {
      if (value.kind() != VariantKind::kArray ||
          static_cast<uint32_t>(segment.index) >= value.numElements()) {
        return std::nullopt;
      }
      value = value.elementAt(segment.index);
    }
  }
  return value;
}

// Returns the name Spark uses for 'type' in error messages.
std::string sparkTypeName(const TypePtr& type) {
  if (isVariantType(type)) {
    return "VARIANT";
  }
  if (type->isDecimal()) {
    const auto [precision, scale] = getDecimalPrecisionScale(*type);
    return fmt::format("DECIMAL({},{})", precision, scale);
  }
  if (type->isDate()) {
    return "DATE";
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
      return "BINARY";
    case TypeKind::TIMESTAMP:
      return "TIMESTAMP";
    case TypeKind::ARRAY:
      return fmt::format("ARRAY<{}>", sparkTypeName(type->childAt(0)));
    case TypeKind::MAP:
      return fmt::format(
          "MAP<{}, {}>",
          sparkTypeName(type->childAt(0)),
          sparkTypeName(type->childAt(1)));
    case TypeKind::ROW: {
      std::string out{"STRUCT<"};
      const auto& rowType = type->asRow();
      for (column_index_t i = 0; i < rowType.size(); ++i) {
        if (i > 0) {
          out.append(", ");
        }
        out.append(
            fmt::format(
                "{}: {}",
                rowType.nameOf(i),
                sparkTypeName(rowType.childAt(i))));
      }
      out.push_back('>');
      return out;
    }
    default:
      return type->toString();
  }
}

// Returns true if variant_get supports 'type' as the result type.
bool isSupportedType(const TypePtr& type) {
  if (isVariantType(type)) {
    return true;
  }
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
    case TypeKind::TIMESTAMP:
      return true;
    case TypeKind::HUGEINT:
      return type->isDecimal();
    case TypeKind::ARRAY:
      return isSupportedType(type->childAt(0));
    case TypeKind::MAP:
      return type->childAt(0)->kind() == TypeKind::VARCHAR &&
          isSupportedType(type->childAt(1));
    case TypeKind::ROW:
      for (const auto& child : type->asRow().children()) {
        if (!isSupportedType(child)) {
          return false;
        }
      }
      return true;
    default:
      return false;
  }
}

// Spark types of scalar Variant values, as inputs to a cast.
enum class SourceKind {
  kBoolean,
  kBigint,
  kVarchar,
  kDouble,
  kReal,
  kDecimal,
  kDate,
  kTimestamp,
  kTimestampNtz,
  kBinary,
};

bool isNumeric(SourceKind kind) {
  return kind == SourceKind::kBigint || kind == SourceKind::kDouble ||
      kind == SourceKind::kReal || kind == SourceKind::kDecimal;
}

// Mirrors Spark's Cast.canAnsiCast for the scalar types a Variant holds.
bool canAnsiCast(SourceKind source, const TypePtr& target) {
  if (target->isVarchar()) {
    return true;
  }
  if (target->isBoolean()) {
    return source == SourceKind::kBoolean || source == SourceKind::kVarchar ||
        isNumeric(source);
  }
  if (target->isDate()) {
    return source == SourceKind::kDate || source == SourceKind::kVarchar ||
        source == SourceKind::kTimestamp || source == SourceKind::kTimestampNtz;
  }
  if (target->isTimestamp()) {
    return source == SourceKind::kTimestamp || source == SourceKind::kVarchar ||
        source == SourceKind::kDate || source == SourceKind::kTimestampNtz ||
        isNumeric(source);
  }
  if (target->isVarbinary()) {
    return source == SourceKind::kBinary || source == SourceKind::kVarchar;
  }
  // Numeric targets.
  return isNumeric(source) || source == SourceKind::kVarchar ||
      source == SourceKind::kBoolean || source == SourceKind::kTimestamp;
}

constexpr int64_t kMicrosecondsInDay{86'400'000'000};

// A value to convert, and the top-level row it came from.
struct Item {
  std::optional<VariantValue> value;
  vector_size_t row;
};

class VariantGetFunction : public exec::VectorFunction {
 public:
  VariantGetFunction(bool failOnError, std::string name)
      : failOnError_{failOnError}, name_{std::move(name)} {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    const auto& config = context.execCtx()->queryCtx()->queryConfig();
    Converter converter{
        context,
        failOnError_,
        getTimeZoneFromConfig(config),
        std::make_shared<SparkCastHooks>(config, /*allowOverflow=*/false)};

    VariantReader input(*args[0], rows);
    DecodedVector paths(*args[1], rows);
    std::optional<std::vector<PathSegment>> constantPath;
    if (paths.isConstantMapping() && !paths.isNullAt(rows.begin())) {
      const auto path = paths.valueAt<StringView>(rows.begin());
      constantPath = parseValidPath(path);
    }

    // Variant views point into these bytes. Short strings live inside the
    // StringViews, so the vectors must not be resized.
    std::vector<StringView> metadataBytes(rows.end());
    std::vector<StringView> valueBytes(rows.end());
    std::vector<std::optional<VariantMetadata>> metadata(rows.end());
    std::vector<Item> items(rows.end());
    context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
      items[row].row = row;
      if (input.isNullAt(row) || paths.isNullAt(row)) {
        return;
      }
      std::vector<PathSegment> rowPath;
      if (!constantPath.has_value()) {
        const auto pathText = paths.valueAt<StringView>(row);
        rowPath = parseValidPath(pathText);
      }
      metadataBytes[row] = input.metadata(row);
      valueBytes[row] = input.value(row);
      metadata[row].emplace(metadataBytes[row]);
      items[row].value = navigate(
          VariantValue(*metadata[row], valueBytes[row]),
          constantPath.has_value() ? *constantPath : rowPath);
    });

    auto converted = converter.convert(items, outputType);
    context.moveOrCopyResult(converted, rows, result);
  }

 private:
  std::vector<PathSegment> parseValidPath(std::string_view path) const {
    auto parsed = parsePath(path);
    VELOX_USER_CHECK(
        parsed.has_value(),
        "The path is not a valid variant extraction path in {}: {}",
        name_,
        path);
    return std::move(*parsed);
  }

  // Converts Variant values to vectors of a given type.
  class Converter {
   public:
    Converter(
        exec::EvalCtx& context,
        bool failOnError,
        const tz::TimeZone* timeZone,
        std::shared_ptr<exec::CastHooks> castHooks)
        : context_{context},
          failOnError_{failOnError},
          timeZone_{timeZone},
          castHooks_{std::move(castHooks)} {}

    VectorPtr convert(const std::vector<Item>& items, const TypePtr& type) {
      if (isVariantType(type)) {
        return toVariant(items);
      }
      switch (type->kind()) {
        case TypeKind::ARRAY:
          return toArray(items, type);
        case TypeKind::MAP:
          return toMap(items, type);
        case TypeKind::ROW:
          return toRow(items, type);
        default:
          return toScalar(items, type);
      }
    }

   private:
    memory::MemoryPool* pool() const {
      return context_.pool();
    }

    // Returns true if 'item' holds a value other than a Variant null.
    bool hasValue(const Item& item) const {
      return item.value.has_value() && !item.value->isNull();
    }

    // Reports that 'item' cannot be converted to 'type'.
    void invalidCast(const Item& item, const TypePtr& type) {
      if (!failOnError_) {
        return;
      }
      std::string json;
      VariantJson::toJson(*item.value, timeZone_, json);
      context_.setStatus(
          item.row,
          Status::UserError(
              "The variant value cannot be cast into {}. Please use try_variant_get instead: {}",
              sparkTypeName(type),
              json));
    }

    VectorPtr toVariant(const std::vector<Item>& items) {
      VariantWriter writer(items.size(), pool());
      for (vector_size_t i = 0; i < items.size(); ++i) {
        // A Variant null stays a Variant null.
        if (!items[i].value.has_value()) {
          writer.setNull(i);
          continue;
        }
        VariantBuilder builder(/*allowDuplicateKeys=*/false);
        builder.appendVariant(*items[i].value);
        const auto metadata = builder.metadata();
        writer.set(i, metadata, builder.value());
      }
      return writer.result();
    }

    VectorPtr toArray(const std::vector<Item>& items, const TypePtr& type) {
      auto result = BaseVector::create<ArrayVector>(type, items.size(), pool());
      auto* offsets =
          result->mutableOffsets(items.size())->asMutable<vector_size_t>();
      auto* sizes =
          result->mutableSizes(items.size())->asMutable<vector_size_t>();
      std::vector<Item> elements;
      for (vector_size_t i = 0; i < items.size(); ++i) {
        offsets[i] = elements.size();
        sizes[i] = 0;
        const auto& item = items[i];
        if (!hasValue(item)) {
          result->setNull(i, true);
          continue;
        }
        if (item.value->kind() != VariantKind::kArray) {
          invalidCast(item, type);
          result->setNull(i, true);
          continue;
        }
        for (uint32_t j = 0; j < item.value->numElements(); ++j) {
          elements.push_back({item.value->elementAt(j), item.row});
        }
        sizes[i] = elements.size() - offsets[i];
      }
      result->setElements(convert(elements, type->childAt(0)));
      return result;
    }

    VectorPtr toMap(const std::vector<Item>& items, const TypePtr& type) {
      auto result = BaseVector::create<MapVector>(type, items.size(), pool());
      auto* offsets =
          result->mutableOffsets(items.size())->asMutable<vector_size_t>();
      auto* sizes =
          result->mutableSizes(items.size())->asMutable<vector_size_t>();
      std::vector<Item> values;
      std::vector<std::string_view> keys;
      for (vector_size_t i = 0; i < items.size(); ++i) {
        offsets[i] = values.size();
        sizes[i] = 0;
        const auto& item = items[i];
        if (!hasValue(item)) {
          result->setNull(i, true);
          continue;
        }
        if (item.value->kind() != VariantKind::kObject) {
          invalidCast(item, type);
          result->setNull(i, true);
          continue;
        }
        for (uint32_t j = 0; j < item.value->numElements(); ++j) {
          keys.push_back(item.value->fieldNameAt(j));
          values.push_back({item.value->fieldValueAt(j), item.row});
        }
        sizes[i] = values.size() - offsets[i];
      }
      auto keyVector = BaseVector::create<FlatVector<StringView>>(
          VARCHAR(), keys.size(), pool());
      for (size_t i = 0; i < keys.size(); ++i) {
        keyVector->set(i, StringView(keys[i]));
      }
      result->setKeysAndValues(keyVector, convert(values, type->childAt(1)));
      return result;
    }

    VectorPtr toRow(const std::vector<Item>& items, const TypePtr& type) {
      const auto& rowType = type->asRow();
      std::vector<std::vector<Item>> fields(rowType.size());
      for (auto& field : fields) {
        field.resize(items.size());
      }
      auto nulls = allocateNulls(items.size(), pool());
      auto* rawNulls = nulls->asMutable<uint64_t>();
      for (vector_size_t i = 0; i < items.size(); ++i) {
        const auto& item = items[i];
        for (auto& field : fields) {
          field[i].row = item.row;
        }
        if (!hasValue(item)) {
          bits::setNull(rawNulls, i);
          continue;
        }
        if (item.value->kind() != VariantKind::kObject) {
          invalidCast(item, type);
          bits::setNull(rawNulls, i);
          continue;
        }
        for (column_index_t j = 0; j < rowType.size(); ++j) {
          fields[j][i].value = item.value->field(rowType.nameOf(j));
        }
      }
      std::vector<VectorPtr> children;
      children.reserve(rowType.size());
      for (column_index_t j = 0; j < rowType.size(); ++j) {
        children.push_back(convert(fields[j], rowType.childAt(j)));
      }
      return std::make_shared<RowVector>(
          pool(), type, std::move(nulls), items.size(), std::move(children));
    }

    // Values of one Spark scalar type, collected for a single cast.
    struct CastGroup {
      SourceKind kind;
      // Scale of decimal values after stripping trailing zeros.
      uint8_t scale{0};
      // Positions of the values in the items and the result.
      std::vector<vector_size_t> targets;
    };

    VectorPtr toScalar(const std::vector<Item>& items, const TypePtr& type) {
      auto result = BaseVector::create(type, items.size(), pool());
      for (vector_size_t i = 0; i < items.size(); ++i) {
        result->setNull(i, true);
      }

      // Groups by source type, and decimals also by scale.
      folly::F14FastMap<int32_t, CastGroup> groups;
      auto addToGroup = [&](SourceKind kind, vector_size_t i, uint8_t scale) {
        if (!canAnsiCast(kind, type)) {
          invalidCast(items[i], type);
          return;
        }
        auto& group = groups[static_cast<int32_t>(kind) * 64 + scale];
        group.kind = kind;
        group.scale = scale;
        group.targets.push_back(i);
      };

      for (vector_size_t i = 0; i < items.size(); ++i) {
        const auto& item = items[i];
        if (!hasValue(item)) {
          continue;
        }
        const auto& value = *item.value;
        switch (value.kind()) {
          case VariantKind::kObject:
          case VariantKind::kArray: {
            if (!type->isVarchar()) {
              invalidCast(item, type);
              break;
            }
            std::string json;
            VariantJson::toJson(value, timeZone_, json);
            result->asFlatVector<StringView>()->set(i, StringView(json));
            break;
          }
          case VariantKind::kUuid: {
            if (!type->isVarchar()) {
              invalidCast(item, type);
              break;
            }
            std::string uuid;
            VariantJson::appendUuid(value.asUuid(), uuid);
            result->asFlatVector<StringView>()->set(i, StringView(uuid));
            break;
          }
          case VariantKind::kBoolean:
            addToGroup(SourceKind::kBoolean, i, 0);
            break;
          case VariantKind::kInt8:
          case VariantKind::kInt16:
          case VariantKind::kInt32:
          case VariantKind::kInt64: {
            // Spark reads integers as seconds when casting to a timestamp.
            if (type->isTimestamp()) {
              int64_t micros;
              if (__builtin_mul_overflow(
                      value.asInteger(), int64_t{1'000'000}, &micros)) {
                invalidCast(item, type);
              } else {
                result->asFlatVector<Timestamp>()->set(
                    i, Timestamp::fromMicros(micros));
              }
              break;
            }
            addToGroup(SourceKind::kBigint, i, 0);
            break;
          }
          case VariantKind::kString:
            addToGroup(SourceKind::kVarchar, i, 0);
            break;
          case VariantKind::kDouble:
            addToGroup(SourceKind::kDouble, i, 0);
            break;
          case VariantKind::kFloat:
            addToGroup(SourceKind::kReal, i, 0);
            break;
          case VariantKind::kDecimal4:
          case VariantKind::kDecimal8:
          case VariantKind::kDecimal16: {
            const auto decimal = stripTrailingZeros(value.asDecimal());
            if (type->isTimestamp()) {
              const auto micros = decimalToMicros(decimal);
              if (micros.has_value()) {
                result->asFlatVector<Timestamp>()->set(
                    i, Timestamp::fromMicros(*micros));
              } else {
                invalidCast(item, type);
              }
              break;
            }
            addToGroup(SourceKind::kDecimal, i, decimal.scale);
            break;
          }
          case VariantKind::kDate:
            addToGroup(SourceKind::kDate, i, 0);
            break;
          case VariantKind::kTimestamp:
            addToGroup(SourceKind::kTimestamp, i, 0);
            break;
          case VariantKind::kTimestampNtz:
            fromTimestampNtz(value.asTimestamp(), item, type, *result, i);
            break;
          case VariantKind::kBinary:
            addToGroup(SourceKind::kBinary, i, 0);
            break;
          default:
            context_.setStatus(
                item.row,
                Status::UserError(
                    "Variant value kind is not supported by Spark: {}",
                    VariantKindName::toName(value.kind())));
            break;
        }
      }

      for (auto& [_, group] : groups) {
        castGroup(group, items, type, *result);
      }
      return result;
    }

    // Spark reads Variant decimals with trailing fractional zeros removed.
    static VariantDecimal stripTrailingZeros(VariantDecimal decimal) {
      while (decimal.scale > 0 && decimal.unscaledValue % 10 == 0) {
        decimal.unscaledValue /= 10;
        --decimal.scale;
      }
      return decimal;
    }

    // Returns a flat vector of the source type of 'group' holding its values.
    VectorPtr makeSourceVector(
        const CastGroup& group,
        const std::vector<Item>& items) {
      const auto size = group.targets.size();
      auto fill = [&](const TypePtr& sourceType, auto read) {
        using T = decltype(read(std::declval<const VariantValue&>()));
        auto vector =
            BaseVector::create<FlatVector<T>>(sourceType, size, pool());
        for (vector_size_t i = 0; i < size; ++i) {
          vector->set(i, read(*items[group.targets[i]].value));
        }
        return vector;
      };
      switch (group.kind) {
        case SourceKind::kBoolean:
          return fill(BOOLEAN(), [](const auto& v) { return v.asBoolean(); });
        case SourceKind::kBigint:
          return fill(BIGINT(), [](const auto& v) { return v.asInteger(); });
        case SourceKind::kVarchar:
          return fill(VARCHAR(), [](const auto& v) {
            return StringView(v.asString());
          });
        case SourceKind::kDouble:
          return fill(DOUBLE(), [](const auto& v) { return v.asDouble(); });
        case SourceKind::kReal:
          return fill(REAL(), [](const auto& v) { return v.asFloat(); });
        case SourceKind::kDecimal:
          return fill(DECIMAL(38, group.scale), [](const auto& v) {
            return stripTrailingZeros(v.asDecimal()).unscaledValue;
          });
        case SourceKind::kDate:
          return fill(DATE(), [](const auto& v) { return v.asDate(); });
        case SourceKind::kTimestamp:
          return fill(TIMESTAMP(), [](const auto& v) {
            return Timestamp::fromMicros(v.asTimestamp());
          });
        case SourceKind::kBinary:
          return fill(VARBINARY(), [](const auto& v) {
            return StringView(v.asBinary());
          });
        case SourceKind::kTimestampNtz:
          break;
      }
      VELOX_UNREACHABLE();
    }

    // Converts a decimal number of seconds to microseconds, truncating toward
    // zero. Returns std::nullopt on overflow.
    static std::optional<int64_t> decimalToMicros(VariantDecimal decimal) {
      int128_t micros = decimal.unscaledValue;
      if (decimal.scale >= 6) {
        for (int i = 6; i < decimal.scale; ++i) {
          micros /= 10;
        }
      } else {
        for (int i = decimal.scale; i < 6; ++i) {
          if (__builtin_mul_overflow(micros, 10, &micros)) {
            return std::nullopt;
          }
        }
      }
      if (micros < std::numeric_limits<int64_t>::min() ||
          micros > std::numeric_limits<int64_t>::max()) {
        return std::nullopt;
      }
      return static_cast<int64_t>(micros);
    }

    // Converts a timestamp without time zone. Velox has no such type, so the
    // conversions Spark allows are done here.
    void fromTimestampNtz(
        int64_t micros,
        const Item& item,
        const TypePtr& type,
        BaseVector& result,
        vector_size_t target) {
      if (type->isVarchar()) {
        std::string text;
        VariantJson::appendTimestamp(micros, nullptr, text);
        result.asFlatVector<StringView>()->set(target, StringView(text));
      } else if (type->isDate()) {
        const auto days = micros >= 0
            ? micros / kMicrosecondsInDay
            : (micros - kMicrosecondsInDay + 1) / kMicrosecondsInDay;
        result.asFlatVector<int32_t>()->set(target, days);
      } else if (type->isTimestamp()) {
        auto timestamp = Timestamp::fromMicros(micros);
        if (timeZone_ != nullptr) {
          timestamp.toGMT(*timeZone_);
        }
        result.asFlatVector<Timestamp>()->set(target, timestamp);
      } else {
        invalidCast(item, type);
      }
    }

    // Casts the values of 'group' to 'type' with Spark try_cast semantics and
    // stores them in 'result'.
    void castGroup(
        CastGroup& group,
        const std::vector<Item>& items,
        const TypePtr& type,
        BaseVector& result) {
      const auto size = group.targets.size();
      const auto values = makeSourceVector(group, items);
      VectorPtr casted;
      {
        exec::EvalCtx castContext(context_.execCtx());
        ScopedVarSetter throwOnError{castContext.mutableThrowOnError(), false};
        ScopedVarSetter captureErrorDetails{
            castContext.mutableCaptureErrorDetails(), false};
        ScopedThreadSkipErrorDetails skipErrorDetails(true);

        SparkCastExpr cast(
            type,
            std::make_shared<exec::FieldReference>(
                values->type(), std::vector<exec::ExprPtr>{}, "c0"),
            /*trackCpuUsage=*/false,
            /*isTryCast=*/true,
            castHooks_);
        const SelectivityVector rows(size);
        cast.apply(rows, values, castContext, values->type(), type, casted);
      }

      for (vector_size_t i = 0; i < size; ++i) {
        const auto target = group.targets[i];
        if (casted->isNullAt(i)) {
          invalidCast(items[target], type);
        } else {
          result.copy(casted.get(), target, i, 1);
        }
      }
    }

    exec::EvalCtx& context_;
    const bool failOnError_;
    const tz::TimeZone* const timeZone_;
    const std::shared_ptr<exec::CastHooks> castHooks_;
  };

  const bool failOnError_;
  const std::string name_;
};

} // namespace

TypePtr VariantGetCallToSpecialForm::resolveType(
    const std::vector<TypePtr>& /*argTypes*/) {
  VELOX_FAIL("variant_get does not support type resolution.");
}

exec::ExprPtr VariantGetCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<exec::ExprPtr>&& args,
    bool trackCpuUsage,
    const core::QueryConfig& /*config*/) {
  const auto* name = failOnError_ ? kVariantGet : kTryVariantGet;
  VELOX_USER_CHECK_EQ(args.size(), 2, "{} expects two arguments.", name);
  VELOX_USER_CHECK(
      isVariantType(args[0]->type()),
      "The first argument of {} must be VARIANT: {}",
      name,
      args[0]->type()->toString());
  VELOX_USER_CHECK(
      args[1]->type()->isVarchar(),
      "The second argument of {} must be VARCHAR: {}",
      name,
      args[1]->type()->toString());
  VELOX_USER_CHECK(
      isSupportedType(type),
      "Unsupported result type for {}: {}",
      name,
      type->toString());

  return std::make_shared<exec::Expr>(
      type,
      std::move(args),
      std::make_shared<VariantGetFunction>(failOnError_, name),
      exec::VectorFunctionMetadata{},
      name,
      trackCpuUsage);
}

} // namespace facebook::velox::functions::sparksql
