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
#include "velox/functions/sparksql/VariantFunctions.h"

#include <memory>
#include <variant>

#include "velox/expression/VectorFunction.h"
#include "velox/functions/sparksql/SparkQueryConfig.h"
#include "velox/functions/sparksql/VariantJson.h"
#include "velox/functions/sparksql/VariantVectors.h"
#include "velox/type/variant/VariantBuilder.h"
#include "velox/type/variant/VariantRegistration.h"
#include "velox/type/variant/VariantType.h"

namespace facebook::velox::functions::sparksql {
namespace {

// parse_json(varchar) -> variant and try_parse_json(varchar) -> variant.
class ParseJsonFunction : public exec::VectorFunction {
 public:
  explicit ParseJsonFunction(bool failOnError) : failOnError_{failOnError} {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    const bool allowDuplicateKeys =
        SparkQueryConfig{context.execCtx()->queryCtx()->queryConfig()}
            .variantAllowDuplicateKeys();
    DecodedVector json(*args[0], rows);
    VariantWriter writer(rows.end(), context.pool());
    context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
      writer.setNull(row);
      if (json.isNullAt(row)) {
        return;
      }
      VariantBuilder builder(allowDuplicateKeys);
      try {
        const auto text = json.valueAt<StringView>(row);
        VariantJson::parse(text, builder);
        const auto metadata = builder.metadata();
        writer.set(row, metadata, builder.value());
      } catch (const VeloxUserError&) {
        if (failOnError_) {
          throw;
        }
      }
    });
    context.moveOrCopyResult(writer.result(), rows, result);
  }

 private:
  const bool failOnError_;
};

// is_variant_null(variant) -> boolean. Returns false for a SQL NULL.
class IsVariantNullFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    context.ensureWritable(rows, BOOLEAN(), result);
    auto* flatResult = result->asFlatVector<bool>();
    VariantReader input(*args[0], rows);
    context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
      if (input.isNullAt(row)) {
        flatResult->set(row, false);
        return;
      }
      const auto value = input.value(row);
      VELOX_USER_CHECK_GT(value.size(), 0, "Variant binary is malformed.");
      // A Variant null is a primitive with type id 0.
      flatResult->set(row, value.data()[0] == 0);
    });
  }
};

// Schema of a Variant value, in the types schema_of_variant reports.
struct VariantSchema {
  enum class Kind {
    kVoid,
    kBoolean,
    kBigint,
    kFloat,
    kDouble,
    kDecimal,
    kString,
    kBinary,
    kDate,
    kTimestampNtz,
    kTimestamp,
    kUuid,
    kObject,
    kArray,
    kVariant,
  };

  Kind kind{Kind::kVoid};
  uint8_t precision{0};
  uint8_t scale{0};
  // Fields of an object, sorted by name.
  std::vector<std::pair<std::string, VariantSchema>> fields;
  // Element of an array.
  std::shared_ptr<VariantSchema> element;

  bool operator==(const VariantSchema& other) const {
    if (kind != other.kind || precision != other.precision ||
        scale != other.scale || fields != other.fields) {
      return false;
    }
    if (kind == Kind::kArray) {
      return *element == *other.element;
    }
    return true;
  }
};

using SchemaKind = VariantSchema::Kind;

VariantSchema makeDecimalSchema(uint8_t precision, uint8_t scale) {
  VariantSchema schema;
  schema.kind = SchemaKind::kDecimal;
  schema.precision = precision;
  schema.scale = scale;
  return schema;
}

VariantSchema simpleSchema(SchemaKind kind) {
  VariantSchema schema;
  schema.kind = kind;
  return schema;
}

// Returns the precision and scale Spark assigns to a Variant decimal after
// stripping trailing zeros.
std::pair<uint8_t, uint8_t> decimalPrecisionScale(VariantDecimal decimal) {
  auto unscaled = decimal.unscaledValue;
  int32_t scale = decimal.scale;
  while (scale > 0 && unscaled % 10 == 0) {
    unscaled /= 10;
    --scale;
  }
  if (unscaled == 0) {
    return {1, 0};
  }
  auto magnitude = unscaled < 0 ? -static_cast<uint128_t>(unscaled) : unscaled;
  int32_t digits{0};
  while (magnitude > 0) {
    magnitude /= 10;
    ++digits;
  }
  return {
      static_cast<uint8_t>(std::max(digits, scale)),
      static_cast<uint8_t>(scale)};
}

bool isIntegralOrFloating(SchemaKind kind) {
  return kind == SchemaKind::kBigint || kind == SchemaKind::kFloat ||
      kind == SchemaKind::kDouble;
}

bool isDateTime(SchemaKind kind) {
  return kind == SchemaKind::kDate || kind == SchemaKind::kTimestampNtz ||
      kind == SchemaKind::kTimestamp;
}

// Merges two schemas like Spark's JsonInferSchema.compatibleType with VARIANT
// as the fallback.
VariantSchema mergeSchema(
    const VariantSchema& left,
    const VariantSchema& right) {
  if (left == right) {
    return left;
  }
  if (left.kind == SchemaKind::kVoid) {
    return right;
  }
  if (right.kind == SchemaKind::kVoid) {
    return left;
  }
  // BIGINT widens to DECIMAL(20, 0).
  if (left.kind == SchemaKind::kBigint && right.kind == SchemaKind::kDecimal) {
    return mergeSchema(makeDecimalSchema(20, 0), right);
  }
  if (left.kind == SchemaKind::kDecimal && right.kind == SchemaKind::kBigint) {
    return mergeSchema(left, makeDecimalSchema(20, 0));
  }
  // The kinds are declared in order of precedence.
  if (isIntegralOrFloating(left.kind) && isIntegralOrFloating(right.kind)) {
    return simpleSchema(std::max(left.kind, right.kind));
  }
  if (isDateTime(left.kind) && isDateTime(right.kind)) {
    return simpleSchema(std::max(left.kind, right.kind));
  }
  if ((left.kind == SchemaKind::kDecimal &&
       (right.kind == SchemaKind::kDouble ||
        right.kind == SchemaKind::kFloat)) ||
      (right.kind == SchemaKind::kDecimal &&
       (left.kind == SchemaKind::kDouble || left.kind == SchemaKind::kFloat))) {
    return simpleSchema(SchemaKind::kDouble);
  }
  if (left.kind == SchemaKind::kDecimal && right.kind == SchemaKind::kDecimal) {
    const auto scale = std::max(left.scale, right.scale);
    const auto range =
        std::max(left.precision - left.scale, right.precision - right.scale);
    if (range + scale > 38) {
      return simpleSchema(SchemaKind::kDouble);
    }
    return makeDecimalSchema(range + scale, scale);
  }
  if (left.kind == SchemaKind::kObject && right.kind == SchemaKind::kObject) {
    VariantSchema merged = simpleSchema(SchemaKind::kObject);
    size_t i{0};
    size_t j{0};
    while (i < left.fields.size() && j < right.fields.size()) {
      const auto& leftField = left.fields[i];
      const auto& rightField = right.fields[j];
      if (leftField.first == rightField.first) {
        merged.fields.emplace_back(
            leftField.first, mergeSchema(leftField.second, rightField.second));
        ++i;
        ++j;
      } else if (leftField.first < rightField.first) {
        merged.fields.push_back(leftField);
        ++i;
      } else {
        merged.fields.push_back(rightField);
        ++j;
      }
    }
    merged.fields.insert(
        merged.fields.end(), left.fields.begin() + i, left.fields.end());
    merged.fields.insert(
        merged.fields.end(), right.fields.begin() + j, right.fields.end());
    return merged;
  }
  if (left.kind == SchemaKind::kArray && right.kind == SchemaKind::kArray) {
    VariantSchema merged = simpleSchema(SchemaKind::kArray);
    merged.element = std::make_shared<VariantSchema>(
        mergeSchema(*left.element, *right.element));
    return merged;
  }
  return simpleSchema(SchemaKind::kVariant);
}

VariantSchema schemaOf(const VariantValue& value) {
  switch (value.kind()) {
    case VariantKind::kObject: {
      VariantSchema schema = simpleSchema(SchemaKind::kObject);
      for (uint32_t i = 0; i < value.numElements(); ++i) {
        std::string name{value.fieldNameAt(i)};
        VELOX_USER_CHECK(
            schema.fields.empty() || schema.fields.back().first < name,
            "Variant binary is malformed: object fields are not sorted.");
        schema.fields.emplace_back(
            std::move(name), schemaOf(value.fieldValueAt(i)));
      }
      return schema;
    }
    case VariantKind::kArray: {
      VariantSchema element;
      for (uint32_t i = 0; i < value.numElements(); ++i) {
        element = mergeSchema(element, schemaOf(value.elementAt(i)));
      }
      VariantSchema schema = simpleSchema(SchemaKind::kArray);
      schema.element = std::make_shared<VariantSchema>(std::move(element));
      return schema;
    }
    case VariantKind::kNull:
      return simpleSchema(SchemaKind::kVoid);
    case VariantKind::kBoolean:
      return simpleSchema(SchemaKind::kBoolean);
    case VariantKind::kInt8:
    case VariantKind::kInt16:
    case VariantKind::kInt32:
    case VariantKind::kInt64:
      return simpleSchema(SchemaKind::kBigint);
    case VariantKind::kString:
      return simpleSchema(SchemaKind::kString);
    case VariantKind::kDouble:
      return simpleSchema(SchemaKind::kDouble);
    case VariantKind::kFloat:
      return simpleSchema(SchemaKind::kFloat);
    case VariantKind::kDecimal4:
    case VariantKind::kDecimal8:
    case VariantKind::kDecimal16: {
      const auto [precision, scale] = decimalPrecisionScale(value.asDecimal());
      return makeDecimalSchema(precision, scale);
    }
    case VariantKind::kDate:
      return simpleSchema(SchemaKind::kDate);
    case VariantKind::kTimestamp:
      return simpleSchema(SchemaKind::kTimestamp);
    case VariantKind::kTimestampNtz:
      return simpleSchema(SchemaKind::kTimestampNtz);
    case VariantKind::kBinary:
      return simpleSchema(SchemaKind::kBinary);
    case VariantKind::kUuid:
      return simpleSchema(SchemaKind::kUuid);
    default:
      VELOX_USER_FAIL(
          "Variant value kind is not supported by Spark: {}", value.kind());
  }
}

// Quotes a field name with backticks unless it is a plain identifier.
void appendFieldName(std::string_view name, std::string& out) {
  bool plain = !name.empty() &&
      (std::isalpha(static_cast<unsigned char>(name[0])) || name[0] == '_');
  for (auto c : name) {
    plain = plain && (std::isalnum(static_cast<unsigned char>(c)) || c == '_');
  }
  if (plain) {
    out.append(name);
    return;
  }
  out.push_back('`');
  for (auto c : name) {
    if (c == '`') {
      out.push_back('`');
    }
    out.push_back(c);
  }
  out.push_back('`');
}

void printSchema(const VariantSchema& schema, std::string& out) {
  switch (schema.kind) {
    case SchemaKind::kVoid:
      out.append("VOID");
      break;
    case SchemaKind::kBoolean:
      out.append("BOOLEAN");
      break;
    case SchemaKind::kBigint:
      out.append("BIGINT");
      break;
    case SchemaKind::kFloat:
      out.append("FLOAT");
      break;
    case SchemaKind::kDouble:
      out.append("DOUBLE");
      break;
    case SchemaKind::kDecimal:
      out.append(fmt::format("DECIMAL({},{})", schema.precision, schema.scale));
      break;
    case SchemaKind::kString:
      out.append("STRING");
      break;
    case SchemaKind::kBinary:
      out.append("BINARY");
      break;
    case SchemaKind::kDate:
      out.append("DATE");
      break;
    case SchemaKind::kTimestampNtz:
      out.append("TIMESTAMP_NTZ");
      break;
    case SchemaKind::kTimestamp:
      out.append("TIMESTAMP");
      break;
    case SchemaKind::kUuid:
      out.append("UUID");
      break;
    case SchemaKind::kVariant:
      out.append("VARIANT");
      break;
    case SchemaKind::kObject:
      out.append("OBJECT<");
      for (size_t i = 0; i < schema.fields.size(); ++i) {
        if (i > 0) {
          out.append(", ");
        }
        appendFieldName(schema.fields[i].first, out);
        out.append(": ");
        printSchema(schema.fields[i].second, out);
      }
      out.push_back('>');
      break;
    case SchemaKind::kArray:
      out.append("ARRAY<");
      printSchema(*schema.element, out);
      out.push_back('>');
      break;
  }
}

// schema_of_variant(variant) -> varchar.
class SchemaOfVariantFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    context.ensureWritable(rows, VARCHAR(), result);
    auto* flatResult = result->asFlatVector<StringView>();
    VariantReader input(*args[0], rows);
    std::string schema;
    context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
      const auto metadataBytes = input.metadata(row);
      const auto valueBytes = input.value(row);
      VariantMetadata metadata(metadataBytes);
      VariantValue value(metadata, valueBytes);
      schema.clear();
      printSchema(schemaOf(value), schema);
      flatResult->set(row, StringView(schema));
    });
  }
};

// Returns true if Spark can convert 'type' to a Variant with
// to_variant_object.
bool isConvertibleToVariant(const TypePtr& type) {
  if (isVariantType(type)) {
    return true;
  }
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::HUGEINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
    case TypeKind::TIMESTAMP:
      return type->kind() != TypeKind::HUGEINT || type->isDecimal();
    case TypeKind::ARRAY:
      return isConvertibleToVariant(type->childAt(0));
    case TypeKind::MAP:
      return type->childAt(0)->kind() == TypeKind::VARCHAR &&
          isConvertibleToVariant(type->childAt(1));
    case TypeKind::ROW:
      for (const auto& child : type->asRow().children()) {
        if (!isConvertibleToVariant(child)) {
          return false;
        }
      }
      return true;
    default:
      return false;
  }
}

template <typename T>
T valueAt(const BaseVector& vector, vector_size_t index) {
  return vector.asUnchecked<SimpleVector<T>>()->valueAt(index);
}

// Appends row 'index' of 'vector' to 'builder' like Spark's castToVariant.
void appendToVariant(
    const BaseVector& vector,
    vector_size_t index,
    VariantBuilder& builder) {
  if (vector.isNullAt(index)) {
    builder.appendNull();
    return;
  }
  const auto& type = vector.type();
  const auto* base = vector.wrappedVector();
  const auto baseIndex = vector.wrappedIndex(index);

  if (isVariantType(type)) {
    const auto* row = base->asUnchecked<RowVector>();
    const auto& metadataVector = row->childAt(VariantType::kMetadataIndex);
    const auto& valueVector = row->childAt(VariantType::kValueIndex);
    VELOX_USER_CHECK(
        !metadataVector->isNullAt(baseIndex) &&
            !valueVector->isNullAt(baseIndex),
        "Variant binary is malformed.");
    const auto metadataBytes = valueAt<StringView>(*metadataVector, baseIndex);
    const auto valueBytes = valueAt<StringView>(*valueVector, baseIndex);
    VariantMetadata metadata(metadataBytes);
    builder.appendVariant(VariantValue(metadata, valueBytes));
    return;
  }

  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      builder.appendBoolean(valueAt<bool>(*base, baseIndex));
      return;
    case TypeKind::TINYINT:
      builder.appendLong(valueAt<int8_t>(*base, baseIndex));
      return;
    case TypeKind::SMALLINT:
      builder.appendLong(valueAt<int16_t>(*base, baseIndex));
      return;
    case TypeKind::INTEGER:
      if (type->isDate()) {
        builder.appendDate(valueAt<int32_t>(*base, baseIndex));
      } else {
        builder.appendLong(valueAt<int32_t>(*base, baseIndex));
      }
      return;
    case TypeKind::BIGINT:
      if (type->isDecimal()) {
        builder.appendDecimal(
            valueAt<int64_t>(*base, baseIndex),
            getDecimalPrecisionScale(*type).second);
      } else {
        builder.appendLong(valueAt<int64_t>(*base, baseIndex));
      }
      return;
    case TypeKind::HUGEINT:
      builder.appendDecimal(
          valueAt<int128_t>(*base, baseIndex),
          getDecimalPrecisionScale(*type).second);
      return;
    case TypeKind::REAL:
      builder.appendFloat(valueAt<float>(*base, baseIndex));
      return;
    case TypeKind::DOUBLE:
      builder.appendDouble(valueAt<double>(*base, baseIndex));
      return;
    case TypeKind::VARCHAR:
      builder.appendString(valueAt<StringView>(*base, baseIndex).str());
      return;
    case TypeKind::VARBINARY:
      builder.appendBinary(valueAt<StringView>(*base, baseIndex).str());
      return;
    case TypeKind::TIMESTAMP:
      builder.appendTimestamp(valueAt<Timestamp>(*base, baseIndex).toMicros());
      return;
    case TypeKind::ARRAY: {
      const auto* array = base->asUnchecked<ArrayVector>();
      const auto offset = array->offsetAt(baseIndex);
      const auto size = array->sizeAt(baseIndex);
      const auto start = builder.writePosition();
      std::vector<uint64_t> offsets;
      offsets.reserve(size);
      for (vector_size_t i = 0; i < size; ++i) {
        offsets.push_back(builder.writePosition() - start);
        appendToVariant(*array->elements(), offset + i, builder);
      }
      builder.finishArray(start, offsets);
      return;
    }
    case TypeKind::MAP: {
      const auto* map = base->asUnchecked<MapVector>();
      const auto offset = map->offsetAt(baseIndex);
      const auto size = map->sizeAt(baseIndex);
      const auto start = builder.writePosition();
      std::vector<VariantBuilder::Field> fields;
      fields.reserve(size);
      for (vector_size_t i = 0; i < size; ++i) {
        const auto key = valueAt<StringView>(
                             *map->mapKeys()->wrappedVector(),
                             map->mapKeys()->wrappedIndex(offset + i))
                             .str();
        fields.push_back(
            {builder.addKey(key), builder.writePosition() - start});
        appendToVariant(*map->mapValues(), offset + i, builder);
      }
      builder.finishObject(start, fields);
      return;
    }
    case TypeKind::ROW: {
      const auto* row = base->asUnchecked<RowVector>();
      const auto& rowType = type->asRow();
      const auto start = builder.writePosition();
      std::vector<VariantBuilder::Field> fields;
      fields.reserve(rowType.size());
      for (column_index_t i = 0; i < rowType.size(); ++i) {
        fields.push_back(
            {builder.addKey(rowType.nameOf(i)),
             builder.writePosition() - start});
        appendToVariant(*row->childAt(i), baseIndex, builder);
      }
      builder.finishObject(start, fields);
      return;
    }
    default:
      VELOX_UNREACHABLE();
  }
}

// to_variant_object(array | map | row) -> variant.
class ToVariantObjectFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    const auto& inputType = args[0]->type();
    const auto kind = inputType->kind();
    VELOX_USER_CHECK(
        (kind == TypeKind::ARRAY || kind == TypeKind::MAP ||
         kind == TypeKind::ROW) &&
            !isVariantType(inputType) && isConvertibleToVariant(inputType),
        "Cannot convert type to VARIANT: {}",
        inputType->toString());

    VariantWriter writer(rows.end(), context.pool());
    context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
      writer.setNull(row);
      if (args[0]->isNullAt(row)) {
        return;
      }
      VariantBuilder builder(/*allowDuplicateKeys=*/false);
      appendToVariant(*args[0], row, builder);
      const auto metadata = builder.metadata();
      writer.set(row, metadata, builder.value());
    });
    context.moveOrCopyResult(writer.result(), rows, result);
  }
};

} // namespace

void registerVariantFunctions(const std::string& prefix) {
  registerVariantType();

  const std::vector<exec::FunctionSignaturePtr> parseJsonSignatures{
      exec::FunctionSignatureBuilder()
          .returnType("variant")
          .argumentType("varchar")
          .build(),
  };
  exec::registerVectorFunction(
      prefix + "parse_json",
      parseJsonSignatures,
      std::make_unique<ParseJsonFunction>(/*failOnError=*/true));
  exec::registerVectorFunction(
      prefix + "try_parse_json",
      parseJsonSignatures,
      std::make_unique<ParseJsonFunction>(/*failOnError=*/false));

  exec::registerVectorFunction(
      prefix + "is_variant_null",
      {exec::FunctionSignatureBuilder()
           .returnType("boolean")
           .argumentType("variant")
           .build()},
      std::make_unique<IsVariantNullFunction>(),
      exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build());

  exec::registerVectorFunction(
      prefix + "schema_of_variant",
      {exec::FunctionSignatureBuilder()
           .returnType("varchar")
           .argumentType("variant")
           .build()},
      std::make_unique<SchemaOfVariantFunction>());

  exec::registerVectorFunction(
      prefix + "to_variant_object",
      {exec::FunctionSignatureBuilder()
           .typeVariable("T")
           .returnType("variant")
           .argumentType("T")
           .build()},
      std::make_unique<ToVariantObjectFunction>());
}

} // namespace facebook::velox::functions::sparksql
