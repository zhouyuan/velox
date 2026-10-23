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
#include "velox/type/variant/VariantEncoding.h"

#include <cstring>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox {
namespace {

// Builders below follow the layout in
// https://github.com/apache/parquet-format/blob/master/VariantEncoding.md.

void appendLittleEndian(std::string& out, uint64_t value, uint8_t size) {
  for (uint8_t i = 0; i < size; ++i) {
    out.push_back(static_cast<char>((value >> (8 * i)) & 0xFF));
  }
}

template <typename T>
std::string bytesOf(T value) {
  std::string out(sizeof(T), '\0');
  std::memcpy(out.data(), &value, sizeof(T));
  return out;
}

std::string makeMetadata(
    const std::vector<std::string>& keys,
    bool sorted,
    uint8_t offsetSize = 1) {
  std::string out;
  out.push_back(
      static_cast<char>(1 | (sorted ? 1 << 4 : 0) | ((offsetSize - 1) << 6)));
  appendLittleEndian(out, keys.size(), offsetSize);
  uint64_t offset{0};
  appendLittleEndian(out, offset, offsetSize);
  for (const auto& key : keys) {
    offset += key.size();
    appendLittleEndian(out, offset, offsetSize);
  }
  for (const auto& key : keys) {
    out += key;
  }
  return out;
}

std::string primitive(uint8_t typeId, std::string_view payload = {}) {
  std::string out(1, static_cast<char>(typeId << 2));
  out += payload;
  return out;
}

std::string shortString(std::string_view value) {
  std::string out(1, static_cast<char>((value.size() << 2) | 1));
  out += value;
  return out;
}

std::string longString(std::string_view value) {
  return primitive(16, bytesOf<uint32_t>(value.size()) + std::string(value));
}

struct ContainerLayout {
  uint8_t fieldIdSize{1};
  uint8_t offsetSize{1};
  bool isLarge{false};
};

// Encodes an object from (field id, encoded value) pairs that are already
// ordered by field name. Values are stored in 'valueOrder', which defaults to
// field order.
std::string makeObject(
    const std::vector<std::pair<uint32_t, std::string>>& fields,
    ContainerLayout layout = {},
    std::vector<size_t> valueOrder = {}) {
  if (valueOrder.empty()) {
    for (size_t i = 0; i < fields.size(); ++i) {
      valueOrder.push_back(i);
    }
  }
  std::vector<uint64_t> offsets(fields.size());
  std::string values;
  for (auto index : valueOrder) {
    offsets[index] = values.size();
    values += fields[index].second;
  }

  std::string out;
  out.push_back(
      static_cast<char>(
          2 |
          (((layout.offsetSize - 1) | ((layout.fieldIdSize - 1) << 2) |
            (layout.isLarge ? 1 << 4 : 0))
           << 2)));
  appendLittleEndian(out, fields.size(), layout.isLarge ? 4 : 1);
  for (const auto& [id, _] : fields) {
    appendLittleEndian(out, id, layout.fieldIdSize);
  }
  for (auto offset : offsets) {
    appendLittleEndian(out, offset, layout.offsetSize);
  }
  appendLittleEndian(out, values.size(), layout.offsetSize);
  return out + values;
}

std::string makeArray(
    const std::vector<std::string>& elements,
    ContainerLayout layout = {}) {
  std::string out;
  out.push_back(
      static_cast<char>(
          3 |
          (((layout.offsetSize - 1) | (layout.isLarge ? 1 << 2 : 0)) << 2)));
  appendLittleEndian(out, elements.size(), layout.isLarge ? 4 : 1);
  uint64_t offset{0};
  for (const auto& element : elements) {
    appendLittleEndian(out, offset, layout.offsetSize);
    offset += element.size();
  }
  appendLittleEndian(out, offset, layout.offsetSize);
  for (const auto& element : elements) {
    out += element;
  }
  return out;
}

class VariantEncodingTest : public testing::Test {
 protected:
  const std::string emptyMetadataBytes_{makeMetadata({}, true)};
  const VariantMetadata emptyMetadata_{emptyMetadataBytes_};

  // VariantValue does not own 'bytes'. A value built from a temporary is only
  // valid within the same full expression.
  VariantValue value(const std::string& bytes) const {
    return VariantValue(emptyMetadata_, bytes);
  }
};

TEST_F(VariantEncodingTest, metadata) {
  const auto bytes = makeMetadata({"a", "bc", "def"}, true);
  VariantMetadata metadata(bytes);
  EXPECT_EQ(metadata.dictionarySize(), 3);
  EXPECT_TRUE(metadata.sortedStrings());
  EXPECT_EQ(metadata.keyAt(0), "a");
  EXPECT_EQ(metadata.keyAt(1), "bc");
  EXPECT_EQ(metadata.keyAt(2), "def");
  VELOX_ASSERT_USER_THROW(
      metadata.keyAt(3), "Variant field id is out of range.");

  EXPECT_EQ(emptyMetadata_.dictionarySize(), 0);

  const auto wideBytes = makeMetadata({"x", "y"}, false, 4);
  VariantMetadata wide(wideBytes);
  EXPECT_FALSE(wide.sortedStrings());
  EXPECT_EQ(wide.keyAt(1), "y");
}

TEST_F(VariantEncodingTest, malformedMetadata) {
  VELOX_ASSERT_USER_THROW(
      VariantMetadata(""), "Variant metadata must not be empty.");

  // Version 2.
  VELOX_ASSERT_USER_THROW(
      VariantMetadata(std::string("\x02\x00\x00", 3)),
      "Unsupported Variant metadata version.");

  // Dictionary size 5 with no offsets.
  VELOX_ASSERT_USER_THROW(
      VariantMetadata(std::string("\x01\x05", 2)),
      "Variant bytes are truncated.");

  // Offset of the only key points past the end.
  const auto bytes = std::string(
      "\x01\x01\x00\x09"
      "ab",
      6);
  VariantMetadata metadata(bytes);
  VELOX_ASSERT_USER_THROW(metadata.keyAt(0), "Variant bytes are truncated.");
}

TEST_F(VariantEncodingTest, primitives) {
  EXPECT_TRUE(value(primitive(0)).isNull());
  EXPECT_TRUE(value(primitive(1)).asBoolean());
  EXPECT_FALSE(value(primitive(2)).asBoolean());

  EXPECT_EQ(value(primitive(3, bytesOf<int8_t>(-5))).asInteger(), -5);
  EXPECT_EQ(value(primitive(4, bytesOf<int16_t>(-300))).asInteger(), -300);
  EXPECT_EQ(
      value(primitive(5, bytesOf<int32_t>(-70'000))).asInteger(), -70'000);
  EXPECT_EQ(
      value(primitive(6, bytesOf<int64_t>(-5'000'000'000))).asInteger(),
      -5'000'000'000);
  EXPECT_EQ(value(primitive(3, bytesOf<int8_t>(1))).kind(), VariantKind::kInt8);
  EXPECT_EQ(
      value(primitive(6, bytesOf<int64_t>(1))).kind(), VariantKind::kInt64);

  EXPECT_EQ(value(primitive(14, bytesOf<float>(1.5f))).asFloat(), 1.5f);
  EXPECT_EQ(value(primitive(7, bytesOf<double>(-2.25))).asDouble(), -2.25);

  EXPECT_EQ(value(primitive(11, bytesOf<int32_t>(19'000))).asDate(), 19'000);
  for (uint8_t typeId : {12, 13, 17, 18, 19}) {
    EXPECT_EQ(
        value(primitive(typeId, bytesOf<int64_t>(1'234'567))).asTimestamp(),
        1'234'567);
  }
  EXPECT_EQ(
      value(primitive(12, bytesOf<int64_t>(0))).kind(),
      VariantKind::kTimestamp);
  EXPECT_EQ(
      value(primitive(19, bytesOf<int64_t>(0))).kind(),
      VariantKind::kTimestampNtzNanos);
}

TEST_F(VariantEncodingTest, decimals) {
  const auto decimal4Bytes = primitive(8, "\x02" + bytesOf<int32_t>(-12'345));
  auto decimal4 = value(decimal4Bytes);
  EXPECT_EQ(decimal4.kind(), VariantKind::kDecimal4);
  EXPECT_EQ(decimal4.asDecimal().unscaledValue, -12'345);
  EXPECT_EQ(decimal4.asDecimal().scale, 2);

  const auto decimal8Bytes =
      primitive(9, "\x0A" + bytesOf<int64_t>(123'456'789'012));
  auto decimal8 = value(decimal8Bytes);
  EXPECT_EQ(decimal8.asDecimal().unscaledValue, 123'456'789'012);
  EXPECT_EQ(decimal8.asDecimal().scale, 10);

  const int128_t large = HugeInt::build(0x1234, 0x5678);
  const auto decimal16Bytes = primitive(10, "\x26" + bytesOf<int128_t>(-large));
  auto decimal16 = value(decimal16Bytes);
  EXPECT_EQ(decimal16.asDecimal().unscaledValue, -large);
  EXPECT_EQ(decimal16.asDecimal().scale, 38);
}

TEST_F(VariantEncodingTest, stringsAndBinaries) {
  EXPECT_EQ(value(shortString("")).asString(), "");
  EXPECT_EQ(value(shortString("hello")).asString(), "hello");

  const std::string longValue(100, 'x');
  EXPECT_EQ(value(longString(longValue)).asString(), longValue);
  EXPECT_EQ(value(longString("short")).asString(), "short");

  const std::string binary("\x00\x01\x02", 3);
  EXPECT_EQ(
      value(primitive(15, bytesOf<uint32_t>(3) + binary)).asBinary(), binary);

  const std::string uuid(
      "\x55\x0e\x84\x00\xe2\x9b\x41\xd4\xa7\x16\x44\x66\x55\x44\x00\x00", 16);
  const auto uuidBytes = primitive(20, uuid);
  auto uuidValue = value(uuidBytes);
  EXPECT_EQ(uuidValue.kind(), VariantKind::kUuid);
  EXPECT_EQ(uuidValue.asUuid(), uuid);
}

TEST_F(VariantEncodingTest, kindMismatch) {
  const auto stringBytes = shortString("abc");
  auto stringValue = value(stringBytes);
  VELOX_ASSERT_USER_THROW(
      stringValue.asInteger(), "Variant value is not an integer: STRING");
  VELOX_ASSERT_USER_THROW(
      stringValue.asBoolean(), "Unexpected Variant value kind.");
  VELOX_ASSERT_USER_THROW(
      stringValue.asDecimal(), "Variant value is not a decimal: STRING");
  VELOX_ASSERT_USER_THROW(
      stringValue.numElements(),
      "Variant value is not an object or array: STRING");
  VELOX_ASSERT_USER_THROW(
      value(primitive(0)).asTimestamp(),
      "Variant value is not a time or timestamp: NULL");
  VELOX_ASSERT_USER_THROW(
      value(makeArray({})).field("a"), "Unexpected Variant value kind.");
}

TEST_F(VariantEncodingTest, malformedValue) {
  VELOX_ASSERT_USER_THROW(value(""), "Variant value must not be empty.");
  VELOX_ASSERT_USER_THROW(
      value(primitive(21)), "Unsupported Variant primitive type: 21");
  VELOX_ASSERT_USER_THROW(
      value(primitive(6, "\x01\x02")), "Variant bytes are truncated.");
  VELOX_ASSERT_USER_THROW(
      value(longString("abc").substr(0, 6)), "Variant bytes are truncated.");
  VELOX_ASSERT_USER_THROW(
      value(shortString("abc").substr(0, 3)), "Variant bytes are truncated.");

  // Array claims 3 elements but has no offsets.
  VELOX_ASSERT_USER_THROW(
      value(std::string("\x03\x03", 2)), "Variant bytes are truncated.");

  // Array whose last offset points past the end.
  auto array = makeArray({primitive(1)});
  array[3] = 9;
  VELOX_ASSERT_USER_THROW(value(array), "Variant bytes are truncated.");

  // Array whose element offset is past the values region.
  array = makeArray({primitive(1)});
  array[2] = 1;
  VELOX_ASSERT_USER_THROW(
      value(array).elementAt(0), "Variant child offset is out of range.");
}

TEST_F(VariantEncodingTest, object) {
  // Dictionary is unsorted; object fields are still ordered by name.
  const auto metadataBytes = makeMetadata({"name", "age", "city"}, false);
  VariantMetadata metadata(metadataBytes);
  const auto bytes = makeObject({
      {1, primitive(3, bytesOf<int8_t>(30))},
      {2, primitive(0)},
      {0, shortString("Alice")},
  });
  VariantValue object(metadata, bytes);

  EXPECT_EQ(object.kind(), VariantKind::kObject);
  ASSERT_EQ(object.numElements(), 3);
  std::vector<std::string_view> names;
  for (uint32_t i = 0; i < object.numElements(); ++i) {
    names.push_back(object.fieldNameAt(i));
  }
  EXPECT_THAT(names, testing::ElementsAre("age", "city", "name"));
  EXPECT_EQ(object.fieldValueAt(0).asInteger(), 30);
  EXPECT_TRUE(object.fieldValueAt(1).isNull());

  EXPECT_EQ(object.field("name")->asString(), "Alice");
  EXPECT_EQ(object.field("age")->asInteger(), 30);
  EXPECT_TRUE(object.field("city")->isNull());
  EXPECT_FALSE(object.field("zip").has_value());
  EXPECT_FALSE(object.field("").has_value());
  VELOX_ASSERT_USER_THROW(object.fieldNameAt(3), "");

  const auto emptyBytes = makeObject({});
  auto empty = value(emptyBytes);
  EXPECT_EQ(empty.numElements(), 0);
  EXPECT_FALSE(empty.field("a").has_value());
}

TEST_F(VariantEncodingTest, objectValuesOutOfOrder) {
  // The encoding allows field values to be stored in any order.
  const auto metadataBytes = makeMetadata({"a", "b", "c"}, true);
  VariantMetadata metadata(metadataBytes);
  const auto bytes = makeObject(
      {
          {0, shortString("first")},
          {1, longString("second")},
          {2, primitive(5, bytesOf<int32_t>(3))},
      },
      {},
      {2, 0, 1});
  VariantValue object(metadata, bytes);
  EXPECT_EQ(object.field("a")->asString(), "first");
  EXPECT_EQ(object.field("b")->asString(), "second");
  EXPECT_EQ(object.field("c")->asInteger(), 3);
}

TEST_F(VariantEncodingTest, largeObject) {
  // 300 fields need 2-byte field ids and is_large.
  std::vector<std::string> keys;
  for (int i = 0; i < 300; ++i) {
    keys.push_back(fmt::format("k{:03}", i));
  }
  const auto metadataBytes = makeMetadata(keys, true, 2);
  VariantMetadata metadata(metadataBytes);

  std::vector<std::pair<uint32_t, std::string>> fields;
  for (uint32_t i = 0; i < keys.size(); ++i) {
    fields.emplace_back(i, primitive(4, bytesOf<int16_t>(i)));
  }
  const auto bytes =
      makeObject(fields, {.fieldIdSize = 2, .offsetSize = 2, .isLarge = true});
  VariantValue object(metadata, bytes);

  EXPECT_EQ(object.numElements(), 300);
  EXPECT_EQ(object.field("k000")->asInteger(), 0);
  EXPECT_EQ(object.field("k257")->asInteger(), 257);
  EXPECT_EQ(object.field("k299")->asInteger(), 299);
  EXPECT_FALSE(object.field("k300").has_value());
  EXPECT_EQ(object.fieldNameAt(123), "k123");
}

TEST_F(VariantEncodingTest, array) {
  const auto arrayBytes = makeArray({
      primitive(3, bytesOf<int8_t>(1)),
      shortString("two"),
      primitive(0),
      makeArray({primitive(1)}),
  });
  auto array = value(arrayBytes);
  EXPECT_EQ(array.kind(), VariantKind::kArray);
  ASSERT_EQ(array.numElements(), 4);
  EXPECT_EQ(array.elementAt(0).asInteger(), 1);
  EXPECT_EQ(array.elementAt(1).asString(), "two");
  EXPECT_TRUE(array.elementAt(2).isNull());
  EXPECT_TRUE(array.elementAt(3).elementAt(0).asBoolean());
  VELOX_ASSERT_USER_THROW(array.elementAt(4), "");

  std::vector<std::string> elements(300, primitive(5, bytesOf<int32_t>(7)));
  const auto largeBytes =
      makeArray(elements, {.offsetSize = 2, .isLarge = true});
  auto large = value(largeBytes);
  EXPECT_EQ(large.numElements(), 300);
  EXPECT_EQ(large.elementAt(299).asInteger(), 7);
}

TEST_F(VariantEncodingTest, nestedPath) {
  // {"owner": "Alice",
  //  "contacts": [{"age": 30, "name": "Bob",
  //                "phones": [{"number": "+1-555-1000", "type": "home"}]}]}
  const auto metadataBytes = makeMetadata(
      {"age", "contacts", "name", "number", "owner", "phones", "type"}, true);
  VariantMetadata metadata(metadataBytes);
  const auto phone = makeObject({
      {3, shortString("+1-555-1000")},
      {6, shortString("home")},
  });
  const auto contact = makeObject({
      {0, primitive(3, bytesOf<int8_t>(30))},
      {2, shortString("Bob")},
      {5, makeArray({phone})},
  });
  const auto bytes = makeObject({
      {1, makeArray({contact})},
      {4, shortString("Alice")},
  });
  VariantValue root(metadata, bytes);

  EXPECT_EQ(root.field("owner")->asString(), "Alice");
  auto firstContact = root.field("contacts")->elementAt(0);
  EXPECT_EQ(firstContact.field("age")->asInteger(), 30);
  EXPECT_EQ(
      firstContact.field("phones")->elementAt(0).field("number")->asString(),
      "+1-555-1000");
}

TEST_F(VariantEncodingTest, literalBytes) {
  // {"a": 1, "b": "x"} encoded by hand without the builders.
  const std::string metadataBytes(
      "\x11\x02\x00\x01\x02"
      "ab",
      7);
  const std::string bytes(
      "\x02\x02\x00\x01\x00\x02\x04"
      "\x0C\x01\x05x",
      11);
  VariantMetadata metadata(metadataBytes);
  VariantValue object(metadata, bytes);
  EXPECT_EQ(object.numElements(), 2);
  EXPECT_EQ(object.field("a")->asInteger(), 1);
  EXPECT_EQ(object.field("b")->asString(), "x");
}

TEST_F(VariantEncodingTest, kindNames) {
  EXPECT_EQ(
      VariantKindName::toName(VariantKind::kTimestampNtz), "TIMESTAMP_NTZ");
  EXPECT_EQ(fmt::format("{}", VariantKind::kObject), "OBJECT");
}

} // namespace
} // namespace facebook::velox
