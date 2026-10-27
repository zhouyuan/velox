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
#include "velox/type/variant/VariantBuilder.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox {
namespace {

// Expected bytes below were produced by Spark 4.0.2 parse_json.

std::string toHex(std::string_view bytes) {
  std::string out;
  for (auto byte : bytes) {
    out += fmt::format("{:02x}", static_cast<uint8_t>(byte));
  }
  return out;
}

template <typename Append>
std::string scalarHex(Append append) {
  VariantBuilder builder(false);
  append(builder);
  EXPECT_EQ(toHex(builder.metadata()), "010000");
  return toHex(builder.value());
}

TEST(VariantBuilderTest, scalars) {
  EXPECT_EQ(scalarHex([](auto& b) { b.appendNull(); }), "00");
  EXPECT_EQ(scalarHex([](auto& b) { b.appendBoolean(true); }), "04");
  EXPECT_EQ(scalarHex([](auto& b) { b.appendBoolean(false); }), "08");
  EXPECT_EQ(scalarHex([](auto& b) { b.appendLong(-1); }), "0cff");
  EXPECT_EQ(scalarHex([](auto& b) { b.appendLong(128); }), "108000");
  EXPECT_EQ(scalarHex([](auto& b) { b.appendLong(-129); }), "107fff");
  EXPECT_EQ(scalarHex([](auto& b) { b.appendLong(32'768); }), "1400800000");
  EXPECT_EQ(
      scalarHex([](auto& b) { b.appendLong(2'147'483'648); }),
      "180000008000000000");
  EXPECT_EQ(
      scalarHex(
          [](auto& b) { b.appendLong(std::numeric_limits<int64_t>::min()); }),
      "180000000000000080");
  EXPECT_EQ(
      scalarHex([](auto& b) { b.appendDouble(100.0); }), "1c0000000000005940");
  EXPECT_EQ(scalarHex([](auto& b) { b.appendString(""); }), "01");
  EXPECT_EQ(scalarHex([](auto& b) { b.appendString("abc"); }), "0d616263");
  std::string longStringHex{"4040000000"};
  for (int i = 0; i < 64; ++i) {
    longStringHex += "78";
  }
  EXPECT_EQ(
      scalarHex([](auto& b) { b.appendString(std::string(64, 'x')); }),
      longStringHex);
}

TEST(VariantBuilderTest, decimals) {
  EXPECT_EQ(scalarHex([](auto& b) { b.appendDecimal(15, 1); }), "20010f000000");
  EXPECT_EQ(
      scalarHex([](auto& b) { b.appendDecimal(150, 2); }), "200296000000");
  EXPECT_EQ(scalarHex([](auto& b) { b.appendDecimal(1, 3); }), "200301000000");
  EXPECT_EQ(scalarHex([](auto& b) { b.appendDecimal(0, 1); }), "200100000000");
  EXPECT_EQ(
      scalarHex([](auto& b) {
        b.appendDecimal(HugeInt::build(0, 9'223'372'036'854'775'808ULL), 0);
      }),
      "280000000000000000800000000000000000");
  // 12345678901234567890123456789012345678.
  const int128_t large =
      HugeInt::build(0x0949'B0F6'F002'3313ULL, 0xC449'9050'DE38'F34EULL);
  EXPECT_EQ(
      scalarHex([&](auto& b) { b.appendDecimal(large, 0); }),
      "28004ef338de509049c4133302f0f6b04909");
  // 10 digits needs DECIMAL8.
  EXPECT_EQ(
      scalarHex([](auto& b) { b.appendDecimal(1'234'567'890, 0); }),
      "2400d202964900000000");
  VELOX_ASSERT_USER_THROW(
      scalarHex([](auto& b) { b.appendDecimal(1, 39); }),
      "Variant decimal exceeds the maximum precision.");
}

TEST(VariantBuilderTest, object) {
  // {"b":1,"a":2}
  VariantBuilder builder(false);
  const auto start = builder.writePosition();
  std::vector<VariantBuilder::Field> fields;
  fields.push_back({builder.addKey("b"), builder.writePosition() - start});
  builder.appendLong(1);
  fields.push_back({builder.addKey("a"), builder.writePosition() - start});
  builder.appendLong(2);
  builder.finishObject(start, fields);

  EXPECT_EQ(toHex(builder.metadata()), "01020001026261");
  EXPECT_EQ(toHex(builder.value()), "020201000200040c010c02");
}

TEST(VariantBuilderTest, nested) {
  // {"a":{"c":[1,2],"b":"x"},"d":3.25}
  VariantBuilder builder(false);
  const auto start = builder.writePosition();
  std::vector<VariantBuilder::Field> fields;
  fields.push_back({builder.addKey("a"), builder.writePosition() - start});
  {
    const auto innerStart = builder.writePosition();
    std::vector<VariantBuilder::Field> innerFields;
    innerFields.push_back(
        {builder.addKey("c"), builder.writePosition() - innerStart});
    const auto arrayStart = builder.writePosition();
    std::vector<uint64_t> offsets;
    offsets.push_back(builder.writePosition() - arrayStart);
    builder.appendLong(1);
    offsets.push_back(builder.writePosition() - arrayStart);
    builder.appendLong(2);
    builder.finishArray(arrayStart, offsets);
    innerFields.push_back(
        {builder.addKey("b"), builder.writePosition() - innerStart});
    builder.appendString("x");
    builder.finishObject(innerStart, innerFields);
  }
  fields.push_back({builder.addKey("d"), builder.writePosition() - start});
  builder.appendDecimal(325, 2);
  builder.finishObject(start, fields);

  EXPECT_EQ(toHex(builder.metadata()), "0104000102030461636264");
  EXPECT_EQ(
      toHex(builder.value()),
      "020200030012180202020109000b03020002040c010c020578200245010000");
}

TEST(VariantBuilderTest, duplicateKeys) {
  // {"k":1,"j":[1],"k":"two"}
  auto build = [](VariantBuilder& builder) {
    const auto start = builder.writePosition();
    std::vector<VariantBuilder::Field> fields;
    fields.push_back({builder.addKey("k"), builder.writePosition() - start});
    builder.appendLong(1);
    fields.push_back({builder.addKey("j"), builder.writePosition() - start});
    const auto arrayStart = builder.writePosition();
    builder.appendLong(1);
    builder.finishArray(arrayStart, {0});
    fields.push_back({builder.addKey("k"), builder.writePosition() - start});
    builder.appendString("two");
    builder.finishObject(start, fields);
  };

  VariantBuilder allowed(true);
  build(allowed);
  EXPECT_EQ(toHex(allowed.metadata()), "01020001026b6a");
  EXPECT_EQ(toHex(allowed.value()), "0202010000060a030100020c010d74776f");

  VariantBuilder rejected(false);
  VELOX_ASSERT_USER_THROW(
      build(rejected),
      "Failed to build variant because of a duplicate object key: k");
}

TEST(VariantBuilderTest, appendVariant) {
  // Build {"a":{"z":1,"y":[true]},"b":2}, then copy the value of "a" into a
  // new builder. The copy's dictionary only has the keys it uses.
  VariantBuilder source(false);
  const auto start = source.writePosition();
  std::vector<VariantBuilder::Field> fields;
  fields.push_back({source.addKey("a"), source.writePosition() - start});
  {
    const auto innerStart = source.writePosition();
    std::vector<VariantBuilder::Field> innerFields;
    innerFields.push_back(
        {source.addKey("z"), source.writePosition() - innerStart});
    source.appendLong(1);
    innerFields.push_back(
        {source.addKey("y"), source.writePosition() - innerStart});
    const auto arrayStart = source.writePosition();
    source.appendBoolean(true);
    source.finishArray(arrayStart, {0});
    source.finishObject(innerStart, innerFields);
  }
  fields.push_back({source.addKey("b"), source.writePosition() - start});
  source.appendLong(2);
  source.finishObject(start, fields);

  const auto metadataBytes = source.metadata();
  VariantMetadata metadata(metadataBytes);
  VariantValue root(metadata, source.value());

  VariantBuilder copy(false);
  copy.appendVariant(*root.field("a"));
  EXPECT_EQ(toHex(copy.metadata()), "0102000102797a");
  EXPECT_EQ(toHex(copy.value()), "0202000100050703010001040c01");
}

TEST(VariantBuilderTest, largeContainers) {
  VariantBuilder builder(false);
  const auto start = builder.writePosition();
  std::vector<VariantBuilder::Field> fields;
  for (int i = 0; i < 300; ++i) {
    fields.push_back(
        {builder.addKey(fmt::format("k{:03}", 299 - i)),
         builder.writePosition() - start});
    builder.appendLong(299 - i);
  }
  builder.finishObject(start, fields);

  const auto metadataBytes = builder.metadata();
  VariantMetadata metadata(metadataBytes);
  VariantValue object(metadata, builder.value());
  ASSERT_EQ(object.numElements(), 300);
  EXPECT_EQ(object.fieldNameAt(0), "k000");
  EXPECT_EQ(object.fieldNameAt(299), "k299");
  for (int i = 0; i < 300; ++i) {
    EXPECT_EQ(object.field(fmt::format("k{:03}", i))->asInteger(), i);
  }
  EXPECT_EQ(object.data().size(), builder.value().size());
}

TEST(VariantBuilderTest, roundTripScalars) {
  VariantBuilder builder(false);
  const auto start = builder.writePosition();
  std::vector<uint64_t> offsets;
  auto next = [&] { offsets.push_back(builder.writePosition() - start); };
  next();
  builder.appendFloat(1.5f);
  next();
  builder.appendDate(19'000);
  next();
  builder.appendTimestamp(-1);
  next();
  builder.appendTimestampNtz(2);
  next();
  builder.appendBinary(std::string("\x00\xff", 2));
  next();
  builder.appendUuid(std::string(16, '\x11'));
  builder.finishArray(start, offsets);

  const auto metadataBytes = builder.metadata();
  VariantMetadata metadata(metadataBytes);
  VariantValue array(metadata, builder.value());
  EXPECT_EQ(array.elementAt(0).asFloat(), 1.5f);
  EXPECT_EQ(array.elementAt(1).asDate(), 19'000);
  EXPECT_EQ(array.elementAt(2).kind(), VariantKind::kTimestamp);
  EXPECT_EQ(array.elementAt(2).asTimestamp(), -1);
  EXPECT_EQ(array.elementAt(3).kind(), VariantKind::kTimestampNtz);
  EXPECT_EQ(array.elementAt(4).asBinary(), std::string("\x00\xff", 2));
  EXPECT_EQ(array.elementAt(5).asUuid(), std::string(16, '\x11'));
}

} // namespace
} // namespace facebook::velox
