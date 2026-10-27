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
#include "velox/type/variant/VariantType.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/type/variant/VariantRegistration.h"

namespace facebook::velox {
namespace {

class VariantTypeTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    Type::registerSerDe();
    registerVariantType();
  }
};

TEST_F(VariantTypeTest, basic) {
  EXPECT_STREQ(VARIANT()->name(), "VARIANT");
  EXPECT_STREQ(VARIANT()->kindName(), "ROW");
  EXPECT_EQ(VARIANT()->toString(), "VARIANT");
  EXPECT_TRUE(VARIANT()->parameters().empty());
  EXPECT_THAT(VARIANT()->names(), testing::ElementsAre("metadata", "value"));
  EXPECT_EQ(*VARIANT()->childAt(VariantType::kMetadataIndex), *VARBINARY());
  EXPECT_EQ(*VARIANT()->childAt(VariantType::kValueIndex), *VARBINARY());

  EXPECT_TRUE(isVariantType(VARIANT()));
  EXPECT_FALSE(isVariantType(ROW({"metadata", "value"}, VARBINARY())));
  EXPECT_FALSE(VARIANT()->equivalent(*ROW({"metadata", "value"}, VARBINARY())));

  EXPECT_TRUE(hasType("VARIANT"));
  EXPECT_EQ(getType("VARIANT", {}), VARIANT());
}

TEST_F(VariantTypeTest, notComparable) {
  EXPECT_FALSE(VARIANT()->isComparable());
  EXPECT_FALSE(VARIANT()->isOrderable());
  EXPECT_FALSE(ARRAY(VARIANT())->isComparable());
  EXPECT_FALSE(ROW({"v"}, {VARIANT()})->isOrderable());
}

TEST_F(VariantTypeTest, serde) {
  auto copy =
      ISerializable::deserialize<Type>(ISerializable::serialize(VARIANT()));
  EXPECT_EQ(copy, VARIANT());

  const auto nested = MAP(VARCHAR(), ARRAY(VARIANT()));
  copy = ISerializable::deserialize<Type>(ISerializable::serialize(nested));
  EXPECT_EQ(*copy, *nested);
  EXPECT_EQ(copy->toString(), "MAP<VARCHAR,ARRAY<VARIANT>>");
}

} // namespace
} // namespace facebook::velox
