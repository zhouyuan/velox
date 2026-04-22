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

#include "velox/exec/MultiGroupingSetHashAggregation.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

namespace facebook::velox::exec {
namespace {

class MultiGroupingSetHashAggregationTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
  }

  RowVectorPtr makeTestData(vector_size_t size) {
    return makeRowVector(
        {"k1", "k2", "k3", "a", "b"},
        {
            makeFlatVector<int64_t>(size, [](auto row) { return row % 11; }),
            makeFlatVector<int64_t>(size, [](auto row) { return row % 17; }),
            makeFlatVector<int64_t>(size, [](auto row) { return row % 7; }),
            makeFlatVector<int64_t>(size, [](auto row) { return row; }),
            makeFlatVector<std::string>(
                size, [](auto row) { return std::string(row % 15, 'x'); }),
        });
  }
};

TEST_F(MultiGroupingSetHashAggregationTest, basicRollup) {
  auto data = makeTestData(1000);
  createDuckDbTable({data});

  // Test ROLLUP with 2 keys
  auto plan =
      PlanBuilder()
          .values({data})
          .expand(
              {{"k1", "k2", "a", "b", "0 as gid"},
               {"k1", "null::bigint as k2", "a", "b", "1 as gid"},
               {"null::bigint as k1", "null::bigint as k2", "a", "b", "2 as gid"}})
          .singleAggregation(
              {"k1", "k2", "gid"},
              {"count(1) as count_1", "sum(a) as sum_a", "max(b) as max_b"})
          .project({"k1", "k2", "count_1", "sum_a", "max_b"})
          .planNode();

  assertQuery(
      plan,
      "SELECT k1, k2, count(1), sum(a), max(b) FROM tmp GROUP BY ROLLUP (k1, k2)");
}

TEST_F(MultiGroupingSetHashAggregationTest, basicCube) {
  auto data = makeTestData(1000);
  createDuckDbTable({data});

  // Test CUBE with 2 keys
  auto plan =
      PlanBuilder()
          .values({data})
          .expand({
              {"k1", "k2", "a", "b", "0 as gid"},
              {"k1", "null::bigint as k2", "a", "b", "1 as gid"},
              {"null::bigint as k1", "k2", "a", "b", "2 as gid"},
              {"null::bigint as k1", "null::bigint as k2", "a", "b", "3 as gid"},
          })
          .singleAggregation(
              {"k1", "k2", "gid"},
              {"count(1) as count_1", "sum(a) as sum_a", "max(b) as max_b"})
          .project({"k1", "k2", "count_1", "sum_a", "max_b"})
          .planNode();

  assertQuery(
      plan,
      "SELECT k1, k2, count(1), sum(a), max(b) FROM tmp GROUP BY CUBE (k1, k2)");
}

TEST_F(MultiGroupingSetHashAggregationTest, threeKeyRollup) {
  auto data = makeTestData(1000);
  createDuckDbTable({data});

  // Test ROLLUP with 3 keys
  auto plan =
      PlanBuilder()
          .values({data})
          .expand(
              {{"k1", "k2", "k3", "a", "b", "0 as gid"},
               {"k1", "k2", "null::bigint as k3", "a", "b", "1 as gid"},
               {"k1", "null::bigint as k2", "null::bigint as k3", "a", "b", "2 as gid"},
               {"null::bigint as k1", "null::bigint as k2", "null::bigint as k3", "a", "b", "3 as gid"}})
          .singleAggregation(
              {"k1", "k2", "k3", "gid"},
              {"count(1) as count_1", "sum(a) as sum_a"})
          .project({"k1", "k2", "k3", "count_1", "sum_a"})
          .planNode();

  assertQuery(
      plan,
      "SELECT k1, k2, k3, count(1), sum(a) FROM tmp GROUP BY ROLLUP (k1, k2, k3)");
}

TEST_F(MultiGroupingSetHashAggregationTest, customGroupingSets) {
  auto data = makeTestData(1000);
  createDuckDbTable({data});

  // Test custom GROUPING SETS
  auto plan =
      PlanBuilder()
          .values({data})
          .expand(
              {{"k1", "k2", "a", "b", "0 as gid"},
               {"k1", "null::bigint as k2", "a", "b", "1 as gid"},
               {"null::bigint as k1", "k2", "a", "b", "2 as gid"}})
          .singleAggregation(
              {"k1", "k2", "gid"},
              {"count(1) as count_1", "sum(a) as sum_a"})
          .project({"k1", "k2", "count_1", "sum_a"})
          .planNode();

  assertQuery(
      plan,
      "SELECT k1, k2, count(1), sum(a) FROM tmp GROUP BY GROUPING SETS ((k1, k2), (k1), (k2))");
}

TEST_F(MultiGroupingSetHashAggregationTest, multipleAggregates) {
  auto data = makeTestData(500);
  createDuckDbTable({data});

  // Test with multiple aggregate functions
  auto plan =
      PlanBuilder()
          .values({data})
          .expand(
              {{"k1", "k2", "a", "b", "0 as gid"},
               {"k1", "null::bigint as k2", "a", "b", "1 as gid"},
               {"null::bigint as k1", "null::bigint as k2", "a", "b", "2 as gid"}})
          .singleAggregation(
              {"k1", "k2", "gid"},
              {"count(1) as count_1",
               "sum(a) as sum_a",
               "min(a) as min_a",
               "max(a) as max_a",
               "avg(a) as avg_a"})
          .project({"k1", "k2", "count_1", "sum_a", "min_a", "max_a", "avg_a"})
          .planNode();

  assertQuery(
      plan,
      "SELECT k1, k2, count(1), sum(a), min(a), max(a), avg(a) FROM tmp GROUP BY ROLLUP (k1, k2)");
}

TEST_F(MultiGroupingSetHashAggregationTest, emptyInput) {
  auto data = makeTestData(0);
  createDuckDbTable({data});

  auto plan =
      PlanBuilder()
          .values({data})
          .expand(
              {{"k1", "k2", "a", "b", "0 as gid"},
               {"k1", "null::bigint as k2", "a", "b", "1 as gid"}})
          .singleAggregation(
              {"k1", "k2", "gid"},
              {"count(1) as count_1", "sum(a) as sum_a"})
          .project({"k1", "k2", "count_1", "sum_a"})
          .planNode();

  assertQuery(
      plan,
      "SELECT k1, k2, count(1), sum(a) FROM tmp GROUP BY GROUPING SETS ((k1, k2), (k1))");
}

TEST_F(MultiGroupingSetHashAggregationTest, singleGroupingSet) {
  auto data = makeTestData(1000);
  createDuckDbTable({data});

  // Single grouping set (should still work, though not typical use case)
  auto plan =
      PlanBuilder()
          .values({data})
          .expand({{"k1", "k2", "a", "b", "0 as gid"}})
          .singleAggregation(
              {"k1", "k2", "gid"},
              {"count(1) as count_1", "sum(a) as sum_a"})
          .project({"k1", "k2", "count_1", "sum_a"})
          .planNode();

  assertQuery(
      plan,
      "SELECT k1, k2, count(1), sum(a) FROM tmp GROUP BY k1, k2");
}

TEST_F(MultiGroupingSetHashAggregationTest, withNullValues) {
  // Create data with some null values in keys
  auto data = makeRowVector(
      {"k1", "k2", "a"},
      {
          makeNullableFlatVector<int64_t>(
              {1, std::nullopt, 1, 2, std::nullopt, 2, 3, 3}),
          makeNullableFlatVector<int64_t>(
              {10, 10, std::nullopt, 20, 20, std::nullopt, 30, 30}),
          makeFlatVector<int64_t>({100, 200, 300, 400, 500, 600, 700, 800}),
      });
  createDuckDbTable({data});

  auto plan =
      PlanBuilder()
          .values({data})
          .expand(
              {{"k1", "k2", "a", "0 as gid"},
               {"k1", "null::bigint as k2", "a", "1 as gid"},
               {"null::bigint as k1", "null::bigint as k2", "a", "2 as gid"}})
          .singleAggregation(
              {"k1", "k2", "gid"},
              {"count(1) as count_1", "sum(a) as sum_a"})
          .project({"k1", "k2", "count_1", "sum_a"})
          .planNode();

  assertQuery(
      plan,
      "SELECT k1, k2, count(1), sum(a) FROM tmp GROUP BY ROLLUP (k1, k2)");
}

TEST_F(MultiGroupingSetHashAggregationTest, partialAggregation) {
  auto data = makeTestData(1000);
  createDuckDbTable({data});

  // Test partial aggregation step
  auto plan =
      PlanBuilder()
          .values({data})
          .expand(
              {{"k1", "k2", "a", "b", "0 as gid"},
               {"k1", "null::bigint as k2", "a", "b", "1 as gid"}})
          .partialAggregation(
              {"k1", "k2", "gid"},
              {"count(1) as count_1", "sum(a) as sum_a"})
          .finalAggregation()
          .project({"k1", "k2", "count_1", "sum_a"})
          .planNode();

  assertQuery(
      plan,
      "SELECT k1, k2, count(1), sum(a) FROM tmp GROUP BY GROUPING SETS ((k1, k2), (k1))");
}

TEST_F(MultiGroupingSetHashAggregationTest, largeNumberOfGroupingSets) {
  auto data = makeTestData(500);
  createDuckDbTable({data});

  // Test with 4 grouping sets (CUBE of 2 keys = 4 sets)
  auto plan =
      PlanBuilder()
          .values({data})
          .expand({
              {"k1", "k2", "a", "0 as gid"},
              {"k1", "null::bigint as k2", "a", "1 as gid"},
              {"null::bigint as k1", "k2", "a", "2 as gid"},
              {"null::bigint as k1", "null::bigint as k2", "a", "3 as gid"},
          })
          .singleAggregation(
              {"k1", "k2", "gid"},
              {"sum(a) as sum_a"})
          .project({"k1", "k2", "sum_a"})
          .planNode();

  assertQuery(
      plan,
      "SELECT k1, k2, sum(a) FROM tmp GROUP BY CUBE (k1, k2)");
}

TEST_F(MultiGroupingSetHashAggregationTest, stringAggregates) {
  auto data = makeTestData(500);
  createDuckDbTable({data});

  // Test with string aggregates
  auto plan =
      PlanBuilder()
          .values({data})
          .expand(
              {{"k1", "k2", "a", "b", "0 as gid"},
               {"k1", "null::bigint as k2", "a", "b", "1 as gid"}})
          .singleAggregation(
              {"k1", "k2", "gid"},
              {"count(1) as count_1", "min(b) as min_b", "max(b) as max_b"})
          .project({"k1", "k2", "count_1", "min_b", "max_b"})
          .planNode();

  assertQuery(
      plan,
      "SELECT k1, k2, count(1), min(b), max(b) FROM tmp GROUP BY GROUPING SETS ((k1, k2), (k1))");
}

} // namespace
} // namespace facebook::velox::exec

// Made with Bob
