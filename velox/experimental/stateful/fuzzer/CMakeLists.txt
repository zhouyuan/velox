# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The Flink query runner talks to the Flink SQL Gateway over HTTP with cpr,
# which is only available with testing enabled.
add_library(velox_flink_query_runner FlinkSql.cpp FlinkQueryRunner.cpp)

target_link_libraries(
  velox_flink_query_runner
  velox_fuzzer_util
  velox_exec_test_lib
  velox_core
  velox_type
  velox_vector
  velox_encode
  cpr::cpr
  Folly::folly)

add_executable(flink_expression_fuzzer_test FlinkExpressionFuzzerTest.cpp)

target_link_libraries(
  flink_expression_fuzzer_test
  velox_flink_query_runner
  velox_expression_fuzzer
  velox_fuzzer_util
  velox_hive_connector
  velox_functions_flink
  velox_stateful_udf
  GTest::gtest
  GTest::gtest_main)

add_subdirectory(tests)
