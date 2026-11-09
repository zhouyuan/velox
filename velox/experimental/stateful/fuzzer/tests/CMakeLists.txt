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

include(GoogleTest)

add_executable(velox_flink_sql_test FlinkSqlTest.cpp)
add_test(velox_flink_sql_test velox_flink_sql_test)

target_link_libraries(
  velox_flink_sql_test
  velox_flink_query_runner
  velox_functions_flink
  velox_stateful_udf
  velox_vector_test_lib
  velox_parse_parser
  velox_parse_expression
  velox_exec_test_lib
  GTest::gtest
  GTest::gtest_main)

# Requires a running Flink SQL Gateway, see
# velox/docs/develop/testing/flink-query-runner.rst.
add_executable(velox_flink_query_runner_test FlinkQueryRunnerTest.cpp)

gtest_add_tests(TARGET velox_flink_query_runner_test
                TEST_LIST velox_flink_query_runner_test_cases)
set_tests_properties(${velox_flink_query_runner_test_cases} PROPERTIES DISABLED
                                                                       TRUE)

target_link_libraries(
  velox_flink_query_runner_test
  velox_flink_query_runner
  velox_functions_flink
  velox_vector_test_lib
  velox_exec_test_lib
  GTest::gtest
  GTest::gtest_main)
