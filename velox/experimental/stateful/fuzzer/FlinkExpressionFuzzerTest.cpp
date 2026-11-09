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

#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "velox/common/memory/Memory.h"
#include "velox/core/QueryConfig.h"
#include "velox/experimental/stateful/fuzzer/FlinkQueryRunner.h"
#include "velox/experimental/stateful/udf/Register.h"
#include "velox/expression/fuzzer/FuzzerRunner.h"
#include "velox/expression/fuzzer/SpecialFormSignatureGenerator.h"
#include "velox/functions/flinksql/registration/RegistrationFunctions.h"

DEFINE_int64(
    seed,
    123456,
    "Initial seed for random number generator "
    "(use it to reproduce previous results).");

DEFINE_string(
    flink_gateway_url,
    "",
    "Flink SQL Gateway REST endpoint, e.g. http://localhost:8083. If set, "
    "Flink is used as the reference DB and results are verified against it. "
    "Otherwise, results are only compared across Velox evaluation paths.");

DEFINE_uint32(
    req_timeout_ms,
    60000,
    "Timeout in milliseconds of each HTTP request and query sent to Flink.");

using facebook::velox::fuzzer::ArgTypesGenerator;
using facebook::velox::fuzzer::FuzzerRunner;
using facebook::velox::stateful::fuzzer::FlinkQueryRunner;
using facebook::velox::test::ReferenceQueryRunner;

int main(int argc, char** argv) {
  facebook::velox::functions::flinksql::registerFunctions("");
  facebook::velox::stateful::udf::registerFunctions("");

  // Velox casts follow Presto semantics unless configured otherwise, while
  // Flink casts follow Flink's. Leave 'cast' out unless asked for explicitly.
  gflags::SetCommandLineOptionWithMode(
      "special_forms", "and,or,coalesce,if,switch", gflags::SET_FLAGS_DEFAULT);

  ::testing::InitGoogleTest(&argc, argv);

  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  folly::Init init(&argc, &argv);

  std::unordered_set<std::string> skipFunctions = {
      // Non-deterministic: they read the current time.
      "current_date",
      "current_time",
      "current_timestamp",
      "now",
      "localtime",
      "localtimestamp",
      "unix_timestamp() -> bigint",
      // Flink returns TIMESTAMP_LTZ while Velox returns TIMESTAMP holding the
      // UTC instant. The results are not comparable without a conversion.
      "to_timestamp_ltz",
      // The integer arguments encode the RETURNING type and the ON EMPTY / ON
      // ERROR behaviors from the Flink plan, so random values are invalid.
      "json_value",
      // Not a Flink built-in function; there is nothing to compare against.
      "count_char",
  };

  // Velox runs the expressions with these configs. Flink query runner sets
  // 'table.local-time-zone' in the Flink session from 'session_timezone'.
  // Timestamps are not adjusted to the session timezone, as Flink TIMESTAMP
  // has no timezone.
  const std::unordered_map<std::string, std::string> queryConfigs = {
      {facebook::velox::core::QueryConfig::kSessionTimezone,
       "America/Los_Angeles"},
  };

  std::shared_ptr<ReferenceQueryRunner> referenceQueryRunner{nullptr};
  std::shared_ptr<facebook::velox::memory::MemoryPool> rootPool;
  if (!FLAGS_flink_gateway_url.empty()) {
    rootPool = facebook::velox::memory::memoryManager()->addRootPool();
    referenceQueryRunner = std::make_shared<FlinkQueryRunner>(
        rootPool.get(),
        FLAGS_flink_gateway_url,
        queryConfigs,
        std::chrono::milliseconds(FLAGS_req_timeout_ms));
    LOG(INFO) << "Using Flink as the reference DB.";
  }

  return FuzzerRunner::run(
      FLAGS_seed,
      skipFunctions,
      {{}},
      queryConfigs,
      std::unordered_map<std::string, std::shared_ptr<ArgTypesGenerator>>{},
      {{}},
      referenceQueryRunner,
      std::make_shared<
          facebook::velox::fuzzer::SpecialFormSignatureGenerator>());
}
