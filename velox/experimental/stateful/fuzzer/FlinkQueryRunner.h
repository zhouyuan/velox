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
#pragma once

#include <folly/json/dynamic.h>
#include <chrono>
#include <stdexcept>
#include <string>
#include <unordered_map>

#include "velox/exec/fuzzer/ReferenceQueryRunner.h"

namespace facebook::velox::stateful::fuzzer {

/// Thrown when Flink rejects a query while parsing or validating it, i.e. the
/// query uses syntax, functions or argument types that Flink does not support.
class FlinkQueryUnsupportedError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

/// Thrown when the Flink SQL Gateway cannot be reached.
class FlinkConnectionError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

/// Reference query runner that executes Flink SQL through the REST API of the
/// Flink SQL Gateway, e.g.
///
///   POST /v1/sessions                                   -> sessionHandle
///   POST /v1/sessions/{session}/statements              -> operationHandle
///   GET  /v2/sessions/{session}/operations/{op}/result/{token}?rowFormat=JSON
///
/// Input rows of the plan are inlined into the SQL as a VALUES clause, so the
/// Flink cluster does not need access to files written by Velox. Results are
/// fetched as JSON and converted to Velox vectors of the plan output type.
///
/// Designed to run against Flink 1.19.
class FlinkQueryRunner : public velox::exec::test::ReferenceQueryRunner {
 public:
  /// @param gatewayUrl Flink SQL Gateway REST endpoint, e.g.
  /// http://localhost:8083.
  /// @param queryConfigs Velox query configs. The session timezone
  /// ('session_timezone') is set as 'table.local-time-zone' of the Flink
  /// session, so that Flink runs queries with the same timezone as Velox
  /// instead of the Flink default. UTC is used if not set, as in Velox.
  /// @param timeout Timeout of each HTTP request and of each query.
  FlinkQueryRunner(
      memory::MemoryPool* pool,
      std::string gatewayUrl,
      const std::unordered_map<std::string, std::string>& queryConfigs,
      std::chrono::milliseconds timeout = std::chrono::seconds(60));

  ~FlinkQueryRunner() override;

  RunnerType runnerType() const override {
    return RunnerType::kFlinkQueryRunner;
  }

  const std::vector<TypePtr>& supportedScalarTypes() const override;

  const std::unordered_map<std::string, exec::test::DataSpec>&
  aggregationFunctionDataSpecs() const override;

  /// Converts a ProjectNode over a ValuesNode into Flink SQL. Returns
  /// std::nullopt for other plans.
  std::optional<std::string> toSql(const core::PlanNodePtr& plan) override;

  /// Returns false if the signature uses types not supported by this runner,
  /// e.g. intervals or complex types.
  bool isSupported(const exec::FunctionSignature& signature) override;

  bool isConstantExprSupported(const core::TypedExprPtr& expr) override;

  using ReferenceQueryRunner::execute;

  std::pair<
      std::optional<std::multiset<std::vector<velox::variant>>>,
      exec::test::ReferenceQueryErrorCode>
  execute(const core::PlanNodePtr& plan) override;

  std::pair<
      std::optional<std::vector<velox::RowVectorPtr>>,
      exec::test::ReferenceQueryErrorCode>
  executeAndReturnVector(const core::PlanNodePtr& plan) override;

  bool supportsVeloxVectorResults() const override {
    return true;
  }

  /// Executes 'sql' and returns the results converted to 'resultType'.
  /// @throw FlinkQueryUnsupportedError if Flink rejects the query while
  /// parsing or validating it.
  std::vector<RowVectorPtr> execute(
      const std::string& sql,
      const RowTypePtr& resultType);

  /// Executes 'sql' and returns the results. The result type is derived from
  /// the column types returned by Flink.
  std::vector<RowVectorPtr> execute(const std::string& sql) override;

  /// Converts the Flink logical type serialized as JSON by the SQL Gateway,
  /// e.g. {"type": "DECIMAL", "precision": 10, "scale": 2}, into a Velox
  /// type.
  static TypePtr toVeloxType(const folly::dynamic& logicalType);

  /// Converts the rows serialized as JSON by the SQL Gateway into a
  /// RowVector of 'type'.
  static RowVectorPtr toRowVector(
      const std::vector<folly::dynamic>& rows,
      const RowTypePtr& type,
      memory::MemoryPool* pool);

 private:
  struct Response {
    long statusCode;
    folly::dynamic body;
  };

  // Sends an HTTP request to the gateway and parses the JSON response body.
  Response send(
      const std::string& method,
      const std::string& path,
      const std::string& body = "");

  // Opens a Flink session if none is open.
  void ensureSession();

  void closeSession();

  // Throws the error of a failed response. Errors raised while parsing or
  // validating the query are thrown as FlinkQueryUnsupportedError.
  [[noreturn]] void throwError(
      const std::string& action,
      const Response& response) const;

  // Submits 'sql', fetches all its results and closes the operation. Returns
  // the result columns and rows.
  std::pair<folly::dynamic, std::vector<folly::dynamic>> runStatement(
      const std::string& sql);

  // Fetches all results of an operation. Returns the result columns and rows.
  std::pair<folly::dynamic, std::vector<folly::dynamic>> fetchResults(
      const std::string& operationHandle);

  const std::string gatewayUrl_;
  const std::chrono::milliseconds timeout_;
  // Flink session configs derived from the Velox query configs.
  folly::dynamic sessionProperties_;
  std::string sessionHandle_;
  std::shared_ptr<memory::MemoryPool> pool_;
};

} // namespace facebook::velox::stateful::fuzzer
