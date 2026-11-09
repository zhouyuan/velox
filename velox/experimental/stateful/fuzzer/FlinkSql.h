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

#include <optional>
#include <string>

#include "velox/core/ITypedExpr.h"
#include "velox/core/PlanNode.h"
#include "velox/vector/BaseVector.h"

/// Converts Velox plans, expressions, types and values into Flink SQL. Every
/// function returns std::nullopt for anything that has no Flink SQL
/// counterpart, so that the caller can skip verification instead of reporting
/// a false mismatch.
namespace facebook::velox::stateful::fuzzer {

/// Returns the Flink SQL type of 'type', e.g. 'STRING' for VARCHAR and
/// 'TIMESTAMP(9)' for TIMESTAMP.
std::optional<std::string> toFlinkTypeSql(const TypePtr& type);

/// Returns a typed Flink SQL literal for the value at 'row' in 'vector', e.g.
/// "CAST(1 AS TINYINT)" or "CAST(NULL AS STRING)".
std::optional<std::string> toFlinkLiteralSql(
    const BaseVector& vector,
    vector_size_t row);

/// Returns the Flink SQL of a Velox expression that only uses functions
/// registered by velox/functions/flinksql and velox/experimental/stateful/udf.
std::optional<std::string> toFlinkExprSql(
    const core::TypedExprPtr& expr,
    memory::MemoryPool* pool);

/// Returns the Flink SQL of a plan made of a ProjectNode over a ValuesNode,
/// the shape of the plans built by the expression fuzzer. The input rows are
/// inlined as a VALUES clause, e.g.
///
///   SELECT (`c0` IS TRUE) AS `p0`, `row_number` AS `p1`
///   FROM (VALUES (CAST(TRUE AS BOOLEAN), CAST(0 AS BIGINT)))
///       AS t(`c0`, `row_number`)
///
/// Returns std::nullopt if the plan has any other shape, the input has no rows,
/// or a type, value or expression is not supported.
std::optional<std::string> toFlinkSql(
    const core::PlanNodePtr& plan,
    memory::MemoryPool* pool);

} // namespace facebook::velox::stateful::fuzzer
