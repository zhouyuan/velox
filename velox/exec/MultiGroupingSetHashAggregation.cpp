#include "velox/exec/MultiGroupingSetHashAggregation.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateInfo.h"
#include "velox/exec/HashAggregation.h" // for AggregateInfo helpers
#include "velox/exec/OperatorType.h"
#include "velox/exec/VectorHasher.h"

namespace facebook::velox::exec {

namespace {
// Extract the int64 constant value from a ConstantTypedExpr.
int64_t extractGroupingId(const core::TypedExprPtr& expr) {
  auto* c = dynamic_cast<const core::ConstantTypedExpr*>(expr.get());
  VELOX_CHECK_NOT_NULL(c, "Expected constant grouping_id");
  return c->value().value<int64_t>();
}
} // namespace

MultiGroupingSetHashAggregation::MultiGroupingSetHashAggregation(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::AggregationNode>& aggregationNode,
    const std::shared_ptr<const core::ExpandNode>& expandNode)
    : Operator(
          driverCtx,
          aggregationNode->outputType(),
          operatorId,
          aggregationNode->id(),
          OperatorType::kAggregation),
      aggregationNode_(aggregationNode),
      expandNode_(expandNode) {
  const auto& projections = expandNode_->projections();
  numGroupingSets_ = static_cast<int32_t>(projections.size());
  VELOX_CHECK_GE(
      numGroupingSets_,
      1,
      "MultiGroupingSetHashAggregation requires at least 1 grouping set");

  const auto& inputType = expandNode_->inputType();
  // Last column of each projection row is the grouping_id constant.
  // Columns before that are either field-access (key present) or null constant
  // (key nulled for this grouping set).
  const int32_t numOutputCols = static_cast<int32_t>(projections[0].size());

  gsKeyChannels_.resize(numGroupingSets_);
  groupingIds_.resize(numGroupingSets_);

  for (int32_t gs = 0; gs < numGroupingSets_; ++gs) {
    const auto& row = projections[gs];
    // Last column is grouping_id.
    groupingIds_[gs] = extractGroupingId(row.back());

    // Remaining columns are the original group-by keys (possibly null).
    for (int32_t col = 0; col + 1 < numOutputCols; ++col) {
      if (auto* field =
              dynamic_cast<const core::FieldAccessTypedExpr*>(row[col].get())) {
        gsKeyChannels_[gs].push_back(inputType->getChildIdx(field->name()));
      } else {
        // Null constant: this key is not part of this grouping set.
        gsKeyChannels_[gs].push_back(kConstantChannel);
      }
    }
  }

  isPartialOutput_ = isPartialOutput(aggregationNode_->step());
}

void MultiGroupingSetHashAggregation::initialize() {
  Operator::initialize();
  // Build GroupingSet with the Expand node's output type as input.
  // The Expand output contains: original columns + key columns + grouping_id
  //
  // Build hashers using the actual column positions of grouping keys in the
  // expand output type. Keys are referenced by name and may not be at
  // sequential positions (e.g. k1=0, k2=1, gid=4 when non-key cols appear
  // between them in the expand output).
  const auto& groupingKeys = aggregationNode_->groupingKeys();
  auto hashers =
      createVectorHashers(expandNode_->outputType(), groupingKeys);

  // Re-use the HashAggregation helper to build AggregateInfo list.
  std::shared_ptr<core::ExpressionEvaluator> expressionEvaluator;
  auto aggregates = toAggregateInfo(
      *aggregationNode_,
      *operatorCtx_,
      groupingKeys.size(),
      expressionEvaluator);

  // The input type for GroupingSet is the Expand node's output type
  groupingSet_ = std::make_unique<GroupingSet>(
      expandNode_->outputType(),
      std::move(hashers),
      /*preGroupedKeys=*/std::vector<column_index_t>{},
      /*groupingKeyOutputProjections=*/
      [&]() {
        std::vector<column_index_t> proj(groupingKeys.size());
        std::iota(proj.begin(), proj.end(), 0);
        return proj;
      }(),
      std::move(aggregates),
      /*ignoreNullKeys=*/aggregationNode_->ignoreNullKeys(),
      isPartialOutput_,
      /*isRawInput=*/isRawInput(aggregationNode_->step()),
      aggregationNode_->globalGroupingSets(),
      /*groupIdChannel=*/std::nullopt,
      /*spillConfig=*/nullptr,
      &nonReclaimableSection_,
      &operatorCtx_->driverCtx()->queryConfig(),
      operatorCtx_->pool(),
      spillStats_.get());
}

bool MultiGroupingSetHashAggregation::needsInput() const {
  return !noMoreInput_ && !partialFull_;
}

RowVectorPtr MultiGroupingSetHashAggregation::projectGroupingSet(
    const RowVectorPtr& input,
    int32_t gsIdx) {
  const auto numRows = input->size();

  // The ExpandNode output columns are the projected expressions.
  // Each projection row contains expressions that either:
  // 1. Reference an input field (FieldAccessTypedExpr)
  // 2. Are constant values (ConstantTypedExpr) - typically null or grouping_id
  const auto& projRow = expandNode_->projections()[gsIdx];
  const int32_t numExpandCols = static_cast<int32_t>(projRow.size());

  // Build output columns by evaluating each projection expression.
  std::vector<VectorPtr> outCols;
  outCols.reserve(numExpandCols);

  auto rowType = std::dynamic_pointer_cast<const RowType>(input->type());
  VELOX_CHECK_NOT_NULL(rowType, "Input must be a RowVector");

  for (int32_t col = 0; col < numExpandCols; ++col) {
    const auto& expr = projRow[col];
    if (auto* field =
            dynamic_cast<const core::FieldAccessTypedExpr*>(expr.get())) {
      // Pass-through the real input column.
      auto idx = rowType->getChildIdx(field->name());
      outCols.push_back(input->childAt(idx));
    } else if (
        auto* constant =
            dynamic_cast<const core::ConstantTypedExpr*>(expr.get())) {
      // Constant (null or grouping_id literal) — create constant vector.
      auto constantVec = constant->toConstantVector(pool());
      VELOX_CHECK_NOT_NULL(constantVec, "Constant vector must not be null");
      outCols.push_back(BaseVector::wrapInConstant(numRows, 0, constantVec));
    } else {
      VELOX_FAIL(
          "Unexpected expression type in grouping set projection: {}",
          expr->toString());
    }
  }

  VELOX_CHECK_EQ(
      outCols.size(),
      expandNode_->outputType()->size(),
      "Number of output columns ({}) must match output type size ({})",
      outCols.size(),
      expandNode_->outputType()->size());

  return std::make_shared<RowVector>(
      pool(), expandNode_->outputType(), nullptr, numRows, std::move(outCols));
}

void MultiGroupingSetHashAggregation::addInput(RowVectorPtr input) {
  numInputRows_ += input->size();

  for (int32_t gs = 0; gs < numGroupingSets_; ++gs) {
    auto projected = projectGroupingSet(input, gs);
    groupingSet_->addInput(projected, /*mayPushdown=*/false);

    if (isPartialOutput_ &&
        groupingSet_->isPartialFull(operatorCtx_->driverCtx()
                                        ->queryConfig()
                                        .maxPartialAggregationMemoryUsage())) {
      partialFull_ = true;
      break;
    }
  }
}

void MultiGroupingSetHashAggregation::noMoreInput() {
  Operator::noMoreInput();
  groupingSet_->noMoreInput();
}

RowVectorPtr MultiGroupingSetHashAggregation::getOutput() {
  if (!groupingSet_->hasOutput()) {
    return nullptr;
  }

  const auto estimatedRowSize = groupingSet_->estimateOutputRowSize();
  const int32_t batchSize = outputBatchRows(estimatedRowSize);
  const int32_t maxOutputBytes =
      estimatedRowSize.has_value() ? batchSize * estimatedRowSize.value() : std::numeric_limits<int32_t>::max();
  
  // Prepare output vector
  if (output_) {
    VectorPtr output = std::move(output_);
    BaseVector::prepareForReuse(output, batchSize);
    output_ = std::static_pointer_cast<RowVector>(output);
  } else {
    output_ = std::static_pointer_cast<RowVector>(
        BaseVector::create(outputType_, batchSize, pool()));
  }

  bool hasData = groupingSet_->getOutput(
      batchSize,
      maxOutputBytes,
      resultIterator_,
      output_);

  if (!hasData) {
    resultIterator_.reset();
    if (noMoreInput_) {
      finished_ = true;
    } else if (isPartialOutput_) {
      // Partial table is full and has been flushed; reset it to accept more
      // input.
      partialFull_ = false;
      groupingSet_->resetTable(/*freeTable=*/false);
    }
    return nullptr;
  }

  numOutputRows_ += output_->size();
  return output_;
}

bool MultiGroupingSetHashAggregation::isFinished() {
  return finished_ || (noMoreInput_ && !groupingSet_->hasOutput());
}

} // namespace facebook::velox::exec
