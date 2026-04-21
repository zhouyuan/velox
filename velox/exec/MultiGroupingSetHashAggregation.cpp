#include "velox/exec/MultiGroupingSetHashAggregation.h"
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
          OperatorType::kHashAggregation),
      aggregationNode_(aggregationNode),
      expandNode_(expandNode) {
  const auto& projections = expandNode_->projections();
  numGroupingSets_ = static_cast<int32_t>(projections.size());
  VELOX_CHECK_GT(
      numGroupingSets_,
      1,
      "MultiGroupingSetHashAggregation requires at least 2 grouping sets");

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

  isPartialOutput_ =
      aggregationNode_->step() == core::AggregationNode::Step::kPartial ||
      aggregationNode_->step() == core::AggregationNode::Step::kSingle;
}

void MultiGroupingSetHashAggregation::initialize() {
  Operator::initialize();
  // Build GroupingSet with a key schema prepended by grouping_id.
  // grouping_id is always the last groupingKey in aggregationNode_ when
  // coming from an Expand node — we rely on that ordering from the planner.
  //
  // Build hashers: one per groupingExpression in aggregationNode_.
  std::vector<std::unique_ptr<VectorHasher>> hashers;
  const auto& groupingKeys = aggregationNode_->groupingKeys();
  hashers.reserve(groupingKeys.size());
  for (column_index_t i = 0; i < groupingKeys.size(); ++i) {
    hashers.push_back(VectorHasher::create(groupingKeys[i]->type(), i));
  }

  // Re-use the HashAggregation helper to build AggregateInfo list.
  auto aggregates = toAggregateInfo(
      aggregationNode_->aggregates(),
      aggregationNode_->aggregateNames(),
      aggregationNode_->step(),
      operatorCtx_->pool());

  groupingSet_ = std::make_unique<GroupingSet>(
      aggregationNode_->outputType(),
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
      /*isRawInput=*/aggregationNode_->step() ==
              core::AggregationNode::Step::kPartial ||
          aggregationNode_->step() == core::AggregationNode::Step::kSingle,
      aggregationNode_->globalGroupingSets(),
      aggregationNode_->groupId(),
      operatorCtx_->driverCtx()->queryConfig().spillConfig(),
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
  const auto& keyChannels = gsKeyChannels_[gsIdx];
  const int32_t numKeys = static_cast<int32_t>(keyChannels.size());

  // Reconstruct the output schema that the aggregation node expects:
  //   [key0, key1, ..., keyN-1, grouping_id, agg_input_cols...]
  // The aggregationNode_->outputType() schema is produced AFTER grouping,
  // but here we need to produce the *input* schema for the GroupingSet,
  // which is exactly what the ExpandNode would have produced.
  //
  // The ExpandNode output columns are:
  //   original_input_cols... | key_alias_0 | ... | key_alias_N-1 | gid
  // We replicate that layout here without materializing extra rows.

  const auto& projRow = expandNode_->projections()[gsIdx];
  const int32_t numExpandCols = static_cast<int32_t>(projRow.size());

  // Build output: original input columns first, then key columns (some null).
  std::vector<VectorPtr> outCols;
  outCols.reserve(numExpandCols);

  // Pass-through original input columns unchanged.
  const auto numInputCols = static_cast<int32_t>(input->childrenSize());
  for (int32_t c = 0; c < numInputCols; ++c) {
    outCols.push_back(input->childAt(c));
  }

  // Key alias columns (possibly null) + grouping_id.
  for (int32_t col = 0; col < numExpandCols - numInputCols; ++col) {
    const auto& expr = projRow[numInputCols + col];
    if (auto* field =
            dynamic_cast<const core::FieldAccessTypedExpr*>(expr.get())) {
      // Pass-through the real key column.
      auto idx = input->type()->as<RowType>().getChildIdx(field->name());
      outCols.push_back(input->childAt(idx));
    } else if (
        auto* constant =
            dynamic_cast<const core::ConstantTypedExpr*>(expr.get())) {
      // Constant (null or grouping_id literal) — wrap as constant vector.
      outCols.push_back(BaseVector::wrapInConstant(
          numRows, 0, constant->toConstantVector(pool())));
    } else {
      VELOX_FAIL("Unexpected expression type in grouping set projection");
    }
  }

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

  const int32_t batchSize =
      outputBatchRows(groupingSet_->estimateOutputRowSize());
  prepareOutput(batchSize);

  bool hasData = groupingSet_->getOutput(
      batchSize,
      outputBatchBytes(groupingSet_->estimateOutputRowSize()),
      resultIterator_,
      output_);

  if (!hasData) {
    if (isPartialOutput_) {
      partialFull_ = false;
      groupingSet_->resetTable(/*freeTable=*/false);
    } else {
      finished_ = true;
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
