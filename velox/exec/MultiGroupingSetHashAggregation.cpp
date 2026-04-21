#include "velox/exec/MultiGroupingSetHashAggregation.h"
#include "velox/exec/AggregateInfo.h"
#include "velox/exec/HashAggregation.h" // for AggregateInfo helpers
#include "velox/exec/OperatorType.h"
#include "velox/exec/VectorHasher.h"

namespace facebook::velox::exec {

MultiGroupingSetHashAggregation::MultiGroupingSetHashAggregation(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::GroupingSetAggregationNode>& groupingSetAggNode)
    : Operator(
          driverCtx,
          groupingSetAggNode->outputType(),
          operatorId,
          groupingSetAggNode->id(),
          OperatorType::kAggregation),
      groupingSetAggNode_(groupingSetAggNode) {
  numGroupingSets_ = static_cast<int32_t>(groupingSetAggNode_->groupingSets().size());
  VELOX_CHECK_GT(
      numGroupingSets_,
      0,
      "MultiGroupingSetHashAggregation requires at least 1 grouping set");

  groupingSets_ = groupingSetAggNode_->groupingSets();
  groupingSetIds_ = groupingSetAggNode_->groupingSetIds();

  isPartialOutput_ = isPartialOutput(groupingSetAggNode_->step());
}

void MultiGroupingSetHashAggregation::initialize() {
  Operator::initialize();
  
  // Build hashers for grouping keys + grouping_id
  const auto& groupingKeys = groupingSetAggNode_->groupingKeys();
  const auto numKeys = groupingKeys.size();
  
  std::vector<std::unique_ptr<VectorHasher>> hashers;
  hashers.reserve(numKeys + 1); // +1 for grouping_id
  
  // Hashers for grouping keys
  for (column_index_t i = 0; i < numKeys; ++i) {
    hashers.push_back(VectorHasher::create(groupingKeys[i]->type(), i));
  }
  
  // Hasher for grouping_id (BIGINT)
  hashers.push_back(VectorHasher::create(BIGINT(), numKeys));

  // Build AggregateInfo list
  std::shared_ptr<core::ExpressionEvaluator> expressionEvaluator;
  
  // Create a temporary AggregationNode for toAggregateInfo helper
  auto tempAggNode = std::make_shared<core::AggregationNode>(
      groupingSetAggNode_->id(),
      groupingSetAggNode_->step(),
      groupingSetAggNode_->groupingKeys(),
      std::vector<core::FieldAccessTypedExprPtr>{}, // preGroupedKeys
      groupingSetAggNode_->aggregateNames(),
      groupingSetAggNode_->aggregates(),
      groupingSetAggNode_->ignoreNullKeys(),
      false, // noGroupsSpanBatches
      groupingSetAggNode_->sources()[0]);
  
  auto aggregates = toAggregateInfo(
      *tempAggNode,
      *operatorCtx_,
      numKeys + 1, // +1 for grouping_id
      expressionEvaluator);

  groupingSet_ = std::make_unique<GroupingSet>(
      groupingSetAggNode_->outputType(),
      std::move(hashers),
      /*preGroupedKeys=*/std::vector<column_index_t>{},
      /*groupingKeyOutputProjections=*/
      [&]() {
        std::vector<column_index_t> proj(numKeys + 1);
        std::iota(proj.begin(), proj.end(), 0);
        return proj;
      }(),
      std::move(aggregates),
      /*ignoreNullKeys=*/groupingSetAggNode_->ignoreNullKeys(),
      isPartialOutput_,
      /*isRawInput=*/groupingSetAggNode_->step() ==
              core::AggregationNode::Step::kPartial ||
          groupingSetAggNode_->step() == core::AggregationNode::Step::kSingle,
      /*globalGroupingSets=*/std::vector<vector_size_t>{},
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
  const auto& groupingKeys = groupingSetAggNode_->groupingKeys();
  const auto& groupingSetMask = groupingSets_[gsIdx];
  const auto groupingId = groupingSetIds_[gsIdx];

  // Build output: grouping keys (with nulls for inactive keys) + grouping_id + aggregate inputs
  std::vector<VectorPtr> outCols;
  outCols.reserve(groupingKeys.size() + 1 + input->childrenSize());

  auto inputRowType = std::dynamic_pointer_cast<const RowType>(input->type());
  VELOX_CHECK_NOT_NULL(inputRowType, "Input must be a RowVector");

  // Add grouping key columns (null if not active in this grouping set)
  for (size_t i = 0; i < groupingKeys.size(); ++i) {
    if (groupingSetMask[i]) {
      // Key is active: pass through the input column
      auto idx = inputRowType->getChildIdx(groupingKeys[i]->name());
      outCols.push_back(input->childAt(idx));
    } else {
      // Key is inactive: create null constant
      outCols.push_back(BaseVector::createNullConstant(
          groupingKeys[i]->type(), numRows, pool()));
    }
  }

  // Add grouping_id column
  auto groupingIdVector = std::make_shared<ConstantVector<int64_t>>(
      pool(), numRows, false, BIGINT(), groupingId);
  outCols.push_back(groupingIdVector);

  // Add all input columns for aggregate functions
  for (size_t i = 0; i < input->childrenSize(); ++i) {
    outCols.push_back(input->childAt(i));
  }

  // Build output type: grouping keys + grouping_id + input columns
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  
  for (const auto& key : groupingKeys) {
    names.push_back(key->name());
    types.push_back(key->type());
  }
  
  names.push_back("grouping_id");
  types.push_back(BIGINT());
  
  for (size_t i = 0; i < input->childrenSize(); ++i) {
    names.push_back(inputRowType->nameOf(i));
    types.push_back(inputRowType->childAt(i));
  }

  auto outputType = ROW(std::move(names), std::move(types));
  
  return std::make_shared<RowVector>(
      pool(), outputType, nullptr, numRows, std::move(outCols));
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
      estimatedRowSize.has_value() ? batchSize * estimatedRowSize.value() 
                                    : std::numeric_limits<int32_t>::max();
  
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
      // Partial table is full and has been flushed; reset it to accept more input.
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

// Made with Bob
