#pragma once
#include "velox/core/PlanNode.h"
#include "velox/exec/GroupingSet.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

/// HashAggregation operator for ROLLUP/CUBE/GROUPING SETS that works directly
/// with the original input without requiring a separate Expand node.
///
/// For each input batch, this operator:
///   1. Iterates over all N grouping sets.
///   2. For grouping set S, creates a view of the input with appropriate keys
///      masked as null according to the grouping set specification.
///   3. Probes the single shared hash table using the masked key + grouping_id.
///   4. Updates accumulators in-place.
///
/// The hash table key is extended by one column: the grouping_id value,
/// which acts as a namespace so that rows belonging to different grouping
/// levels never collide.
///
/// Memory savings: for an 8-column ROLLUP (9 grouping sets), this avoids
/// materializing 9× expanded rows, processing each input row 9 times inline.
class MultiGroupingSetHashAggregation : public Operator {
 public:
  MultiGroupingSetHashAggregation(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::GroupingSetAggregationNode>& groupingSetAggNode);

  void initialize() override;
  bool needsInput() const override;
  void addInput(RowVectorPtr input) override;
  RowVectorPtr getOutput() override;
  void noMoreInput() override;
  bool isFinished() override;

  BlockingReason isBlocked(ContinueFuture*) override {
    return BlockingReason::kNotBlocked;
  }

 private:
  // Creates a view of the input with keys masked according to grouping set `gsIdx`.
  // Non-participating keys are replaced with null constants.
  // Adds the grouping_id as an additional column.
  RowVectorPtr projectGroupingSet(const RowVectorPtr& input, int32_t gsIdx);

  // --- plan-level metadata ---
  std::shared_ptr<const core::GroupingSetAggregationNode> groupingSetAggNode_;

  // Number of grouping sets
  int32_t numGroupingSets_{0};

  // For each grouping set, boolean mask indicating which keys are active
  std::vector<std::vector<bool>> groupingSets_;

  // The grouping_id value for each grouping set
  std::vector<int64_t> groupingSetIds_;

  // Shared hash table — one GroupingSet covers all grouping levels.
  // Its key schema is: (key0, key1, ..., keyN-1, grouping_id BIGINT).
  std::unique_ptr<GroupingSet> groupingSet_;

  bool isPartialOutput_{false};
  bool partialFull_{false};
  bool finished_{false};
  int64_t numInputRows_{0};
  int64_t numOutputRows_{0};

  RowContainerIterator resultIterator_;
  RowVectorPtr output_;
};

} // namespace facebook::velox::exec
