#pragma once
#include "velox/core/PlanNode.h"
#include "velox/exec/GroupingSet.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

/// Fused Expand + HashAggregation operator for ROLLUP/CUBE/GROUPING SETS.
///
/// Instead of physically replicating each input row N times (once per
/// grouping set) as the vanilla Expand operator does, this operator:
///   1. For each input batch, iterates over all N grouping sets.
///   2. For grouping set S, projects key columns according to the
///      null-mask of that set (columns not in S become null).
///   3. Probes the single shared hash table using the masked key.
///   4. Updates accumulators in-place.
///
/// The hash table key is extended by one column: the grouping_id value,
/// which acts as a namespace so that rows belonging to different grouping
/// levels never collide.
///
/// Memory savings: for an 8-column ROLLUP (9 grouping sets), this reduces
/// rows entering the hash table from 9 × input to 1 × input.
class MultiGroupingSetHashAggregation : public Operator {
 public:
  MultiGroupingSetHashAggregation(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::AggregationNode>& aggregationNode,
      const std::shared_ptr<const core::ExpandNode>& expandNode);

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
  // Applies the null-mask for grouping set `gsIdx` to `input`, producing
  // a projected RowVector where non-participating key columns are null.
  // The grouping_id constant column is appended at the end.
  RowVectorPtr projectGroupingSet(const RowVectorPtr& input, int32_t gsIdx);

  // Feeds the projected batch for one grouping set into the shared GroupingSet.
  void processOneGroupingSet(const RowVectorPtr& input, int32_t gsIdx);

  // --- plan-level metadata ---
  std::shared_ptr<const core::AggregationNode> aggregationNode_;
  std::shared_ptr<const core::ExpandNode> expandNode_;

  // Number of grouping sets (= projections.size() in ExpandNode).
  int32_t numGroupingSets_{0};

  // For each grouping set, the column_index of each key column in the
  // *original* input, or kConstantChannel if that column is nulled out.
  // Dimensions: [numGroupingSets][numGroupingKeys].
  std::vector<std::vector<column_index_t>> gsKeyChannels_;

  // The grouping_id value for each grouping set (extracted from the
  // constant literal in the last column of each projection row).
  std::vector<int64_t> groupingIds_;

  // Shared hash table — one GroupingSet covers all grouping levels.
  // Its key schema is: (grouping_id BIGINT, key0, key1, ..., keyN-1).
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
