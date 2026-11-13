#!/bin/bash
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

# Writes the GitHub issue for a failed nightly Flink fuzzer run, see
# .github/workflows/flink-fuzzer.yml.
#
# Usage: fuzzer-report.sh <output-dir>
#
# Writes <output-dir>/title.txt and <output-dir>/body.md. Issues are
# deduplicated by title, so the title holds the failure reason without run
# specific details such as seeds.
#
# Reads from the environment: REPRO_DIR, SEED, SHA, FUZZ_REF, DURATION_SEC,
# RUN_URL, ARTIFACT_URL and the step outcomes OUTCOME_CONFIGURE,
# OUTCOME_BUILD, OUTCOME_SQL_TEST, OUTCOME_FLINK, OUTCOME_RUNNER_TEST and
# OUTCOME_FUZZER.

set -euo pipefail

OUT_DIR="$1"
mkdir -p "${OUT_DIR}"
LOG="${REPRO_DIR:-/tmp/flink_fuzzer_repro}/fuzzer-output.log"

# Returns the failure reason of the fuzzer from its output: the first
# 'Reason:' line of a Velox exception, or else the last error line.
fuzzer_reason() {
  local reason=""
  if [[ -f "${LOG}" ]]; then
    reason=$(grep -m1 -E '^\s*Reason: ' "${LOG}" | sed -E 's/^\s*Reason: //') || true
    if [[ -z "${reason}" ]]; then
      reason=$(grep -E 'Check failed|terminate called|what\(\):|\[  FAILED  \]|Segmentation|Aborted|^E[0-9]{4}' "${LOG}" \
        | tail -n 1 | sed -E 's/^E[0-9]{4} [0-9:.]+ +[0-9]+ [^]]+\] //') || true
    fi
  fi
  # Drop numbers so that the same failure maps to the same issue across runs.
  echo "${reason:-unknown failure}" | sed -E 's/[0-9]+/N/g' | cut -c1-150
}

if [[ "${OUTCOME_CONFIGURE:-}" == "failure" || "${OUTCOME_BUILD:-}" == "failure" ]]; then
  stage="build"
  title="[flink-fuzzer] Build failed"
elif [[ "${OUTCOME_SQL_TEST:-}" == "failure" ]]; then
  stage="velox_flink_sql_test"
  title="[flink-fuzzer] velox_flink_sql_test failed"
elif [[ "${OUTCOME_FLINK:-}" == "failure" ]]; then
  stage="flink"
  title="[flink-fuzzer] Flink cluster or SQL Gateway failed to start"
elif [[ "${OUTCOME_RUNNER_TEST:-}" == "failure" ]]; then
  stage="velox_flink_query_runner_test"
  title="[flink-fuzzer] velox_flink_query_runner_test failed"
elif [[ "${OUTCOME_FUZZER:-}" == "failure" ]]; then
  stage="fuzzer"
  title="[flink-fuzzer] $(fuzzer_reason)"
else
  stage="workflow"
  title="[flink-fuzzer] Nightly workflow failed"
fi
echo "${title}" > "${OUT_DIR}/title.txt"

{
  echo "The nightly Flink expression fuzzer failed at stage \`${stage}\`."
  echo
  echo "| | |"
  echo "|---|---|"
  echo "| Run | ${RUN_URL:-} |"
  echo "| Ref | \`${FUZZ_REF:-}\` @ \`${SHA:-}\` |"
  echo "| Seed | \`${SEED:-n/a}\` |"
  echo "| Duration | ${DURATION_SEC:-} s |"
  echo "| Artifacts | ${ARTIFACT_URL:-see the run} |"
  echo

  if [[ "${stage}" == "fuzzer" && -f "${LOG}" ]]; then
    # The iteration seed reproduces the failing iteration as the first one.
    iteration_seed=$(grep -hoE 'Started iteration [0-9]+ \(seed: [0-9]+\)' \
      "${REPRO_DIR}"/logs/*.INFO.* 2>/dev/null | tail -n 1 \
      | sed -E 's/.*seed: ([0-9]+)\)/\1/') || true
    echo "### Reproduce"
    echo
    echo "Start Flink with \`scripts/flink/start-flink.sh\`, then run:"
    echo
    echo '```'
    echo "flink_expression_fuzzer_test --flink_gateway_url=http://localhost:8083 \\"
    echo "    --seed ${iteration_seed:-${SEED:-<seed>}} --steps 1 --logtostderr=1 --minloglevel=0"
    echo '```'
    echo
    echo "The artifacts contain the repro files written by \`--repro_persist_path\`"
    echo "(input vectors, SQL and expressions) and the tail of the fuzzer log."
    echo
    echo "### Failure"
    echo
    echo '```'
    # The exception and the expressions that led to it.
    grep -n -m1 -B2 -A40 -E '^\s*Reason: ' "${LOG}" | cut -c1-500 \
      || tail -n 80 "${LOG}" | cut -c1-500
    echo '```'
  else
    echo "See the logs of the failed step in the run."
  fi
} > "${OUT_DIR}/body.md"

echo "Title: ${title}"
