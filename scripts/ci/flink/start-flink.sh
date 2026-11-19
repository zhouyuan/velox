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

# Starts a local Flink cluster and the Flink SQL Gateway used by the Flink query
# runner of the Flink expression fuzzer. Requires FLINK_HOME to point to a Flink
# 1.19 distribution, e.g. one extracted from
# https://archive.apache.org/dist/flink/flink-1.19.2/flink-1.19.2-bin-scala_2.12.tgz.
#
# The gateway listens at http://localhost:${FLINK_GATEWAY_PORT:-8083}.

set -e

: "${FLINK_HOME:?FLINK_HOME must be set}"
FLINK_GATEWAY_PORT="${FLINK_GATEWAY_PORT:-8083}"

"$FLINK_HOME"/bin/start-cluster.sh

# The fuzzer keeps one session open for the whole run, so raise the idle
# timeout above the default 10 minutes.
"$FLINK_HOME"/bin/sql-gateway.sh start \
  -Dsql-gateway.endpoint.rest.address=localhost \
  -Dsql-gateway.endpoint.rest.port="$FLINK_GATEWAY_PORT" \
  -Dsql-gateway.session.idle-timeout=6h

# Wait for the gateway to accept requests.
for _ in $(seq 1 60); do
  if curl -sf "http://localhost:${FLINK_GATEWAY_PORT}/v1/info" >/dev/null; then
    echo "Flink SQL Gateway is ready at http://localhost:${FLINK_GATEWAY_PORT}"
    exit 0
  fi
  sleep 2
done
echo "Flink SQL Gateway did not start in time" >&2
exit 1
