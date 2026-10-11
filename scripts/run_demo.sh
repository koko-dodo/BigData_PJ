#!/usr/bin/env bash
set -euo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
command -v spark-shell >/dev/null || { echo "Install Spark 3.5.1 (Scala 2.12) and add spark-shell to PATH." >&2; exit 127; }
command -v python3 >/dev/null || { echo "python3 is required to generate synthetic data." >&2; exit 127; }
mkdir -p "$PROJECT_ROOT/.demo-runs"
DEMO_RUN_ROOT="$(mktemp -d "$PROJECT_ROOT/.demo-runs/run.XXXXXX")"
export DEMO_RUN_ROOT
export SPARK_MASTER='local[2]' SHUFFLE_PARTITIONS=2 SPARK_DRIVER_MEMORY=1g
export RAW_INPUT_PATH="$DEMO_RUN_ROOT/raw" CLEAN_OUTPUT_PATH="$DEMO_RUN_ROOT/clean"
export INPUT_PATH="$CLEAN_OUTPUT_PATH" OUTPUT_ROOT="$DEMO_RUN_ROOT/results" METRIC_TYPE=service
export FINAL_RULE=and Z_THRESHOLD=3.0 WINDOW_ROWS=100 MIN_WINDOW_POINTS=30 TOP_K=30 MIN_ANOMALY_METRICS=2
python3 "$PROJECT_ROOT/scripts/generate_demo.py" "$RAW_INPUT_PATH"
for stage in clean v1 v2 v3 v4; do
  bash "$PROJECT_ROOT/scripts/run_cluster.sh" "$stage" 2>&1 | tee "$DEMO_RUN_ROOT/$stage.log"
done
METRIC_TYPES=service,node,jvm,istio,container OUTPUT_ROOT="$DEMO_RUN_ROOT/batch-results" WRITE_SUMMARY=true \
  bash "$PROJECT_ROOT/scripts/run_cluster.sh" v2-all 2>&1 | tee "$DEMO_RUN_ROOT/v2-all.log"
cd "$PROJECT_ROOT"
printf ":load scripts/verify_demo.scala\n:quit\n" | \
  spark-shell --master "$SPARK_MASTER" --driver-memory 1g --conf spark.sql.shuffle.partitions=2 \
  2>&1 | tee "$DEMO_RUN_ROOT/verification.log"
[[ -f "$DEMO_RUN_ROOT/VERIFIED" ]] || { echo "Verification failed; inspect $DEMO_RUN_ROOT" >&2; exit 1; }
echo "Verified synthetic demo: $DEMO_RUN_ROOT"
