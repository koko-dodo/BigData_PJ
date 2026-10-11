#!/usr/bin/env bash
set -euo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
stage="${1:-}"
case "$stage" in
  clean|v1|v2|v2-all|v3|v4) ;;
  *) echo "Usage: bash scripts/run_cluster.sh {clean|v1|v2|v2-all|v3|v4} [trusted-config.env]" >&2; exit 2 ;;
esac
if [[ $# -gt 1 ]]; then
  [[ -f "$2" ]] || { echo "Config not found: $2" >&2; exit 2; }
  set -a
  # Configuration is shell code. Only load files you trust.
  source "$2"
  set +a
fi
command -v spark-shell >/dev/null || { echo "spark-shell is required (validated project version: Spark 3.5.1 / Scala 2.12)." >&2; exit 127; }
: "${SPARK_MASTER:?Set SPARK_MASTER}"
: "${SHUFFLE_PARTITIONS:=32}"
[[ "$SHUFFLE_PARTITIONS" =~ ^[1-9][0-9]*$ ]] || { echo "SHUFFLE_PARTITIONS must be positive" >&2; exit 2; }
export SHUFFLE_PARTITIONS
args=(--master "$SPARK_MASTER" --driver-memory "${SPARK_DRIVER_MEMORY:-3g}" --conf "spark.sql.shuffle.partitions=$SHUFFLE_PARTITIONS")
if [[ "$SPARK_MASTER" == spark://* ]]; then
  args+=(--executor-memory "${SPARK_EXECUTOR_MEMORY:-2g}" --total-executor-cores "${SPARK_TOTAL_EXECUTOR_CORES:-4}")
fi
case "$stage" in
  clean)
    : "${RAW_INPUT_PATH:?Set RAW_INPUT_PATH}" "${CLEAN_OUTPUT_PATH:?Set CLEAN_OUTPUT_PATH}"
    export RAW_INPUT_PATH CLEAN_OUTPUT_PATH
    script="$PROJECT_ROOT/scripts/cleaning/clean_metrics.scala"
    ;;
  *)
    : "${INPUT_PATH:?Set INPUT_PATH}" "${OUTPUT_ROOT:?Set OUTPUT_ROOT to a dedicated run directory}"
    case "$stage" in
      v1) name=iqr ;;
      v2) name=iqr_zscore ;;
      v2-all) name=iqr_zscore_all_types ;;
      v3) name=window_zscore ;;
      v4) name=multimetric_fusion ;;
    esac
    output_name="$name"
    [[ "$stage" != v2-all ]] || output_name=iqr_zscore
    export INPUT_PATH OUTPUT_PATH="${OUTPUT_ROOT%/}/$output_name"
    script="$PROJECT_ROOT/scripts/detection/$name.scala"
    ;;
esac
echo "Running $stage on $SPARK_MASTER. Existing outputs for this stage/type are overwritten."
# spark-shell may return zero after an initialization error; verify artifacts too.
cd "$PROJECT_ROOT"
printf ":load %s\n:quit\n" "${script#"$PROJECT_ROOT/"}" | spark-shell "${args[@]}"
