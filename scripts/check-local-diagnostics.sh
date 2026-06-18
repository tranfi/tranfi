#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

patterns=(
  label_write_error
  label_limit_error
  across_write_error
  stack_write_error
  pivot_write_error
  onehot_write_error
  onehot_limit_error
  join_write_error
  join_limit_error_context
  join_limit_error
  unique_write_error
  unique_limit_error
  set_write_error
  set_limit_error
  select_write_error
  relocate_write_error
  frequency_write_error
  frequency_limit_error
  group_agg_write_error
  group_agg_limit_error
)

fail=0
for fn in "${patterns[@]}"; do
  while IFS=: read -r file line text; do
    case "$text" in
      *"return ${fn}"*|*"if (${fn}"*|*"if (!${fn}"*|*"int err_rc = ${fn}"*|*"int rc = ${fn}"*|*"TF_WARN_UNUSED static int ${fn}"*)
        continue
        ;;
    esac
    printf '%s:%s: ignored %s result: %s\n' "$file" "$line" "$fn" "$text"
    fail=1
  done < <(grep -nE "\\b${fn}\\([^;]*;" src/*.c || true)
done

exit "$fail"
