#!/usr/bin/env bash
set -euo pipefail

# Default to `shellcheck` only when SHELLCHECK is unset; an explicit empty value
# (e.g. SHELLCHECK="") must fail closed rather than silently falling back.
if [ "${SHELLCHECK+set}" = "set" ]; then
  shellcheck_bin="$SHELLCHECK"
else
  shellcheck_bin="shellcheck"
fi

scripts=()
if [ "$#" -gt 0 ]; then
  scripts=("$@")
else
  while IFS= read -r -d '' file; do
    scripts+=("$file")
  done < <(find . -path './.*' -prune -o -type f -name '*.sh' -print0)
fi

if [ "${#scripts[@]}" -eq 0 ]; then
  exit 0
fi

if [ -z "$shellcheck_bin" ] || ! command -v -- "$shellcheck_bin" >/dev/null 2>&1; then
  echo "missing ${shellcheck_bin:-shellcheck}; install via 'brew install shellcheck' (macOS) or your system package manager" >&2
  exit 1
fi

for s in "${scripts[@]}"; do
  bash -n "$s"
done

"$shellcheck_bin" "${scripts[@]}"
