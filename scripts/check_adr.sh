#!/usr/bin/env bash
set -euo pipefail

failed=0

# 1. Verify ADR numeric prefixes under docs/adr/ are unique.
if [ -d docs/adr ]; then
  dups="$(
    shopt -s nullglob
    for f in docs/adr/[0-9][0-9][0-9][0-9]-*; do
      base="${f##*/}"
      printf '%s\n' "${base%%-*}"
    done | sort | uniq -d
  )"
  if [ -n "$dups" ]; then
    printf 'Duplicate ADR numeric prefixes:\n%s\n' "$dups" >&2
    failed=1
  fi
fi

# 2. Reject bare ADR-NNNN references in code/scripts (require full docs/adr/NNNN-*.md paths).
code_dirs=()
for dir in cmd internal tests scripts migrations; do
  if [ -d "$dir" ]; then
    code_dirs+=("$dir")
  fi
done

if [ "${#code_dirs[@]}" -gt 0 ]; then
  set +e
  code_bare="$(grep -rnE --exclude='*.md' '(^|[^0-9A-Za-z])ADR-[0-9]{4}([^0-9]|$)' "${code_dirs[@]}")"
  grep_rc=$?
  set -e
  if [ "$grep_rc" -ne 0 ] && [ "$grep_rc" -ne 1 ]; then
    exit "$grep_rc"
  fi
  if [ -n "$code_bare" ]; then
    printf 'Bare ADR references in code/scripts (use full repo path docs/adr/NNNN-*.md):\n%s\n' "$code_bare" >&2
    failed=1
  fi
fi

# 3. Reject bare ADR-NNNN references in Markdown (require links to NNNN-*.md; allow H1 titles in docs/adr/).
md_files=()
while IFS= read -r -d '' file; do
  md_files+=("$file")
done < <(
  find . \
    \( -path './.git' -o -path './.worktrees' -o -path './.agents' -o -path './.claude' -o -path './.build' \) -prune \
    -o -type f -name '*.md' -print0
)

if [ "${#md_files[@]}" -gt 0 ]; then
  doc_bare="$(
    awk '
      {
        clean = FILENAME
        sub(/^\.\//, "", clean)
        if (clean ~ /^docs\/adr\/[0-9][0-9][0-9][0-9]-.*\.md$/ && FNR == 1 && $0 ~ /^# ADR-[0-9][0-9][0-9][0-9]: /) {
          next
        }
        line = $0
        gsub(/\[([^][]|\[[^][]*\])*\]\(([^)]*\/)?[0-9][0-9][0-9][0-9]-[^)]+\.md(#[^)]+)?\)/, " ", line)
        if (" " line " " ~ /[^0-9A-Za-z]ADR-[0-9][0-9][0-9][0-9][^0-9]/) {
          printf "%s:%d:%s\n", clean, FNR, $0
        }
      }
    ' "${md_files[@]}"
  )"
  if [ -n "$doc_bare" ]; then
    printf 'Bare ADR references in Markdown (link to docs/adr/NNNN-*.md):\n%s\n' "$doc_bare" >&2
    failed=1
  fi
fi

exit "$failed"
