#!/usr/bin/env bash
set -euo pipefail

base_revision="${1:?Usage: palp-build-needed.sh <base-revision> <head-revision>}"
head_revision="${2:?Usage: palp-build-needed.sh <base-revision> <head-revision>}"

if [[ "$base_revision" =~ ^0+$ ]]; then
  printf '%s\n' true
  exit 0
fi

palp_gitlink() {
  local revision="$1"
  local entry mode type object path

  entry="$(git ls-tree "$revision" -- PALP)"
  [[ -n "$entry" ]] || return 1
  read -r mode type object path <<<"$entry"
  [[ "$mode" == 160000 && "$type" == commit ]] || return 1
  printf '%s\n' "$object"
}

if ! base_palp="$(palp_gitlink "$base_revision")"; then
  if palp_gitlink "$head_revision" >/dev/null; then
    # PALP was added in this revision range, so there is no earlier source tree
    # to compare against.
    printf '%s\n' true
  else
    printf '%s\n' false
  fi
  exit 0
fi

if ! head_palp="$(palp_gitlink "$head_revision")"; then
  printf '%s\n' false
  exit 0
fi

if [[ "$base_palp" == "$head_palp" ]]; then
  printf '%s\n' false
elif git -C PALP diff --quiet "$base_palp" "$head_palp" -- '*.c' '*.h' Makefile GNUmakefile; then
  printf '%s\n' false
else
  printf '%s\n' true
fi
