#!/usr/bin/env bash
set -euo pipefail

message_file="${1:?Git must pass the commit message file}"
subject="$(sed -n '1{s/[[:space:]]*$//;p;}' "$message_file")"

if [[ -z "$subject" ]]; then
  echo "Commit message must not be empty." >&2
  exit 1
fi

if [[ ! "$subject" =~ ^(feat|fix|chore|task|docs|test|refactor|build|ci|perf|style|revert)(\([[:alnum:]._/-]+\))?(!)?:[[:space:]]+[^[:space:]].*$ ]]; then
  echo "Invalid commit subject: $subject" >&2
  echo "Use <type>(optional-scope): <description>; allowed types: feat, fix, chore, task, docs, test, refactor, build, ci, perf, style, revert." >&2
  exit 1
fi
