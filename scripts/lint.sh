#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
palp_root="$repo_root/PALP"
mode="${1:---check}"

case "$mode" in
--check) ;;
--fix) ;;
*)
  echo "Usage: $0 [--check|--fix]" >&2
  exit 2
  ;;
esac

cd "$repo_root"

mapfile -d '' -t c_files < <(git -C "$palp_root" ls-files -z -- '*.c' '*.h')
mapfile -d '' -t root_markdown < <(git ls-files -z -- '*.md' '*.markdown')
mapfile -d '' -t palp_markdown_paths < <(git -C "$palp_root" ls-files -z -- '*.md' '*.markdown' 'tests/README')
mapfile -d '' -t root_shell < <(git ls-files -z -- '*.sh' '*.bash' '.githooks/commit-msg')
mapfile -d '' -t palp_shell_paths < <(git -C "$palp_root" ls-files -z -- '*.sh' '*.bash')

markdown=("${root_markdown[@]}")
shell_files=("${root_shell[@]}")
for path in "${palp_markdown_paths[@]}"; do
  markdown+=("PALP/$path")
done
for path in "${palp_shell_paths[@]}"; do
  shell_files+=("PALP/$path")
done

if [[ "$mode" == --fix ]]; then
  (cd "$palp_root" && clang-format -i "${c_files[@]}")
  prettier --parser markdown --write "${markdown[@]}"
  shfmt -w -i 2 "${shell_files[@]}"
else
  (cd "$palp_root" && clang-format --dry-run --Werror "${c_files[@]}")
  prettier --parser markdown --check "${markdown[@]}"
  shfmt -d -i 2 "${shell_files[@]}"
fi
