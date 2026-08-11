#!/usr/bin/env bash
set -euo pipefail

: "${SM_PATH:?Set SM_PATH to the addons/sourcemod directory}"

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$repo_dir/build"

"$SM_PATH/scripting/spcomp" \
  "$repo_dir/sourcemod/scripting/plugin_statistics.sp" \
  -i "$SM_PATH/scripting/include" \
  -i "$repo_dir/sourcemod/scripting/include" \
  -o"$repo_dir/build/plugin_statistics.smx"
