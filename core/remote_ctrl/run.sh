#!/usr/bin/env bash
# remote_ctrl — demonstrate `wfadm init --repo` + `wfadm conf update` remote
# rule-source sync. Mirrors wparse's wp-examples/core/remote_ctrl.
#
# Flow:
#   1. `wfadm init --repo wf-conf-example@v0.1.1` bootstraps the work root;
#      the pulled conf/wfusion.toml already carries [project_remote] dual-repo
#      config (models=wf-rules, infra=wf-conf-example).
#   2-3. `wfadm conf update --group models` switches the models group from
#      wf-rules v0.1.0 → v0.1.1.
#   4. Verify models were synced from wf-rules (rules use the 01-recon/...
#      layout that wf-conf-example does not have).
#
# wfusion does not yet have admin_api reload (online switch); this example
# covers the offline `conf update` path only.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

CONF_REPO="${CONF_REPO:-https://github.com/wp-labs/wf-conf-example.git}"
# wf-conf-example tag carrying the [project_remote] config (bootstrap target)
CONF_INIT_VERSION="${CONF_INIT_VERSION:-0.1.1}"
# wf-rules models-group versions (first sync → switch)
INIT_VERSION="${INIT_VERSION:-0.1.0}"
TARGET_VERSION="${TARGET_VERSION:-0.1.1}"
WORK_ROOT="${WORK_ROOT:-$PWD/.tmp-work}"

STATE_FILE="$WORK_ROOT/.run/project_remote_state.json"

for cmd in wfadm; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Error: command '$cmd' not found in PATH"
    exit 1
  fi
done

echo "1> bootstrap work root from $CONF_REPO @ $CONF_INIT_VERSION via wfadm init --repo"
rm -rf "$WORK_ROOT"
wfadm init --dir "$WORK_ROOT" --repo "$CONF_REPO" --version "$CONF_INIT_VERSION" >/dev/null

echo "2> conf update models group to $INIT_VERSION (rules from wf-rules)"
wfadm conf update --work-root "$WORK_ROOT" --group models --version "$INIT_VERSION" --json

if [[ ! -f "$STATE_FILE" ]]; then
  echo "Error: state file not created at $STATE_FILE"
  exit 1
fi
if ! grep -Eq "\"version\"[[:space:]]*:[[:space:]]*\"$INIT_VERSION\"" "$STATE_FILE"; then
  echo "Error: models group did not sync to $INIT_VERSION"
  cat "$STATE_FILE"
  exit 1
fi
echo "   models group at $INIT_VERSION ✓"

echo "3> conf update models group to $TARGET_VERSION (version switch)"
wfadm conf update --work-root "$WORK_ROOT" --group models --version "$TARGET_VERSION" --json

if ! grep -Eq "\"version\"[[:space:]]*:[[:space:]]*\"$TARGET_VERSION\"" "$STATE_FILE"; then
  echo "Error: models group did not switch to $TARGET_VERSION"
  cat "$STATE_FILE"
  exit 1
fi
echo "   models group switched to $TARGET_VERSION ✓"

echo "4> verify models dir was synced from wf-rules"
# wf-rules uses categorized rule dirs (models/rules/01-recon/...); wf-conf-example
# uses a flat layout (models/rules/port_scan.wfl). The 01-recon path is therefore
# wf-rules-specific — its presence proves the models group was replaced.
if [[ ! -f "$WORK_ROOT/models/rules/01-recon/port_scan.wfl" ]]; then
  echo "Error: expected wf-rules rule layout missing after sync"
  exit 1
fi
echo "   models/rules present ✓"

echo "PASS: remote conf update switched models group $INIT_VERSION → $TARGET_VERSION"
