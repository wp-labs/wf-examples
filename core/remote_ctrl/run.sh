#!/usr/bin/env bash
# remote_ctrl — demonstrate `wfadm conf update` remote rule-source sync.
#
# Mirrors wparse's wp-examples/core/remote_ctrl but adapted to wfusion's
# current capability: `wfadm conf update` (offline sync + validate). wfusion
# does not yet have `init --repo` or admin_api reload, so this example seeds
# the work root from wf-conf-example and exercises version switching on the
# models group (wf-rules: v0.1.0 → v0.1.1).
#
# Dual-repo layout:
#   - infra  ← wf-conf-example (conf/topology/connectors)
#   - models ← wf-rules        (models/), the group we switch versions on
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
if [[ ! -f "$WORK_ROOT/models/rules/01-recon/port_scan.wfl" ]]; then
  echo "Error: expected rule file missing after sync"
  exit 1
fi
echo "   models/rules present ✓"

echo "PASS: remote conf update switched models group $INIT_VERSION → $TARGET_VERSION"
