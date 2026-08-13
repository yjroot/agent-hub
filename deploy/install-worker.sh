#!/bin/bash
# hub-worker 설치 (macOS launchd). desktop 등 새 머신에서 실행.
# 사용: HUB_WORKER_TOKEN=<토큰> HUB_HOME=<머신이름> ./install-worker.sh
# 사전: 이 레포 clone + python3 + (부활용) claude/codex CLI. 토큰은 relay Secret 에 등록돼 있어야 한다:
#   kubectl -n agent-hub edit secret agent-hub-tokens  (HUB_WORKER_TOKENS="mac:tok1,desktop:tok2")
set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
: "${HUB_WORKER_TOKEN:?HUB_WORKER_TOKEN 필요}"
: "${HUB_HOME:?HUB_HOME(머신 이름) 필요}"
HUB_RELAY="${HUB_RELAY:-http://RELAY_HOST:8790}"
PY="$(command -v python3)"
PLIST=~/Library/LaunchAgents/dev.agent-hub.worker.plist

mkdir -p ~/Library/LaunchAgents
cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>dev.agent-hub.worker</string>
  <key>ProgramArguments</key>
  <array><string>${PY}</string><string>${REPO_DIR}/worker/worker.py</string></array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>HUB_RELAY</key><string>${HUB_RELAY}</string>
    <key>HUB_WORKER_TOKEN</key><string>${HUB_WORKER_TOKEN}</string>
    <key>HUB_HOME</key><string>${HUB_HOME}</string>
    <key>PATH</key><string>${HOME}/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
  </dict>
  <key>KeepAlive</key><true/>
  <key>RunAtLoad</key><true/>
  <key>StandardOutPath</key><string>/tmp/agent-hub-worker.log</string>
  <key>StandardErrorPath</key><string>/tmp/agent-hub-worker.log</string>
</dict>
</plist>
EOF
sudo_needed=""
[ -w /opt/homebrew/bin ] && ln -sf "$REPO_DIR/cli/am" /opt/homebrew/bin/am || sudo_needed="am 심링크는 수동으로: sudo ln -sf $REPO_DIR/cli/am /usr/local/bin/am"
launchctl bootout "gui/$(id -u)/dev.agent-hub.worker" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$PLIST"
sleep 2
curl -s -m 3 http://127.0.0.1:8791/health && echo
echo "설치 완료. ${sudo_needed}"
echo "레포별 훅: .claude/settings.local.json 에 hooks 4종 + Bash(am:*) — README 참조"
