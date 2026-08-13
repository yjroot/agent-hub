#!/usr/bin/env python3
"""Claude Code 훅 진입점 (v0). 설계 §2-3.

settings.json 에서 이벤트별로 `am_hook.py <event>` 로 호출된다. 모든 등록에 "timeout": 2 필수.
- session_start: register + 밀린 inbox 주입
- user_prompt_submit: inbox 주입 (유휴 복귀 배달 지점)
- post_tool_use: inbox 요약을 additionalContext 로 주입
- session_end: dormant 전환 보고
응답자 세션(agent-hub-responder *)은 모든 진입점에서 조기 반환 (설계 §2-3).
"""
import json
import os
import subprocess
import sys
import urllib.request

WORKER = os.environ.get("HUB_WORKER", "http://127.0.0.1:8791")
AM = os.environ.get("AM_BIN", "am")


def worker(method, path, body=None, timeout=1.5):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{WORKER}{path}", data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def inbox_context(session):
    try:
        out = subprocess.run(
            [AM, "inbox", "--check"], capture_output=True, text=True, timeout=1.8,
            env={**os.environ, "CLAUDE_CODE_SESSION_ID": session})
        return out.stdout.strip() if out.returncode == 0 and out.stdout.strip() else None
    except Exception:  # noqa: BLE001 — 페일오픈
        return None


def main():
    event = sys.argv[1] if len(sys.argv) > 1 else ""
    try:
        data = json.load(sys.stdin)
    except json.JSONDecodeError:
        sys.exit(0)
    session = data.get("session_id", "")
    if not session:
        sys.exit(0)
    # 응답자 세션 제외 — 세션명 확인 수단이 훅 입력에 없으므로 env 마커 사용
    if os.environ.get("AGENT_HUB_RESPONDER"):
        sys.exit(0)

    if event == "session_start":
        try:
            worker("POST", "/register", {
                "session": session, "name": os.environ.get("AM_NAME", ""),
                "cwd": data.get("cwd", ""), "cli": "claude",
                "home": os.environ.get("HUB_HOME", "local")})
        except Exception:  # noqa: BLE001
            pass
        ctx = inbox_context(session)
        if ctx:
            print(json.dumps({"hookSpecificOutput": {
                "hookEventName": "SessionStart", "additionalContext": ctx}}))
    elif event in ("user_prompt_submit", "post_tool_use"):
        ctx = inbox_context(session)
        if ctx:
            name = "UserPromptSubmit" if event == "user_prompt_submit" else "PostToolUse"
            print(json.dumps({"hookSpecificOutput": {
                "hookEventName": name, "additionalContext": ctx}}))
    elif event == "session_end":
        try:
            commit = subprocess.run(
                ["git", "-C", data.get("cwd", "."), "rev-parse", "--short", "HEAD"],
                capture_output=True, text=True, timeout=1).stdout.strip()
            worker("POST", "/liveness",
                   {"agents": [], "session_ended": session, "commit": commit})
        except Exception:  # noqa: BLE001
            pass
    sys.exit(0)


if __name__ == "__main__":
    main()
