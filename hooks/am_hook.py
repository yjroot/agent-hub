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
# am 실행 파일 위치는 이 훅 파일에서 유도한다 — 절대경로를 박아 두면 설치 위치가
# 다른 머신에서 훅이 조용히 죽는다(공개 전 감사에서 발견).
_HUB_ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
AM = os.environ.get("AM_BIN", os.path.join(_HUB_ROOT, "cli", "am"))


def worker(method, path, body=None, timeout=1.5):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{WORKER}{path}", data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


# ⚠️ 웨이크 소켓 주소는 훅이 보내지 않는다 (일부러 뺐다).
#
# 훅은 CLAUDE_CODE_MESSAGING_SOCKET 을 env 로 알고 있지만, 그 값을 로컬 API 로 실어
# 보내면 '자가 신고 주소'가 된다 — 로컬 API 는 127.0.0.1 이어도 같은 머신의 아무
# 프로세스나 호출할 수 있으므로, 남의 세션 이름으로 자기 소켓을 실어 보내면 그 뒤 그
# 에이전트 앞으로 온 메시지가 통째로 공격자에게 배달되고 원 수신자는 무음 유실된다
# (적대 리뷰가 E2E 로 재현). 그래서 워커가 ~/.claude/sessions 레지스트리에서 직접 읽은
# 값만 쓴다. 실측(2026-08-22): SessionStart 훅이 발화하는 시점에 그 레지스트리 항목은
# 이미 존재하고 messagingSocketPath 도 env 값과 일치한다 — 훅이 실어 줄 이유가 없다.
# CLAUDE_CODE_MESSAGING_TOKEN 도 같은 이유로 보내지 않는다(워커가 0600 키파일에서 읽는다).


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
            sys.path.insert(0, os.path.dirname(os.path.dirname(
                os.path.realpath(__file__))))
            from common.cmux import resolve_self
            _cmux = resolve_self()          # 실측 132ms — 2초 예산 안
        except Exception:  # noqa: BLE001
            _cmux = ("", "")
        reg = None
        try:
            reg = worker("POST", "/register", {
                "session": session, "name": os.environ.get("AM_NAME", ""),
                # am hire 가 기동 env 로 실어 준 배속 — SessionStart 시점에 이미
                # 역할이 서 있어야 신입이 첫 화면부터 자기 위치를 안다(미배정으로
                # 한 세션을 보내는 함정의 근본 치유). 빈 값은 relay 가 무시한다.
                "role": os.environ.get("AM_ROLE", ""),
                "reports_to": os.environ.get("AM_REPORTS_TO", ""),
                "cwd": data.get("cwd", ""), "cli": "claude",
                # AM_EPHEMERAL=1 로 띄운 프로브·일회용 세션은 로스터에서 감춘다
                # (검증 세션이 실 에이전트 목록을 밀어내던 실측 반영).
                "ephemeral": bool(os.environ.get("AM_EPHEMERAL")),
                # 🔑 권한 모드 attest 의 원천. 수신 세션이 bypass 계급이면 발신자가
                # 모드를 밝히지 않는 한 CC 가 **무조건 hold** 한다(번들 게이트 실측).
                # 훅 입력에만 실려 오므로 여기서 걷어 두지 않으면 정직한 attest 가 불가능하다.
                "permission_mode": data.get("permission_mode", ""),
                # cmux 워크스페이스(=팀) 해석용 조인 키. 지도는 워커가 cmux 에서 읽는다.
                # 팀 정본은 cmux 워크스페이스. 해석은 **세션 안에서** 한다 —
                # 워커(launchd)엔 cmux 소켓 capability 가 없어 조용히 빈 결과가 온다.
                "team": _cmux[0], "cmux_title": _cmux[1],
                "home": os.environ.get("HUB_HOME", "local")})
        except Exception:  # noqa: BLE001
            pass
        # 🔑 스스로의 역할·위치를 매 세션 시작에 명시한다. 강제 라우팅이 없는 모델이라
        # (사용자 결정) 각자가 자기 자리를 아는 것이 규약의 유일한 지반이다.
        org = (reg or {}).get("org") or {}
        role_ko = {"chairman": "회장", "secretary": "비서", "lead": "팀장",
                   "member": "팀원"}.get(org.get("role") or "")
        if role_ko:
            where = f"너는 **{org.get('team') or '(팀 미지정)'} 팀의 {role_ko}**"
            if org.get("reports_to"):
                where += f", 보고선은 `{org['reports_to']}`"
            place = (f"[agent-hub 조직] {where} 이다. "
                     "지시·보고·에스컬레이션은 **보고선을 따라** 주고받아라(라인을 건너뛰면 "
                     "차단되진 않지만 `am board` 에 '라인밖'으로 남는다). "
                     "기술 질의(`am ask --owner-of <파일>`)는 팀 경계를 넘어도 된다 — "
                     "그게 이 도구의 존재 이유다. 전체 조직도는 `am board`.")
        else:
            place = ("[agent-hub 조직] 네 역할이 아직 미배정이다. "
                     "`am register --role member --team <팀> --reports-to <팀장>` 으로 "
                     "선언하거나 팀장에게 배정을 요청해라. 조직도는 `am board`.")
        intro = (
            "[agent-hub] 이 머신에는 에이전트 간 메신저 `am` 이 있다 (Bash 로 호출). "
            "다른 세션이 작성한 코드의 의도가 궁금하면 `am who --path <파일>` 로 저자를 찾고 "
            "`am ask --owner-of <파일> --blocking \"질문\"` 으로 물어라(저자 세션이 종료됐어도 "
            "복원되어 답한다). 여러 세션이 겹칠 만한 범위를 만질 땐 `am claim --paths <글롭>` 으로 "
            "선언하라. 전체 현황은 `am agents`. 사소한 질문에 남용하지 말 것 — 부활 응답은 유료다.")
        # codex 팀원의 push 채널(탭 초인종)을 도는 벨 데몬을 여기서 보증한다.
        # 워커는 launchd 라 cmux 인가가 없어 스스로 못 띄운다 — cmux 안에서 도는
        # 이 훅이 부트스트랩 지점이다. 이미 살아 있으면 헬스체크 한 번으로 끝난다.
        try:
            from common.bell_boot import ensure_bell
            ensure_bell()
        except Exception:  # noqa: BLE001
            pass
        ctx = inbox_context(session)
        print(json.dumps({"hookSpecificOutput": {
            "hookEventName": "SessionStart",
            "additionalContext": ((ctx + "\n" if ctx else "") + place + "\n" + intro)}}))
    elif event in ("user_prompt_submit", "post_tool_use"):
        if event == "user_prompt_submit" and data.get("prompt"):
            try:  # 첫 프롬프트를 task 로 (registry 조망성 — 비어 있을 때만 반영됨)
                # 소켓 주소는 여기서도 싣지 않는다 — 워커가 레지스트리에서 직접 갱신한다
                worker("POST", "/register",
                       {"session": session, "cwd": data.get("cwd", ""),
                        "partial": True, "task_hint": data["prompt"][:120],
                        # 모드는 세션 도중 바뀐다(/permissions) — 매 프롬프트마다 갱신
                        "permission_mode": data.get("permission_mode", "")})
            except Exception:  # noqa: BLE001
                pass
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
