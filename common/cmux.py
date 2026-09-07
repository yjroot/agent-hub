"""cmux 조직 축 해석 — 세션이 **자기** 워크스페이스를 스스로 알아낸다.

🔑 정본은 cmux 다. 이 머신의 Claude 세션은 전부 cmux 가 띄우고, 사용자가 손으로
워크스페이스(=팀)와 서피스 제목(=역할·과제)을 붙여 놨다 — 실측: 워크스페이스 '팀A'
안에 사장·비서·팀G·#N…. am 이 --team 을 따로 선언받으면 같은 것을 두 곳에서
관리하게 되고, 둘이 어긋나면 어느 쪽이 참인지 아무도 모른다.

🪤 조회는 **세션 안에서** 해야 한다. cmux 소켓은 세션 env 의 capability 로 인가하는데,
launchd 로 뜬 워커에는 그게 없어 조용히 빈 출력을 준다(실측: 워커에서 지도 0건,
같은 코드가 세션에서는 31건). 그래서 해석 주체는 워커가 아니라 훅·CLI 다.
"""
import json
import os
import subprocess

CMUX_BIN = os.environ.get(
    "CMUX_BIN", "/Applications/cmux.app/Contents/Resources/bin/cmux")


def _run(args, timeout=2.0):
    try:
        out = subprocess.run([CMUX_BIN, *args], capture_output=True, text=True,
                             timeout=timeout).stdout
        return json.loads(out) if out.strip() else None
    except Exception:  # noqa: BLE001
        return None          # cmux 밖(터미널 직접 실행)에서도 조용히 동작해야 한다


def resolve_self():
    """내 (워크스페이스 제목, 서피스 제목). 못 찾으면 ("", "").

    실측 20ms/호출 — 워크스페이스를 순회해도 훅 예산 안이다(최악 8회 ≈ 160ms).
    """
    me = os.environ.get("CMUX_SURFACE_ID", "")
    if not me:
        return "", ""
    ws = _run(["workspace", "list", "--json", "--id-format", "both"])
    for w in (ws or {}).get("workspaces", []):
        ref = w.get("ref")
        if not ref:
            continue
        d = _run(["list-pane-surfaces", "--workspace", ref, "--json",
                  "--id-format", "both"])
        for sf in (d or {}).get("surfaces", []):
            if (sf.get("id") or sf.get("uuid")) == me:
                return (w.get("custom_title") or w.get("title") or ref,
                        sf.get("title") or "")
    return "", ""


def cmux_failed(rc, stdout, stderr):
    """cmux CLI 호출이 실패했는가. 실패면 사유 문자열, 성공이면 None.

    🔴 **cmux 는 실패해도 종료코드 0 을 돌려준다.** 오류를 본문에 찍을 뿐이다
    (실측 2026-09-08):
        성공 → stdout "OK surface:17 workspace:6"        rc=0
        실패 → stdout "Error: Surface is not a terminal"  rc=0
        실패 → stdout "Error: Workspace not found"        rc=0
    rc 만 보면 **모든 실패가 성공으로 계상된다**. 실제 피해: 닫힌 탭에 초인종을
    울리고 `submit=ok` 를 기록했다. 배달은 안 됐는데 재시도 상한만 소진되고,
    '탭 주소가 죽었다' 판정 경로는 영영 발화하지 않는다. 수신자는 지시가 온 줄도
    모른 채 유휴로 남고, 로스터의 idle 이 자라 팀장은 「죽었다」로 읽는다.
    """
    if rc != 0:
        return f"rc={rc} {(stderr or stdout or '').strip()[:160]}"
    for stream in (stdout, stderr):
        s = (stream or "").strip()
        if s.startswith("Error:"):
            return s[:160]
    return None
