#!/usr/bin/env python3
"""cmux 초인종 데몬 — codex 팀원 탭에 키를 쳐 넣는 유일한 합법 경로.

왜 별도 프로세스인가:
  cmux CLI 는 **cmux 안에서 시작된 프로세스만** 접속을 허용한다
  (실측 거부 메시지: "Access denied - only processes started inside cmux can
  connect"). launchd 로 뜨는 hub-worker 는 그 바깥이라 영원히 거부당한다.
  반면 인가는 **기동 시점에 상속**되고 고아가 되어도 남는다(이중 포크 실측:
  부모가 죽어 launchd 로 재부모화된 뒤에도 cmux 호출 성공). 그래서 cmux 세션
  안에서 한 번만 띄워 두면, 그 탭이 닫혀도 계속 종을 칠 수 있다.

왜 임의 문자열을 안 받는가:
  남의 탭에 아무 텍스트나 타이핑해 주는 로컬 엔드포인트는 그 자체가 권한 상승
  통로다(타이핑된 줄은 그 에이전트의 프롬프트가 된다). 그래서 이 데몬은 본문을
  받지 않는다 — 개수 n 만 받고 문구는 common.envelope.doorbell_line 이 만든다.
  토큰까지 요구하지만, 본문 고정이 더 강한 방어선이다.
"""
import json
import os
import secrets
import subprocess
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from common.cmux import cmux_failed  # noqa: E402
from common.envelope import doorbell_line  # noqa: E402

HUB_DIR = os.environ.get("HUB_DIR") or os.path.expanduser("~/.agent-hub")
TOKEN_PATH = os.path.join(HUB_DIR, "bell.token")
BELL_PORT = int(os.environ.get("HUB_BELL_PORT", "8792"))
SUBMIT_GAP_S = 0.6
CMUX_BIN = os.environ.get("CMUX_BIN") or next(
    (p for p in ("/Applications/cmux.app/Contents/Resources/bin/cmux",
                 os.path.expanduser(
                     "~/Applications/cmux.app/Contents/Resources/bin/cmux"))
     if os.path.exists(p)), None)


def load_token(create=False):
    try:
        with open(TOKEN_PATH) as f:
            t = f.read().strip()
        if t:
            return t
    except OSError:
        pass
    if not create:
        return ""
    os.makedirs(HUB_DIR, exist_ok=True)
    t = secrets.token_hex(16)
    fd = os.open(TOKEN_PATH, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(t)
    return t


def cmux(args, timeout=8):
    if not CMUX_BIN:
        return False, "cmux-not-found"
    try:
        r = subprocess.run([CMUX_BIN, *args], capture_output=True, text=True,
                           timeout=timeout)
        # 🔴 cmux 는 실패해도 rc=0 이다 — 본문의 "Error:" 가 유일한 판별자다.
        why = cmux_failed(r.returncode, r.stdout, r.stderr)
        return (why is None), (why or "")
    except Exception as e:  # noqa: BLE001
        return False, str(e)


def cmux_out(args, timeout=8):
    """cmux 호출의 **stdout** 을 돌려준다. 실패면 None.

    🪤 위 cmux() 는 (성공여부, **오류문자열**)을 돌려준다 — 성공 시 둘째 값은 빈
    문자열이다. 그걸 stdout 으로 알고 JSON 파싱하면 **항상 실패**한다(내가 그렇게
    썼고, 워크스페이스 재해석이 통째로 죽어 있었다).
    """
    if not CMUX_BIN:
        return None
    try:
        r = subprocess.run([CMUX_BIN, *args], capture_output=True, text=True,
                           timeout=timeout)
    except Exception:  # noqa: BLE001
        return None
    if cmux_failed(r.returncode, r.stdout, r.stderr):
        return None
    return r.stdout


def _workspace_of(surface):
    """서피스가 **지금** 속한 워크스페이스 UUID. 못 찾으면 None.

    🔴 `cmux send` 는 workspace 와 surface 가 **짝이 맞아야** 한다. 안 맞으면
    서피스가 멀쩡해도 `Error: invalid_params: Surface is not a terminal` 이다
    (실측 2026-09-08). 그런데 우리가 저장해 둔 워크스페이스는 낡을 수 있다 —
    탭은 워크스페이스 사이를 옮겨 다니고, 여러 명을 고용해 워크스페이스가 여럿이면
    그 확률이 커진다. 수선 전에는 이 실패가 전부 submit=ok 로 계상됐다.
    그러니 저장값을 믿지 말고, 실패하면 지금 자리를 찾아 다시 건다.
    """
    out = cmux_out(["workspace", "list", "--json", "--id-format", "both"])
    if out is None:
        return None
    try:
        wss = json.loads(out or "{}").get("workspaces", [])
    except ValueError:
        return None
    for w in wss:
        ref = w.get("ref")
        if not ref:
            continue
        out2 = cmux_out(["list-pane-surfaces", "--workspace", ref,
                         "--json", "--id-format", "both"])
        if out2 is None:
            continue
        try:
            sfs = json.loads(out2 or "{}").get("surfaces", [])
        except ValueError:
            continue
        for sf in sfs:
            if (sf.get("id") or sf.get("uuid")) == surface:
                return w.get("id") or ref
    return None


def ring(workspace, surface, n):
    """탭에 한 줄 타이핑 + 엔터. (ok, submitted, err)

    텍스트와 엔터를 **나눠** 보낸다. 개행을 텍스트에 붙이면 TUI 는 제출이 아니라
    줄바꿈으로 먹어, 지시가 입력창에 눌러앉은 채 사람이 엔터를 눌러야 한다
    (실측: 채용 첫지시가 그렇게 남았다).
    """
    ok, err = cmux(["send", "--workspace", workspace, "--surface", surface,
                    "--", doorbell_line(n)])
    if not ok:
        # 저장된 워크스페이스가 낡았을 수 있다 — 지금 자리를 찾아 한 번 더 건다.
        cur = _workspace_of(surface)
        if not cur or cur == workspace:
            return False, False, err
        workspace = cur
        ok, err = cmux(["send", "--workspace", workspace, "--surface", surface,
                        "--", doorbell_line(n)])
        if not ok:
            return False, False, err
    time.sleep(SUBMIT_GAP_S)
    ok2, err2 = cmux(["send-key", "--workspace", workspace, "--surface", surface,
                      "--", "enter"])
    return True, ok2, err2


class Handler(BaseHTTPRequestHandler):
    token = ""

    def _reply(self, code, obj):
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        if self.path == "/health":
            return self._reply(200, {"ok": True, "cmux_bin": CMUX_BIN,
                                     "cmux_ok": cmux(["workspace", "list",
                                                      "--json"])[0]})
        self._reply(404, {"error": "not-found"})

    def do_POST(self):
        if self.path != "/ring":
            return self._reply(404, {"error": "not-found"})
        if self.headers.get("X-Bell-Token", "") != Handler.token:
            return self._reply(403, {"error": "bad-token"})
        try:
            n = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(n) or b"{}")
        except Exception:  # noqa: BLE001
            return self._reply(400, {"error": "bad-body"})
        ws, sref = body.get("workspace", ""), body.get("surface", "")
        if not ws or not sref:
            return self._reply(400, {"error": "workspace/surface 필요"})
        ok, submitted, err = ring(ws, sref, body.get("n", 1))
        self._reply(200 if ok else 502,
                    {"ok": ok, "submitted": submitted, "err": err[:200]})

    def log_message(self, *_a):
        pass


def main():
    Handler.token = load_token(create=True)
    srv = ThreadingHTTPServer(("127.0.0.1", BELL_PORT), Handler)
    print(f"hub-bell listening 127.0.0.1:{BELL_PORT} cmux={CMUX_BIN}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
