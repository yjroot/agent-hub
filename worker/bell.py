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
        return r.returncode == 0, (r.stderr or "").strip()
    except Exception as e:  # noqa: BLE001
        return False, str(e)


def ring(workspace, surface, n):
    """탭에 한 줄 타이핑 + 엔터. (ok, submitted, err)

    텍스트와 엔터를 **나눠** 보낸다. 개행을 텍스트에 붙이면 TUI 는 제출이 아니라
    줄바꿈으로 먹어, 지시가 입력창에 눌러앉은 채 사람이 엔터를 눌러야 한다
    (실측: 채용 첫지시가 그렇게 남았다).
    """
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
