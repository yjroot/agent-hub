#!/usr/bin/env python3
"""워커 UDS 웨이크 회귀 테스트.

핵심 불변식:
  - 소켓 신원 검증(레지스트리 sessionId → pid → procStart → connect)을 통과해야만 주입
  - 웨이크 실패 시 항목은 캐시에 남는다 (훅 주입·부활 폴백이 살아 있어야 한다)
  - 주입 본문은 훅 경로와 같은 봉투 렌더러를 쓴다 (프롬프트 인젝션 방어선 단일화)
"""
import json
import os
import socket
import sys
import tempfile
import threading
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "worker"))
os.environ.setdefault("HUB_DIR", tempfile.mkdtemp())
import worker as W  # noqa: E402


class FakeSession:
    """일회용 UDS 리스너 — Claude Code 세션 소켓 흉내. 받은 라인을 보관한다."""

    def __init__(self, path):
        self.path = path
        self.lines = []
        try:
            os.unlink(path)
        except OSError:
            pass
        self.s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.s.bind(path)
        os.chmod(path, 0o600)
        self.s.listen(4)
        self.t = threading.Thread(target=self._serve, daemon=True)
        self.t.start()

    def _serve(self):
        while True:
            try:
                c, _ = self.s.accept()
            except OSError:
                return
            buf = b""
            c.settimeout(2)
            try:
                while True:
                    d = c.recv(65536)
                    if not d:
                        break
                    buf += d
            except OSError:
                pass
            c.close()
            for ln in buf.decode("utf8", "replace").splitlines():
                if ln.strip():
                    self.lines.append(ln.strip())

    def close(self):
        self.s.close()
        try:
            os.unlink(self.path)
        except OSError:
            pass


class WakeCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.sessdir = os.path.join(self.tmp, "sessions")
        os.makedirs(self.sessdir)
        self._orig = W.CC_SESSIONS_DIR
        self._orig_relay, self._orig_wake = W.relay_try, W.wake_session
        W.CC_SESSIONS_DIR = self.sessdir
        self.sock_path = os.path.join(self.tmp, "s.sock")
        self.srv = None
        W.wake_state.clear()
        W.inbox_cache.clear()
        for k in W.wake_stats:
            W.wake_stats[k] = 0

    def tearDown(self):
        W.CC_SESSIONS_DIR = self._orig
        W.relay_try, W.wake_session = self._orig_relay, self._orig_wake
        if self.srv:
            self.srv.close()

    def write_registry(self, session, pid, sock, proc_start=None):
        meta = {"pid": pid, "sessionId": session, "messagingSocketPath": sock,
                "status": "idle", "kind": "interactive"}
        if proc_start:
            meta["procStart"] = proc_start
        with open(os.path.join(self.sessdir, "%d.json" % pid), "w") as f:
            json.dump(meta, f)

    def my_proc_start_utc(self):
        """자기 자신(pid)의 기동시각을 레지스트리 형식(UTC)으로."""
        import calendar
        import subprocess
        ps = subprocess.run(["ps", "-p", str(os.getpid()), "-o", "lstart="],
                            capture_output=True, text=True).stdout.strip()
        epoch = time.mktime(time.strptime(" ".join(ps.split()), W.PROC_START_FMT))
        return time.strftime(W.PROC_START_FMT, time.gmtime(epoch))

    # ── 신원 검증 ───────────────────────────────────────
    def test_resolves_live_socket_from_registry(self):
        self.srv = FakeSession(self.sock_path)
        self.write_registry("sid-1", os.getpid(), self.sock_path,
                            self.my_proc_start_utc())
        self.assertEqual(W.resolve_socket("sid-1"), self.sock_path)

    def test_rejects_dead_socket(self):
        """소켓 파일이 없으면(세션 종료) 주입하지 않는다."""
        self.write_registry("sid-1", os.getpid(), self.sock_path,
                            self.my_proc_start_utc())
        self.assertIsNone(W.resolve_socket("sid-1"))

    def test_rejects_pid_reuse_via_proc_start(self):
        """pid 는 살아 있고 소켓도 열려 있지만 기동시각이 다르면 다른 프로세스다."""
        self.srv = FakeSession(self.sock_path)
        self.write_registry("sid-1", os.getpid(), self.sock_path,
                            "Mon Jan  1 00:00:00 2001")
        self.assertIsNone(W.resolve_socket("sid-1"))

    def test_registry_hit_does_not_fall_back_to_stale_relay_socket(self):
        """레지스트리에 있는데 죽었다면 relay 의 값은 더 낡았다 — 폴백하면 오배달."""
        self.srv = FakeSession(self.sock_path)
        other = os.path.join(self.tmp, "other.sock")
        self.write_registry("sid-1", os.getpid(), other, self.my_proc_start_utc())
        self.assertIsNone(W.resolve_socket("sid-1", relay_socket=self.sock_path))

    def test_falls_back_to_hook_registered_socket_when_registry_missing(self):
        """방금 뜬 세션은 레지스트리 json 이 아직 없다 — 훅이 실어 준 값이 유일한 단서."""
        self.srv = FakeSession(self.sock_path)
        self.assertEqual(W.resolve_socket("sid-unknown", relay_socket=self.sock_path),
                         self.sock_path)

    def test_no_socket_at_all_returns_none(self):
        self.assertIsNone(W.resolve_socket("sid-nope"))

    def test_corrupt_registry_entry_is_skipped_not_fatal(self):
        with open(os.path.join(self.sessdir, "999999.json"), "w") as f:
            f.write("{not json")
        self.srv = FakeSession(self.sock_path)
        self.write_registry("sid-1", os.getpid(), self.sock_path,
                            self.my_proc_start_utc())
        self.assertEqual(W.resolve_socket("sid-1"), self.sock_path)

    def test_peer_token_hash_uses_abspath_not_realpath(self):
        """해시는 path.resolve 기준 — realpath 로 심링크를 풀면 키파일을 못 찾는다."""
        import hashlib
        h = hashlib.sha256(os.path.abspath(self.sock_path).encode()).hexdigest()
        with open(os.path.join(self.sessdir, "123.%s.key" % h), "w") as f:
            json.dump({"peerToken": "deadbeef", "procStart": "x"}, f)
        self.assertEqual(W._peer_token(self.sock_path), "deadbeef")

    def test_missing_token_is_not_fatal(self):
        self.assertIsNone(W._peer_token(self.sock_path))

    # ── 와이어 포맷 ─────────────────────────────────────
    def test_frame_shape(self):
        raw = W._wake_frame("hello", "alice", "m-1", "tok")
        lines = raw.decode().strip().split("\n")
        self.assertEqual(json.loads(lines[0]), {"type": "auth", "token": "tok"})
        f = json.loads(lines[1])
        self.assertEqual(f["type"], "user")
        self.assertEqual(f["message"], {"role": "user", "content": "hello"})
        self.assertIn(f["priority"], ("now", "next", "later"))
        # session_id 를 실으면 불일치 시 무음 드랍인데 되먹임 채널이 없다 (실측)
        self.assertNotIn("session_id", f)

    def test_from_field_is_sanitized_and_per_sender(self):
        """from 은 신원이 아니라 수신 측 레이트 버킷 키 — 허용 문자만 남긴다."""
        f = json.loads(W._wake_frame("x", "세션/alice!@#", "m-1", None).decode()
                       .strip().split("\n")[0])
        self.assertRegex(f["from"], r"^[A-Za-z0-9%:_/.\-]+$")

    def test_frame_without_token_omits_auth_line(self):
        raw = W._wake_frame("hello", "alice", "m-1", None)
        self.assertEqual(len(raw.decode().strip().split("\n")), 1)

    # ── 주입 본문 = 훅과 같은 봉투 ───────────────────────
    def test_injected_body_uses_shared_envelope(self):
        from common.envelope import HEADER
        self.srv = FakeSession(self.sock_path)
        items = [{"id": "m-1", "thread": "t-1", "from_agent": "alice",
                  "type": "consult", "priority": "blocking", "body": "질문",
                  "created": time.time()}]
        self.assertTrue(W.wake_session(self.sock_path, items, "alice"))
        time.sleep(0.4)
        frame = json.loads(self.srv.lines[-1])
        content = frame["message"]["content"]
        for line in HEADER:
            self.assertIn(line, content)
        self.assertIn("am reply t-1", content)
        self.assertIn("유휴 세션 웨이크로 배달됨", content)

    def test_long_body_is_truncated_below_line_limit(self):
        """라인 길이 상한을 넘기면 수신 측이 연결 자체를 파기한다."""
        self.srv = FakeSession(self.sock_path)
        items = [{"id": "m-%d" % i, "thread": "t", "from_agent": "a",
                  "type": "consult", "priority": "normal", "body": "X" * 200,
                  "created": time.time()} for i in range(50)]
        self.assertTrue(W.wake_session(self.sock_path, items, "a"))
        time.sleep(0.4)
        content = json.loads(self.srv.lines[-1])["message"]["content"]
        self.assertLessEqual(len(content), W.WAKE_MAX_CHARS + 64)

    def test_wake_session_returns_false_on_dead_socket(self):
        self.assertFalse(W.wake_session(os.path.join(self.tmp, "nope.sock"),
                                        [{"id": "m-1", "thread": "t",
                                          "from_agent": "a", "type": "consult",
                                          "priority": "normal", "body": "b",
                                          "created": time.time()}], "a"))

    # ── 폴백 보장 (회귀 금지) ────────────────────────────
    def _fake_relay(self):
        calls = []

        def fake(method, path, body=None, **kw):
            calls.append((method, path, body))
            return {"ok": True}
        W.relay_try = fake
        return calls

    def test_failed_wake_keeps_items_in_cache_for_hook_path(self):
        calls = self._fake_relay()
        W.inbox_cache["sid-x"] = [{"id": "m-1", "thread": "t", "from_agent": "a",
                                   "type": "consult", "priority": "normal",
                                   "body": "b", "created": time.time()}]
        W._wake_once()
        self.assertIn("sid-x", W.inbox_cache)      # 훅이 여전히 집어갈 수 있다
        self.assertEqual(W.wake_stats["ok"], 0)
        # 소켓이 아예 없으면 relay 에 실패 보고조차 하지 않는다 (구버전 세션 소음 방지).
        # 조회(GET /agent-by-session)만 나가고 /ack 은 나가지 않아야 한다.
        self.assertEqual([c for c in calls if c[1] == "/ack"], [])

    def test_write_failure_reports_wake_failed_without_state_change(self):
        """소켓은 있었는데 쓰기가 깨진 경우엔 relay 에 실패를 알린다(계측)."""
        calls = self._fake_relay()
        self.srv = FakeSession(self.sock_path)
        self.write_registry("sid-w", os.getpid(), self.sock_path,
                            self.my_proc_start_utc())
        W.wake_session = lambda *a, **k: False        # 쓰기 실패 주입
        W.inbox_cache["sid-w"] = [{"id": "m-1", "thread": "t", "from_agent": "a",
                                   "type": "consult", "priority": "normal",
                                   "body": "b", "created": time.time()}]
        W._wake_once()
        self.assertIn("sid-w", W.inbox_cache)
        self.assertEqual([c[2] for c in calls if c[1] == "/ack"],
                         [{"id": "m-1", "state": "wake_failed",
                           "detail": "uds-write-failed"}])

    def test_successful_wake_pops_cache_and_acks_injected(self):
        acks = []
        W.relay_try = lambda m, p, b=None, **kw: acks.append(b) or {"ok": True}
        self.srv = FakeSession(self.sock_path)
        self.write_registry("sid-y", os.getpid(), self.sock_path,
                            self.my_proc_start_utc())
        W.inbox_cache["sid-y"] = [{"id": "m-1", "thread": "t", "from_agent": "a",
                                   "type": "consult", "priority": "normal",
                                   "body": "b", "created": time.time()}]
        W._wake_once()
        self.assertNotIn("sid-y", W.inbox_cache)   # 훅이 두 번 보여주지 않는다
        self.assertEqual(acks, [{"id": "m-1", "state": "injected", "via": "uds"}])
        self.assertEqual(W.wake_stats["ok"], 1)

    def test_batch_cap_leaves_remainder_for_next_sweep(self):
        W.relay_try = lambda m, p, b=None, **kw: {"ok": True}
        self.srv = FakeSession(self.sock_path)
        self.write_registry("sid-z", os.getpid(), self.sock_path,
                            self.my_proc_start_utc())
        W.inbox_cache["sid-z"] = [
            {"id": "m-%d" % i, "thread": "t", "from_agent": "a", "type": "consult",
             "priority": "normal", "body": "b", "created": time.time()}
            for i in range(W.WAKE_MAX_ITEMS + 3)]
        W._wake_once()
        self.assertEqual(len(W.inbox_cache["sid-z"]), 3)

    def test_cooldown_prevents_hammering_after_failure(self):
        W.relay_try = lambda m, p, b=None, **kw: {"ok": True}
        W.inbox_cache["sid-c"] = [{"id": "m-1", "thread": "t", "from_agent": "a",
                                   "type": "consult", "priority": "normal",
                                   "body": "b", "created": time.time()}]
        W._wake_once()
        first = W.wake_state["sid-c"]["next_try"]
        self.assertGreater(first, time.time())
        W._wake_once()
        self.assertEqual(W.wake_state["sid-c"]["next_try"], first)


class AckSpoolCase(unittest.TestCase):
    """relay 가 잠깐 죽어도 배달 회신(ack)은 유실되지 않아야 한다."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self._orig_spool, W.SPOOL_DIR = W.SPOOL_DIR, os.path.join(self.tmp, "spool")
        self._orig_call = W.relay_call

    def tearDown(self):
        W.SPOOL_DIR, W.relay_call = self._orig_spool, self._orig_call

    def _boom(self, *a, **k):
        raise OSError("relay down")

    def test_failed_ack_is_spooled_and_drained(self):
        W.relay_call = self._boom
        W.relay_try("POST", "/ack", {"id": "m-1", "state": "injected", "via": "uds"})
        spooled = os.listdir(W.SPOOL_DIR)
        self.assertEqual(len(spooled), 1)
        body = json.load(open(os.path.join(W.SPOOL_DIR, spooled[0])))
        self.assertEqual(body["path"], "/ack")
        self.assertEqual(body["body"]["id"], "m-1")
        sent = []
        W.relay_call = lambda m, p, b=None, *a, **k: sent.append((p, b)) or {"ok": True}
        W.drain_spool()
        self.assertEqual(sent, [("/ack", {"id": "m-1", "state": "injected",
                                          "via": "uds"})])
        self.assertEqual(os.listdir(W.SPOOL_DIR), [])

    def test_reads_are_not_spooled(self):
        W.relay_call = self._boom
        self.assertIsNone(W.relay_try("GET", "/agent-by-session"))
        self.assertFalse(os.path.isdir(W.SPOOL_DIR) and os.listdir(W.SPOOL_DIR))


class DefaultNameCase(unittest.TestCase):
    """워커의 기본 이름 부여 규칙 (부분 갱신은 개명하지 않는다)."""

    def _name_for(self, body):
        return W.apply_default_name(body)

    def test_full_register_without_name_gets_default(self):
        self.assertEqual(self._name_for({"session": "abcdefgh-1111"}),
                         "session-abcdefgh")

    def test_partial_register_gets_no_name(self):
        self.assertIsNone(self._name_for({"session": "abcdefgh-1111",
                                          "partial": True, "task_hint": "x"}))

    def test_explicit_name_survives(self):
        self.assertEqual(self._name_for({"session": "abcdefgh-1111",
                                         "name": "hub-architect"}), "hub-architect")


if __name__ == "__main__":
    unittest.main(verbosity=2)
