#!/usr/bin/env python3
"""워커 UDS 웨이크 회귀 테스트.

핵심 불변식:
  - 소켓 신원 검증(레지스트리 sessionId → pid → procStart → connect)을 통과해야만 주입
  - 주입 주소는 워커가 레지스트리에서 직접 확인한 값만 쓴다 (자가 신고 무시 = 탈취 차단)
  - 웨이크 실패 시 항목은 캐시에 남는다 (훅 주입·부활 폴백이 살아 있어야 한다)
  - sendall 성공은 배달이 아니다 — 부정 영수증(held/refused…)이면 미배달로 계상한다
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
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "worker"))
os.environ.setdefault("HUB_DIR", tempfile.mkdtemp())
import worker as W  # noqa: E402


class FakeSession:
    """일회용 UDS 리스너 — Claude Code 세션 소켓 흉내. 받은 라인을 보관한다.

    receipt_status 를 주면 실측한 peer_message_status 프레임 형태 그대로 회신한다
    (회신 주소는 받은 프레임의 from 에서 유도 — 번들과 동일한 규약).
    """

    def __init__(self, path, receipt_status=None, reset_after_read=False):
        self.path = path
        self.lines = []
        self.receipt_status = receipt_status
        self.reset_after_read = reset_after_read
        self.last_frame = None
        self.on_user_frame = None      # '실제로 처리했다'는 부수효과 훅 (레지스트리 갱신)
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
            if self.reset_after_read:      # 인증 거부·프로토콜 파기 형상 = RST
                c.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                             __import__("struct").pack("ii", 1, 0))
            c.close()
            for ln in buf.decode("utf8", "replace").splitlines():
                if ln.strip():
                    self.lines.append(ln.strip())
                    try:
                        f = json.loads(ln)
                    except ValueError:
                        continue
                    if f.get("type") == "user":
                        self.last_frame = f
                        if self.receipt_status:
                            self.send_receipt(f, self.receipt_status)
                        elif self.on_user_frame:
                            self.on_user_frame(f)

    def send_receipt(self, frame, status, detail=None):
        """번들 실측 형상: expired+status_detail=refused / held / delivered."""
        target = frame.get("from", "")
        if not target.startswith("uds:"):
            return False          # 주소가 규약 밖 = 번들도 영수증을 안 보낸다
        body = {"type": "control", "action": "peer_message_status",
                "status": status, "reason": "test", "from": "uds:" + self.path,
                "orig_msg_id": frame.get("msg_id"), "msgV": 1,
                "msg_id": str(uuid.uuid4())}
        if detail:
            body["status_detail"] = detail
        c = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        c.settimeout(2)
        try:
            c.connect(target[4:])
            c.sendall((json.dumps(body) + "\n").encode())
            return True
        except OSError:
            return False
        finally:
            c.close()

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
        self._orig_wait = W.RECEIPT_WAIT_S
        W.RECEIPT_WAIT_S = 0.6          # 실측 영수증 지연 0.16s — 여유 4배
        W.CC_SESSIONS_DIR = self.sessdir
        self.sock_path = os.path.join(self.tmp, "s.sock")
        self.srv = None
        W.wake_state.clear()
        W.inbox_cache.clear()
        self._reset_receipts()
        for k in W.wake_stats:
            W.wake_stats[k] = 0

    def _reset_receipts(self):
        with W.receipt_lock:
            for ent in W.receipt_listeners.values():
                try:
                    ent["srv"].close()
                except OSError:
                    pass
            W.receipt_listeners.clear()
            W.pending_wakes.clear()

    def tearDown(self):
        W.CC_SESSIONS_DIR = self._orig
        W.relay_try, W.wake_session = self._orig_relay, self._orig_wake
        W.RECEIPT_WAIT_S = self._orig_wait
        self._reset_receipts()
        if self.srv:
            self.srv.close()

    def write_registry(self, session, pid, sock, proc_start=None, status="idle"):
        meta = {"pid": pid, "sessionId": session, "messagingSocketPath": sock,
                "status": status, "statusUpdatedAt": 1000, "updatedAt": 1000,
                "kind": "interactive"}
        if proc_start:
            meta["procStart"] = proc_start
        with open(os.path.join(self.sessdir, "%d.json" % pid), "w") as f:
            json.dump(meta, f)

    def bump_registry(self, session, pid, sock, proc_start=None):
        """수신 세션이 실제로 일을 시작한 형상 (실측: 배달 시 idle→busy, 0.06s)."""
        meta = {"pid": pid, "sessionId": session, "messagingSocketPath": sock,
                "status": "busy", "statusUpdatedAt": 2000, "updatedAt": 2000,
                "kind": "interactive"}
        if proc_start:
            meta["procStart"] = proc_start
        with open(os.path.join(self.sessdir, "%d.json" % pid), "w") as f:
            json.dump(meta, f)

    def live_session(self, sid, deliver=True, receipt=None, reset=False):
        """레지스트리에 등록된 살아있는 세션 + 소켓. deliver=True 면 주입 시 지문이 움직인다."""
        self.write_registry(sid, os.getpid(), self.sock_path,
                            self.my_proc_start_utc())
        srv = FakeSession(self.sock_path, receipt_status=receipt,
                          reset_after_read=reset)
        if deliver:
            srv.on_user_frame = lambda f: self.bump_registry(
                sid, os.getpid(), self.sock_path, self.my_proc_start_utc())
        self.srv = srv
        return srv

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

    def test_relay_socket_is_refused_when_registry_cannot_attest_it(self):
        """H1: 자가 신고 주소는 배달 직전에도 못 쓴다.

        relay 의 msg_socket 은 워커가 레지스트리에서 읽어 올린 값일 때만 유효하다.
        귀속 확인 없이 쓰던 시절, 남의 세션 이름으로 /register 를 쏘면 그 뒤의 메시지가
        통째로 공격자 소켓으로 갔다(원 수신자는 무음 유실).
        """
        self.srv = FakeSession(self.sock_path)      # '공격자' 소켓: 살아 있지만 미귀속
        self.assertIsNone(W.resolve_socket("sid-unknown", relay_socket=self.sock_path))

    def test_relay_socket_is_used_when_registry_attests_the_same_session(self):
        """귀속이 확인되면 relay 값도 그대로 쓴다 (다중 소스 구조는 유지)."""
        self.srv = FakeSession(self.sock_path)
        # 레지스트리는 소켓 경로만 알고 세션 키로는 안 잡히는 형상(대소문자·다른 키 등)을
        # 흉내내기 위해 owner 조회로만 귀속되게 한다
        self.write_registry("sid-1", os.getpid(), self.sock_path,
                            self.my_proc_start_utc())
        self.assertEqual(W.socket_owner(self.sock_path), "sid-1")
        self.assertEqual(W.resolve_socket("sid-1", relay_socket=self.sock_path),
                         self.sock_path)

    def test_socket_owner_maps_path_to_the_owning_session_only(self):
        self.write_registry("sid-1", os.getpid(), self.sock_path,
                            self.my_proc_start_utc())
        self.assertEqual(W.socket_owner(self.sock_path), "sid-1")
        self.assertIsNone(W.socket_owner(os.path.join(self.tmp, "other.sock")))
        self.assertIsNone(W.socket_owner(""))

    def test_no_socket_at_all_returns_none(self):
        self.assertIsNone(W.resolve_socket("sid-nope"))

    # ── H1: 로컬 API 등록 인가 ──────────────────────────
    def test_verified_msg_socket_comes_only_from_registry(self):
        self.assertEqual(W.verified_msg_socket("sid-1"), "")
        self.write_registry("sid-1", os.getpid(), self.sock_path,
                            self.my_proc_start_utc())
        self.assertEqual(W.verified_msg_socket("sid-1"), self.sock_path)

    def test_verified_msg_socket_rejects_pid_reuse(self):
        self.write_registry("sid-1", os.getpid(), self.sock_path,
                            "Mon Jan  1 00:00:00 2001")
        self.assertEqual(W.verified_msg_socket("sid-1"), "")

    def test_observed_session_requires_worker_side_evidence(self):
        self.assertFalse(W.observed_session("sid-ghost"))
        self.assertFalse(W.observed_session(""))
        self.write_registry("sid-seen", os.getpid(), self.sock_path,
                            self.my_proc_start_utc())
        self.assertTrue(W.observed_session("sid-seen"))

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
        from common.envelope import GUIDE, header_for
        self.live_session("sid-e", deliver=True)
        items = [{"id": "m-1", "thread": "t-1", "from_agent": "alice",
                  "type": "consult", "priority": "blocking", "body": "질문",
                  "created": time.time()}]
        self.assertTrue(W.wake_session(self.sock_path, items, "alice",
                                       session="sid-e",
                                       snapshot=W.session_snapshot("sid-e")))
        time.sleep(0.4)
        frame = json.loads(self.srv.lines[-1])
        content = frame["message"]["content"]
        for line in header_for(items):
            self.assertIn(line, content)
        # blocking 단독 배달엔 normal/fyi 안내가 실리지 않는다
        self.assertNotIn(GUIDE["normal"], content)
        self.assertNotIn(GUIDE["fyi"], content)
        self.assertIn("am reply t-1", content)
        self.assertIn("유휴 세션 웨이크로 배달됨", content)

    def test_long_body_is_truncated_below_line_limit(self):
        """라인 길이 상한을 넘기면 수신 측이 연결 자체를 파기한다."""
        self.live_session("sid-t", deliver=True)
        items = [{"id": "m-%d" % i, "thread": "t", "from_agent": "a",
                  "type": "consult", "priority": "normal", "body": "X" * 200,
                  "created": time.time()} for i in range(50)]
        self.assertTrue(W.wake_session(self.sock_path, items, "a", session="sid-t",
                                       snapshot=W.session_snapshot("sid-t")))
        time.sleep(0.4)
        content = json.loads(self.srv.lines[-1])["message"]["content"]
        self.assertLessEqual(len(content), W.WAKE_MAX_CHARS + 64)

    def test_wake_session_returns_false_on_dead_socket(self):
        self.assertFalse(W.wake_session(os.path.join(self.tmp, "nope.sock"),
                                        [{"id": "m-1", "thread": "t",
                                          "from_agent": "a", "type": "consult",
                                          "priority": "normal", "body": "b",
                                          "created": time.time()}], "a"))

    # ── H2/H3: 배달 회계 (sendall ≠ 배달) ────────────────
    def _item(self, mid="m-1"):
        return {"id": mid, "thread": "t", "from_agent": "alice", "type": "consult",
                "priority": "normal", "body": "b", "created": time.time()}

    def test_reply_address_is_a_sock_in_the_recipients_socket_dir(self):
        """영수증은 '수신 세션 소켓과 같은 디렉터리의 .sock' 으로만 온다(번들 검증)."""
        self.srv = FakeSession(self.sock_path)
        W.wake_session(self.sock_path, [self._item()], "alice")
        frm = self.srv.last_frame["from"]
        self.assertTrue(frm.startswith("uds:"))
        path = frm[4:]
        self.assertEqual(os.path.dirname(path), os.path.dirname(self.sock_path))
        self.assertTrue(path.endswith(".sock"))

    def test_frame_msg_id_is_a_uuid_so_receipts_can_correlate(self):
        """msg_id 가 UUID 가 아니면 수신 측이 origin 에 싣지 않아 orig_msg_id 가 빈다."""
        self.srv = FakeSession(self.sock_path)
        W.wake_session(self.sock_path, [self._item()], "alice")
        uuid.UUID(self.srv.last_frame["msg_id"])      # 형식 위반이면 예외

    def test_held_receipt_is_not_counted_as_delivered(self):
        """H2 본체: 사람 승인 대기(hold)를 배달로 찍던 것이 거짓 양성이었다."""
        self.srv = FakeSession(self.sock_path, receipt_status="held")
        res = W.wake_session(self.sock_path, [self._item()], "alice")
        self.assertFalse(res)
        self.assertEqual(res.status, "held")

    def test_refused_receipt_is_not_counted_as_delivered(self):
        self.srv = FakeSession(self.sock_path)
        self.srv.receipt_status = None

        def serve_refuse(frame):
            self.srv.send_receipt(frame, "expired", detail="refused")
        # expired+refused 조합은 'refused' 로 정규화되어야 한다 (번들 실측 형상)
        self.srv.receipt_status = None
        res_holder = {}

        def run():
            res_holder["r"] = W.wake_session(self.sock_path, [self._item()], "alice")
        t = threading.Thread(target=run)
        t.start()
        deadline = time.time() + 3
        while time.time() < deadline and not self.srv.last_frame:
            time.sleep(0.02)
        serve_refuse(self.srv.last_frame)
        t.join(5)
        self.assertFalse(res_holder["r"])
        self.assertEqual(res_holder["r"].status, "refused")

    def test_accept_path_is_confirmed_by_recipient_activity_not_by_sendall(self):
        """실측: accept 는 영수증을 안 보낸다. 대신 수신 세션이 실제로 돌기 시작한다."""
        self.live_session("sid-a", deliver=True)
        snap = W.session_snapshot("sid-a")
        res = W.wake_session(self.sock_path, [self._item()], "alice",
                             session="sid-a", snapshot=snap)
        self.assertTrue(res)
        self.assertEqual(res.status, "activity")

    def test_listener_that_just_closes_is_not_counted_as_delivered(self):
        """H2 재현 형상: 수락 후 아무것도 안 하는 리스너. sendall 은 성공한다."""
        self.live_session("sid-q", deliver=False)     # 지문이 안 움직인다
        snap = W.session_snapshot("sid-q")
        res = W.wake_session(self.sock_path, [self._item()], "alice",
                             session="sid-q", snapshot=snap)
        self.assertFalse(res)
        self.assertEqual(res.status, "unconfirmed")

    def test_forged_receipt_from_wrong_address_is_ignored(self):
        """같은 uid 의 다른 프로세스가 영수증을 위조해 미배달로 뒤집지 못한다."""
        self.live_session("sid-f", deliver=False)
        res_holder = {}

        def run():
            res_holder["r"] = W.wake_session(self.sock_path, [self._item()], "alice",
                                             session="sid-f")
        t = threading.Thread(target=run)
        t.start()
        deadline = time.time() + 3
        while time.time() < deadline and not self.srv.last_frame:
            time.sleep(0.02)
        frame = self.srv.last_frame
        forged = {"type": "control", "action": "peer_message_status",
                  "status": "refused", "from": "uds:/tmp/somebody-else.sock",
                  "orig_msg_id": frame["msg_id"]}
        c = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        c.connect(frame["from"][4:])
        c.sendall((json.dumps(forged) + "\n").encode())
        c.close()
        t.join(5)
        # 위조 영수증은 무시된다 — 'refused' 로 뒤집히지 않고 미확인으로 남는다
        self.assertEqual(res_holder["r"].status, "unconfirmed")

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
        W.wake_session = lambda *a, **k: W.WakeResult(False, "wire-failed",
                                                      "uds-write-failed")
        W.inbox_cache["sid-w"] = [{"id": "m-1", "thread": "t", "from_agent": "a",
                                   "type": "consult", "priority": "normal",
                                   "body": "b", "created": time.time()}]
        W._wake_once()
        self.assertIn("sid-w", W.inbox_cache)
        self.assertEqual([c[2] for c in calls if c[1] == "/ack"],
                         [{"id": "m-1", "state": "wake_failed", "via": "uds",
                           "detail": "uds-write-failed"}])

    def test_activity_only_wake_keeps_fallback(self):
        """🔴 결함 B: 활동은 약한 증거다 — 계상은 하되 폴백을 끊으면 안 된다.

        실측 사고(2026-08-22): 바쁜 세션의 스냅샷 변화를 배달로 읽어 'confirmed' 로
        찍고 캐시에서 뺐는데 수신자는 못 봤다. 재시도·훅 폴백이 함께 사라져 영구 유실.
        중복 1회가 유실보다 싸다 — 항목은 남아 훅 레인이 다시 집어가야 한다.
        """
        acks = []
        W.relay_try = lambda m, p, b=None, **kw: acks.append(b) or {"ok": True}
        self.live_session("sid-y", deliver=True)     # 활동만 발생(영수증 없음)
        W.inbox_cache["sid-y"] = [{"id": "m-1", "thread": "t", "from_agent": "a",
                                   "type": "consult", "priority": "normal",
                                   "body": "b", "created": time.time()}]
        W._wake_once()
        self.assertIn("sid-y", W.inbox_cache)        # 폴백 유지가 핵심
        self.assertEqual(acks[0]["id"], "m-1")
        self.assertEqual(acks[0]["state"], "wake_activity")
        self.assertTrue(acks[0]["detail"].startswith("confirmed:"))
        self.assertEqual(W.wake_stats["ok"], 1)

    def test_delivered_receipt_pops_cache_and_acks_injected(self):
        """확정 증거(delivered 영수증)일 때만 폴백을 끊는다."""
        acks = []
        W.relay_try = lambda m, p, b=None, **kw: acks.append(b) or {"ok": True}
        self.live_session("sid-y2", deliver=False, receipt="delivered")
        W.inbox_cache["sid-y2"] = [{"id": "m-1", "thread": "t", "from_agent": "a",
                                    "type": "consult", "priority": "normal",
                                    "body": "b", "created": time.time()}]
        W._wake_once()
        self.assertNotIn("sid-y2", W.inbox_cache)    # 훅이 두 번 보여주지 않는다
        self.assertEqual(acks[0]["state"], "injected")
        self.assertEqual(acks[0]["evidence"], "receipt-delivered")
        self.assertEqual(W.wake_stats["confirmed"], 1)

    def test_unconfirmed_wake_keeps_cache_and_acks_unconfirmed(self):
        """증거 없는 주입은 배달로 계상하지 않는다 — 폴백이 계속 살아 있어야 한다."""
        acks = []
        W.relay_try = lambda m, p, b=None, **kw: acks.append((p, b)) or {"ok": True}
        self.live_session("sid-u", deliver=False)
        W.inbox_cache["sid-u"] = [self._item()]
        W._wake_once()
        self.assertIn("sid-u", W.inbox_cache)
        self.assertEqual([b["state"] for p, b in acks if p == "/ack"],
                         ["wake_unconfirmed"])
        self.assertEqual(W.wake_stats["ok"], 0)

    def test_late_activity_is_counted_but_never_claimed_as_delivery(self):
        """늦은 활동은 배달이 아니다 — hold 가 정확히 같은 지문을 만든다(실측).

        옛 테스트는 여기서 state='injected' + 캐시 pop 을 요구했다. 그 규칙 때문에
        승인 대기(hold)로 파킹된 봉투가 relay 에 배달로 기록되고 훅 폴백까지 사라졌다
        (2026-08-22 격리 E2E: TUI 는 'not delivered to Claude (1 held)', relay 는
        state=injected/wake_status=activity-late).
        """
        acks = []
        W.relay_try = lambda m, p, b=None, **kw: acks.append((p, b)) or {"ok": True}
        self.live_session("sid-v", deliver=False)
        W.inbox_cache["sid-v"] = [self._item()]
        W._wake_once()
        self.assertIn("sid-v", W.inbox_cache)
        self.bump_registry("sid-v", os.getpid(), self.sock_path,
                           self.my_proc_start_utc())
        W._wake_once()
        self.assertIn("sid-v", W.inbox_cache)          # 폴백은 끊기지 않는다
        states = [b["state"] for p, b in acks if p == "/ack"]
        self.assertEqual(states, ["wake_unconfirmed", "wake_activity"])
        self.assertNotIn("injected", states)
        self.assertEqual([b for p, b in acks if p == "/ack"][-1]["detail"],
                         "activity-late")
        # 같은 활동으로 매 스윕 같은 ack 를 반복하지 않는다 (스냅샷 재기준선)
        W.wake_state["sid-v"]["next_try"] = time.time() + 999
        W._wake_once()
        self.assertEqual(len([b for p, b in acks if p == "/ack"]), 2)

    def test_late_negative_receipt_pins_the_reason_without_flipping_delivery(self):
        """영수증 지연은 0.15초~3초 이상으로 널뛴다(실측) — 늦게 와도 반영돼야 한다."""
        acks = []
        W.relay_try = lambda m, p, b=None, **kw: acks.append((p, b)) or {"ok": True}
        self.live_session("sid-lr", deliver=False)
        W.inbox_cache["sid-lr"] = [self._item()]
        W._wake_once()
        self.assertEqual([b["state"] for p, b in acks if p == "/ack"],
                         ["wake_unconfirmed"])
        self.srv.send_receipt(self.srv.last_frame, "held")
        deadline = time.time() + 3
        while time.time() < deadline and len(acks) < 2:
            time.sleep(0.02)
        self.assertEqual([b["state"] for p, b in acks if p == "/ack"],
                         ["wake_unconfirmed", "held"])
        self.assertIn("sid-lr", W.inbox_cache)      # 폴백은 계속 살아 있다
        self.assertGreater(W.wake_state["sid-lr"]["next_try"],
                           time.time() + W.WAKE_COOLDOWN_S)

    def test_late_negative_receipt_never_undoes_a_confirmed_delivery(self):
        """활동으로 확증된 배달을 늦은 부정 영수증이 뒤집으면 안 된다."""
        acks = []
        W.relay_try = lambda m, p, b=None, **kw: acks.append((p, b)) or {"ok": True}
        self.live_session("sid-cf", deliver=False, receipt="delivered")
        W.inbox_cache["sid-cf"] = [self._item()]
        W._wake_once()
        self.assertNotIn("sid-cf", W.inbox_cache)
        self.srv.send_receipt(self.srv.last_frame, "held")
        time.sleep(0.5)
        self.assertEqual([b["state"] for p, b in acks if p == "/ack"], ["injected"])
        self.assertNotIn("sid-cf", W.inbox_cache)

    def test_unconfirmed_retries_are_capped_without_faking_delivery(self):
        """상한은 재주입 소음만 멈춘다 — 없는 배달을 지어내면 그게 H2 의 거짓 양성이다."""
        acks = []
        W.relay_try = lambda m, p, b=None, **kw: acks.append((p, b)) or {"ok": True}
        self.live_session("sid-c2", deliver=False)
        W.inbox_cache["sid-c2"] = [self._item()]
        for _ in range(W.WAKE_UNCONFIRMED_MAX + 2):
            W.wake_state.setdefault("sid-c2", {})["next_try"] = 0
            W._wake_once()
        # 항목은 캐시에 남아 훅 폴백이 집어간다. injected 는 단 한 번도 나가지 않는다.
        self.assertIn("sid-c2", W.inbox_cache)
        states = [b["state"] for p, b in acks if p == "/ack"]
        self.assertNotIn("injected", states)
        self.assertEqual(set(states), {"wake_unconfirmed"})
        self.assertIn("/capped", [b["detail"] for p, b in acks if p == "/ack"][-1])
        # 상한 뒤에는 ack 조차 더 나가지 않는다 — 주입을 아예 멈췄다는 뜻이다.
        # (긴 백오프로 대신하던 시절엔 300초마다 같은 배치가 계속 나갔다.)
        self.assertEqual(len(states), W.WAKE_UNCONFIRMED_MAX)

    def _user_frames(self, srv):
        out = []
        for ln in srv.lines:
            try:
                f = json.loads(ln)
            except ValueError:
                continue
            if f.get("type") == "user":
                out.append(f)
        return out

    def test_capped_unconfirmed_stops_writing_the_frame(self):
        """상한은 **와이어 쓰기**를 멈춰야 한다 — 로그만 '재주입 중단'이면 아무것도 아니다.

        실측 회귀(2026-08-22 워커 로그): 같은 배치가 session 85a4d512 에 10회,
        cdf73585 에 5회 주입됐다. 상한이 next_try 만 늘리고 있었기 때문이다.
        """
        W.relay_try = lambda m, p, b=None, **kw: {"ok": True}
        srv = self.live_session("sid-cap", deliver=False)
        W.inbox_cache["sid-cap"] = [self._item()]
        for _ in range(W.WAKE_UNCONFIRMED_MAX + 4):
            W.wake_state.setdefault("sid-cap", {})["next_try"] = 0
            W._wake_once()
        self.assertEqual(len(self._user_frames(srv)), W.WAKE_UNCONFIRMED_MAX)
        self.assertIn("sid-cap", W.inbox_cache)   # 훅·부활 폴백은 그대로 산다

    def test_capped_session_still_reports_late_activity(self):
        """재주입을 멈춰도 늦은 신호는 계속 봐야 한다 — 단 배달로 승격하지는 않는다."""
        acks = []
        W.relay_try = lambda m, p, b=None, **kw: acks.append((p, b)) or {"ok": True}
        self.live_session("sid-cs", deliver=False)
        W.inbox_cache["sid-cs"] = [self._item()]
        for _ in range(W.WAKE_UNCONFIRMED_MAX + 2):
            W.wake_state.setdefault("sid-cs", {})["next_try"] = 0
            W._wake_once()
        self.bump_registry("sid-cs", os.getpid(), self.sock_path,
                           self.my_proc_start_utc())
        W._wake_once()
        self.assertIn("sid-cs", W.inbox_cache)         # 훅·부활 폴백 유지
        last = [b for p, b in acks if p == "/ack"][-1]
        self.assertEqual(last["state"], "wake_activity")
        self.assertEqual(last["detail"], "activity-late")

    def test_new_message_rearms_a_capped_session(self):
        """상한은 그 배치에만 걸린다 — 새 메시지는 자기 몫의 시도를 받아야 한다."""
        W.relay_try = lambda m, p, b=None, **kw: {"ok": True}
        srv = self.live_session("sid-re", deliver=False)
        W.inbox_cache["sid-re"] = [self._item()]
        for _ in range(W.WAKE_UNCONFIRMED_MAX + 2):
            W.wake_state.setdefault("sid-re", {})["next_try"] = 0
            W._wake_once()
        sent = len(self._user_frames(srv))
        W.inbox_cache["sid-re"].append(self._item("m-2"))
        W._wake_once()                     # next_try 를 만지지 않아도 열려야 한다
        self.assertEqual(len(self._user_frames(srv)), sent + 1)

    def test_held_keeps_items_for_fallback_and_acks_held_not_injected(self):
        """H2/H3: hold 는 사람 승인 대기 = 미배달. 폴백(훅·부활)이 살아 있어야 한다."""
        acks = []
        W.relay_try = lambda m, p, b=None, **kw: acks.append((p, b)) or {"ok": True}
        self.live_session("sid-h", deliver=False, receipt="held")
        W.inbox_cache["sid-h"] = [self._item()]
        W._wake_once()
        self.assertIn("sid-h", W.inbox_cache)          # 훅이 그대로 집어간다
        self.assertEqual([b["state"] for p, b in acks if p == "/ack"], ["held"])
        self.assertEqual(W.wake_stats["ok"], 0)
        self.assertEqual(W.wake_stats["held"], 1)
        # 재주입은 홀드 큐만 불린다 — 긴 쿨다운이 걸려야 한다
        self.assertGreater(W.wake_state["sid-h"]["next_try"],
                           time.time() + W.WAKE_COOLDOWN_S)

    def test_terminal_receipt_does_not_leave_a_pending_record(self):
        """확정 상태는 회계 레코드를 남기지 않는다 — 남기면 6시간 GC 까지 쌓이고,
        중복 영수증이 뒤늦게 와 **다른 배치**의 미확인 상태를 지운다."""
        W.relay_try = lambda m, p, b=None, **kw: {"ok": True}
        self.live_session("sid-t1", deliver=False, receipt="refused")
        W.inbox_cache["sid-t1"] = [self._item()]
        W._wake_once()
        with W.receipt_lock:
            self.assertEqual(len(W.pending_wakes), 0)

    def test_held_receipt_keeps_the_record_for_a_late_approval(self):
        W.relay_try = lambda m, p, b=None, **kw: {"ok": True}
        self.live_session("sid-t2", deliver=False, receipt="held")
        W.inbox_cache["sid-t2"] = [self._item()]
        W._wake_once()
        with W.receipt_lock:
            self.assertEqual(len(W.pending_wakes), 1)

    def test_late_delivered_receipt_settles_a_held_message(self):
        """사람이 승인하면 delivered 영수증이 늦게 온다 — 그때 배달로 확정한다."""
        acks = []
        W.relay_try = lambda m, p, b=None, **kw: acks.append((p, b)) or {"ok": True}
        self.live_session("sid-l", deliver=False, receipt="held")
        W.inbox_cache["sid-l"] = [self._item()]
        W._wake_once()
        self.assertIn("sid-l", W.inbox_cache)
        self.srv.send_receipt(self.srv.last_frame, "delivered")
        deadline = time.time() + 3
        while time.time() < deadline and "sid-l" in W.inbox_cache:
            time.sleep(0.02)
        self.assertNotIn("sid-l", W.inbox_cache)
        self.assertEqual([b["state"] for p, b in acks if p == "/ack"],
                         ["held", "injected"])
        self.assertEqual([b for p, b in acks if p == "/ack"][-1]["evidence"],
                         "receipt-delivered-late")
        self.assertEqual(W.wake_stats["late_delivered"], 1)

    def test_batch_cap_leaves_remainder_for_next_sweep(self):
        W.relay_try = lambda m, p, b=None, **kw: {"ok": True}
        self.live_session("sid-z", deliver=True)
        W.inbox_cache["sid-z"] = [
            {"id": "m-%d" % i, "thread": "t", "from_agent": "a", "type": "consult",
             "priority": "normal", "body": "b", "created": time.time()}
            for i in range(W.WAKE_MAX_ITEMS + 3)]
        W._wake_once()
        # 활동 확인은 폴백을 끊지 않으므로 배치분도 캐시에 남는다.
        # 상한이 지키는 것은 '한 번에 미는 항목 수'다.
        self.assertEqual(len(W.inbox_cache["sid-z"]), W.WAKE_MAX_ITEMS + 3)
        self.assertEqual(len(self.srv.last_frame["message"]["content"].split("- ")) - 1,
                         W.WAKE_MAX_ITEMS)

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

    def test_ack_burst_in_one_second_keeps_every_item(self):
        """초 해상도 + 고정 pid 파일명은 같은 초의 ack 들이 서로를 덮어썼다."""
        W.relay_call = self._boom
        for i in range(6):
            W.relay_try("POST", "/ack", {"id": f"m-{i}", "state": "injected"})
        got = []
        for f in os.listdir(W.SPOOL_DIR):
            with open(os.path.join(W.SPOOL_DIR, f)) as fh:
                got.append(json.load(fh)["body"]["id"])
        self.assertEqual(sorted(got), [f"m-{i}" for i in range(6)])

    def test_spool_drains_in_send_order(self):
        """파일명 정렬 == 시간 순서라는 가정이 깨지면 회신이 뒤섞인다."""
        W.relay_call = self._boom
        for i in range(12):
            W.relay_try("POST", "/ack", {"id": f"m-{i}", "state": "injected"})
        sent = []
        W.relay_call = lambda m, p, b=None, *a, **k: sent.append(b["id"]) or {"ok": True}
        W.drain_spool()
        self.assertEqual(sent, [f"m-{i}" for i in range(12)])

    def test_partial_write_is_never_drained(self):
        """rename 전 임시 파일을 읽어 반쪽 JSON 을 relay 에 보내면 안 된다."""
        os.makedirs(W.SPOOL_DIR, exist_ok=True)
        with open(os.path.join(W.SPOOL_DIR, "0000000000000000001-x.json.tmp"),
                  "w") as f:
            f.write('{"path": "/ack", "bod')
        sent = []
        W.relay_call = lambda m, p, b=None, *a, **k: sent.append(b) or {"ok": True}
        W.drain_spool()
        self.assertEqual(sent, [])

    def test_corrupt_spool_file_does_not_wedge_the_queue(self):
        """손상 파일 하나가 뒤에 줄 선 회신 전부를 영원히 막던 경로."""
        os.makedirs(W.SPOOL_DIR, exist_ok=True)
        with open(os.path.join(W.SPOOL_DIR, "0000000000000000001-a.json"), "w") as f:
            f.write("{ this is not json")
        W.relay_call = self._boom
        W.relay_try("POST", "/ack", {"id": "m-good", "state": "injected"})
        sent = []
        W.relay_call = lambda m, p, b=None, *a, **k: sent.append(b["id"]) or {"ok": True}
        W.drain_spool()
        self.assertEqual(sent, ["m-good"])
        self.assertTrue(any(f.endswith(".corrupt") for f in os.listdir(W.SPOOL_DIR)))

    def test_relay_still_down_stops_the_drain_and_keeps_order(self):
        """양성 대조: relay 장애는 격리 대상이 아니다 — 남겨 두고 다음 기회에."""
        W.relay_call = self._boom
        for i in range(3):
            W.relay_try("POST", "/ack", {"id": f"m-{i}", "state": "injected"})
        W.relay_call = self._boom
        W.drain_spool()
        self.assertEqual(len([f for f in os.listdir(W.SPOOL_DIR)
                              if f.endswith(".json")]), 3)


class WakeTextLimitCase(unittest.TestCase):
    """주입 라인 상한은 와이어 바이트 축이다 (적대 리뷰 MED-7)."""

    def test_korean_batch_is_clamped_by_bytes(self):
        items = [{"id": f"m{i}", "thread": "t-한글스레드", "from": "발신자",
                  "type": "consult", "priority": "blocking", "body": "가" * 400,
                  "created": time.time()} for i in range(W.WAKE_MAX_ITEMS)]
        raw = W.render_inbox(items, delivered_via="uds", stamp=time.time())
        self.assertLess(len(raw), W.WAKE_MAX_CHARS)          # 문자 가드는 발화조차 안 한다
        self.assertGreater(len(raw.encode("utf8")), W.WAKE_MAX_BYTES)
        out = W.clamp_wake_text(raw)
        self.assertLessEqual(len(out.encode("utf8")), W.WAKE_MAX_BYTES)

    def test_truncation_never_splits_a_multibyte_char(self):
        out = W.clamp_wake_text("가" * 5000)
        out.encode("utf8").decode("utf8")                     # 깨진 경계면 여기서 터진다
        self.assertTrue(out.endswith(W.TRUNC_MARK))

    def test_short_ascii_text_is_untouched(self):
        self.assertEqual(W.clamp_wake_text("hello"), "hello")

    def test_char_cap_still_applies_to_ascii(self):
        out = W.clamp_wake_text("x" * (W.WAKE_MAX_CHARS + 500))
        self.assertLessEqual(len(out), W.WAKE_MAX_CHARS)


class ProcStartLocaleCase(unittest.TestCase):
    """pid 재사용 방어가 로케일에 걸려 조용히 전면 중단되면 안 된다 (MED-8)."""

    def setUp(self):
        self._env = dict(os.environ)
        for k in list(W.wake_stats):
            W.wake_stats[k] = 0

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._env)

    def _my_proc_start_utc(self):
        import calendar  # noqa: F401
        import subprocess
        ps = subprocess.run(["ps", "-p", str(os.getpid()), "-o", "lstart="],
                            capture_output=True, text=True,
                            env={**os.environ, "LC_ALL": "C"}).stdout.strip()
        epoch = time.mktime(time.strptime(" ".join(ps.split()), W.PROC_START_FMT))
        return time.strftime(W.PROC_START_FMT, time.gmtime(epoch))

    def test_matches_under_a_korean_locale(self):
        want = self._my_proc_start_utc()
        os.environ["LC_ALL"] = "ko_KR.UTF-8"
        os.environ["LC_TIME"] = "ko_KR.UTF-8"
        self.assertTrue(W._proc_start_ok(os.getpid(), want))

    def test_mismatch_is_still_refused_and_counted_separately(self):
        os.environ["LC_ALL"] = "ko_KR.UTF-8"
        self.assertFalse(W._proc_start_ok(os.getpid(), "Mon Jan  1 00:00:00 2001"))
        self.assertEqual(W.wake_stats["proc_start_mismatch"], 1)
        self.assertEqual(W.wake_stats["proc_start_unparsed"], 0)

    def test_unparsable_registry_value_is_counted_not_silent(self):
        self.assertFalse(W._proc_start_ok(os.getpid(), "무슨 시각인지 모를 문자열"))
        self.assertEqual(W.wake_stats["proc_start_unparsed"], 1)
        self.assertEqual(W.wake_stats["proc_start_mismatch"], 0)


class LivenessReportCase(unittest.TestCase):
    """열거 실패를 '세션 없음'으로 보고하면 relay 가 함대를 강등한다 (MED-5)."""

    def setUp(self):
        self.calls = []
        self._orig_try, self._orig_run = W.relay_try, W.subprocess.run
        W.relay_try = lambda m, p, b=None, params="", timeout=10: \
            self.calls.append((p, b))

    def tearDown(self):
        W.relay_try, W.subprocess.run = self._orig_try, self._orig_run
        W._stop.clear()

    def _run_once(self, fake):
        W.subprocess.run = fake
        t = threading.Thread(target=W.poll_liveness, daemon=True)
        t.start()
        time.sleep(0.6)
        W._stop.set()
        t.join(timeout=3)
        W._stop.clear()

    def _proc(self, stdout, rc=0):
        class R:
            returncode = rc
            stderr = ""

            def __init__(self, out):
                self.stdout = out
        return lambda *a, **k: R(stdout)

    def test_successful_enumeration_reports_observed(self):
        self._run_once(self._proc(json.dumps(
            [{"sessionId": "s-1", "pid": os.getpid(), "status": "busy"}])))
        body = [b for p, b in self.calls if p == "/liveness"][0]
        self.assertTrue(body["observed"])
        self.assertEqual(body["home"], W.HOME_NAME)
        self.assertEqual(body["agents"], [{"session": "s-1", "state": "live-active"}])

    def test_empty_but_successful_enumeration_is_still_reported(self):
        """세션 0개도 사실이다 — 안 보내면 죽은 세션이 영원히 live 로 남는다."""
        self._run_once(self._proc("[]"))
        body = [b for p, b in self.calls if p == "/liveness"][0]
        self.assertTrue(body["observed"])
        self.assertEqual(body["agents"], [])

    def test_enumeration_failure_reports_nothing(self):
        def boom(cmd, *a, **k):
            raise OSError("claude: not found")
        self._run_once(boom)
        self.assertEqual([p for p, _ in self.calls if p == "/liveness"], [])

    def test_nonzero_exit_is_not_an_observation(self):
        self._run_once(self._proc("", rc=1))
        self.assertEqual([p for p, _ in self.calls if p == "/liveness"], [])


class LocalApiHangupCase(unittest.TestCase):
    """호출자가 먼저 끊으면 조용히 드롭 — 트레이스백이 로그를 덮으면 안 된다 (LOW-9).

    🪤 이 테스트는 두 번 틀렸었다. (1) socketserver 의 트레이스백은 **sys.stderr**(파이썬
    객체)로 나간다 — fd 2 를 dup2 로 가로채면 아무것도 안 잡힌다. (2) 즉답 엔드포인트로는
    재현되지 않는다 — 응답이 RST 보다 먼저 나가서 쓰기가 성공한다. 가드를 떼고 돌려서
    실패하는지 확인해야 테스트다(실측: 고치기 전 두 형상 모두 초록 = 아무것도 안 재고
    있었다). 지금 형상은 가드 제거 시 트레이스백 3건을 잡는다.
    """

    def setUp(self):
        self.srv = W.ThreadingHTTPServer(("127.0.0.1", 0), W.LocalHandler)
        self.port = self.srv.server_address[1]
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        # 응답을 늦춘다 — 쓰기 시점에 상대가 이미 없어야 BrokenPipe 가 난다
        self._orig_try = W.relay_try
        W.relay_try = lambda *a, **k: (time.sleep(1.0), {"ok": True})[1]

    def tearDown(self):
        W.relay_try = self._orig_try
        self.srv.shutdown()

    def test_client_hangup_leaves_no_traceback(self):
        import contextlib
        import io
        import struct
        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            for _ in range(3):
                s = socket.socket()
                s.connect(("127.0.0.1", self.port))
                s.sendall(b"GET /who?path=x HTTP/1.1\r\nHost: x\r\n\r\n")
                s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                             struct.pack("ii", 1, 0))
                s.close()
            time.sleep(2.0)
        self.assertNotIn("Traceback", buf.getvalue())

    def test_health_still_answers_a_polite_client(self):
        import urllib.request
        with urllib.request.urlopen(
                f"http://127.0.0.1:{self.port}/health", timeout=3) as r:
            self.assertIn("wake", json.loads(r.read()))


class InboundPolicyCase(unittest.TestCase):
    """H4: 웨이크의 하드 전제 — 수신 세션의 crossSessionInbound."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self._orig_dir, W.CLAUDE_DIR = W.CLAUDE_DIR, self.tmp
        self._orig_managed, W.MANAGED_SETTINGS = W.MANAGED_SETTINGS, [
            os.path.join(self.tmp, "managed.json")]

    def tearDown(self):
        W.CLAUDE_DIR, W.MANAGED_SETTINGS = self._orig_dir, self._orig_managed

    def _write(self, name, obj):
        with open(os.path.join(self.tmp, name), "w") as f:
            json.dump(obj, f)

    def test_unset_is_reported_as_unset(self):
        """미설정이 곧 accept 가 아니다 — bypassPermissions 세션의 기본은 hold 다."""
        self.assertEqual(W.effective_inbound_policy(), (None, "unset"))

    def test_user_settings_value_is_read(self):
        self._write("settings.json", {"crossSessionInbound": "accept"})
        value, source = W.effective_inbound_policy()
        self.assertEqual(value, "accept")
        self.assertTrue(source.endswith("settings.json"))

    def test_managed_policy_wins_over_user_settings(self):
        self._write("settings.json", {"crossSessionInbound": "accept"})
        self._write("managed.json", {"crossSessionInbound": "refuse"})
        self.assertEqual(W.effective_inbound_policy()[0], "refuse")

    def test_check_warns_but_never_blocks(self):
        self._write("settings.json", {"crossSessionInbound": "hold"})
        W.check_inbound_policy()              # 예외 없이 통과해야 한다
        self.assertEqual(W.health["inbound_policy"], "hold")


class LocalRegisterGateCase(unittest.TestCase):
    """H1: 로컬 API 는 127.0.0.1 이어도 같은 머신의 아무 프로세스나 부를 수 있다."""

    def setUp(self):
        import http.server
        self.tmp = tempfile.mkdtemp()
        self.sessdir = os.path.join(self.tmp, "sessions")
        os.makedirs(self.sessdir)
        self._orig_sess, W.CC_SESSIONS_DIR = W.CC_SESSIONS_DIR, self.sessdir
        self._orig_proj, W.PROJECTS_DIR = W.PROJECTS_DIR, os.path.join(self.tmp, "p")
        self._orig_relay = W.relay_try
        self.sent = []
        W.relay_try = lambda m, p, b=None, **kw: (self.sent.append((p, b))
                                                  or {"ok": True})
        self.srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), W.LocalHandler)
        self.port = self.srv.server_address[1]
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def tearDown(self):
        self.srv.shutdown()
        W.CC_SESSIONS_DIR, W.PROJECTS_DIR = self._orig_sess, self._orig_proj
        W.relay_try = self._orig_relay

    def _post(self, path, body):
        import urllib.error
        import urllib.request
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}",
                                     data=json.dumps(body).encode(), method="POST",
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=5) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read() or b"{}")

    def _registry(self, sid, sock):
        import subprocess
        ps = subprocess.run(["ps", "-p", str(os.getpid()), "-o", "lstart="],
                            capture_output=True, text=True).stdout.strip()
        epoch = time.mktime(time.strptime(" ".join(ps.split()), W.PROC_START_FMT))
        with open(os.path.join(self.sessdir, "%d.json" % os.getpid()), "w") as f:
            json.dump({"pid": os.getpid(), "sessionId": sid,
                       "messagingSocketPath": sock, "status": "idle",
                       "procStart": time.strftime(W.PROC_START_FMT,
                                                  time.gmtime(epoch))}, f)

    def test_unobserved_session_register_is_refused(self):
        code, body = self._post("/register", {"session": "ghost-session",
                                              "name": "victim-agent"})
        self.assertEqual(code, 403)
        self.assertEqual(body["error"], "unobserved-session")
        self.assertEqual(self.sent, [])       # relay 까지 가지도 않는다

    def test_self_reported_msg_socket_is_replaced_by_the_verified_one(self):
        real = os.path.join(self.tmp, "real.sock")
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.bind(real)
        s.listen(1)
        try:
            self._registry("sid-real", real)
            code, _ = self._post("/register",
                                 {"session": "sid-real", "name": "victim-agent",
                                  "msg_socket": "/tmp/attacker.sock"})
            self.assertEqual(code, 200)
            path, body = self.sent[-1]
            self.assertEqual(path, "/register")
            self.assertEqual(body["msg_socket"], real)   # 자가 신고는 버려진다
        finally:
            s.close()


class PollDedupCase(unittest.TestCase):
    """at-least-once dedup 이 '되살린 메시지'까지 버리면 안 된다.

    relay 쪽 커서를 고쳐 defer 재배달을 되살려도, 워커가 msg id 로만 중복을 판정하면
    봉투는 끝내 도착하지 않는다(격리 E2E 실측: 재배달 후 수신함 0건). dedup 축은
    배달 인스턴스 = (id, cursor) 여야 한다 — 되살릴 때마다 새 커서가 붙는다.
    """

    def setUp(self):
        self._orig = (W.relay_call, W._agent_by_name, dict(W.cursor_state))
        W.delivered_ids.clear()
        W.inbox_cache.clear()
        W.cursor_state["cursor"] = 0
        W._agent_by_name = lambda name: {"session": "s-bob", "name": name}

    def tearDown(self):
        W.relay_call, W._agent_by_name, cs = self._orig
        W.cursor_state.update(cs)
        W.delivered_ids.clear()
        W.inbox_cache.clear()
        W._stop.clear()

    def _pump(self, rounds):
        """poll_relay 를 rounds 회분 대본대로 돌린다 (마지막에 스스로 멈춘다)."""
        seq = list(rounds)

        def fake(method, path, params="", timeout=0, **kw):
            if not seq:
                W._stop.set()
                return {"deliveries": []}
            return {"deliveries": seq.pop(0)}
        W.relay_call = fake
        W.drain_spool = lambda: None
        t = threading.Thread(target=W.poll_relay, daemon=True)
        t.start()
        t.join(timeout=5)
        self.assertFalse(t.is_alive())

    def _msg(self, mid, cursor):
        return {"id": mid, "cursor": cursor, "to_agent": "bob", "thread": "t",
                "priority": "normal", "type": "consult", "body": "b",
                "created": time.time()}

    def test_same_row_polled_twice_is_delivered_once(self):
        """양성 대조: at-least-once 재전송(같은 커서)은 여전히 한 번만 들어간다."""
        self._pump([[self._msg("m-1", 1)], [self._msg("m-1", 1)]])
        self.assertEqual(len(W.inbox_cache.get("s-bob", [])), 1)

    def test_requeued_row_with_a_new_cursor_is_delivered_again(self):
        """defer 재배달·재큐는 새 커서를 달고 온다 — 이건 통과해야 한다."""
        self._pump([[self._msg("m-1", 1)], [self._msg("m-1", 7)]])
        self.assertEqual(len(W.inbox_cache.get("s-bob", [])), 2)
        self.assertEqual(W.cursor_state["cursor"], 7)

    def test_a_skipped_recipient_lookup_holds_the_batch_cursor(self):
        """수신자 조회가 빈 건을 건너뛰면 **배치 전체**가 그 앞에서 멈춰야 한다.

        불변식이 주석에만 있던 자리다 — 같은 배치의 뒤 메시지가 커서를 밀어 올려
        건너뛴 건을 영영 못 보게 만들었다(실측: 커서 6 → 5번 유실).
        """
        W._agent_by_name = lambda n: None if n == "ghost" else {"session": "s-bob"}
        lost = self._msg("m-lost", 5)
        lost["to_agent"] = "ghost"
        self._pump([[lost, self._msg("m-ok", 6)]])
        self.assertEqual(W.cursor_state["cursor"], 4)      # 건너뛴 5번 앞에서 멈춘다
        self.assertEqual([i["id"] for i in W.inbox_cache.get("s-bob", [])], ["m-ok"])

    def test_a_recovered_lookup_delivers_the_held_message_without_duplicating(self):
        """양성 대조: 다음 폴에서 조회가 살아나면 유실분만 새로 들어간다."""
        state = {"down": True}

        def lookup(name):
            if name == "ghost" and state["down"]:
                state["down"] = False      # 첫 폴에서만 실패, 다음 폴엔 살아난다
                return None
            return {"session": "s-bob"}
        W._agent_by_name = lookup
        lost = self._msg("m-lost", 5)
        lost["to_agent"] = "ghost"
        batch = [lost, self._msg("m-ok", 6)]
        self._pump([list(batch), list(batch)])
        ids = [i["id"] for i in W.inbox_cache.get("s-bob", [])]
        self.assertEqual(sorted(ids), ["m-lost", "m-ok"])   # 중복 없음
        self.assertEqual(W.cursor_state["cursor"], 6)


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


class ModeAttestCase(unittest.TestCase):
    """from-mode attest — 수신자가 bypass 계급이면 이게 없으면 무조건 hold 된다.

    수신 측 파서가 **재렌더 왕복 대조**를 하므로 형식이 한 글자만 어긋나도 통째로
    무효가 된다(번들 실측). 그래서 문자열을 그대로 못박는다.
    """

    def test_envelope_exact_wire_format(self):
        got = W.envelope_with_mode("본문", "uds:/tmp/x.sock", "bypass")
        self.assertEqual(
            got,
            '<cross-session-message from="uds:/tmp/x.sock" from-name="agent-hub" '
            'from-mode="bypass">\n본문\n</cross-session-message>')

    def test_no_attest_leaves_body_untouched(self):
        # 모르면 주장하지 않는다 — 과대 주장은 mode-mismatch 로 통째 미배달이 된다
        self.assertEqual(W.envelope_with_mode("본문", "uds:/tmp/x.sock", None), "본문")

    def test_mode_class_folds_to_two_values_only(self):
        self.assertEqual(W.mode_class("bypassPermissions"), "bypass")
        for m in ("default", "acceptEdits", "auto", "dontAsk"):
            self.assertEqual(W.mode_class(m), "prompting")
        # plan 은 bypass 가용 여부를 훅 입력만으로 못 가른다 → 침묵
        self.assertIsNone(W.mode_class("plan"))
        self.assertIsNone(W.mode_class(""))

    def test_frame_carries_mode_inside_content_not_toplevel(self):
        # 🔑 최상위 from_mode 키는 type:"user" 에서 안 읽힌다 — content 안에서만 온다
        payload = W._wake_frame("본문", "alice", "id-1", None,
                                reply_from="uds:/tmp/r.sock", from_mode="bypass")
        frame = json.loads(payload.decode().strip().splitlines()[-1])
        self.assertNotIn("from_mode", frame)
        self.assertIn('from-mode="bypass"', frame["message"]["content"])


class TerminalLearningCase(WakeCase):
    """종착 학습은 경로별 특권이 아니라 공통 규약이다.

    실측 사고(08-25): 수신자가 reply·read·defer·decide 를 다 했는데도 같은 봉투가
    1시간 넘게 매분 재주입됐다. relay 는 answered 로 알고 있었지만 activity-late·
    unconfirmed 분기가 ack 응답을 버려서 워커가 배울 길이 없었다.
    """

    def test_unconfirmed_branch_drops_terminal_items(self):
        acks = []

        def fake(m, p, b=None, **kw):
            acks.append(b)
            return {"ok": True, "terminal": True, "current_state": "answered"}

        W.relay_try = fake
        self.live_session("sid-t", deliver=False)      # 활동 없음 → unconfirmed
        W.inbox_cache["sid-t"] = [self._item()]
        W._wake_once()
        self.assertEqual(acks[0]["state"], "wake_unconfirmed")
        # 종착이라고 배웠으면 캐시에서 빠져야 한다 — 안 그러면 5초마다 다시 민다
        self.assertNotIn("sid-t", W.inbox_cache)

    def test_non_terminal_keeps_fallback(self):
        W.relay_try = lambda m, p, b=None, **kw: {"ok": True, "terminal": False,
                                                  "current_state": "queued"}
        self.live_session("sid-u", deliver=False)
        W.inbox_cache["sid-u"] = [self._item()]
        W._wake_once()
        self.assertIn("sid-u", W.inbox_cache)   # 아직 안 봤다 → 폴백 유지
