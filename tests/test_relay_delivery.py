#!/usr/bin/env python3
"""relay 배달 파이프라인 회귀 테스트 (stdlib unittest, DB 격리).

8일 적체의 진범 3종을 못박는다:
  D1 TTL 이 blocking 타이머에만 달려 normal 은 영원히 만료되지 않음 (expired 전 생애 0건)
  D2 injected 를 created 기준으로 무조건 재큐 → 워커 커서가 이미 지나가 좀비화
  D3 live 보고가 끊긴 행이 'live-active' 로 남아 디스패치 분기를 오도
"""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.realpath(__file__))), "relay"))


class RelayCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        os.environ["HUB_DB"] = os.path.join(self.tmp, "relay.db")
        for mod in [m for m in list(sys.modules) if m == "relay"]:
            del sys.modules[mod]
        import relay
        self.r = relay
        relay.DB_PATH = os.environ["HUB_DB"]
        relay._local.__dict__.pop("conn", None)
        c = relay.db()
        c.executescript(relay.SCHEMA)
        relay.migrate(c)
        c.commit()
        self.c = c

    def agent(self, name, session=None, state="live-active", last_seen=None,
              msg_socket=""):
        self.r.h_register({"name": name, "session": session or name, "state": state,
                           "msg_socket": msg_socket, "home": "local"}, {})
        if last_seen is not None:
            self.c.execute("UPDATE agents SET last_seen=? WHERE name=?",
                           (last_seen, name))
        self.c.commit()

    def msg(self, to, frm="alice", created=None, state="queued", ttl=3600,
            mtype="consult", priority="normal", injected_at=None, inject_count=0):
        mid = self.r.insert_message(
            thread="t-1", from_agent=frm, from_session=frm, to_agent=to,
            mtype=mtype, priority=priority, body="b", state=state, ttl_s=ttl)
        if created is not None:
            self.c.execute("UPDATE messages SET created=? WHERE id=?", (created, mid))
        self.c.execute("UPDATE messages SET injected_at=?, inject_count=? WHERE id=?",
                       (injected_at, inject_count, mid))
        self.c.commit()
        return mid

    def state_of(self, mid):
        return self.c.execute("SELECT state FROM messages WHERE id=?",
                              (mid,)).fetchone()["state"]

    # ── D1: TTL 은 우선순위 무관 ────────────────────────
    def test_ttl_expires_normal_priority(self):
        """normal 도 만료된다 — 이게 없어서 8.7일짜리 queued 가 생겼다."""
        self.agent("bob")
        old = self.msg("bob", created=self.r.now() - 7200, ttl=3600)
        fresh = self.msg("bob", created=self.r.now() - 60, ttl=3600)
        self.r._sweep_ttl(self.c)
        self.assertEqual(self.state_of(old), "expired")
        self.assertEqual(self.state_of(fresh), "queued")

    def test_ttl_notifies_only_live_sender(self):
        """죽은 발신자에게 notice 를 만들면 그 notice 가 같은 블랙홀로 들어간다."""
        self.agent("bob")
        self.agent("live-sender", state="live-idle")
        self.agent("dead-sender", state="dormant", last_seen=self.r.now() - 99999)
        self.msg("bob", frm="live-sender", created=self.r.now() - 7200)
        self.msg("bob", frm="dead-sender", created=self.r.now() - 7200)
        self.r._sweep_ttl(self.c)
        notices = self.c.execute(
            "SELECT to_agent FROM messages WHERE type='notice'").fetchall()
        self.assertEqual([n["to_agent"] for n in notices], ["live-sender"])

    def test_ttl_never_notices_about_a_notice(self):
        """notice 만료가 또 notice 를 낳으면 자기증식한다."""
        self.agent("bob")
        self.agent("__relay__", state="live-active")
        self.msg("bob", frm="__relay__", mtype="notice", created=self.r.now() - 7200)
        self.r._sweep_ttl(self.c)
        self.assertEqual(self.c.execute(
            "SELECT count(*) n FROM messages WHERE type='notice' "
            "AND state='queued'").fetchone()["n"], 0)

    def test_ttl_cancels_open_ticket(self):
        self.agent("bob")
        mid = self.msg("bob", created=self.r.now() - 7200, priority="blocking")
        self.c.execute("INSERT INTO tickets VALUES(?,?,?,?,?)",
                       ("tk-1", mid, "alice", "open", self.r.now()))
        self.r._sweep_ttl(self.c)
        self.assertEqual(self.c.execute(
            "SELECT status FROM tickets WHERE id='tk-1'").fetchone()["status"],
            "cancelled")

    def test_ttl_leaves_escalated_review_alone(self):
        """review 는 부활 전용 상태(escalated) — 스윕 대상이 아니다."""
        self.agent("bob")
        mid = self.msg("bob", created=self.r.now() - 7200, state="escalated")
        self.r._sweep_ttl(self.c)
        self.assertEqual(self.state_of(mid), "escalated")

    # ── D2: 재큐는 injected_at 기준 + 상한 + 커서 재발급 ──
    def test_requeue_uses_injected_at_not_created(self):
        """오래 전에 '생성'됐지만 방금 '주입'된 메시지를 되돌리면 안 된다."""
        self.agent("bob")
        mid = self.msg("bob", created=self.r.now() - 99999, state="injected",
                       injected_at=self.r.now() - 10)
        self.assertEqual(self.r._sweep_requeue(self.c), 0)
        self.assertEqual(self.state_of(mid), "injected")

    def test_requeue_fires_once_injection_goes_unanswered(self):
        self.agent("bob")
        mid = self.msg("bob", state="injected",
                       injected_at=self.r.now() - self.r.REQUEUE_AFTER_S - 10)
        self.assertEqual(self.r._sweep_requeue(self.c), 1)
        self.assertEqual(self.state_of(mid), "queued")

    def test_requeue_respects_attempt_cap(self):
        self.agent("bob")
        mid = self.msg("bob", state="injected",
                       injected_at=self.r.now() - self.r.REQUEUE_AFTER_S - 10,
                       inject_count=self.r.MAX_INJECT_ATTEMPTS)
        self.assertEqual(self.r._sweep_requeue(self.c), 0)
        self.assertEqual(self.state_of(mid), "injected")

    def test_requeue_never_touches_blocking_or_replies(self):
        """blocking 은 lease·debounce 경로가, reply 는 발신자 쪽이 따로 관리한다."""
        self.agent("bob")
        old = self.r.now() - self.r.REQUEUE_AFTER_S - 10
        b = self.msg("bob", state="injected", injected_at=old, priority="blocking")
        self.r._sweep_requeue(self.c)
        self.assertEqual(self.state_of(b), "injected")

    def test_requeue_skips_terminal_types(self):
        """notice·reply 는 응답이 올 리 없다 — 재큐하면 같은 내용을 반복 주입한다."""
        self.agent("bob")
        old = self.r.now() - self.r.REQUEUE_AFTER_S - 10
        n = self.msg("bob", state="injected", injected_at=old, mtype="notice")
        rp = self.msg("bob", state="injected", injected_at=old, mtype="reply")
        consult = self.msg("bob", state="injected", injected_at=old)
        self.assertEqual(self.r._sweep_requeue(self.c), 1)
        self.assertEqual(self.state_of(n), "injected")
        self.assertEqual(self.state_of(rp), "injected")
        self.assertEqual(self.state_of(consult), "queued")

    def test_requeue_reissues_cursor_so_worker_sees_it_again(self):
        """커서를 재발급하지 않으면 h_poll 의 cursor > ? 때문에 영원히 안 나온다."""
        self.agent("bob")
        mid = self.msg("bob", state="injected",
                       injected_at=self.r.now() - self.r.REQUEUE_AFTER_S - 10)
        before = self.c.execute("SELECT cursor FROM messages WHERE id=?",
                                (mid,)).fetchone()["cursor"]
        self.msg("bob")           # 뒤에 더 큰 커서를 만든다
        self.r._sweep_requeue(self.c)
        after = self.c.execute("SELECT cursor FROM messages WHERE id=?",
                               (mid,)).fetchone()["cursor"]
        self.assertGreater(after, before)
        # 재큐분이 워커의 다음 폴(cursor > before)에 실제로 잡히는지
        self.assertIn(mid, [d["id"] for d in self.r.h_poll(
            {}, {"home": ["local"], "cursor": [str(before)],
                 "wait": ["1"]})["deliveries"]])

    def test_ack_injected_stamps_injected_at(self):
        self.agent("bob")
        mid = self.msg("bob")
        self.r.h_ack({"id": mid, "state": "injected", "via": "uds"}, {})
        row = self.c.execute("SELECT state,injected_at,inject_count FROM messages "
                             "WHERE id=?", (mid,)).fetchone()
        self.assertEqual(row["state"], "injected")
        self.assertIsNotNone(row["injected_at"])
        self.assertEqual(row["inject_count"], 1)

    def test_wake_failed_does_not_change_state(self):
        """웨이크 실패는 배달 실패가 아니다 — 훅·부활 폴백이 그대로 집어가야 한다."""
        self.agent("bob")
        mid = self.msg("bob")
        out = self.r.h_ack({"id": mid, "state": "wake_failed",
                            "detail": "uds-write-failed"}, {})
        self.assertTrue(out["ok"])
        self.assertEqual(self.state_of(mid), "queued")
        self.assertEqual(self.c.execute(
            "SELECT count(*) n FROM metrics WHERE key='wake.fail'").fetchone()["n"], 1)

    def test_ack_rejects_forged_answered(self):
        self.agent("bob")
        mid = self.msg("bob")
        self.assertFalse(self.r.h_ack({"id": mid, "state": "answered"}, {})["ok"])
        self.assertEqual(self.state_of(mid), "queued")

    # ── D3: stale live 강등 ─────────────────────────────
    def test_stale_agent_demoted(self):
        self.agent("ghost", state="live-active", last_seen=self.r.now() - 99999)
        self.agent("fresh", state="live-idle")
        self.r._sweep_stale_agents(self.c)
        st = dict(self.c.execute(
            "SELECT name,state FROM agents WHERE name IN ('ghost','fresh')").fetchall()[0])
        rows = {r["name"]: r["state"] for r in self.c.execute(
            "SELECT name,state FROM agents")}
        self.assertEqual(rows["ghost"], "dormant")
        self.assertEqual(rows["fresh"], "live-idle")
        self.assertIsNotNone(st)

    # ── msg_socket 저장/노출 ────────────────────────────
    def test_msg_socket_updated_on_reregister(self):
        """pid 가 바뀌면 소켓 경로도 바뀐다 — 갱신 안 되면 stale 소켓을 쥔다."""
        self.agent("bob", msg_socket="/tmp/cc-socks/1.sock")
        self.agent("bob", msg_socket="/tmp/cc-socks/2.sock")
        self.assertEqual(self.c.execute(
            "SELECT msg_socket FROM agents WHERE name='bob'").fetchone()["msg_socket"],
            "/tmp/cc-socks/2.sock")

    def test_empty_msg_socket_does_not_wipe(self):
        """소켓 없는 구버전 세션의 재등록이 기존 값을 지우면 안 된다."""
        self.agent("bob", msg_socket="/tmp/cc-socks/1.sock")
        self.agent("bob", msg_socket="")
        self.assertEqual(self.c.execute(
            "SELECT msg_socket FROM agents WHERE name='bob'").fetchone()["msg_socket"],
            "/tmp/cc-socks/1.sock")

    def test_partial_register_keeps_existing_name(self):
        """부분 갱신이 이름을 덮으면 그 이름 앞으로 쌓인 메시지가 배달에서 떨어진다."""
        self.agent("hub-architect", session="s-1")
        self.r.h_register({"session": "s-1", "cwd": "/x", "task_hint": "일",
                           "msg_socket": "/tmp/cc-socks/9.sock"}, {})
        row = self.c.execute("SELECT name,msg_socket FROM agents WHERE session='s-1'"
                             ).fetchone()
        self.assertEqual(row["name"], "hub-architect")
        self.assertEqual(row["msg_socket"], "/tmp/cc-socks/9.sock")

    def test_msg_socket_not_leaked_in_agents_or_who(self):
        """조망용 응답에 다른 세션의 주입 주소를 뿌리지 않는다."""
        self.agent("bob", msg_socket="/tmp/cc-socks/1.sock")
        self.c.execute("UPDATE agents SET paths=? WHERE name='bob'", ('["src/**"]',))
        self.c.commit()
        self.assertNotIn("msg_socket", str(self.r.h_agents({}, {})))
        self.assertNotIn("msg_socket", str(self.r.h_who({}, {"path": ["src/a.rs"]})))
        self.assertIn("msg_socket", str(self.r.h_agent_by_session(
            {}, {"session": ["bob"]})))

    # ── 배달 회계: injected 는 근거와 함께만 (H2) ────────
    def test_peer_reject_states_do_not_mark_delivered(self):
        """held/refused/denied/expired/dropped 는 '안 받았다'는 회신이다.

        injected 로 찍던 시절 배달 회계가 거짓 양성이었다 — 사람 승인 대기로 파킹된
        메시지를 relay 는 배달됨으로 알고 재큐·TTL 안전망이 전부 어긋났다.
        """
        self.agent("bob")
        for st in self.r.PEER_REJECT_STATES:
            mid = self.msg("bob")
            out = self.r.h_ack({"id": mid, "state": st, "via": "uds",
                                "detail": "d"}, {})
            self.assertTrue(out["ok"])
            self.assertFalse(out["delivered"])
            self.assertEqual(self.state_of(mid), "queued")   # 폴백이 그대로 집어간다
            self.assertTrue(self.c.execute(
                "SELECT wake_status FROM messages WHERE id=?",
                (mid,)).fetchone()["wake_status"].startswith(st))

    def test_unconfirmed_wake_does_not_mark_delivered(self):
        self.agent("bob")
        mid = self.msg("bob")
        out = self.r.h_ack({"id": mid, "state": "wake_unconfirmed", "via": "uds",
                            "detail": "no-signal/clean-eof"}, {})
        self.assertTrue(out["ok"])
        self.assertEqual(self.state_of(mid), "queued")
        self.assertEqual(self.c.execute(
            "SELECT count(*) n FROM metrics WHERE key='wake.unconfirmed'"
        ).fetchone()["n"], 1)

    def test_injected_records_delivery_evidence(self):
        """'injected' 만으로는 확증 배달과 추정 배달을 사후에 못 가른다."""
        self.agent("bob")
        mid = self.msg("bob")
        self.r.h_ack({"id": mid, "state": "injected", "via": "uds",
                      "evidence": "receipt-delivered"}, {})
        self.assertEqual(self.c.execute(
            "SELECT wake_status FROM messages WHERE id=?",
            (mid,)).fetchone()["wake_status"], "receipt-delivered")

    # ── H1: 주입 주소·이름 탈취 차단 ─────────────────────
    def test_msg_socket_is_ignored_when_caller_is_not_a_worker(self):
        """워커 토큰 없는 호출자는 주입 주소를 갱신할 수 없다."""
        self.agent("bob", msg_socket="/tmp/cc-socks/1.sock")
        self.r.TOKENS["mac"] = "secret"        # 토큰 체제 활성화
        try:
            self.r._local.worker = None        # 워커로 인증되지 않은 호출자
            self.r.h_register({"session": "bob", "name": "bob",
                               "msg_socket": "/tmp/attacker.sock"}, {})
            self.assertEqual(self.c.execute(
                "SELECT msg_socket FROM agents WHERE name='bob'"
            ).fetchone()["msg_socket"], "/tmp/cc-socks/1.sock")
            self.r._local.worker = "mac"       # 워커면 갱신된다
            self.r.h_register({"session": "bob", "name": "bob",
                               "msg_socket": "/tmp/cc-socks/2.sock"}, {})
            self.assertEqual(self.c.execute(
                "SELECT msg_socket FROM agents WHERE name='bob'"
            ).fetchone()["msg_socket"], "/tmp/cc-socks/2.sock")
        finally:
            self.r.TOKENS.clear()
            self.r._local.worker = None

    def test_live_agents_name_cannot_be_squatted_by_a_new_session(self):
        """이름을 뺏기면 그 이름 앞으로 오는 배달이 통째로 신규 행으로 넘어간다."""
        self.agent("hub-architect", session="s-real")
        out = self.r.h_register({"session": "s-attacker", "name": "hub-architect",
                                 "state": "live-active"}, {})
        self.assertIn("name_conflict", out)
        self.assertEqual(self.c.execute(
            "SELECT name FROM agents WHERE session='s-attacker'"
        ).fetchone()["name"], "session-s-attack")
        # 원 소유자가 여전히 그 이름의 배달 대상이다
        rows = self.c.execute(
            "SELECT session FROM agents WHERE name='hub-architect'").fetchall()
        self.assertEqual([r["session"] for r in rows], ["s-real"])

    def test_dormant_agents_name_can_be_reclaimed(self):
        """세션이 죽으면 이름은 풀린다 — 재기동(새 session id)이 막히면 안 된다."""
        self.agent("worker-a", session="s-old", state="dormant",
                   last_seen=self.r.now() - 99999)
        out = self.r.h_register({"session": "s-new", "name": "worker-a",
                                 "state": "live-active"}, {})
        self.assertNotIn("name_conflict", out)
        self.assertEqual(self.c.execute(
            "SELECT name FROM agents WHERE session='s-new'").fetchone()["name"],
            "worker-a")

    def test_same_session_can_always_rename_itself(self):
        self.agent("hub-architect", session="s-1")
        out = self.r.h_register({"session": "s-1", "name": "hub-architect",
                                 "state": "live-active"}, {})
        self.assertNotIn("name_conflict", out)

    # ── 회귀 금지: 기존 경로 ─────────────────────────────
    def test_migration_preserves_existing_rows(self):
        self.agent("bob")
        mid = self.msg("bob")
        self.r.migrate(self.c)          # 재실행 안전 (idempotent)
        self.assertEqual(self.state_of(mid), "queued")

    def test_send_and_reply_still_work(self):
        self.agent("bob")
        self.agent("alice")
        r = self.r.h_send({"from_session": "alice", "from_agent": "alice",
                           "to": "bob", "type": "consult", "priority": "blocking",
                           "body": "q"}, {})
        self.assertTrue(r["ok"])
        self.assertIsNotNone(r["ticket"])
        rr = self.r.h_reply({"reply_to": r["id"], "from_session": "bob",
                             "from_agent": "bob", "body": "a"}, {})
        self.assertTrue(rr["ok"])
        self.assertEqual(self.state_of(r["id"]), "answered")

    def test_unregistered_sender_still_rejected(self):
        self.agent("bob")
        self.assertFalse(self.r.h_send({"from_session": "nobody", "to": "bob",
                                        "body": "q"}, {})["ok"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
