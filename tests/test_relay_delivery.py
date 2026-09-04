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

    def test_ttl_does_not_call_a_requeued_delivered_message_undelivered(self):
        """재큐가 state 를 queued 로 되돌려도 inject_count 는 남는다 — 배달 사실의 정본.

        운영 실측(2026-08-22): m-5cd30c40 은 inject_count=1·injected_at 17:20:35 인데
        18:20:31 에 '미배달 만료 — 수신자가 유휴/종료 상태였을 수 있다' 로 통지됐다.
        """
        self.agent("bob")
        self.agent("alice", state="live-active")
        mid = self.msg("bob", created=self.r.now() - 7200, state="queued",
                       injected_at=self.r.now() - 7000, inject_count=1)
        self.r._sweep_ttl(self.c)
        self.assertEqual(self.state_of(mid), "delivered")
        self.assertEqual(self.c.execute(
            "SELECT count(*) n FROM messages WHERE type='notice'").fetchone()["n"], 0)

    def test_ttl_does_not_call_a_deferred_message_undelivered(self):
        """defer 는 수신자가 '봤고 미룬다'고 회신한 것이다 — 미배달이 아니다."""
        self.agent("bob")
        self.agent("alice", state="live-active")
        mid = self.msg("bob", created=self.r.now() - 7200, state="deferred",
                       injected_at=self.r.now() - 7000, inject_count=1)
        self.r._sweep_ttl(self.c)
        self.assertEqual(self.state_of(mid), "delivered")
        self.assertEqual(self.c.execute(
            "SELECT count(*) n FROM messages WHERE type='notice'").fetchone()["n"], 0)

    def test_ttl_still_reports_a_never_injected_message(self):
        """양성 대조: 한 번도 주입되지 않은 건은 여전히 미배달로 통지돼야 한다."""
        self.agent("bob")
        self.agent("alice", state="live-active")
        mid = self.msg("bob", created=self.r.now() - 7200, inject_count=0)
        self.r._sweep_ttl(self.c)
        self.assertEqual(self.state_of(mid), "expired")
        self.assertEqual(self.c.execute(
            "SELECT count(*) n FROM messages WHERE type='notice'").fetchone()["n"], 1)

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

    def test_late_wake_failure_does_not_overwrite_a_delivered_messages_evidence(self):
        """다른 레인(훅)으로 이미 배달된 건의 근거를 늦은 웨이크 실패가 덮으면 안 된다.

        운영 데이터 실측: state='injected' 인데 wake_status='wake_unconfirmed:...' 인
        행이 생겨 감사 기록이 거짓말을 했다.
        """
        self.agent("bob")
        mid = self.msg("bob")
        self.r.h_ack({"id": mid, "state": "injected", "evidence": "hook"}, {})
        for st in ("wake_unconfirmed", "wake_failed", "held", "refused"):
            self.r.h_ack({"id": mid, "state": st, "via": "uds", "detail": "late"}, {})
        row = self.c.execute("SELECT state,wake_status FROM messages WHERE id=?",
                             (mid,)).fetchone()
        self.assertEqual(row["state"], "injected")
        self.assertEqual(row["wake_status"], "hook")

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

    # ── defer 재배달은 실제로 돌아와야 한다 (커서 축의 두 번째 구멍) ──
    def test_deferred_message_comes_back_to_the_worker(self):
        """'30분 후 재배달' 을 발신자에게 통지까지 해 놓고 커서를 안 줘서 조용히 깨졌다."""
        self.agent("bob")
        mid = self.msg("bob", priority="blocking")
        out = self.r.h_poll({}, {"home": ["local"], "cursor": ["0"], "wait": ["1"]})
        at = max(d["cursor"] for d in out["deliveries"])      # 워커가 여기까지 소비
        self.r.h_ack({"id": mid, "state": "injected", "via": "uds"}, {})
        self.r.h_defer({"id": mid}, {})
        self.c.execute("UPDATE timers SET due_at=? WHERE msg_id=? AND kind='redeliver'",
                       (self.r.now() - 1, mid))
        self.c.commit()
        self.r._fire_due(self.c)                              # 실물 타이머 분기
        self.c.commit()
        out2 = self.r.h_poll({}, {"home": ["local"], "cursor": [str(at)], "wait": ["1"]})
        self.assertIn(mid, [d["id"] for d in out2["deliveries"]])

    def test_redeliver_does_not_resurrect_an_answered_message(self):
        """양성 대조: 이미 답한 건은 재배달 타이머가 살아 있어도 큐로 돌아오지 않는다."""
        self.agent("bob")
        mid = self.msg("bob", priority="blocking")
        self.r.h_defer({"id": mid}, {})
        self.c.execute("UPDATE messages SET state='answered' WHERE id=?", (mid,))
        self.c.execute("UPDATE timers SET due_at=? WHERE msg_id=? AND kind='redeliver'",
                       (self.r.now() - 1, mid))
        self.c.commit()
        self.r._fire_due(self.c)
        self.c.commit()
        self.assertEqual(self.state_of(mid), "answered")

    # ── 리스 타이머는 lease_expires 를 따른다 ────────────
    def test_refreshed_lease_is_not_escalated_by_the_old_timer(self):
        """재주입으로 리스를 갱신했는데 옛 due_at 이 즉시 발화해 유료 부활을 띄웠다."""
        self.agent("bob")
        mid = self.msg("bob", priority="blocking")
        self.r.h_ack({"id": mid, "state": "injected", "via": "uds"}, {})
        self.c.execute("UPDATE timers SET due_at=? WHERE msg_id=? AND kind='lease'",
                       (self.r.now() - 1800, mid))
        self.c.execute("UPDATE messages SET state='queued' WHERE id=?", (mid,))
        self.c.commit()
        self.r.h_ack({"id": mid, "state": "injected", "via": "uds"}, {})  # 리스 갱신
        self.c.commit()
        self.r._fire_due(self.c)
        self.c.commit()
        self.assertEqual(self.c.execute(
            "SELECT COUNT(*) AS n FROM timers WHERE fired=2").fetchone()["n"], 0)
        due_in = self.c.execute(
            "SELECT due_at FROM timers WHERE msg_id=? AND kind='lease'",
            (mid,)).fetchone()["due_at"] - self.r.now()
        self.assertGreater(due_in, 0)          # 남은 리스만큼 미뤄졌다

    def test_expired_lease_still_escalates(self):
        """양성 대조: 리스가 진짜 끝났으면 승격은 그대로 일어나야 한다."""
        self.agent("bob")
        mid = self.msg("bob", priority="blocking")
        self.r.h_ack({"id": mid, "state": "injected", "via": "uds"}, {})
        self.c.execute("UPDATE timers SET due_at=? WHERE msg_id=? AND kind='lease'",
                       (self.r.now() - 1, mid))
        self.c.execute("UPDATE messages SET lease_expires=? WHERE id=?",
                       (self.r.now() - 1, mid))
        self.c.commit()
        self.r._fire_due(self.c)
        self.c.commit()
        self.assertEqual(self.c.execute(
            "SELECT COUNT(*) AS n FROM timers WHERE fired=2").fetchone()["n"], 1)

    # ── 커서 할당자는 하나 (적대 리뷰 MED-2) ─────────────
    def test_requeue_gives_each_row_its_own_cursor(self):
        """비상관 서브쿼리는 문 단위로 1회 평가된다 — 재큐 전량이 같은 커서를 받았다."""
        self.agent("bob")
        ids = [self.msg("bob", state="injected",
                        injected_at=self.r.now() - 9999, inject_count=1)
               for _ in range(3)]
        self.r._sweep_requeue(self.c)
        self.c.commit()
        curs = [self.c.execute("SELECT cursor FROM messages WHERE id=?",
                               (i,)).fetchone()["cursor"] for i in ids]
        self.assertEqual(len(set(curs)), 3, f"커서 중복: {curs}")

    def test_requeued_cursor_does_not_swallow_the_next_new_message(self):
        """MAX(cursor)+1 이 다음 rowid 와 겹치면 신규 메시지가 영원히 안 보인다."""
        self.agent("bob")
        old = self.msg("bob", state="injected",
                       injected_at=self.r.now() - 9999, inject_count=1)
        self.r._sweep_requeue(self.c)
        self.c.commit()
        at = self.c.execute("SELECT cursor FROM messages WHERE id=?",
                            (old,)).fetchone()["cursor"]        # 워커가 여기까지 소비
        fresh = self.msg("bob")
        out = self.r.h_poll({}, {"home": ["local"], "cursor": [str(at)], "wait": ["1"]})
        self.assertIn(fresh, [d["id"] for d in out["deliveries"]])

    def test_cursor_is_monotonic_across_inserts_and_requeues(self):
        self.agent("bob")
        seen = []
        for _ in range(3):
            mid = self.msg("bob", state="injected",
                           injected_at=self.r.now() - 9999, inject_count=1)
            seen.append(mid)
        self.r._sweep_requeue(self.c)
        self.c.commit()
        after = self.msg("bob")
        curs = [self.c.execute("SELECT cursor FROM messages WHERE id=?",
                               (i,)).fetchone()["cursor"] for i in seen + [after]]
        self.assertEqual(curs, sorted(curs))
        self.assertEqual(len(set(curs)), len(curs))

    # ── 리스 타이머 중복 (적대 리뷰 MED-3) ───────────────
    def _lease_timers(self, mid):
        return self.c.execute("SELECT COUNT(*) AS n FROM timers WHERE msg_id=? "
                              "AND kind='lease'", (mid,)).fetchone()["n"]

    def test_duplicate_injected_ack_does_not_stack_lease_timers(self):
        """h_ack 은 UPDATE 이전 스냅샷으로 판정했다 — 중복 ack 마다 타이머가 늘었다."""
        self.agent("bob")
        mid = self.msg("bob", priority="blocking")
        for _ in range(4):
            self.r.h_ack({"id": mid, "state": "injected", "via": "hook"}, {})
        self.c.commit()
        self.assertEqual(self._lease_timers(mid), 1)
        self.assertEqual(self.c.execute(
            "SELECT inject_count FROM messages WHERE id=?",
            (mid,)).fetchone()["inject_count"], 1)

    def test_defer_redelivery_does_not_stack_lease_timers(self):
        """defer→재배달→재주입은 경합 없이도 같은 결함을 밟는 경로였다."""
        self.agent("bob")
        mid = self.msg("bob", priority="blocking")
        self.r.h_ack({"id": mid, "state": "injected", "via": "uds"}, {})
        self.r.h_defer({"id": mid}, {})
        self.c.execute("UPDATE messages SET state='queued' WHERE id=?", (mid,))
        self.r.h_ack({"id": mid, "state": "injected", "via": "hook"}, {})
        self.c.commit()
        self.assertEqual(self._lease_timers(mid), 1)

    def test_a_consumed_lease_can_be_rearmed_on_real_redelivery(self):
        """양성 대조: 이미 발화한 리스는 새 주입의 리스를 막지 않아야 한다."""
        self.agent("bob")
        mid = self.msg("bob", priority="blocking")
        self.r.h_ack({"id": mid, "state": "injected", "via": "uds"}, {})
        self.c.execute("UPDATE timers SET fired=1 WHERE msg_id=? AND kind='lease'",
                       (mid,))
        self.c.execute("UPDATE messages SET state='queued' WHERE id=?", (mid,))
        self.r.h_ack({"id": mid, "state": "injected", "via": "hook"}, {})
        self.c.commit()
        self.assertEqual(self._lease_timers(mid), 2)
        self.assertEqual(self.c.execute(
            "SELECT COUNT(*) AS n FROM timers WHERE msg_id=? AND kind='lease' "
            "AND fired=0", (mid,)).fetchone()["n"], 1)

    # ── 훅 레인도 배달 스탬프를 남긴다 (적대 리뷰 MED-6) ──
    def test_hook_lane_ack_stamps_injected_at_and_count(self):
        """스탬프가 없으면 관측 불가 + _sweep_requeue 대상에서 통째로 빠진다."""
        self.agent("bob")
        mid = self.msg("bob")
        self.r.h_ack({"id": mid, "state": "injected"}, {})   # via 없음 = 훅 레인
        self.c.commit()
        row = self.c.execute("SELECT injected_at, inject_count, wake_status "
                             "FROM messages WHERE id=?", (mid,)).fetchone()
        self.assertIsNotNone(row["injected_at"])
        self.assertEqual(row["inject_count"], 1)
        self.assertEqual(row["wake_status"], "hook")

    def test_hook_delivered_message_is_requeueable(self):
        self.agent("bob")
        mid = self.msg("bob")
        self.r.h_ack({"id": mid, "state": "injected"}, {})
        self.c.execute("UPDATE messages SET injected_at=? WHERE id=?",
                       (self.r.now() - 9999, mid))
        self.c.commit()
        self.assertEqual(self.r._sweep_requeue(self.c), 1)

    # ── 강등은 죽음의 증거를 요구한다 (적대 리뷰 MED-5) ──
    def test_liveness_outage_does_not_demote_the_whole_fleet(self):
        """워커의 관측이 죽은 것과 세션이 죽은 것은 다른 축이다."""
        for i in range(5):
            self.agent(f"a{i}", session=f"s{i}", state="live-active",
                       last_seen=self.r.now() - 700)
        self.r._sweep_stale_agents(self.c)
        self.c.commit()
        states = [r["state"] for r in self.c.execute("SELECT state FROM agents")]
        self.assertEqual(states.count("dormant"), 0)

    def test_observed_sweep_demotes_the_sessions_it_omitted(self):
        """양성 대조: 워커가 열거했는데 없는 세션 = 죽음의 적극적 증거."""
        for i in range(3):
            self.agent(f"a{i}", session=f"s{i}", state="live-active",
                       last_seen=self.r.now() - 700)
        self.r.h_liveness({"agents": [{"session": "s0", "state": "live-idle"}],
                           "observed": True, "home": "local"}, {})
        self.c.commit()
        self.r._sweep_stale_agents(self.c)
        self.c.commit()
        rows = {r["session"]: r["state"] for r in
                self.c.execute("SELECT session, state FROM agents")}
        self.assertEqual(rows["s0"], "live-idle")
        self.assertEqual(rows["s1"], "dormant")
        self.assertEqual(rows["s2"], "dormant")

    def test_a_stale_observation_is_not_evidence(self):
        """워커가 4분 전에 죽었다면 그때의 열거는 지금의 근거가 될 수 없다."""
        self.agent("a0", session="s0", state="live-active",
                   last_seen=self.r.now() - 700)
        self.r.h_liveness({"agents": [], "observed": True, "home": "local"}, {})
        self.c.execute("UPDATE worker_sweeps SET last_ok=?",
                       (self.r.now() - self.r.LIVENESS_FRESH_S - 60,))
        self.c.commit()
        self.r._sweep_stale_agents(self.c)
        self.c.commit()
        self.assertEqual(self.c.execute(
            "SELECT state FROM agents WHERE session='s0'").fetchone()["state"],
            "live-active")

    def test_failed_enumeration_never_counts_as_an_observation(self):
        """워커는 실패한 스윕을 보내지 않는다 — observed 없는 보고는 근거가 아니다."""
        self.agent("a0", session="s0", state="live-active",
                   last_seen=self.r.now() - 700)
        self.r.h_liveness({"agents": []}, {})          # observed 플래그 없음
        self.c.commit()
        self.assertIsNone(self.c.execute(
            "SELECT last_ok FROM worker_sweeps WHERE home='local'").fetchone())
        self.r._sweep_stale_agents(self.c)
        self.c.commit()
        self.assertEqual(self.c.execute(
            "SELECT state FROM agents WHERE session='s0'").fetchone()["state"],
            "live-active")

    def test_hard_backstop_demotes_an_unobserved_home_after_a_day(self):
        self.agent("a0", session="s0", state="live-active",
                   last_seen=self.r.now() - 25 * 3600)
        self.r._sweep_stale_agents(self.c)
        self.c.commit()
        self.assertEqual(self.c.execute(
            "SELECT state FROM agents WHERE session='s0'").fetchone()["state"],
            "dormant")

    # ── 동명 세션 팬아웃 (적대 리뷰 MED-10) ──────────────
    def test_poll_does_not_fan_out_across_duplicate_agent_names(self):
        """이름은 조회 축일 뿐 배달 단위가 아니다 — 조인은 1건을 N건으로 불린다."""
        self.agent("dup", session="s-1")
        self.agent("other", session="s-2")
        self.c.execute("UPDATE agents SET name='dup' WHERE session='s-2'")
        mid = self.msg("dup")
        self.c.commit()
        out = self.r.h_poll({}, {"home": ["local"], "cursor": ["0"], "wait": ["1"]})
        self.assertEqual([d["id"] for d in out["deliveries"]], [mid])

    def test_poll_still_scopes_by_home(self):
        """양성 대조: 다른 홈의 수신자 앞 메시지는 이 워커가 가져가지 않는다."""
        self.r.h_register({"name": "remote", "session": "s-r", "home": "desktop"}, {})
        self.msg("remote")
        self.c.commit()
        out = self.r.h_poll({}, {"home": ["local"], "cursor": ["0"], "wait": ["1"]})
        self.assertEqual(out["deliveries"], [])

    # ── ephemeral 로스터 (적대 리뷰 LOW-11) ──────────────
    def test_ephemeral_session_is_hidden_from_roster_and_who(self):
        self.r.h_register({"name": "probe-1", "session": "s-p", "home": "local",
                           "ephemeral": True, "paths": ["worker/worker.py"]}, {})
        self.agent("real", session="s-real")
        self.c.execute("UPDATE agents SET paths=? WHERE session='s-real'",
                       ('["worker/worker.py"]',))
        self.c.commit()
        names = [a["name"] for a in self.r.h_agents({}, {})["agents"]]
        self.assertNotIn("probe-1", names)
        self.assertIn("real", names)
        who = self.r.h_who({}, {"path": ["worker/worker.py"]})
        self.assertEqual([m["agent"] for m in who["matches"]], ["real"])

    def test_ephemeral_session_still_gets_its_messages(self):
        """감추는 것은 조망 화면뿐 — 배달·조회 경로는 그대로여야 한다."""
        self.r.h_register({"name": "probe-1", "session": "s-p", "home": "local",
                           "ephemeral": True}, {})
        mid = self.msg("probe-1")
        self.c.commit()
        out = self.r.h_poll({}, {"home": ["local"], "cursor": ["0"], "wait": ["1"]})
        self.assertEqual([d["id"] for d in out["deliveries"]], [mid])
        self.assertIsNotNone(self.r.h_agent({}, {"name": ["probe-1"]})["agent"])

    def test_ephemeral_flag_is_sticky_across_later_registers(self):
        """훅의 부분 등록은 env 를 못 싣는다 — 표식이 지워지면 프로브가 되살아난다."""
        self.r.h_register({"name": "probe-1", "session": "s-p", "home": "local",
                           "ephemeral": True}, {})
        self.r.h_register({"session": "s-p", "home": "local", "partial": True}, {})
        self.c.commit()
        self.assertNotIn("probe-1",
                         [a["name"] for a in self.r.h_agents({}, {})["agents"]])

    def test_audit_view_can_still_see_ephemeral(self):
        self.r.h_register({"name": "probe-1", "session": "s-p", "home": "local",
                           "ephemeral": True}, {})
        self.c.commit()
        names = [a["name"] for a in self.r.h_agents({}, {"all": ["1"]})["agents"]]
        self.assertIn("probe-1", names)

    def test_normal_register_is_not_ephemeral(self):
        self.agent("real")
        self.assertIn("real", [a["name"] for a in self.r.h_agents({}, {})["agents"]])


    def test_fyi_is_not_requeued(self):
        """fyi 는 정의상 '읽고 넘겨도 되는' 등급 — 응답이 안 와도 재노출하면 소음이다.

        실측: 공지 하나가 10분 간격으로 두 번 배달됐다(수신자에게 답할 의무 없음).
        판별축은 배달 레인이 아니라 응답 기대 여부다 — 훅 배달분 재큐는 유지된다
        (표시됐다는 사실이 응답을 보장하지 않으므로, 위 test_hook_delivered… 참조).
        """
        self.agent("bob")
        mid = self.msg("bob", priority="fyi")
        self.r.h_ack({"id": mid, "state": "injected"}, {})
        self.c.execute("UPDATE messages SET injected_at=? WHERE id=?",
                       (self.r.now() - 9999, mid))
        self.c.commit()
        self.assertEqual(self.r._sweep_requeue(self.c), 0)

    def test_fyi_lands_terminal_on_first_delivery(self):
        """fyi 는 1회 배달로 끝난다 — 'injected' 로 두면 워커가 영원히 재웨이크한다.

        실측 신고(08-31): fyi 회람이 한 세션에 **1분 간격 60회+** 재배달됐고 DB 에
        injected 상태 fyi 가 914건 쌓여 있었다. fyi 는 회신 금지라 answered 로 갈 일이
        없고 재큐 대상도 아니라, 종착에 못 닿으면 갇힌다.
        """
        self.agent("bob")
        mid = self.msg("bob", priority="fyi")
        self.r.h_ack({"id": mid, "state": "injected"}, {})
        row = self.c.execute("SELECT state, inject_count FROM messages WHERE id=?",
                             (mid,)).fetchone()
        self.assertEqual(row["state"], "delivered")   # 종착 — 워커가 캐시에서 버린다
        self.assertEqual(row["inject_count"], 1)      # 배달 사실은 남는다

    def test_normal_still_lands_injected(self):
        """양성 대조: normal 은 종전대로 injected(응답 기다림·재큐 대상)."""
        self.agent("bob2", session="s-b2")
        mid = self.msg("bob2", priority="normal")
        self.r.h_ack({"id": mid, "state": "injected"}, {})
        self.assertEqual(self.c.execute("SELECT state FROM messages WHERE id=?",
                                        (mid,)).fetchone()["state"], "injected")

    def test_defer_on_fyi_closes_instead_of_rescheduling(self):
        """수신자의 정지 수단 — fyi 를 30분 뒤 또 보여줄 이유가 없다."""
        self.agent("bob3", session="s-b3")
        mid = self.msg("bob3", priority="fyi")
        r = self.r.h_defer({"id": mid}, {})
        self.assertTrue(r.get("closed"))
        self.assertEqual(self.c.execute("SELECT state FROM messages WHERE id=?",
                                        (mid,)).fetchone()["state"], "delivered")
        self.assertEqual(self.c.execute(
            "SELECT COUNT(*) c FROM timers WHERE msg_id=? AND kind='redeliver'",
            (mid,)).fetchone()["c"], 0)

    def test_agents_listing_reports_truncation(self):
        """절단은 말해야 한다 — 조용한 절단은 조용한 오답이다.

        실측(08-31): 기본 limit 40 · 함대 208 에서 PM 이 실재 세션 7곳을 '없음'으로
        판독해 유령 소동이 났다. 오늘만 같은 형상 다섯 번째(타임아웃→'없음',
        cwd 오답→'소유자 없음', 미측정→'0분', 인용 축 불일치→'빈 응답').
        """
        for i in range(5):
            self.agent(f"a{i}", session=f"trunc-{i}")
        r = self.r.h_agents({}, {"limit": ["2"]})
        self.assertEqual(r["shown"], 2)
        self.assertGreaterEqual(r["total"], 5)
        self.assertTrue(r["truncated"])

    def test_no_truncation_flag_when_all_shown(self):
        """양성 대조 — 전부 보여줬으면 truncated 는 거짓이어야 한다."""
        self.agent("solo", session="solo-1")
        r = self.r.h_agents({}, {"limit": ["500"]})
        self.assertFalse(r["truncated"])
        self.assertEqual(r["shown"], r["total"])

    def test_supersede_only_when_a_revival_answered_first(self):
        """supersede 는 '부활 사본이 먼저 답했다'일 때만 — 두 번 답한 것과 다르다.

        실측 2회 오발동. 그중 한 번은 받은 쪽이 '숨은 조건이 있나' 의심해 전송 계약층
        머지를 보류하는 실비용을 냈다(그때도 부활은 없었다).
        """
        self.agent("author", session="s-au")
        mid = self.msg("author", priority="normal")
        # 같은 세션이 두 번 답한다 — 부활 아님
        self.r.h_reply({"reply_to": mid, "from_agent": "author",
                        "from_session": "s-au", "body": "1차"}, {})
        r2 = self.r.h_reply({"reply_to": mid, "from_agent": "author",
                             "from_session": "s-au", "body": "2차"}, {})
        self.assertIsNone(r2.get("supersedes"))
        notices = self.c.execute(
            "SELECT COUNT(*) c FROM messages WHERE type='notice' "
            "AND body LIKE '%정정%'").fetchone()["c"]
        self.assertEqual(notices, 0)

    def test_supersede_fires_for_a_real_revival(self):
        """양성 대조 — 워커가 대리 게시한 부활 응답이 먼저면 supersede 가 맞다."""
        self.agent("author2", session="s-au2")
        mid = self.msg("author2", priority="normal")
        self.r.h_reply({"reply_to": mid, "from_agent": "author2",
                        "from_session": "__worker__", "body": "부활 사본 답변",
                        "meta": {"responder_session": "fork-1"}}, {})
        r2 = self.r.h_reply({"reply_to": mid, "from_agent": "author2",
                             "from_session": "s-au2", "body": "본체 늦은 답변"}, {})
        self.assertIsNotNone(r2.get("supersedes"))


    def test_agents_listing_exposes_registered_at(self):
        """소비자가 시각으로 신규를 가른다 — 없는 축을 물으면 조용히 0 이 된다.
        실측: am hire 의 codex 식별 필터가 registered_at 을 보는데 로스터가 그 칸을
        주지 않아 항상 거짓이 됐다. 등록은 정상이었는데 채용만 타임아웃났다.
        """
        self.agent("ra", session="s-ra")
        r = self.r.h_agents({}, {})
        self.assertIn("registered_at", r["agents"][0])
        self.assertIsNotNone(r["agents"][0]["registered_at"])


    def test_agents_cli_and_since_filter_server_side(self):
        """기계 소비자는 서버에서 걸러 받아야 한다 — 전체를 긁으면 절단에 걸린다.

        실측: codex 행은 last_activity 가 NULL 이라 정렬 꼴찌인데 225곳에 limit=200 이면
        정확히 그것들이 잘렸다. 등록은 정상인데 am hire 가 못 찾아 타임아웃났다.
        사람용 절단은 꼬리표로 고쳤지만 기계 경로는 조용히 유실됐다.
        """
        import time as _t
        self.agent("cx1", session="s-cx1")
        self.c.execute("UPDATE agents SET cli='codex' WHERE name='cx1'")
        self.agent("cl1", session="s-cl1")
        self.c.commit()
        r = self.r.h_agents({}, {"cli": ["codex"], "all": ["1"]})
        names = [a["name"] for a in r["agents"]]
        self.assertIn("cx1", names)
        self.assertNotIn("cl1", names)          # 축 필터가 실제로 거른다
        # since 로 과거 행을 잘라낸다
        r2 = self.r.h_agents({}, {"cli": ["codex"], "all": ["1"],
                                  "since": [str(_t.time() + 60)]})
        self.assertEqual(r2["agents"], [])


    def test_partial_register_does_not_blank_tab_coords(self):
        self.r.h_register({"session": "s-c", "name": "codex-x", "cli": "codex",
                           "cmux_surface": "S-UUID", "cmux_workspace": "W-UUID"}, {})
        # 훅의 부분 등록은 좌표를 안 싣는다 — 그게 기존 값을 지우면 안 된다
        self.r.h_register({"session": "s-c", "cwd": "/x", "partial": True}, {})
        row = self.r.db().execute(
            "SELECT cmux_surface, cmux_workspace FROM agents WHERE session='s-c'"
        ).fetchone()
        self.assertEqual(row["cmux_surface"], "S-UUID")
        self.assertEqual(row["cmux_workspace"], "W-UUID")

    def test_org_can_seed_tab_coords(self):
        """채용은 등록 직후 /org 로 좌표를 세운다 — 그 경로가 살아 있어야 한다."""
        self.r.h_register({"session": "s-c", "name": "codex-x", "cli": "codex"}, {})
        self.r.h_org({"name": "codex-x", "cmux_surface": "S1",
                      "cmux_workspace": "W1"}, {})
        row = self.r.db().execute(
            "SELECT cmux_surface FROM agents WHERE session='s-c'").fetchone()
        self.assertEqual(row["cmux_surface"], "S1")


    def test_live_codex_outranks_dormant_in_the_roster(self):
        """🔴 codex 는 last_activity 가 NULL 이라 활동 축 하나로는 늘 꼴찌다.

        기본 limit 40 / 함대 292 에서는 그게 곧 **로스터에서 사라짐**이고,
        팀장은 "고용 실패"로 읽는다(실측 보고). 생사가 1순위여야 한다.
        """
        self.r.h_register({"session": "s-dead", "name": "old-claude",
                           "cli": "claude", "state": "dormant"}, {})
        self.r.db().execute("UPDATE agents SET last_activity=? WHERE session='s-dead'",
                            (self.r.now(),))
        self.r.h_register({"session": "s-live", "name": "new-codex",
                           "cli": "codex", "state": "live-idle"}, {})
        # codex 행의 활동 축은 비어 있다 — 그래도 살아 있으면 앞이어야 한다
        self.r.db().execute("UPDATE agents SET last_activity=NULL "
                            "WHERE session='s-live'")
        names = [a["name"] for a in self.r.h_agents({}, {"limit": ["40"]})["agents"]]
        self.assertIn("new-codex", names)
        self.assertLess(names.index("new-codex"), names.index("old-claude"))


    def test_worker_notice_is_worker_only(self):
        """아무나 __relay__ 이름으로 통지를 찍으면 그 표식이 무의미해진다."""
        self.r.h_register({"session": "s-b", "name": "boss"}, {})
        self.r.caller_is_worker = lambda: False
        try:
            self.assertFalse(self.r.h_worker_notice(
                {"to_agent": "boss", "body": "x"}, {})["ok"])
        finally:
            self.r.caller_is_worker = lambda: True
        out = self.r.h_worker_notice({"to_agent": "boss", "body": "죽었다"}, {})
        self.assertTrue(out["ok"])
        row = self.r.db().execute(
            "SELECT from_agent, type, body FROM messages WHERE id=?",
            (out["id"],)).fetchone()
        self.assertEqual((row["from_agent"], row["type"]), ("__relay__", "notice"))

    def test_worker_notice_refuses_an_unknown_recipient(self):
        """없는 이름으로 보내면 조용히 사라진다 — 그건 알림이 아니라 침묵이다."""
        self.assertEqual(self.r.h_worker_notice(
            {"to_agent": "nobody", "body": "x"}, {})["error"], "unknown-agent")


    def test_ambiguous_recipient_is_refused_before_insert(self):
        """🔴 같은 이름의 살아있는 행이 둘이면 조용히 하나를 고르면 안 된다.

        실측: 자동 이름 codex-<id8> 이 유일하지 않아(코퍼스에서 51쌍 충돌) 서로
        다른 두 팀의 작업자가 같은 주소를 가졌다. 하나를 골라 배달하면 남의
        작업자를 친다 — 「누구에게 갔는지 아무도 모르는 성공」이라 유실보다 나쁘다.
        """
        self.r.h_register({"session": "s-me", "name": "me"}, {})
        # 🔑 생산에서 중복이 생긴 경로를 그대로 재현한다: 스캐너가 등록할 때 둘 다
        # dormant 였다. 선점 가드(_name_is_squatted)는 **살아있는** 행만 보므로
        # 통과하고, 나중에 둘 다 살아나면서 같은 주소가 둘이 된다.
        self.r.h_register({"session": "s-1", "name": "twin", "cli": "codex",
                           "state": "dormant"}, {})
        self.r.h_register({"session": "s-2", "name": "twin", "cli": "codex",
                           "state": "dormant"}, {})
        self.r.db().execute("UPDATE agents SET state='live-idle' "
                            "WHERE session IN ('s-1','s-2')")
        self.assertEqual(self.r.db().execute(
            "SELECT COUNT(*) c FROM agents WHERE name='twin'").fetchone()["c"], 2,
            "픽스처가 중복을 못 만들었다 — 이 테스트는 그 상태에서만 판별력이 있다")
        before = self.r.db().execute("SELECT COUNT(*) c FROM messages").fetchone()["c"]
        out = self.r.h_send({"from_session": "s-me", "from_agent": "me",
                             "to": "twin", "body": "x"}, {})
        self.assertEqual(out["error"], "ambiguous-recipient")
        self.assertEqual(len(out["candidates"]), 2)
        # 🪤 거절하면서 고아 메시지를 남기면 안 된다 — 검문은 insert 앞이어야 한다
        after = self.r.db().execute("SELECT COUNT(*) c FROM messages").fetchone()["c"]
        self.assertEqual(after, before)

    def test_a_unique_name_still_delivers(self):
        """대조군 — 모호성 검문이 정상 배달까지 막으면 기능이 통째로 죽는다."""
        self.r.h_register({"session": "s-me", "name": "me"}, {})
        self.r.h_register({"session": "s-1", "name": "solo", "state": "live-idle"}, {})
        out = self.r.h_send({"from_session": "s-me", "from_agent": "me",
                             "to": "solo", "body": "x"}, {})
        self.assertTrue(out["ok"], out)


    def _twins(self):
        """생산에서 중복이 생긴 형상: 둘 다 dormant 로 등록된 뒤 살아난다."""
        self.r.h_register({"session": "s-1", "name": "twin", "cli": "codex",
                           "state": "dormant"}, {})
        self.r.h_register({"session": "s-2", "name": "twin", "cli": "codex",
                           "state": "dormant"}, {})
        self.r.db().execute("UPDATE agents SET state='live-idle' "
                            "WHERE session IN ('s-1','s-2')")
        self.assertEqual(self.r.db().execute(
            "SELECT COUNT(*) c FROM agents WHERE name='twin'").fetchone()["c"], 2)

    def test_org_refuses_an_ambiguous_name(self):
        """🔴 개명은 **모호성을 푸는 도구**다 — 그 도구가 어느 쪽을 고쳤는지 모르면
        모호성이 그대로 남는다."""
        self._twins()
        out = self.r.h_org({"name": "twin", "rename_to": "solo"}, {})
        self.assertEqual(out["error"], "ambiguous-agent")
        self.assertEqual(len(out["candidates"]), 2)
        # 아무것도 안 바뀌어야 한다
        self.assertEqual(self.r.db().execute(
            "SELECT COUNT(*) c FROM agents WHERE name='twin'").fetchone()["c"], 2)

    def test_org_session_targeting_breaks_the_tie(self):
        """탈출구가 없으면 모호성 가드가 막다른 길이 된다 — 실측으로 그렇게 됐다."""
        self._twins()
        out = self.r.h_org({"session": "s-2", "rename_to": "solo"}, {})
        self.assertTrue(out["ok"], out)
        rows = dict(self.r.db().execute(
            "SELECT session, name FROM agents WHERE session IN ('s-1','s-2')"
        ).fetchall())
        self.assertEqual(rows["s-2"], "solo")
        self.assertEqual(rows["s-1"], "twin")     # 다른 쪽은 건드리지 않는다

    def test_org_still_works_for_a_unique_name(self):
        """대조군 — 모호성 검문이 정상 배정까지 막으면 조직도를 못 그린다."""
        self.r.h_register({"session": "s-solo", "name": "solo"}, {})
        self.assertTrue(self.r.h_org({"name": "solo", "role": "lead"}, {})["ok"])


    def test_live_codex_without_a_channel_goes_to_revive_not_the_void(self):
        """🔴 '살아 있다'와 '닿는다'는 다른 축이다.

        codex 를 dormant 로 박아 두던 시절엔 모든 배달이 부활 경로로 갔고 —
        비쌌지만 닿았다. 생사를 실제로 재게 고치자 live 가 되면서 웨이크 경로로
        옮겨졌는데, 초인종 주소가 없는 기존 세션은 그 경로에 채널이 없어 통째로
        만료됐다(실측: 아침까지 오가던 왕복이 배포 직후 4건 연속 만료).
        """
        self.r.h_register({"session": "s-me", "name": "me"}, {})
        self.r.h_register({"session": "s-cx", "name": "cx", "cli": "codex",
                           "state": "live-idle"}, {})
        # 🪤 디스패치 분기는 **blocking 에만** 있다 — normal 은 타이머 없이 워커
        # 웨이크 루프에만 의존한다. 그래서 이 안전망은 blocking 만 덮는다.
        # normal 의 진짜 해법은 워커가 탭 주소를 스스로 세우는 것이다.
        out = self.r.h_send({"from_session": "s-me", "from_agent": "me",
                             "to": "cx", "body": "x", "priority": "blocking"}, {})
        self.assertTrue(out["ok"], out)
        jobs = self.r.db().execute(
            "SELECT kind FROM timers WHERE msg_id=?", (out["id"],)).fetchall()
        self.assertIn("revive-now", [j["kind"] for j in jobs])

    def test_live_codex_with_a_doorbell_address_uses_the_wake_lane(self):
        """대조군 — 주소가 있으면 부활(유료)로 보내면 안 된다."""
        self.r.h_register({"session": "s-me", "name": "me"}, {})
        self.r.h_register({"session": "s-cx", "name": "cx", "cli": "codex",
                           "state": "live-idle",
                           "cmux_surface": "S-UUID",
                           "cmux_workspace": "W-UUID"}, {})
        out = self.r.h_send({"from_session": "s-me", "from_agent": "me",
                             "to": "cx", "body": "x", "priority": "blocking"}, {})
        jobs = self.r.db().execute(
            "SELECT kind FROM timers WHERE msg_id=?", (out["id"],)).fetchall()
        self.assertNotIn("revive-now", [j["kind"] for j in jobs])
        self.assertIn("debounce", [j["kind"] for j in jobs])

    def test_hint_register_applies_tab_coords(self):
        """🔴 hint_only 는 _apply_hints 에서 끝난다 — 거기서 안 받으면 워커가 매 주기
        좌표를 보내도 **통째로 버려진다**(영원히 반영 안 됨)."""
        self.r.h_register({"session": "s-cx", "name": "cx", "cli": "codex"}, {})
        self.r.h_register({"session": "s-cx", "hint_only": True,
                           "cmux_workspace": "W1", "cmux_surface": "S1"}, {})
        row = self.r.db().execute(
            "SELECT cmux_workspace, cmux_surface FROM agents WHERE session='s-cx'"
        ).fetchone()
        self.assertEqual((row["cmux_workspace"], row["cmux_surface"]), ("W1", "S1"))
        # 빈 값이 기존 좌표를 지우면 안 된다
        self.r.h_register({"session": "s-cx", "hint_only": True}, {})
        row = self.r.db().execute(
            "SELECT cmux_surface FROM agents WHERE session='s-cx'").fetchone()
        self.assertEqual(row["cmux_surface"], "S1")


class RelayHangupCase(unittest.TestCase):
    """클라이언트가 먼저 끊으면 조용히 드롭 — 파드 로그는 모두가 보는 화면이다.

    워커에만 달았던 가드가 relay 에도 필요했다(실측: 격리 E2E 한 번에 트레이스백 2건).
    예전 _serve 는 응답 쓰기 실패를 500 응답으로 갚으려다 같은 자리에서 또 터졌다.
    """

    def setUp(self):
        import threading
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
        self.srv = relay.ThreadingHTTPServer(("127.0.0.1", 0), relay.Handler)
        self.port = self.srv.server_address[1]
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def tearDown(self):
        self.srv.shutdown()

    def test_client_hangup_leaves_no_traceback(self):
        import contextlib
        import io
        import socket
        import struct
        import time
        buf = io.StringIO()
        # 🪤 트레이스백은 sys.stderr(파이썬 객체)로 나간다 — fd 2 를 가로채면 안 잡힌다.
        # 그리고 즉답 엔드포인트로는 재현되지 않는다(응답이 RST 보다 먼저 나간다).
        # 롱폴처럼 응답이 늦는 경로여야 쓰기 시점에 상대가 이미 없다.
        with contextlib.redirect_stderr(buf):
            for _ in range(3):
                s = socket.socket()
                s.connect(("127.0.0.1", self.port))
                s.sendall(b"GET /poll?home=x&cursor=0&wait=1 HTTP/1.1\r\n"
                          b"Host: x\r\n\r\n")
                s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                             struct.pack("ii", 1, 0))
                s.close()
            time.sleep(2.5)
        self.assertNotIn("Traceback", buf.getvalue())

    def test_polite_client_still_gets_its_answer(self):
        import json as _json
        import urllib.request
        with urllib.request.urlopen(
                f"http://127.0.0.1:{self.port}/healthz", timeout=3) as r:
            self.assertTrue(_json.loads(r.read())["ok"])



AM_SRC_ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


class SelfCliCase(unittest.TestCase):
    """`am register` 가 cli 를 claude 로 덮으면 codex 배달 경로가 통째로 죽는다."""

    def test_register_does_not_hardcode_claude(self):
        src = open(os.path.join(AM_SRC_ROOT, "cli", "am")).read()
        i = src.index("def cmd_register(")
        seg = src[i:i + 1200]
        self.assertNotIn('"cli": "claude"', seg)
        self.assertIn("_self_cli(", seg)

    def test_self_cli_falls_back_to_the_existing_registration(self):
        """모르면 기존 등록을 믿는다 — 추측으로 덮는 쪽이 더 나쁘다."""
        src = open(os.path.join(AM_SRC_ROOT, "cli", "am")).read()
        i = src.index("def _self_cli(")
        seg = src[i:i + 700]
        self.assertIn("/agent", seg)
        self.assertIn("prev or", seg)


if __name__ == "__main__":
    unittest.main(verbosity=2)
