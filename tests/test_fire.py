#!/usr/bin/env python3
"""am fire 회귀 테스트 — 해고의 불변식을 못박는다.

  F1 종료는 조인된 PID 로만: 이름·패턴 킬 없음. cwd 불일치면 **죽이지 않는다**
     (kill-by-pid 규율 — pkill -f 가 무관 MCP 서버까지 죽인 08-24 실사고).
  F2 SIGTERM 우선(SessionEnd 훅이 원장을 접는다), 불응 시에만 SIGKILL 승격.
  F3 원장 정리는 프로세스가 이미 없어도 수행한다(보고선 재배선·묘비 개명).
  F4 가드: 자기 해고·회장(--force 없이)·live-active(유예·--force 없이) 거부 —
     전부 **시그널 발사 전에** 거부돼야 한다.
  F5 묘비 개명이 이름을 해제한다 — 같은 이름 재채용 시 옛 큐 오배달·유료 부활 차단.
"""
import argparse
import contextlib
import importlib.machinery
import importlib.util
import io
import json
import os
import signal
import sys
import tempfile
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


def load_am():
    loader = importlib.machinery.SourceFileLoader(
        "am_cli_fire_under_test", os.path.join(ROOT, "cli", "am"))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    mod = importlib.util.module_from_spec(spec)
    loader.exec_module(mod)
    return mod


def fire_ns(**kw):
    base = dict(name="minion", grace=0, reason=None, reassign_to=None,
                force=False, keep_name=False, keep_surface=False, no_kill=False)
    base.update(kw)
    return argparse.Namespace(**base)


class FireCase(unittest.TestCase):
    PID = 54321

    def setUp(self):
        self.am = load_am()
        self.am.FIRE_KILL_TIMEOUT_S = 0.3
        self.am.FIRE_KILL9_TIMEOUT_S = 0.2
        self.am.FIRE_POLL_S = 0.01
        self.tmp = tempfile.mkdtemp()
        # 레지스트리 격리 — 실제 ~/.claude/sessions 를 읽지 않는다
        self.am.CC_SESSIONS_DIR = self.tmp
        self.calls = []          # (method, path, body)
        self.kills = []          # (pid, sig)
        self.cwd = os.path.realpath(self.tmp)
        # 기본 목: 죽일 수 있는 정상 대상
        self.agent = {"name": "minion", "session": "sess-m", "cli": "claude",
                      "cwd": self.cwd, "state": "live-idle"}
        self.roster = [
            {"name": "minion", "state": "live-idle", "role": "member",
             "reports_to": "boss"},
            {"name": "junior", "state": "live-idle", "role": "member",
             "reports_to": "minion"},
            {"name": "gone-sub", "state": "dormant", "role": "member",
             "reports_to": "minion"},   # 죽은 부하는 재배선 안 한다
        ]
        self._alive_until_sig = signal.SIGTERM   # 이 시그널을 받으면 죽는다
        self._dead = False

        def call(method, path, body=None, params="", timeout=10):
            self.calls.append((method, path, body))
            if path == "/agent":
                return {"agent": dict(self.agent)}
            if path == "/agents":
                return {"agents": [dict(x) for x in self.roster]}
            if path == "/org":
                return {"ok": True, "agent": body}
            if path == "/send":
                return {"ok": True, "id": "m-9", "thread": "t-9"}
            return {"ok": True}

        def alive(pid):
            return int(pid) == self.PID and not self._dead

        def kill(pid, sig):
            self.kills.append((int(pid), sig))
            if sig == self._alive_until_sig or sig == signal.SIGKILL and \
                    self._alive_until_sig == signal.SIGKILL:
                self._dead = True

        self.am.call = call
        self.am._alive = alive
        self.am._kill = kill
        self.am._pid_cwd = lambda pid: self.cwd
        self.am._pid_cmux_surface = lambda pid: ""
        self.am._cmux_run = lambda args, timeout=6: (1, "", "cmux 밖")
        self.am._my_name = lambda: "boss"
        self.am.SESSION = "sess-boss"

    def registry(self, session="sess-m", pid=None):
        with open(os.path.join(self.tmp, f"{pid or self.PID}.json"), "w") as f:
            json.dump({"sessionId": session, "pid": pid or self.PID}, f)

    def run_fire(self, ns):
        buf = io.StringIO()
        code = 0
        with contextlib.redirect_stdout(buf):
            try:
                self.am.cmd_fire(ns)
            except SystemExit as e:
                code = e.code or 0
        return code, json.loads(buf.getvalue())

    # ── F4 가드 — 전부 시그널 발사 전 ─────────────────────

    def test_self_fire_is_refused(self):
        code, res = self.run_fire(fire_ns(name="boss"))
        self.assertEqual((code, res["error"]), (1, "self-fire"))
        self.assertEqual(self.kills, [])

    def test_unknown_agent_is_refused(self):
        self.agent = None
        self.am.call = lambda m, p, body=None, params="", timeout=10: (
            {"agent": None} if p == "/agent" else {"agents": []})
        code, res = self.run_fire(fire_ns())
        self.assertEqual((code, res["error"]), (1, "unknown-agent"))
        self.assertEqual(self.kills, [])

    def test_codex_uses_the_lock_holder_as_its_pid_axis(self):
        """codex 도 내릴 수 있어야 한다 — hire 는 앉히는데 fire 가 못 내리면
        채용자가 자기 고아를 못 치운다(팀장 실측). 조인 축은 락 **보유자**다.
        """
        self.agent["cli"] = "codex"
        seen = {}
        self.am._codex_pid = lambda sess: (seen.update(sess=sess),
                                           (self.PID, {}))[1]
        self.am._session_pid = lambda sess: (None, None)   # claude 축은 쓰면 안 된다
        code, res = self.run_fire(fire_ns(force=True))
        self.assertEqual(code, 0, res)
        self.assertEqual(seen["sess"], self.agent["session"])
        self.assertEqual([k[0] for k in self.kills], [self.PID])

    def test_codex_ambiguous_lock_holder_never_kills(self):
        """0명이면 이미 죽었고 2명이면 누구를 죽일지 모른다 — 둘 다 추정 금지."""
        self.agent["cli"] = "codex"
        self.am._codex_pid = lambda sess: (None, {"holders": [11, 22]})
        code, res = self.run_fire(fire_ns(force=True))
        self.assertEqual(res["error"], "ambiguous-pid")
        self.assertEqual(self.kills, [])

    def test_chairman_needs_force(self):
        self.roster[0]["role"] = "chairman"
        code, res = self.run_fire(fire_ns())
        self.assertEqual(res["error"], "target-is-chairman")
        self.assertEqual(self.kills, [])

    def test_busy_target_needs_grace_or_force(self):
        self.agent["state"] = "live-active"
        code, res = self.run_fire(fire_ns())
        self.assertEqual(res["error"], "target-busy")
        self.assertEqual(self.kills, [])

    # ── F1 PID 조인·cwd 대조 ──────────────────────────────

    def test_cwd_mismatch_never_kills(self):
        self.registry()
        self.am._pid_cwd = lambda pid: "/somewhere/else"
        code, res = self.run_fire(fire_ns())
        self.assertEqual(res["error"], "cwd-mismatch")
        self.assertEqual(self.kills, [])

    def test_happy_path_sigterm_only(self):
        self.registry()
        code, res = self.run_fire(fire_ns())
        self.assertEqual(code, 0)
        self.assertTrue(res["ok"])
        self.assertEqual(self.kills, [(self.PID, signal.SIGTERM)])
        self.assertFalse(res["sigkill"])

    # ── F2 SIGKILL 승격 ───────────────────────────────────

    def test_stubborn_process_gets_sigkill(self):
        self.registry()
        self._alive_until_sig = signal.SIGKILL
        code, res = self.run_fire(fire_ns())
        self.assertEqual(code, 0)
        self.assertEqual(self.kills,
                         [(self.PID, signal.SIGTERM), (self.PID, signal.SIGKILL)])
        self.assertTrue(res["sigkill"])

    # ── F3 원장 정리 ─────────────────────────────────────

    def test_already_dead_still_cleans_ledger(self):
        # 레지스트리 항목 없음 = 프로세스 이미 사망
        code, res = self.run_fire(fire_ns())
        self.assertEqual(code, 0)
        self.assertIsNone(res["pid"])
        self.assertEqual(self.kills, [])
        self.assertIn("junior", res["reassigned"])
        self.assertTrue(res["tombstone"].startswith("fired-minion"))

    def test_live_subordinates_reassigned_to_targets_boss(self):
        self.registry()
        code, res = self.run_fire(fire_ns())
        org_calls = [b for m, p, b in self.calls if p == "/org"
                     and b and b.get("reports_to")]
        self.assertEqual([(b["name"], b["reports_to"]) for b in org_calls],
                         [("junior", "boss")])   # 죽은 gone-sub 는 건드리지 않는다

    def test_reassign_to_flag_overrides(self):
        self.registry()
        code, res = self.run_fire(fire_ns(reassign_to="viceboss"))
        self.assertEqual(res["reassigned_to"], "viceboss")

    # ── F5 묘비 개명 ─────────────────────────────────────

    def test_tombstone_frees_the_name(self):
        self.registry()
        code, res = self.run_fire(fire_ns())
        renames = [b for m, p, b in self.calls if p == "/org"
                   and b and b.get("rename_to")]
        self.assertEqual(len(renames), 1)
        self.assertTrue(renames[0]["rename_to"].startswith("fired-minion-"))
        self.assertLessEqual(len(renames[0]["rename_to"]), 32)  # NAME_RE 상한

    def test_keep_name_skips_tombstone(self):
        self.registry()
        code, res = self.run_fire(fire_ns(keep_name=True))
        self.assertIsNone(res["tombstone"])
        self.assertEqual([b for m, p, b in self.calls
                          if p == "/org" and b and b.get("rename_to")], [])

    def test_long_name_tombstone_respects_name_limit(self):
        long_name = "a" * 30
        self.agent["name"] = long_name
        self.roster[0]["name"] = long_name
        self.registry()
        code, res = self.run_fire(fire_ns(name=long_name))
        self.assertEqual(code, 0)
        self.assertLessEqual(len(res["tombstone"]), 32)
        self.assertTrue(self.am.NAME_RE.match(res["tombstone"]))

    # ── 유예 통지 ────────────────────────────────────────

    def test_grace_sends_notice_before_kill(self):
        self.registry()
        code, res = self.run_fire(fire_ns(grace=0.01, reason="프로젝트 종료"))
        send_i = next(i for i, (m, p, b) in enumerate(self.calls) if p == "/send")
        self.assertIn("해고 통지", self.calls[send_i][2]["body"])
        self.assertIn("프로젝트 종료", self.calls[send_i][2]["body"])
        self.assertEqual(self.kills[0][1], signal.SIGTERM)

    def test_grace_notice_skipped_when_already_dead(self):
        code, res = self.run_fire(fire_ns(grace=0.01))
        self.assertEqual([p for m, p, b in self.calls if p == "/send"], [])



    def test_no_kill_retires_without_touching_the_process(self):
        """🔑 죽이면 안 되는데 이름은 회수해야 하는 경우가 실재한다.

        kill-by-pid 가드가 cwd 불일치로 옳게 거부하면, 그 이름 앞으로 배달이
        계속 시도되고 만료 통지가 채용자를 반복해 깨운다 — 도구가 막았는데
        빠져나갈 문이 없는 형상이다(실측: member-x).
        """
        code, r = self.run_fire(fire_ns(no_kill=True))
        self.assertEqual(code, 0, r)
        self.assertEqual(self.kills, [])          # 프로세스는 안 건드린다
        self.assertIsNone(r["pid"])
        self.assertTrue(r["tombstone"])           # 이름은 회수한다
        labels = [x["step"] for x in r["steps"]]
        self.assertIn("no-kill", labels)
        self.assertIn("drain-queue", labels)      # 남은 우편도 닫는다

    def test_no_kill_skips_the_cwd_guard_that_would_refuse(self):
        """가드는 kill 을 막는 것이지 은퇴를 막는 게 아니다."""
        self.am._pid_cwd = lambda pid: "/somewhere/else"
        code, r = self.run_fire(fire_ns(no_kill=True))
        self.assertEqual(code, 0, r)
        self.assertEqual(self.kills, [])


    def test_tombstone_names_do_not_collide_within_a_day(self):
        """🔴 날짜만 붙이면 같은 이름을 하루에 두 번 해고할 때 묘비가 겹친다.

        실측: fired-member-x-0904 가 2행이 됐다. 겹친 이름은 곧 주소 모호성이고
        (아침의 codex-<id8> 충돌과 같은 계열), 그 이름으로는 개명·지목이 막힌다.
        """
        renames = []
        base_call = self.am.call

        def call(m, p, body=None, params="", timeout=10):
            if p == "/org" and (body or {}).get("rename_to"):
                renames.append(body["rename_to"])
            return base_call(m, p, body, params, timeout)

        self.am.call = call
        self.agent["session"] = "aaaa1111-2222"
        self.run_fire(fire_ns(no_kill=True))
        self.agent["session"] = "bbbb3333-4444"
        self.run_fire(fire_ns(no_kill=True))
        self.assertEqual(len(renames), 2)
        self.assertNotEqual(renames[0], renames[1], renames)
        # 그래도 NAME_RE 안이어야 한다 — 넘치면 등록 자체가 거부된다
        for t in renames:
            self.assertTrue(self.am.NAME_RE.match(t), t)

if __name__ == "__main__":
    unittest.main()
