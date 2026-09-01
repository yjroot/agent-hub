#!/usr/bin/env python3
"""am hire 회귀 테스트 — 팀장이 손 채용에서 실측한 함정 3개를 못박는다.

  H1 --type agent-session 금지: cmux 자체 UI 라 훅·소켓이 없어 am 지목 불가.
     스폰은 terminal 하드코드, 파서에 --type 옵션 자체가 없어야 한다.
  H2 훅은 프로젝트별: 대상 레포에 am 훅이 없으면 채용해도 조직에 안 보인다
     (유령 사원). 채용 전 검사 — 없으면 스폰 없이 거부하거나 --install-hooks.
  H3 역할 부여는 첫 지시보다 먼저 + 기동 env(AM_ROLE)로 SessionStart 에 선반영.

식별 축: 이름을 채용자가 정해 AM_NAME 으로 주입 → name+registered_at+cwd 정확 조인.
실패는 크게: 타임아웃·미발견을 '없음'으로 말하지 않는다.
"""
import argparse
import contextlib
import importlib.machinery
import importlib.util
import io
import json
import os
import sys
import tempfile
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


def load_am():
    loader = importlib.machinery.SourceFileLoader(
        "am_cli_under_test", os.path.join(ROOT, "cli", "am"))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    mod = importlib.util.module_from_spec(spec)
    loader.exec_module(mod)
    return mod


def hire_ns(**kw):
    base = dict(name="newbie", cwd="/tmp", workspace="workspace:1", role="member",
                reports_to="boss", task="첫 지시", install_hooks=False,
                ephemeral=False, timeout=1.0)
    base.update(kw)
    return argparse.Namespace(**base)


class HireCase(unittest.TestCase):
    def setUp(self):
        self.am = load_am()
        self.am.HIRE_POLL_S = 0.01
        self.tmp = tempfile.mkdtemp()
        # 이 머신의 전역 설정(~/.claude/settings.json)이 훅 검사에 끼어들지 않게 격리
        self.am.GLOBAL_SETTINGS = os.path.join(self.tmp, "no-such-global.json")
        self.calls = []          # (method, path) 순서 기록
        self.cmux_calls = []

    def with_hooks(self, root=None):
        root = root or self.tmp
        d = os.path.join(root, ".claude")
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "settings.local.json"), "w") as f:
            json.dump({"hooks": {"SessionStart": [{"matcher": "", "hooks": [
                {"type": "command",
                 "command": "python3 /x/hooks/am_hook.py session_start",
                 "timeout": 5}]}]}}, f)
        return root

    def mock_worker(self, agent_results):
        """/agent 응답 시퀀스(마지막 값 반복) + /org·/send 성공 목."""
        seq = list(agent_results)

        def call(method, path, body=None, params="", timeout=10):
            self.calls.append((method, path))
            if path == "/agent":
                return seq.pop(0) if len(seq) > 1 else seq[0]
            if path == "/org":
                return {"ok": True, "agent": {"name": body["name"],
                                              "role": body.get("role"),
                                              "reports_to": body.get("reports_to")}}
            if path == "/send":
                return {"ok": True, "id": "m-1", "thread": "t-1"}
            return {"ok": True}
        self.am.call = call

    def mock_cmux(self, spawn_out="OK surface:7 pane:2 workspace:1"):
        def run(args, timeout=6):
            self.cmux_calls.append(list(args))
            if args[0] == "new-surface":
                return 0, spawn_out, ""
            return 0, "OK", ""
        self.am._cmux_run = run

    def run_hire(self, ns):
        buf = io.StringIO()
        code = 0
        with contextlib.redirect_stdout(buf):
            try:
                self.am.cmd_hire(ns)
            except SystemExit as e:
                code = e.code
        return code, json.loads(buf.getvalue())

    # ── H1: agent-session 봉쇄 ─────────────────────────────
    def test_spawn_is_hardcoded_terminal(self):
        self.with_hooks()
        fresh = {"agent": {"session": "s-new", "cwd": self.tmp,
                           "registered_at": time.time() + 1, "state": "live-active"}}
        self.mock_worker([{"agent": None}, fresh])
        self.mock_cmux()
        code, r = self.run_hire(hire_ns(cwd=self.tmp))
        self.assertEqual(code, 0)
        spawn = next(c for c in self.cmux_calls if c[0] == "new-surface")
        i = spawn.index("--type")
        self.assertEqual(spawn[i + 1], "terminal")
        self.assertNotIn("agent-session", " ".join(sum(self.cmux_calls, [])))

    def test_hire_parser_has_no_type_option(self):
        p = argparse.ArgumentParser()
        self.am._hire_parser(p.add_subparsers())
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                p.parse_args(["hire", "x", "--cwd", "/tmp",
                              "--type", "agent-session"])

    # ── H2: 훅 없는 레포 = 유령 사원 — 스폰 전에 거부 ───────
    def test_refuses_repo_without_am_hook(self):
        self.mock_worker([{"agent": None}])
        self.mock_cmux()
        code, r = self.run_hire(hire_ns(cwd=self.tmp))
        self.assertEqual(code, 1)
        self.assertEqual(r["error"], "no-am-hook")
        self.assertIn("유령", r["reason"])           # 이유를 명시하고 거부
        self.assertEqual(self.cmux_calls, [])        # 스폰까지 가면 안 된다

    def test_install_hooks_writes_merged_settings(self):
        # 기존 설정(permissions)을 보존한 채 훅 4종을 병합해야 한다
        d = os.path.join(self.tmp, ".claude")
        os.makedirs(d)
        pth = os.path.join(d, "settings.local.json")
        with open(pth, "w") as f:
            json.dump({"permissions": {"allow": ["Bash(am:*)"]}}, f)
        fresh = {"agent": {"session": "s-new", "cwd": self.tmp,
                           "registered_at": time.time() + 1, "state": "live-active"}}
        self.mock_worker([{"agent": None}, fresh])
        self.mock_cmux()
        code, r = self.run_hire(hire_ns(cwd=self.tmp, install_hooks=True))
        self.assertEqual(code, 0)
        with open(pth) as f:
            cfg = json.load(f)
        self.assertEqual(cfg["permissions"]["allow"], ["Bash(am:*)"])
        for ev in ("SessionStart", "UserPromptSubmit", "PostToolUse", "SessionEnd"):
            cmds = [h["command"] for e in cfg["hooks"][ev] for h in e["hooks"]]
            self.assertTrue(any("am_hook.py" in c for c in cmds), ev)

    # ── H3: 역할 부여가 첫 지시보다 먼저 + 기동 env 선반영 ──
    def test_org_before_first_task_and_env_prefix(self):
        self.with_hooks()
        fresh = {"agent": {"session": "s-new", "cwd": self.tmp,
                           "registered_at": time.time() + 1, "state": "live-active"}}
        self.mock_worker([{"agent": None}, fresh])
        self.mock_cmux()
        code, r = self.run_hire(hire_ns(cwd=self.tmp))
        self.assertEqual(code, 0)
        paths = [p for _, p in self.calls]
        self.assertIn("/org", paths)
        self.assertIn("/send", paths)
        self.assertLess(paths.index("/org"), paths.index("/send"))
        send = next(c for c in self.cmux_calls if c[0] == "send")
        line = send[-1]
        for tok in ("AM_NAME=newbie", "AM_ROLE=member", "AM_REPORTS_TO=boss"):
            self.assertIn(tok, line)

    def test_session_start_hook_forwards_role_env(self):
        # 훅이 AM_ROLE/AM_REPORTS_TO 를 등록에 싣는다 — SessionStart 시점 선반영의 근거
        loader = importlib.machinery.SourceFileLoader(
            "am_hook_under_test", os.path.join(ROOT, "hooks", "am_hook.py"))
        spec = importlib.util.spec_from_loader(loader.name, loader)
        hook = importlib.util.module_from_spec(spec)
        loader.exec_module(hook)
        seen = []
        hook.worker = lambda m, p, b=None, timeout=1.5: seen.append((p, b)) or {}
        hook.inbox_context = lambda s: None
        env = {"AM_ROLE": "member", "AM_REPORTS_TO": "boss", "AM_NAME": "newbie",
               "CMUX_SURFACE_ID": "", "AGENT_HUB_RESPONDER": ""}
        old = {k: os.environ.get(k) for k in env}
        os.environ.update(env)
        os.environ.pop("AGENT_HUB_RESPONDER", None)
        old_argv, old_stdin = sys.argv, sys.stdin
        sys.argv = ["am_hook.py", "session_start"]
        sys.stdin = io.StringIO(json.dumps(
            {"session_id": "s-hook-test", "cwd": "/tmp"}))
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaises(SystemExit):
                    hook.main()
        finally:
            sys.argv, sys.stdin = old_argv, old_stdin
            for k, v in old.items():
                os.environ.pop(k, None)
                if v is not None:
                    os.environ[k] = v
        reg = next(b for p, b in seen if p == "/register")
        self.assertEqual(reg["role"], "member")
        self.assertEqual(reg["reports_to"], "boss")
        self.assertEqual(reg["name"], "newbie")

    # ── 식별 축: name + registered_at + cwd 정확 조인 ───────
    def test_stale_same_name_row_is_not_the_hire(self):
        # 동명의 옛(dormant, t0 이전) 행이 보이는 동안은 계속 기다리고,
        # 신선한 행이 나타나면 그 세션으로 조인한다.
        self.with_hooks()
        stale = {"agent": {"session": "s-old", "cwd": self.tmp,
                           "registered_at": time.time() - 9999, "state": "dormant"}}
        fresh = {"agent": {"session": "s-new", "cwd": self.tmp,
                           "registered_at": time.time() + 1, "state": "live-active"}}
        self.mock_worker([{"agent": None}, stale, stale, fresh])
        self.mock_cmux()
        code, r = self.run_hire(hire_ns(cwd=self.tmp))
        self.assertEqual(code, 0)
        self.assertEqual(r["session"], "s-new")

    def test_refuses_live_name_squat_before_spawn(self):
        self.with_hooks()
        live = {"agent": {"session": "s-live", "cwd": "/elsewhere",
                          "registered_at": time.time() - 100,
                          "state": "live-active"}}
        self.mock_worker([live])
        self.mock_cmux()
        code, r = self.run_hire(hire_ns(cwd=self.tmp))
        self.assertEqual(code, 1)
        self.assertEqual(r["error"], "name-taken")
        self.assertEqual(self.cmux_calls, [])

    # ── 실패는 크게: 타임아웃을 '없음'으로 말하지 않는다 ────
    def test_register_timeout_is_loud_with_evidence(self):
        self.with_hooks()
        self.mock_worker([{"agent": None}])
        self.mock_cmux()
        code, r = self.run_hire(hire_ns(cwd=self.tmp, timeout=0.05))
        self.assertEqual(code, 1)
        self.assertEqual(r["error"], "register-timeout")
        self.assertIn("reason", r)                    # 왜 못 찾았는지 말한다
        self.assertEqual(r["surface"], "surface:7")   # 증거(열린 서피스) 지목
        self.assertTrue(any("close-surface" in h for h in r["hint"]))
        spawned = [c for c in self.cmux_calls if c[0] == "new-surface"]
        self.assertEqual(len(spawned), 1)             # 스폰은 했고, 그 뒤가 문제였다

    def test_bad_name_rejected_shell_safety(self):
        # 이름은 기동 셸 라인에 실린다 — 메타문자·공백은 스폰 전에 거부
        self.mock_worker([{"agent": None}])
        self.mock_cmux()
        for bad in ("a b", "x;rm", "$(boom)", "한글", ""):
            code, r = self.run_hire(hire_ns(name=bad, cwd=self.tmp))
            self.assertEqual(code, 1, bad)
            self.assertEqual(r["error"], "bad-name", bad)
        self.assertEqual(self.cmux_calls, [])


if __name__ == "__main__":
    unittest.main()
