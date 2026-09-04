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
                ephemeral=False, timeout=1.0, model="claude-opus-5",
                provider="claude", pin_cwd=None, new_workspace=None)
    base.update(kw)
    return argparse.Namespace(**base)


class HireCase(unittest.TestCase):
    def setUp(self):
        self.am = load_am()
        self.am.HIRE_POLL_S = 0.01
        self.tmp = tempfile.mkdtemp()
        # 이 머신의 전역 설정(~/.claude/settings.json)이 훅 검사에 끼어들지 않게 격리
        self.am.GLOBAL_SETTINGS = os.path.join(self.tmp, "no-such-global.json")
        # 실제 ~/.agent-hub/hire.json 핀이 끼어들지 않게 격리
        self.am.HIRE_CONF = os.path.join(self.tmp, "hire.json")
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
        def run(args, timeout=6, ids=False):
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




    def test_failed_hire_writes_a_step_log(self):
        d = tempfile.mkdtemp()
        self.am.HUB_DIR = d
        self.with_hooks()
        self.mock_worker([{"agent": None}, {"agent": None}])   # 끝내 등록 안 됨
        self.mock_cmux()
        code, r = self.run_hire(hire_ns(cwd=self.tmp))
        self.assertEqual(code, 1)
        self.assertEqual(r["error"], "register-timeout")
        logs = [f for f in os.listdir(d) if f.startswith("hire-")]
        self.assertEqual(len(logs), 1, logs)
        saved = json.load(open(os.path.join(d, logs[0])))
        # 파일에는 화면 요약이 아니라 **단계 전부**가 있어야 한다
        self.assertEqual(saved["error"], "register-timeout")
        self.assertTrue(saved["steps"])
        self.assertIn("spawn", [x["step"] for x in saved["steps"]])


    def test_register_says_what_is_wrong_instead_of_bare_403(self):
        """🔴 403 한 줄만 던지면 팀원은 무엇이 잘못됐는지 모른다(실사용 보고)."""
        am = load_am()
        am._resolve_session = lambda: ""
        buf, code = io.StringIO(), 0
        with contextlib.redirect_stdout(buf):
            try:
                am.cmd_register(argparse.Namespace(
                    session="", name="x", task="", paths=[], design="",
                    model="", ephemeral=False))
            except SystemExit as e:
                code = e.code
        self.assertEqual(code, 2)
        r = json.loads(buf.getvalue())
        self.assertEqual(r["error"], "unknown-self")
        self.assertIn("hint", r)

    def test_codex_self_resolution_is_tried_before_the_registry(self):
        """레지스트리 조회는 「등록돼 있어야 등록할 수 있다」는 순환이다 —
        갓 뜬 codex 는 그 길로는 영영 못 푼다. 자기 해결이 먼저여야 한다."""
        src = open(os.path.join(ROOT, "cli", "am")).read()
        i = src.index("def _resolve_session(")
        seg = src[i:i + 900]
        self.assertLess(seg.index("_codex_self_session()"), seg.index('"/agent"'))


class HireCwdPinCase(HireCase):
    """시작 폴더 고정(사용자 결정 2026-09-02) — 엉뚱한 폴더 채용의 뿌리를 막는다."""

    def pin(self, path=None):
        path = path or self.tmp
        with open(self.am.HIRE_CONF, "w") as f:
            json.dump({"cwd": path}, f)
        return path

    def fresh_agent(self):
        return {"agent": {"session": "s-new", "cwd": self.tmp,
                          "registered_at": time.time() + 1,
                          "state": "live-active"}}

    def test_relative_cwd_is_refused(self):
        self.mock_worker([{"agent": None}])
        self.mock_cmux()
        code, r = self.run_hire(hire_ns(cwd="../elsewhere"))
        self.assertEqual(r["error"], "relative-cwd")
        self.assertEqual(self.cmux_calls, [])

    def test_pin_overrides_missing_cwd(self):
        self.pin(self.with_hooks())
        self.mock_worker([{"agent": None}, self.fresh_agent()])
        self.mock_cmux()
        code, r = self.run_hire(hire_ns(cwd=None))
        self.assertEqual(code, 0)
        spawn = next(c for c in self.cmux_calls if c[0] == "new-surface")
        self.assertEqual(spawn[spawn.index("--working-directory") + 1],
                         os.path.realpath(self.tmp))

    def test_conflicting_cwd_is_refused_not_silently_overridden(self):
        self.pin()
        self.mock_worker([{"agent": None}])
        self.mock_cmux()
        other = tempfile.mkdtemp()
        code, r = self.run_hire(hire_ns(cwd=other))
        self.assertEqual(r["error"], "cwd-pinned")
        self.assertEqual(self.cmux_calls, [])

    def test_no_cwd_no_pin_fails_with_pin_hint(self):
        self.mock_worker([{"agent": None}])
        self.mock_cmux()
        code, r = self.run_hire(hire_ns(cwd=None))
        self.assertEqual(r["error"], "no-cwd")
        self.assertIn("pin-cwd", r["hint"])

    def test_pin_cwd_mode_writes_conf_and_does_not_hire(self):
        code, r = self.run_hire(hire_ns(name=None, cwd=None, pin_cwd=self.tmp))
        self.assertEqual(code, 0)
        self.assertEqual(json.load(open(self.am.HIRE_CONF))["cwd"],
                         os.path.realpath(self.tmp))
        self.assertEqual(self.cmux_calls, [])
        self.assertEqual(self.calls, [])          # 워커 호출도 없어야 한다


class HireLeadWorkspaceCase(HireCase):
    """팀장 채용 = 새 워크스페이스의 첫 탭(사용자 결정 2026-09-02)."""

    # id 는 **실제 UUID 형태**여야 한다 — 채용은 UUID 인 좌표만 배달 주소로 저장하고,
    # 가짜 문자열을 쓰면 그 검문(_is_uuid)이 픽스처에서 늘 거짓이라 미측정이 된다.
    WS_FIRST_UUID = "AAAAAAAA-0000-4000-8000-000000000030"
    WS_SURFACES = json.dumps({"surfaces": [
        {"id": "BBBBBBBB-0000-4000-8000-000000000031", "index": 1,
         "ref": "surface:31", "type": "terminal"},
        {"id": WS_FIRST_UUID, "index": 0, "ref": "surface:30", "type": "terminal"},
    ]})
    WS_CREATE_OUT = ("OK workspace:9 (CCCCCCCC-0000-4000-8000-000000000009) "
                     "pane:2 (DDDDDDDD-0000-4000-8000-000000000002)")
    WS_UUID = "CCCCCCCC-0000-4000-8000-000000000009"

    def mock_cmux_ws(self):
        def run(args, timeout=6, ids=False):
            self.cmux_calls.append(list(args))
            if args[0] == "workspace" and args[1] == "create":
                return 0, self.WS_CREATE_OUT, ""
            if args[0] == "list-pane-surfaces":
                return 0, self.WS_SURFACES, ""
            if args[0] == "new-surface":
                return 0, "OK surface:7", ""
            return 0, "OK", ""
        self.am._cmux_run = run

    def fresh_agent(self):
        return {"agent": {"session": "s-new", "cwd": self.tmp,
                          "registered_at": time.time() + 1,
                          "state": "live-active"}}

    def test_lead_without_workspace_is_refused_no_silent_fallback(self):
        os.environ["CMUX_WORKSPACE_ID"] = "workspace:1"   # 폴백 유혹이 있어도
        try:
            self.mock_worker([{"agent": None}])
            self.mock_cmux_ws()
            code, r = self.run_hire(hire_ns(role="lead", workspace=None,
                                            cwd=self.with_hooks()))
            self.assertEqual(r["error"], "lead-needs-workspace")
            self.assertEqual(self.cmux_calls, [])
        finally:
            os.environ.pop("CMUX_WORKSPACE_ID", None)

    def test_new_workspace_uses_its_first_tab_not_a_new_surface(self):
        self.with_hooks()
        self.mock_worker([{"agent": None}, self.fresh_agent()])
        self.mock_cmux_ws()
        code, r = self.run_hire(hire_ns(role="lead", workspace=None,
                                        new_workspace="신설팀", cwd=self.tmp))
        self.assertEqual(code, 0)
        create = next(c for c in self.cmux_calls
                      if c[:2] == ["workspace", "create"])
        self.assertEqual(create[create.index("--name") + 1], "신설팀")
        # 첫 탭(index 0)에 앉는다 — 목록 순서가 아니라 index 축.
        # 주소는 ref 가 아니라 UUID 여야 한다 (ref 는 대기 중 재번호된다).
        sends = [c for c in self.cmux_calls if c[0] == "send"]
        self.assertTrue(sends)
        self.assertTrue(all(c[c.index("--surface") + 1] == self.WS_FIRST_UUID
                            for c in sends))
        self.assertNotIn("new-surface", [c[0] for c in self.cmux_calls])
        self.assertEqual(r["workspace"], self.WS_UUID)

    def test_workspace_and_new_workspace_conflict(self):
        self.mock_worker([{"agent": None}])
        self.mock_cmux_ws()
        code, r = self.run_hire(hire_ns(workspace="workspace:1",
                                        new_workspace="신설팀", cwd=self.tmp))
        self.assertEqual(r["error"], "workspace-conflict")


class CodexPushAddressCase(unittest.TestCase):
    """codex 팀원의 탭 좌표 기록 — push 채널의 주소다.

    codex 엔 주입 소켓이 없어 유일한 push 경로가 탭 키 입력이다. 좌표를 채용
    시점에 안 남기면(등록 타임아웃으로 /org 를 건너뛰는 경우 포함) 그 팀원은
    영원히 pull 전용이 된다 — 실측으로 그렇게 굴렀다.
    """

    def test_org_body_carries_tab_coords_for_codex_only(self):
        src = open(os.path.join(ROOT, "cli", "am")).read()
        i = src.index("org_body = {")
        seg = src[i:i + 1400]
        self.assertIn("cmux_surface", seg)
        self.assertIn("cmux_workspace", seg)
        # claude 는 소켓이 있으므로 좌표를 남기지 않는다 — 조건 밖에 두면 안 된다
        self.assertIn('a.provider == "codex"', seg)
        # 🔴 ref 를 그대로 실으면 안 된다 — 재번호되는 인덱스다.
        # 좌표는 spawn 시점 UUID 이고, 여기서는 그게 UUID 인지만 검문한다.
        self.assertIn("_is_uuid(sref)", seg)

    def test_spawn_pins_the_uuid_not_the_ref(self):
        """🔴 채용은 등록 대기로 최대 240초를 보낸다 — 그 사이 ref 는 재번호된다.

        실측: 워크스페이스 하나가 몇 분 만에 workspace:19 → :3 → :8 로 바뀌었다.
        ref 를 쥔 채 기동 라인·첫 지시를 보내면 **남의 탭에 타이핑한다**.
        """
        am = load_am()
        out = ("OK surface:66 (C7C692A7-1F18-458E-B2FB-F59DC987E8B4) "
               "pane:8 (0D964014-C028-471A-8C43-9AF18690FC82) "
               "workspace:8 (0141B9ED-0E9D-4804-8455-6B05ACE2C55D)")
        self.assertEqual(am._uuid_after(out, "surface"),
                         "C7C692A7-1F18-458E-B2FB-F59DC987E8B4")
        self.assertEqual(am._uuid_after(out, "workspace"),
                         "0141B9ED-0E9D-4804-8455-6B05ACE2C55D")
        # pane 의 UUID 를 surface 로 집어오면 조용히 엉뚱한 곳에 타이핑한다
        self.assertNotEqual(am._uuid_after(out, "surface"),
                            am._uuid_after(out, "pane"))
        self.assertIsNone(am._uuid_after("OK surface:66", "surface"))

    def test_cmux_run_puts_id_format_before_the_subcommand(self):
        """전역 플래그라 서브커맨드 앞이어야 한다 — 뒤에 붙으면 cmux 가 안 받는다.

        그리고 호출부가 아니라 _cmux_run 이 붙여야 args[0] 이 서브커맨드로 남는다.
        """
        am = load_am()
        seen = {}

        class R:
            returncode, stdout, stderr = 0, "OK", ""

        # 🪤 am.subprocess 는 **모듈 전역**이다. 되돌리지 않으면 이 한 줄이 다른
        # 테스트 81개를 죽인다(실측). 반드시 원복한다.
        orig = am.subprocess.run
        am.subprocess.run = lambda argv, **kw: (seen.update(argv=argv), R())[1]
        try:
            am._cmux_run(["new-surface", "--type", "terminal"], ids=True)
            self.assertEqual(seen["argv"][1:4],
                             ["--id-format", "both", "new-surface"])
            am._cmux_run(["new-surface"], ids=False)
            self.assertEqual(seen["argv"][1], "new-surface")
        finally:
            am.subprocess.run = orig

    def test_cmux_uuids_resolves_ref_to_uuid(self):
        am = load_am()
        calls = []

        def fake(args, timeout=6):
            calls.append(args)
            if args[0] == "list-pane-surfaces":
                return 0, json.dumps({"surfaces": [
                    {"ref": "surface:12", "id": "S-UUID"},
                    {"ref": "surface:13", "id": "OTHER"}]}), ""
            return 0, json.dumps({"workspaces": [
                {"ref": "workspace:3", "id": "W-UUID"}]}), ""

        am._cmux_run = fake
        self.assertEqual(am._cmux_uuids("workspace:3", "surface:12"),
                         ("S-UUID", "W-UUID"))
        # 못 찾으면 (None, None) — 틀린 주소를 저장하느니 안 저장한다
        self.assertEqual(am._cmux_uuids("workspace:3", "surface:99"), (None, None))

    def test_codex_gets_a_longer_registration_window(self):
        """claude 는 훅이 즉시 등록하지만 codex 는 첫 턴 완료 + 스캐너 주기를 기다린다.

        같은 90초를 쓰면 codex 채용은 '실패'로 보고되면서 세션은 살아 있고,
        /org 를 건너뛰어 탭 좌표가 비게 된다.
        """
        am = load_am()
        self.assertGreater(am.HIRE_TIMEOUT_DEFAULT["codex"],
                           am.HIRE_TIMEOUT_DEFAULT["claude"])


if __name__ == "__main__":
    unittest.main()


class HireResendGuardCase(unittest.TestCase):
    """재전송은 **기동 흔적이 없을 때만**. 있으면 TUI 를 오염시킨다.

    실사용 보고: codex 채용 후 입력창에
      `AM_NAME=… AM_ROLE=… codex --dangerously-bypass-approvals-and-sandbox`
    가 그대로 타이핑돼 있었다. 셸에는 무해한 재전송이 TUI 에는 오염이다 —
    '이미 떠 있으면 무해하다'는 내 주석이 틀렸다.
    흔적은 **턴 완료 전, 기동 시점**에 생긴다(claude=sessions/<pid>.json,
    codex=thread-writer-locks/<thread>.lock).
    """

    def setUp(self):
        self.am = load_am()
        self.tmp = tempfile.mkdtemp()

    def test_evidence_present_blocks_resend(self):
        self.am.CODEX_LOCKS_DIR = self.tmp
        open(os.path.join(self.tmp, "t.lock"), "w").close()
        self.assertIsNotNone(self.am._launched_since("codex", time.time()))

    def test_no_evidence_allows_resend(self):
        self.am.CODEX_LOCKS_DIR = self.tmp
        self.assertIsNone(self.am._launched_since("codex", time.time()))

    def test_stale_evidence_is_not_evidence(self):
        """t0 이전 파일은 이번 기동의 흔적이 아니다 — 남의 세션 락을 근거로 쓰면 안 된다."""
        self.am.CODEX_LOCKS_DIR = self.tmp
        p2 = os.path.join(self.tmp, "old.lock")
        open(p2, "w").close()
        os.utime(p2, (time.time() - 3600, time.time() - 3600))
        self.assertIsNone(self.am._launched_since("codex", time.time()))

    def test_missing_dir_does_not_resend(self):
        """판정 불가면 재전송하지 않는다 — 오염이 지연보다 비싸다."""
        self.am.CODEX_LOCKS_DIR = os.path.join(self.tmp, "nope")
        self.assertIsNone(self.am._launched_since("codex", time.time()))


class CodexIdentityCase(unittest.TestCase):
    """codex 팀원의 신원 회수 — 없으면 등록·발신이 전부 403 이다.

    실사용 보고: 채용된 codex 가 "세션이 등록되지 않았고 등록 요청도 403" 이라 회신을
    못 했다. 원인 둘 — ①codex 에는 세션 id 를 알려주는 env 가 없다(실측: AM_* 만 있다)
    ②워커의 observed_session 근거 셋이 전부 Claude 전용(레지스트리·트랜스크립트·기존 등록).
    """

    def setUp(self):
        self.am = load_am()
        self.am.SESSION = ""

    def test_resolves_session_from_am_name(self):
        seen = {}

        def fake(method, path, body=None, params="", timeout=10):
            seen["params"] = params
            return {"agent": {"session": "01a0-codex-thread", "name": "cx"}}

        self.am.call = fake
        os.environ["AM_NAME"] = "cx"
        try:
            self.assertEqual(self.am._resolve_session(), "01a0-codex-thread")
            self.assertIn("name=cx", seen["params"])
        finally:
            os.environ.pop("AM_NAME", None)

    def test_no_name_no_guess(self):
        """이름도 없으면 빈 값을 돌려준다 — 아무 세션이나 사칭하지 않는다."""
        os.environ.pop("AM_NAME", None)
        self.am.call = lambda *a, **k: {"agent": {"session": "someone-else"}}
        self.assertEqual(self.am._resolve_session(), "")

    def test_env_session_wins_without_lookup(self):
        """CLAUDE_CODE_SESSION_ID 가 있으면 조회하지 않는다(핫패스 비용)."""
        self.am.SESSION = "claude-sid"
        def boom(*a, **k):
            raise AssertionError("조회하면 안 된다")
        self.am.call = boom
        self.assertEqual(self.am._resolve_session(), "claude-sid")


class InboxIdentityCase(unittest.TestCase):
    """'네가 누군지 모르겠다'를 '메시지 없음'으로 바꾸면 팀원이 지시를 영영 못 본다.

    실측: codex 팀원이 am inbox 를 치면 같은 순간 대기 중인 메시지가 실재하는데도
    {"items": []} 이 나왔다(빈 SESSION 으로 조회). 세션을 직접 준 대조군에서는 보였다.
    """

    def setUp(self):
        self.am = load_am()
        self.am.SESSION = ""

    def _run_inbox(self, check=False):
        buf = io.StringIO()
        ns = argparse.Namespace(check=check)
        with contextlib.redirect_stdout(buf):
            try:
                self.am.cmd_inbox(ns)
                code = 0
            except SystemExit as e:
                code = e.code
        return code, buf.getvalue()

    def test_unknown_self_is_not_empty_inbox(self):
        os.environ.pop("AM_NAME", None)
        called = []
        self.am.call = lambda *a, **k: called.append(a) or {"items": []}
        code, outp = self._run_inbox()
        self.assertEqual(code, 2)
        self.assertIn("unknown-self", outp)
        self.assertIn("비었다는 뜻이 아니다", outp)
        self.assertEqual(called, [])        # 신원 없이 조회조차 하지 않는다

    def test_check_mode_exits_nonzero_with_reason(self):
        """훅 경로(--check)도 조용히 성공하면 안 된다."""
        os.environ.pop("AM_NAME", None)
        self.am.call = lambda *a, **k: {"items": []}
        code, outp = self._run_inbox(check=True)
        self.assertEqual(code, 1)
        self.assertIn("특정하지 못했다", outp)

    def test_resolved_session_is_used_for_query(self):
        os.environ["AM_NAME"] = "cx"
        seen = {}

        def fake(method, path, body=None, params="", timeout=10):
            if path == "/agent":
                return {"agent": {"session": "sid-9", "name": "cx"}}
            seen[path] = params
            return {"items": []}

        self.am.call = fake
        try:
            self._run_inbox()
            self.assertIn("session=sid-9", seen.get("/inbox", ""))
        finally:
            os.environ.pop("AM_NAME", None)


class HireModelCase(unittest.TestCase):
    """기동 라인은 모델을 **항상 명시**한다.

    미지정이면 환경 기본값으로 조용히 폴백하는데 그건 채용자가 의도한 모델이 아니다
    (실측 교훈: 부활 스폰에서 --model 미지정이 최고가 모델로 폴백해 과금됐다).
    팀원 기본은 opus 5 — 실제 구현을 맡는 자리라서다.
    """

    def setUp(self):
        self.am = load_am()

    def _parse(self, argv):
        p = argparse.ArgumentParser(prog="am")
        self.am._hire_parser(p.add_subparsers(dest="cmd"))
        return p.parse_args(argv)

    def test_defaults_are_per_provider(self):
        """provider 마다 모델 이름 공간이 다르다 — 하나를 공유하면 codex 채용에
        claude 이름이 실린다. 파서는 None 을 주고 cmd_hire 가 provider 로 푼다."""
        self.assertIsNone(self._parse(["hire", "x", "--cwd", "/tmp"]).model)
        self.assertEqual(self.am.HIRE_DEFAULT_MODEL["claude"], "claude-opus-5")
        self.assertEqual(self.am.HIRE_DEFAULT_MODEL["codex"], "gpt-5.6-sol")

    def test_model_is_overridable(self):
        a = self._parse(["hire", "x", "--cwd", "/tmp", "--model", "claude-sonnet-5"])
        self.assertEqual(a.model, "claude-sonnet-5")

    def test_launch_line_carries_the_model(self):
        """폴백을 막는 건 파서 기본값이 아니라 **기동 라인에 실제로 실리는 것**이다."""
        line = self.am._launch_line("claude", "claude-sonnet-5", "AM_NAME=x")
        self.assertIn("--model claude-sonnet-5", line)
        self.assertIn("--dangerously-skip-permissions", line)

    def test_a_model_name_that_is_not_shell_safe_is_refused(self):
        """이 값은 남의 탭에 타이핑되는 셸 라인에 들어간다 — 이름과 같은 급으로 막는다."""
        self.assertFalse(self.am.MODEL_RE.match("gpt; rm -rf /"))
        self.assertFalse(self.am.MODEL_RE.match("a$(id)"))
        self.assertTrue(self.am.MODEL_RE.match("gpt-5.6-sol"))
        self.assertTrue(self.am.MODEL_RE.match("claude-opus-5"))


class HireProviderCase(unittest.TestCase):
    """provider 별로 **축이 다르다** — 같은 검사를 두 축에 걸치면 오판한다.

    claude: 훅=레포별(.claude/settings*.json) · 식별=AM_NAME 정확 조인
    codex : 훅 없음, 발견=워커 스캐너(~/.codex/state_5.sqlite) ·
            식별=cwd+등록시각 → 찾은 뒤 개명(스캐너가 codex-<id8> 로 짓는다)
    처음에 codex 에도 '레포에 훅이 있나'를 물어 **모든 codex 채용이 거부**됐다.
    """

    def setUp(self):
        self.am = load_am()

    def _parse(self, argv):
        p = argparse.ArgumentParser(prog="am")
        self.am._hire_parser(p.add_subparsers(dest="cmd"))
        return p.parse_args(argv)

    def test_default_provider_is_claude(self):
        self.assertEqual(self._parse(["hire", "x", "--cwd", "/tmp"]).provider, "claude")

    def test_codex_provider_accepted(self):
        a = self._parse(["hire", "x", "--cwd", "/tmp", "--provider", "codex"])
        self.assertEqual(a.provider, "codex")

    def test_codex_launch_line_uses_codex_not_claude(self):
        am = load_am()
        line = am._launch_line("codex", "gpt-5.6-sol", "AM_NAME=x")
        self.assertIn(" codex -m gpt-5.6-sol", line)
        self.assertIn("--dangerously-bypass-approvals-and-sandbox", line)
        # claude 쪽 플래그가 새어 들어오면 안 된다 (예전엔 기본값을 공유했다)
        self.assertNotIn("--model ", line)
        self.assertNotIn("--dangerously-skip-permissions", line)

    def test_codex_launch_line_pins_the_reasoning_budget(self):
        """~/.codex/config.toml 은 추적되지 않는 로컬 파일이다.

        거기 기본값에 기대면 채용 기록이 '무슨 예산으로 돌았나'를 말하지 못하고,
        그 파일이 바뀌는 순간 조용히 다른 예산으로 돈다.
        """
        am = load_am()
        line = am._launch_line("codex", "gpt-5.6-sol", "AM_NAME=x")
        self.assertIn("-c model_reasoning_effort=xhigh", line)
        self.assertEqual(am.CODEX_REASONING_EFFORT, "xhigh")

    def test_codex_gate_checks_state_db_not_repo_hooks(self):
        src = open(os.path.join(ROOT, "cli", "am")).read()
        self.assertIn("no-codex-state", src)
        self.assertNotIn("no-am-hook-codex", src)   # 폐기된 잘못된 게이트

    def test_codex_first_task_goes_to_tui_not_am(self):
        """codex 는 첫 턴이 돌아야 스캐너에 보인다 — 지시가 곧 발견 트리거다.

        실측: 기동만 한 codex 세션은 threads 에 행이 없고 락 파일·셸 스냅샷에만 있었다
        (그래서 채용이 타임아웃). codex exec 로 한 턴 돌리자 즉시 행이 생겼고 워커가
        codex-<id8> 로 등록했다. 게다가 codex 엔 UDS 소켓이 없어 push 채널이 TUI 뿐이다.
        """
        src = open(os.path.join(ROOT, "cli", "am")).read()
        self.assertIn("first-task-via-tui", src)
        # am 재배달 금지 — TUI 로 이미 줬다
        self.assertIn('if a.task and a.provider != "codex":', src)
