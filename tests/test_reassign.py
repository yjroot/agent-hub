#!/usr/bin/env python3
"""기존 팀원 재배정 — 개명(선택) + 탭 제목 + 새 작업.

핵심 불변식:
  - 라우팅 이름(레지스트리)과 탭 제목이 **함께** 바뀐다 (둘이 갈라지면 사람이 못 찾는다)
  - 탭 지목은 UUID (ref 는 재번호 — 남의 탭을 친다)
  - 탭 개명 실패는 재배정을 막지 않는다 (편의지 관문이 아니다)
  - 새 이름은 인자로 받는다 — 작업 텍스트에서 자동 유도하지 않는다
"""
import argparse
import importlib.machinery
import importlib.util
import io
import json
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


def load_am():
    loader = importlib.machinery.SourceFileLoader(
        "am_cli_reassign", os.path.join(ROOT, "cli", "am"))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    mod = importlib.util.module_from_spec(spec)
    loader.exec_module(mod)
    return mod


class ReassignCase(unittest.TestCase):
    def setUp(self):
        self.am = load_am()
        self.calls = []
        self.cmux = []
        self.agent = {"session": "s-old", "cli": "claude", "role": "member",
                      "name": "lane-a", "state": "live-idle",
                      "last_activity": None}

        def call(method, path, body=None, params="", timeout=10):
            self.calls.append((method, path, body, params))
            if path == "/agent":
                return {"agent": dict(self.agent)}
            if path == "/agent-by-session":
                return {"agent": dict(self.agent)}
            if path == "/org":
                # 개명 반영
                if body.get("rename_to"):
                    self.agent["name"] = body["rename_to"]
                return {"ok": True, "agent": {"name": self.agent["name"],
                                              "role": self.agent["role"]}}
            if path == "/send":
                return {"ok": True, "id": "m-1", "thread": "t-1"}
            return {"ok": True}

        self.am.call = call
        # 탭 좌표는 UUID 로 해석된다고 가정 (실제 경로는 _tab_of 테스트에서 따로)
        self.am._tab_of = lambda sess, cli: ("W-UUID", "S-UUID")

        def crun(args, timeout=6, ids=False):
            self.cmux.append(list(args))
            return 0, "OK", ""

        self.am._cmux_run = crun
        self.am._resolve_session = lambda: "s-lead"
        self.am._my_name = lambda: "some-lead"

    def run_cmd(self, ns):
        buf = io.StringIO()
        code = 0
        with contextlib.redirect_stdout(buf):
            try:
                self.am.cmd_reassign(ns)
            except SystemExit as e:
                code = e.code
        return code, json.loads(buf.getvalue())

    def ns(self, **kw):
        base = dict(name="lane-a", text="새 작업: #N 조사", to_name=None,
                    blocking=False)
        base.update(kw)
        return argparse.Namespace(**base)

    def test_rename_moves_both_the_route_and_the_tab(self):
        code, r = self.run_cmd(self.ns(to_name="item-b"))
        self.assertEqual(code, 0, r)
        # 라우팅 이름: /org rename_to 가 호출됐나
        org = [c for c in self.calls if c[1] == "/org"]
        self.assertEqual(org[0][2]["rename_to"], "item-b")
        # 탭: rename 이 UUID 로 호출됐나
        ren = [c for c in self.cmux if c[:3] == ["tab-action", "--action", "rename"]]
        self.assertEqual(len(ren), 1, self.cmux)
        self.assertEqual(ren[0][ren[0].index("--tab") + 1], "S-UUID")
        self.assertEqual(ren[0][ren[0].index("--title") + 1], "item-b · member")
        # 작업: 새 이름으로 배정됐나
        send = [c for c in self.calls if c[1] == "/send"][-1][2]
        self.assertEqual(send["to"], "item-b")

    def test_no_rename_just_assigns(self):
        code, r = self.run_cmd(self.ns())          # to_name 없음
        self.assertEqual(code, 0, r)
        self.assertEqual([c for c in self.calls if c[1] == "/org"], [])
        self.assertEqual([c for c in self.cmux
                          if c[:2] == ["tab-action", "--action"]], [])
        self.assertEqual([c for c in self.calls if c[1] == "/send"][-1][2]["to"],
                         "lane-a")

    def test_tab_failure_does_not_block_assignment(self):
        """탭 개명은 편의다 — 실패해도 라우팅 개명·작업 배정은 완주한다."""
        self.am._tab_of = lambda sess, cli: (None, None)   # 탭 못 찾음
        code, r = self.run_cmd(self.ns(to_name="item-b"))
        self.assertEqual(code, 0, r)
        self.assertEqual([c for c in self.calls if c[1] == "/send"][-1][2]["to"],
                         "item-b")
        rename_step = next(s for s in r["steps"] if s["step"] == "rename")
        self.assertFalse(rename_step["tab_retitle"])

    def test_unknown_agent_is_refused(self):
        self.am.call = lambda *a, **k: ({"agent": None}
                                        if (a[1] if len(a) > 1 else k.get("path")) == "/agent"
                                        else {"ok": True})
        code, r = self.run_cmd(self.ns(to_name="x"))
        self.assertEqual((code, r["error"]), (1, "unknown-agent"))


class OrgRenameRetitlesTabCase(unittest.TestCase):
    """am org --rename-to 는 탭 제목도 함께 옮긴다 (개명·탭이 갈라져 있던 공백)."""

    def setUp(self):
        self.am = load_am()
        self.cmux = []
        self.am.call = lambda method, path, body=None, params="", timeout=10: (
            {"agent": {"session": "s1", "cli": "claude", "role": "lead"}}
            if path in ("/agent", "/agent-by-session")
            else {"ok": True, "agent": {"name": "lead-b", "role": "lead"}}
            if path == "/org" else {"ok": True})
        self.am._tab_of = lambda s, c: ("W", "S-UUID")

        def crun(args, timeout=6, ids=False):
            self.cmux.append(list(args)); return 0, "OK", ""
        self.am._cmux_run = crun

    def test_org_rename_retitles(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            self.am.cmd_org(argparse.Namespace(
                name="matching-lead", role=None, team=None, reports_to=None,
                rename_to="lead-b", session=None))
        ren = [c for c in self.cmux if c[:3] == ["tab-action", "--action", "rename"]]
        self.assertEqual(len(ren), 1)
        self.assertEqual(ren[0][ren[0].index("--title") + 1], "lead-b · lead")

    def test_org_without_rename_touches_no_tab(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            self.am.cmd_org(argparse.Namespace(
                name="x", role="member", team=None, reports_to=None,
                rename_to=None, session=None))
        self.assertEqual(self.cmux, [])


import contextlib  # noqa: E402

if __name__ == "__main__":
    unittest.main()
