"""테스트 격리 가드.

🪤 **모듈 전역을 몽키패치하고 복원 안 하면 뒤따르는 테스트가 무더기로 죽는다.**
2026-09-08~11 에 같은 실수를 세 번 했다 —
  · am.subprocess.run 교체 후 미복원 → 81건 적색
  · bell.subprocess.run 교체 후 미복원 → 31건 적색
  · glob.glob 을 **이미 교체된 값으로** 복원(원본을 먼저 안 잡음) → 26건 적색
셋 다 원인이 오염 지점이 아니라 **엉뚱한 테스트**에서 터져 진단이 오래 걸렸다.
여기서 매 테스트 뒤에 대조해 **오염시킨 테스트 자신이** 적색이 되게 한다.
"""
import glob
import os
import subprocess

import pytest

_WATCHED = [(subprocess, "run"), (glob, "glob"), (os, "listdir"),
            (os.path, "exists"), (os.path, "expanduser")]


@pytest.fixture(autouse=True)
def _no_global_pollution():
    before = [(m, n, getattr(m, n)) for m, n in _WATCHED]
    yield
    dirty = [f"{m.__name__}.{n}" for m, n, v in before if getattr(m, n) is not v]
    if dirty:
        for m, n, v in before:          # 뒤 테스트까지 물들지 않게 되돌려 준다
            setattr(m, n, v)
        pytest.fail("전역 몽키패치를 복원하지 않았다: " + ", ".join(dirty)
                    + " — 원본을 **교체 전에** 저장하고 finally 에서 되돌려라.")
