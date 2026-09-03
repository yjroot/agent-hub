"""벨 데몬 기동 보증 — cmux 안에서 도는 프로세스만 호출해야 의미가 있다.

worker/bell.py 는 cmux 인가를 **상속으로만** 얻는다. 그래서 launchd 워커는 이걸
띄울 수 없고(띄워도 인가 없는 자식이 된다), cmux 세션 안에서 도는 것 — 훅과
`am hire` — 이 띄워야 한다. 인가는 고아가 돼도 남으니 한 번 뜨면 그 탭이 닫혀도
계속 산다(이중 포크 실측).
"""
import json
import os
import subprocess
import sys
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
BELL_URL = os.environ.get("HUB_BELL_URL", "http://127.0.0.1:8792")


def bell_alive(timeout=1.5):
    try:
        with urllib.request.urlopen(f"{BELL_URL}/health", timeout=timeout) as r:
            return bool(json.loads(r.read() or b"{}").get("ok"))
    except Exception:  # noqa: BLE001
        return False


def ensure_bell():
    """살아 있으면 그대로, 아니면 분리 기동. (alive, started)

    cmux 밖에서 부르면 데몬은 뜨지만 cmux 호출이 전부 거부된다 — 조용한 실패가
    되지 않도록 CMUX_SURFACE_ID 로 안을 먼저 확인한다.
    """
    if bell_alive():
        return True, False
    if not os.environ.get("CMUX_SURFACE_ID"):
        return False, False
    log = open(os.path.join(os.environ.get("HUB_DIR")
                            or os.path.expanduser("~/.agent-hub"),
                            "bell.log"), "a")
    subprocess.Popen([sys.executable, os.path.join(ROOT, "worker", "bell.py")],
                     stdout=log, stderr=log, stdin=subprocess.DEVNULL,
                     start_new_session=True)
    # 기동 직후 한 번만 찔러 보면 경합으로 False 가 나온다 — 그러면 채용 리포트가
    # 살아 있는 데몬을 '못 떴다'고 적는다(조용한 오답). 짧게 여러 번 확인한다.
    for _ in range(8):
        if bell_alive(timeout=1.5):
            return True, True
        time.sleep(0.5)
    return False, True
