#!/usr/bin/env python3
"""hub-relay v0 — Agent Hub 중계서버.

설계 정본: docs/DESIGN-agent-messenger.md (v4.2)
- 유일한 SoT + 유일한 시계 (모든 TTL/SLA/debounce 판정은 relay 시각)
- notice 발행은 relay 전용
- 타이머는 메모리가 아니라 due_at 행으로 영속 (재시작 복구)
- stdlib only: ThreadingHTTPServer + sqlite3(WAL)
"""
import json
import os
import sqlite3
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

DB_PATH = os.environ.get("HUB_DB", os.path.expanduser("~/.agent-hub/relay.db"))
PORT = int(os.environ.get("HUB_RELAY_PORT", "8790"))
TOKENS = {}  # worker_name -> token, loaded from env HUB_WORKER_TOKENS="mac:secret1,desktop:secret2"

# ── 정책 상수 (설계 §5·§6) ──────────────────────────────
LEASE_S = 180                # answer_lease (injected ack 시점 기산 — 툴 작업 중 응답 여유)
DEBOUNCE_S = 120             # 메시지 단위 부활 debounce (첫 주입 실패 기산)
AUTO_GATE_USD = 5.0          # 사전 게이트 자동 승인 문턱
SENDER_DAILY_USD = 20.0
GLOBAL_DAILY_USD = 60.0
DEFAULT_TTL_S = 3600
# fyi 는 살아 있는 수신자를 기다린다 — 다만 무한은 아니다(6시간 상한)
FYI_MAX_RENEWS = 5
CLAIM_TTL_S = 24 * 3600
BODY_MAX = 4000   # 저장 상한. 주입 봉투의 미리보기는 어차피 200자 클램프라 토큰 비용과 무관.
                  # 500 이던 시절 첫 유기 consult(하루 실사용 피드백)가 잘려 유실됨 — 실측 교훈.

_local = threading.local()


def db():
    if not hasattr(_local, "conn"):
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.row_factory = sqlite3.Row
        _local.conn = conn
    return _local.conn


SCHEMA = """
CREATE TABLE IF NOT EXISTS agents(
  name TEXT, session TEXT PRIMARY KEY, cli TEXT, home TEXT, repo TEXT, cwd TEXT,
  task TEXT, paths TEXT, design TEXT, model TEXT,
  state TEXT DEFAULT 'live-active',   -- live-active|live-idle|dormant|lost
  msg_socket TEXT, registered_at REAL, last_seen REAL, session_end_commit TEXT
);
CREATE TABLE IF NOT EXISTS claims(
  id TEXT PRIMARY KEY, session TEXT, agent TEXT, repo TEXT, path TEXT,
  branch TEXT, base TEXT, issue TEXT, joint_thread TEXT,
  created REAL, expires REAL, verified INTEGER DEFAULT 1
);
CREATE TABLE IF NOT EXISTS messages(
  id TEXT PRIMARY KEY, thread TEXT, from_agent TEXT, from_session TEXT,
  to_agent TEXT, type TEXT, priority TEXT, body TEXT, refs TEXT,
  state TEXT DEFAULT 'queued',        -- queued|injected|acknowledged|answered|expired
  cursor INTEGER, lease_holder TEXT, lease_expires REAL,
  meta TEXT DEFAULT '{}', ttl_s INTEGER, reply_to TEXT, created REAL
);
CREATE TABLE IF NOT EXISTS timers(
  id TEXT PRIMARY KEY, kind TEXT, msg_id TEXT, due_at REAL, fired INTEGER DEFAULT 0,
  attempts INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS tickets(
  id TEXT PRIMARY KEY, msg_id TEXT, asker_session TEXT, status TEXT DEFAULT 'open',
  created REAL
);
CREATE TABLE IF NOT EXISTS budget(
  day TEXT, scope TEXT, spent_usd REAL, PRIMARY KEY(day, scope)
);
CREATE TABLE IF NOT EXISTS metrics(
  ts REAL, key TEXT, value REAL, detail TEXT
);
-- 워커의 '성공한 세션 열거' 기록. live 강등의 적극적 근거는 이것뿐이다 (§1-1).
CREATE TABLE IF NOT EXISTS worker_sweeps(
  home TEXT PRIMARY KEY, last_ok REAL, sessions INTEGER
);
CREATE INDEX IF NOT EXISTS idx_msg_to ON messages(to_agent, state);
CREATE INDEX IF NOT EXISTS idx_timers_due ON timers(fired, due_at);
"""

# 기존 DB 보존 마이그레이션 (컬럼 추가만 — 데이터 삭제·재생성 없음)
MIGRATIONS = [
    "ALTER TABLE messages ADD COLUMN injected_at REAL",
    "ALTER TABLE messages ADD COLUMN inject_count INTEGER DEFAULT 0",
    # 배달 회계의 근거. 'injected' 가 무엇을 근거로 찍혔는지(영수증 / 부정영수증 부재)와
    # 미배달 사유(held·refused…)를 남긴다 — 없으면 거짓 양성을 사후에 구분할 수 없다.
    "ALTER TABLE messages ADD COLUMN wake_status TEXT",
    # 발신 세션의 권한 모드 계급(bypassPermissions|plan|default|acceptEdits|…).
    # 웨이크 봉투의 from-mode attest 원천 — 없으면 bypass 수신자가 무조건 hold 한다.
    "ALTER TABLE agents ADD COLUMN permission_mode TEXT",
    # 🔴 idle 표시 축. last_seen 은 워커 liveness 스윕(20s)이 갱신하는 **도달성** 축이라
    # live 행은 항상 0분이 된다 — 실측: live 49행 전부 13~15초 전. 그래서 "5분 idle 과
    # 6시간 idle 은 관리 판단이 다르다"고 요청받아 넣은 IDLE 칸이 정보량 0이었다.
    # 활동 축은 CC 레지스트리의 statusUpdatedAt(그 세션이 실제로 상태를 바꾼 시각)에서 온다.
    "ALTER TABLE agents ADD COLUMN last_activity REAL",
    # TASK 의 두 의미를 칸으로 가른다. 사용자 기대는 '지금 뭘 하고 있나'(최근 프롬프트)
    # 인데 구현은 '정체성 라벨'(첫 프롬프트 고정)이었다 — 실측: 22일 전 첫 프롬프트가
    # 그대로. 한 칸에 두 의미를 담으면 어느 쪽도 못 만족한다.
    "ALTER TABLE agents ADD COLUMN recent_prompt TEXT",
    # 조직 축 — 강제는 하지 않고(사용자 결정) **항상 보이게** 한다. 실측상 문제는
    # 완전그래프가 아니라 ①허브 1명에 1,244건 집중 ②역할 라벨이 시스템에 아예 없어
    # 매번 "누구에게 물어야 하나"를 추측하는 것이었다.
    "ALTER TABLE agents ADD COLUMN role TEXT",        # chairman|secretary|lead|member
    "ALTER TABLE agents ADD COLUMN team TEXT",
    "ALTER TABLE agents ADD COLUMN reports_to TEXT",
    # cmux 서피스 제목(사용자가 손으로 붙인 역할·과제명 — '사장'·'비서'·'#N').
    # 우리 task/recent_prompt 보다 사람이 의도한 라벨이라 조망에서 우선한다.
    "ALTER TABLE agents ADD COLUMN cmux_title TEXT",
    # codex 팀원의 배달 주소. codex 엔 UDS 소켓이 없어 push 채널이 TUI 뿐이라,
    # 그 탭을 지목할 좌표가 필요하다(사용자 제안: "탭에 키보드 입력을 보내자").
    "ALTER TABLE messages ADD COLUMN ttl_renews INTEGER DEFAULT 0",
    "ALTER TABLE messages ADD COLUMN gate_notice TEXT",
    "CREATE TABLE IF NOT EXISTS notice_cooldown("
    "  key TEXT PRIMARY KEY, at REAL)",
    "ALTER TABLE agents ADD COLUMN cmux_surface TEXT",
    "ALTER TABLE agents ADD COLUMN cmux_workspace TEXT",
    "ALTER TABLE agents ADD COLUMN task_explicit INTEGER DEFAULT 0",
    # 일회용(프로브·테스트) 세션 표식 — 조망용 목록(/agents·/who)에서만 감춘다.
    # 배달·부활 경로는 그대로 동작해야 하므로 /agent·/poll 은 이 값을 보지 않는다.
    "ALTER TABLE agents ADD COLUMN ephemeral INTEGER DEFAULT 0",
]


def migrate(conn):
    for stmt in MIGRATIONS:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e):
                raise


def caller_is_worker():
    """이 요청이 워커 토큰으로 인증됐는가 (설계 §2-1).

    msg_socket(=유휴 세션 주입 주소) 갱신은 워커만 할 수 있어야 한다. 워커는 그 값을
    ~/.claude/sessions 레지스트리에서 직접 확인해 올리기 때문이다. 자가 신고를 그대로
    받아주던 시절, 아무 프로세스나 남의 세션 주소를 자기 소켓으로 덮어 메시지를 통째로
    가로챌 수 있었다(적대 리뷰 E2E 재현).
    토큰 미설정(로컬 v0, 127.0.0.1 바인드 강제)에서는 워커로 간주한다.
    """
    if not TOKENS:
        return True
    return bool(getattr(_local, "worker", None))


def now():
    return time.time()


def today():
    return time.strftime("%Y-%m-%d")


def new_id(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def metric(key, value=1.0, detail=""):
    db().execute("INSERT INTO metrics VALUES(?,?,?,?)", (now(), key, value, detail))


def next_cursor(c):
    """다음 커서 값. **커서 할당자는 하나여야 한다.**

    예전엔 두 개였다 — INSERT 는 rowid 를, 재큐는 MAX(cursor)+1 을 썼다. 두 축이
    겹치면서 재큐된 행의 커서가 **미래의 rowid 와 충돌**했다: rowid 1 하나뿐인 DB 에서
    재큐가 cursor=2 를 주고, 그 다음에 들어온 신규 메시지가 rowid=2 → cursor=2 를 받는다.
    워커 커서가 이미 2 라면 h_poll 의 `cursor > 2` 에서 그 신규 메시지는 **영원히**
    보이지 않는다 (실측 재현: new_message_visible=False).
    그래서 rowid 축과 cursor 축의 최댓값을 함께 보고 그 위에서 발급한다.
    """
    row = c.execute("SELECT COALESCE(MAX(cursor),0) AS c, COALESCE(MAX(rowid),0) AS r "
                    "FROM messages").fetchone()
    return max(row["c"], row["r"]) + 1


def insert_message(*, thread, from_agent, from_session, to_agent, mtype, priority,
                   body, refs="{}", state="queued", meta=None, ttl_s=DEFAULT_TTL_S,
                   reply_to=None, body_cap=BODY_MAX, conn=None):
    """모든 메시지 INSERT 의 단일 경로. cursor 는 next_cursor 단일 할당자에서."""
    c = conn or db()
    mid = new_id("m")
    if len(body) > body_cap:
        body = body[:body_cap] + " …[truncated]"
    c.execute(
        "INSERT INTO messages(id,thread,from_agent,from_session,to_agent,type,priority,"
        "body,refs,state,meta,ttl_s,reply_to,created) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (mid, thread or new_id("t"), from_agent, from_session, to_agent, mtype,
         priority, body, refs, state, json.dumps(meta or {}), ttl_s, reply_to, now()))
    c.execute("UPDATE messages SET cursor=? WHERE id=?", (next_cursor(c), mid))
    return mid


def notice(to_agent, body, thread=None, meta=None, conn=None):
    """notice 발행 — relay 전용 (설계 §2-1). CLI 경로에서는 생성 불가."""
    return insert_message(thread=thread, from_agent="__relay__",
                          from_session="__relay__", to_agent=to_agent, mtype="notice",
                          priority="normal", body=body, meta=meta, conn=conn)


def verified_sender(from_session, claimed_name):
    """신원 바인딩 (설계 §2-1): registry 의 세션→이름이 정본, 자가 선언은 표시용."""
    row = db().execute("SELECT name FROM agents WHERE session=?",
                       (from_session,)).fetchone()
    if row and row["name"]:
        return row["name"]
    return claimed_name or (f"session-{from_session[:8]}" if from_session else "unknown")


def spent(scope):
    row = db().execute("SELECT spent_usd FROM budget WHERE day=? AND scope=?",
                       (today(), scope)).fetchone()
    return row["spent_usd"] if row else 0.0


def add_spend(scope, usd):
    db().execute(
        "INSERT INTO budget VALUES(?,?,?) ON CONFLICT(day,scope) "
        "DO UPDATE SET spent_usd=spent_usd+excluded.spent_usd", (today(), scope, usd))


# ── 핸들러 ──────────────────────────────────────────────

import re as _re
_MARKER_RE = _re.compile(r"agent-hub inbox|cross-session-message|task-notification")

NAME_SQUAT_FRESH_S = 600   # 이 시간 안에 살아있다고 보고된 세션의 이름은 못 뺏는다


def _name_is_squatted(name, session):
    """다른 '살아있는' 세션이 이미 쓰는 이름인가.

    agents 조회는 name → registered_at DESC LIMIT 1 이고 h_poll 도 name 으로 조인한다.
    즉 남의 이름으로 새 세션을 등록하면 그 이름 앞으로 오는 배달이 통째로 신규 행으로
    넘어간다(무음 탈취). 이름은 선점자 우선 — 늦게 온 쪽이 비켜난다.
    """
    # 🔴 기본 이름(session-*)을 가드 밖에 두면 안 된다. 함대의 대부분이 기본 이름으로
    # 도는데, 그 전체가 무방비였다 — 아무나 남의 session-<id8> 로 등록하면 h_agent 의
    # `registered_at DESC LIMIT 1` 때문에 늦게 등록한 쪽이 이기고, 그 이름 앞으로 오는
    # 배달이 통째로 넘어간다(피해자는 무음 유실). 적대 검증 실측 지적.
    # 자기 세션에서 파생된 기본 이름만 예외로 둔다(그건 사칭이 아니라 자기 이름이다).
    if not name:
        return False
    if name.startswith("session-") and session.startswith(name[len("session-"):]):
        return False
    row = db().execute(
        "SELECT session FROM agents WHERE name=? AND session!=? "
        "AND state LIKE 'live-%' AND last_seen > ? LIMIT 1",
        (name, session, now() - NAME_SQUAT_FRESH_S)).fetchone()
    return bool(row)


def h_register(body, _q):
    a = body
    if not caller_is_worker():
        # 워커 토큰이 아니면 주입 주소를 실을 수 없다 (자가 신고 무시)
        a.pop("msg_socket", None)
    if a.get("name") and _name_is_squatted(a["name"], a.get("session", "")):
        metric("register.name_squat", 1, f"{a['name']} <- {a.get('session','')[:8]}")
        a = dict(a)
        a["name"] = f"session-{a.get('session', '')[:8]}" or None
        squatted = True
    else:
        squatted = False
    if a.get("hint_only"):
        # 이력 인덱서 등 스캔 등록: 기존 행의 name·state·cwd 를 절대 덮지 않는다
        # (hub-architect 가 session-* 로, live 가 dormant 로 덮인 실사고 반영).
        exists = db().execute("SELECT 1 FROM agents WHERE session=?",
                              (a["session"],)).fetchone()
        if exists:
            _apply_hints(a)
            return {"ok": True, "hint_only": True}
    # 합성 기본 이름은 **이미 사람이 붙인 이름을 덮지 못한다**(위 실측 참조).
    if a.get("name_is_default"):
        cur = db().execute("SELECT name FROM agents WHERE session=?",
                           (a["session"],)).fetchone()
        if cur and cur["name"] and not cur["name"].startswith("session-"):
            a = dict(a)
            a["name"] = ""      # 기존 이름 보존
    db().execute(
        "INSERT INTO agents(name,session,cli,home,repo,cwd,task,paths,design,model,"
        "state,msg_socket,registered_at,last_seen,ephemeral,permission_mode,"
        "role,team,reports_to,cmux_title,cmux_surface,cmux_workspace) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(session) DO UPDATE SET "
        # ephemeral 은 한 번 서면 내려가지 않는다(sticky). 훅은 세션 env 를 매번 싣지
        # 못하므로 뒤이은 부분 등록이 표식을 지우면 프로브가 로스터로 되살아난다.
        "ephemeral=CASE WHEN excluded.ephemeral=1 THEN 1 "
        "ELSE COALESCE(agents.ephemeral,0) END, "
        "home=excluded.home, cwd=COALESCE(NULLIF(excluded.cwd,''), cwd), "
        "name=COALESCE(NULLIF(excluded.name,''), name), "
        "task=COALESCE(NULLIF(excluded.task,''), task), "
        "paths=CASE WHEN excluded.paths='[]' THEN paths ELSE excluded.paths END, "
        "design=COALESCE(NULLIF(excluded.design,''), design), "
        "model=COALESCE(NULLIF(excluded.model,''), model), "
        # msg_socket 갱신 필수: pid 가 바뀌면 소켓 경로도 바뀐다. 이 줄이 없던 동안
        # 두 번째 등록부터 갱신이 안 돼 stale 소켓을 쥐게 되는 구조였다.
        # 빈 값(구버전 세션·소켓 미보유)으로 기존 값을 지우지는 않는다.
        "msg_socket=COALESCE(NULLIF(excluded.msg_socket,''), msg_socket), "
        "permission_mode=COALESCE(NULLIF(excluded.permission_mode,''), permission_mode), "
        "role=COALESCE(NULLIF(excluded.role,''), role), "
        "team=COALESCE(NULLIF(excluded.team,''), team), "
        "reports_to=COALESCE(NULLIF(excluded.reports_to,''), reports_to), "
        "cmux_title=COALESCE(NULLIF(excluded.cmux_title,''), cmux_title), "
        # 탭 좌표(codex 배달 주소)도 빈 값으로 지우지 않는다 — 훅 부분 등록이
        # 매번 비우면 초인종 주소가 사라져 codex 팀원이 조용히 pull 전용이 된다.
        "cmux_surface=COALESCE(NULLIF(excluded.cmux_surface,''), cmux_surface), "
        "cmux_workspace=COALESCE(NULLIF(excluded.cmux_workspace,''), cmux_workspace), "
        "state=excluded.state, last_seen=?",
        (a.get("name"), a["session"], a.get("cli", "claude"), a.get("home", "local"),
         a.get("repo", ""), a.get("cwd", ""), a.get("task", ""),
         json.dumps(a.get("paths", [])), a.get("design", ""), a.get("model", ""),
         a.get("state", "live-active"), a.get("msg_socket", ""), now(), now(),
         1 if a.get("ephemeral") else 0, a.get("permission_mode", ""),
         a.get("role", ""), a.get("team", ""), a.get("reports_to", ""),
         a.get("cmux_title", ""), a.get("cmux_surface", ""),
         a.get("cmux_workspace", ""), now()))
    if (a.get("task") or "").strip() and not a.get("partial"):
        # 사람이/에이전트가 스스로 붙인 라벨은 최근 프롬프트에 밀리지 않는다.
        db().execute("UPDATE agents SET task_explicit=1 WHERE session=?",
                     (a["session"],))
    _apply_hints(a)
    # 등록 응답에 조직 위치를 실어 보낸다 — 훅이 이걸로 SessionStart 안내를 만든다.
    # 강제가 없는 모델에서 '스스로의 위치 인지'는 이 한 줄에 달려 있다.
    org = db().execute("SELECT name, role, team, reports_to FROM agents "
                       "WHERE session=?", (a["session"],)).fetchone()
    if squatted:
        return {"ok": True, "name": a["name"], "org": dict(org) if org else None,
                "name_conflict": "이름을 이미 살아있는 다른 세션이 쓰고 있어 기본 이름으로 "
                                 "등록했다 (선점자 우선)"}
    return {"ok": True, "org": dict(org) if org else None}


def _apply_hints(a):
    # 🔴 hint_only 등록은 여기서 끝난다(기존 행의 name·state·cwd 를 안 덮기 위해).
    # 그래서 워커가 실어 보내는 **탭 좌표도 여기서 받지 않으면 통째로 버려진다** —
    # 스캐너가 매 주기 좌표를 보내도 영원히 반영되지 않는다. 빈 값으로 기존 값을
    # 지우지는 않는다.
    for col in ("cmux_workspace", "cmux_surface"):
        v = (a.get(col) or "").strip()
        if v:
            db().execute(f"UPDATE agents SET {col}=? WHERE session=?",
                         (v, a["session"]))
    hint = (a.get("task_hint") or "").strip()
    # 🔴 하네스 주입 문구를 '세션 정체성'으로 캡처하면 안 된다. 실측 183행 중 46행(25%)이
    # '<' 로 시작했다 — <local-command-caveat>·<task-notification>·<cross-session-message>.
    # 마지막 것은 **내 웨이크 봉투**다: 남을 깨운 내 메시지가 그 세션의 정체성 라벨이 됐다.
    if hint.startswith("<") or _MARKER_RE.search(hint[:40]):
        metric("task_hint.rejected", 1, hint[:60])
        hint = ""
    if hint:
        # 🔑 '지금 뭘 하고 있나'는 **항상** 갱신한다 — 사용자가 목록에서 기대하는 값이다.
        db().execute("UPDATE agents SET recent_prompt=? WHERE session=?",
                     (hint[:120], a["session"]))
        # task(정체성 라벨)는 종전 규칙: 비었거나 오염됐을 때만. 명시 register 는 불침.
        db().execute("UPDATE agents SET task=? WHERE session=? "
                     "AND COALESCE(task_explicit,0)=0 "
                     "AND (task IS NULL OR task='' OR task LIKE '<%')",
                     (hint[:120], a["session"]))
    if a.get("paths_hint"):
        # 이력 인덱서의 소유 경로 — 명시 claim/register 가 없을 때만
        db().execute("UPDATE agents SET paths=? WHERE session=? "
                     "AND (paths IS NULL OR paths='' OR paths='[]')",
                     (json.dumps(a["paths_hint"][:40]), a["session"]))


def h_agents(_body, q):
    """전체 에이전트 목록 — '누가 뭘 하고 있나' 조망용 (am agents).

    ephemeral(프로브·일회용 세션)은 감춘다. 로스터는 '지금 누구와 협업 중인가'를 읽는
    화면인데, 검증용으로 몇 초 살다 죽는 세션이 섞이면 실재 에이전트를 밀어낸다
    (limit 40 기본). all=1 로 감사 시에는 볼 수 있다.
    """
    state = q.get("state", [""])[0]
    show_all = q.get("all", ["0"])[0] in ("1", "true")
    # 🔴 축 필터. 기계 소비자가 로스터 전체를 긁어 거르면 **절단에 걸린다** —
    # codex 행은 last_activity 가 NULL 이라 정렬 꼴찌이고, 225곳에 limit=200 이면
    # 정확히 그것들이 잘렸다(실측: 등록은 됐는데 am hire 가 못 찾아 타임아웃).
    # 사람용 절단은 꼬리표로 고쳤지만 기계 경로는 조용히 유실됐다 — 거르기는 서버에서.
    cli = q.get("cli", [""])[0]
    since = float(q.get("since", ["0"])[0] or 0)
    rows = db().execute(
        "SELECT name, cli, state, role, team, reports_to, cmux_title, task, recent_prompt, "
        # registered_at: 신규 등록을 시각으로 가르는 소비자가 있다(am hire 의 codex
        # 식별 축). 이 칸이 없어서 필터가 항상 거짓이 됐고, **등록은 됐는데**
        # 채용이 타임아웃났다 — 없는 축을 물으면 조용히 0 이 되는 형상.
        "registered_at, "
        "COALESCE(task_explicit,0) ""AS task_explicit, cwd, repo, last_seen, last_activity, "
        "COALESCE(ephemeral,0) AS ephemeral, "
        # 🔑 idle 은 **활동 축**으로 잰다. last_seen 은 워커 liveness 스윕(20s)이 매번
        # 갱신하는 도달성 축이라 live 행이 전부 0분이 된다(실측 49행 13~15초) —
        # "5분 idle 과 6시간 idle 은 판단이 다르다"고 요청받아 넣은 칸이 정보량 0이었다.
        # 미측정은 NULL 로 남긴다: 0 으로 채우면 '방금 활동'이라는 거짓말이 된다.
        "CASE WHEN last_activity IS NULL THEN NULL "
        "ELSE CAST(? - last_activity AS INTEGER) END AS idle_s FROM agents "
        "WHERE (?='' OR state=?) AND name != '' "
        "AND (?='' OR cli=?) AND COALESCE(registered_at,0) >= ? "
        "AND (? OR COALESCE(ephemeral,0)=0) "
        # 정렬도 활동 축으로 — last_seen DESC 는 전부 동률이라 사실상 임의 순서였다.
        # 🔴 정렬 축이 last_activity 하나면 **살아 있는 codex 가 죽은 행 뒤에 선다** —
        # codex 는 last_activity 가 NULL(=0)이라 언제나 꼴찌라서, 기본 limit 40 /
        # 함대 292 에서는 통째로 잘려 나간다. 팀장이 "고용이 실패했다"고 읽고 사장에게
        # 잘못 보고한 근인이 이것이다(실측 보고 t-5890617e).
        # 생사를 1순위로 둔다. 로스터에서 산 사람이 죽은 사람 뒤에 설 이유가 없다.
        "ORDER BY (state LIKE 'live%') DESC, "
        "COALESCE(last_activity, last_seen, 0) DESC LIMIT ?",
        (now(), state, state, cli, cli, since, 1 if show_all else 0,
         int(q.get("limit", ["40"])[0]))).fetchall()
    # 🔴 절단은 **말해야** 한다. 기본 limit 40 인데 함대가 208 이면 로스터에 실재하는
    # 세션이 "없음"으로 읽힌다 — 실측: PM 이 두 번 속아 실재 세션 7곳을 없다고 판독하고
    # 유령 소동이 났다. 오늘 같은 형상만 다섯 번째다(타임아웃→'없음', cwd 오답→'소유자 없음',
    # 미측정→'0분', 인용 축 불일치→'빈 응답'). 조용한 절단은 조용한 오답이다.
    total = db().execute(
        "SELECT COUNT(*) c FROM agents WHERE (?='' OR state=?) AND name != '' "
        "AND (?='' OR cli=?) AND COALESCE(registered_at,0) >= ? "
        "AND (? OR COALESCE(ephemeral,0)=0)",
        (state, state, cli, cli, since, 1 if show_all else 0)).fetchone()["c"]
    return {"agents": [dict(r) for r in rows], "total": total,
            "shown": len(rows), "truncated": total > len(rows)}


def h_liveness(body, _q):
    """워커 보고: [{session, state}] — 판정식(§1-1)은 워커 책임, relay는 기록.

    observed=True 는 '이 워커가 자기 홈의 세션 목록을 **성공적으로 열거했다**'는 뜻이다.
    강등(live→dormant)의 유일한 적극적 근거이므로 열거가 실패한 스윕에서는 절대 오지
    않는다(워커가 안 보낸다). 목록이 비어 있어도 성공이면 보낸다 — '세션 0개'도 사실이다.
    """
    for item in body.get("agents", []):
        db().execute("UPDATE agents SET state=?, last_seen=? WHERE session=?",
                     (item["state"], now(), item["session"]))
        # 활동 축은 last_seen 과 분리한다. 겸직시키면 강등·h_send 분기까지 얽힌다.
        if item.get("last_activity"):
            db().execute("UPDATE agents SET last_activity=? WHERE session=?",
                         (item["last_activity"], item["session"]))
    if body.get("observed"):
        home = str(body.get("home") or "")[:64]
        if home:
            db().execute(
                "INSERT INTO worker_sweeps(home,last_ok,sessions) VALUES(?,?,?) "
                "ON CONFLICT(home) DO UPDATE SET last_ok=excluded.last_ok, "
                "sessions=excluded.sessions",
                (home, now(), len(body.get("agents", []))))
    if body.get("session_ended"):
        db().execute(
            "UPDATE agents SET state='dormant', session_end_commit=? WHERE session=?",
            (body.get("commit", ""), body["session_ended"]))
    return {"ok": True}


def h_retire(body, _q):
    """이름 앞의 미배달을 **조용히** 닫는다 (am fire --no-kill).

    🔑 죽이면 안 되는데 이름은 회수해야 하는 경우가 실재한다 — kill-by-pid 가드가
    cwd 불일치로 옳게 거부하면, 그 이름 앞으로 배달이 계속 시도되고 만료 통지가
    채용자를 반복해서 깨운다(실측: member-x). 은퇴는 라우팅을 끊는 조치이므로
    남은 우편도 함께 닫는다 — 안 닫으면 TTL 이 와서 통지를 낸다.
    조용히 닫는 이유: 은퇴시킨 사람이 곧 그 통지의 수신자다. 자기가 방금 한 일을
    사고 보고로 되받는 건 소음이다.
    """
    name = (body.get("name") or "").strip()
    if not name:
        return {"ok": False, "error": "name-required"}
    rows = db().execute(
        "SELECT id FROM messages WHERE to_agent=? AND state IN "
        "('queued','injected','deferred')", (name,)).fetchall()
    db().executemany("UPDATE messages SET state='expired' WHERE id=?",
                     [(r["id"],) for r in rows])
    metric("retire.drain", len(rows), name)
    return {"ok": True, "name": name, "closed": len(rows)}


def h_worker_notice(body, _q):
    """워커 발 notice — 워커만 아는 사실을 보고선에 알린다 (예: codex 세션 소멸).

    CLI 경로는 notice 를 만들 수 없다(설계 §2-1). 워커 토큰이 있을 때만 연다 —
    아무나 __relay__ 이름으로 통지를 찍을 수 있으면 그 표식이 무의미해진다.
    """
    if not caller_is_worker():
        return {"ok": False, "error": "worker-only"}
    to = (body.get("to_agent") or "").strip()
    text = (body.get("body") or "").strip()
    if not to or not text:
        return {"ok": False, "error": "to_agent/body 필요"}
    if not db().execute("SELECT 1 FROM agents WHERE name=?", (to,)).fetchone():
        return {"ok": False, "error": "unknown-agent", "name": to}
    return {"ok": True, "id": notice(to, text[:4000],
                                     meta=body.get("meta"))}


def h_who(_body, q):
    path = q.get("path", [""])[0]
    repo = q.get("repo", [""])[0]
    # ephemeral 은 소유자 후보에서도 뺀다 — 일회용 세션이 소유자로 잡히면 그 앞으로
    # 간 질의는 곧 죽을(또는 이미 죽은) 세션에 배달돼 부활 경로로 새어 나간다.
    rows = db().execute(
        "SELECT * FROM agents WHERE state != 'lost' AND COALESCE(ephemeral,0)=0 "
        "ORDER BY registered_at DESC").fetchall()
    matches = []
    for r in rows:
        for glob_pat in json.loads(r["paths"] or "[]"):
            if _glob_match(glob_pat, path):
                matches.append({
                    "agent": r["name"], "session": r["session"], "state": r["state"],
                    "task": r["task"], "design": r["design"], "cli": r["cli"],
                    "match": glob_pat,
                })
                break
    # 최장 일치 우선 (설계 §3)
    matches.sort(key=lambda m: -len(m["match"]))
    return {"matches": matches, "repo": repo,
            "fallback_hint": "git log --oneline -5 -- <path> + .claude/DESIGN-*.md"}


def _glob_match(pat, path):
    import fnmatch
    if pat.endswith("/**"):
        return path.startswith(pat[:-3]) or fnmatch.fnmatch(path, pat)
    return fnmatch.fnmatch(path, pat)


def h_claim(body, _q):
    sess = body["session"]
    agent = body.get("agent", "")
    repo = body.get("repo", "")
    branch = body.get("branch", "")
    base = body.get("base", "")
    conflicts = []
    for path in body.get("paths", []):
        rows = db().execute(
            "SELECT * FROM claims WHERE repo=? AND expires>? AND session != ?",
            (repo, now(), sess)).fetchall()
        for r in rows:
            if _overlap(r["path"], path):
                kind = None
                if r["branch"] == branch:
                    kind = "same-branch-different-session"   # v0: 계측 전용 (설계 §3)
                elif r["base"] == base and base:
                    kind = "cross-worktree-same-base"
                if kind and not body.get("joint"):
                    conflicts.append({"with": r["agent"], "session": r["session"],
                                      "path": r["path"], "branch": r["branch"],
                                      "kind": kind})
                    metric("claim.conflict", 1, kind)
        cid = new_id("c")
        db().execute(
            "INSERT INTO claims(id,session,agent,repo,path,branch,base,issue,"
            "joint_thread,created,expires) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (cid, sess, agent, repo, path, branch, base, body.get("issue", ""),
             body.get("joint", ""), now(), now() + CLAIM_TTL_S))
    thread = None
    if conflicts:
        thread = new_id("t")   # 협의 스레드 자동 개설
        for c in conflicts:
            notice(c["with"],
                   f"claim 겹침(계측): {agent} 이(가) {c['path']} 를 선언 "
                   f"(kind={c['kind']}). 협의 스레드 {thread}", thread=thread)
    return {"ok": True, "conflicts": conflicts, "thread": thread}


def _overlap(pat_a, pat_b):
    base_a = pat_a.rstrip("*").rstrip("/")
    base_b = pat_b.rstrip("*").rstrip("/")
    return base_a.startswith(base_b) or base_b.startswith(base_a)


def h_send(body, _q):
    """메시지 발신. blocking 이면 티켓 발급."""
    to_agent = body.get("to")
    if not to_agent and body.get("owner_of"):
        who = h_who({}, {"path": [body["owner_of"]], "repo": [body.get("repo", "")]})
        if not who["matches"]:
            return {"ok": False, "error": "no-owner",
                    "notice": "author-lost: 소유자 없음. " + who["fallback_hint"]}
        target = who["matches"][0]
        to_agent = target["agent"]
    if body.get("type") == "notice":
        return {"ok": False, "error": "notice-is-relay-only"}  # 설계 §2-1
    # 발신자 인가 (설계 §1-3 v1 필수): 등록된 세션만 발신 가능.
    # 등록 자체가 워커 토큰 뒤에 있으므로 "토큰 보유 워커가 확인한 세션"으로 좁혀진다.
    known = db().execute("SELECT 1 FROM agents WHERE session=?",
                         (body.get("from_session", ""),)).fetchone()
    if not known:
        return {"ok": False, "error": "unregistered-sender",
                "hint": "am register 후 발신 가능"}
    # 🔴 빈 본문은 **정보 0에 비용만 든다**. 수신자를 깨우고(턴 하나) 아무것도
    # 말하지 않는다. 실측: 코퍼스에 10건+ 이 있었고, 08-31 한 건은 회람이라
    # **7명을 한꺼번에** 빈 본문으로 깨웠다. 발신 측 인용 오류로 조용히 생긴다.
    # 여기서 막는다 — 클라이언트 버그가 남의 턴을 쓰게 두면 안 된다.
    if not (body.get("body") or "").strip():
        return {"ok": False, "error": "empty-body",
                "hint": "본문이 비었다 — 수신자를 깨우고 아무것도 말하지 않는다. "
                        "인용이 깨졌는지 확인해라(셸에서 따옴표 안 문자열이 "
                        "비는 경우가 흔하다)."}
    # 🔴 이름이 여럿이면 **조용히 하나를 고르지 않는다**. 자동 생성 이름
    # codex-<id8> 이 유일하지 않아 서로 다른 두 팀의 작업자가 같은 주소를 갖는 일이
    # 실제로 있었다(팀B장 실측: 한 이름에 두 행, 정체가 다름). 그 상태에서
    # 하나를 골라 배달하면 **남의 작업자를 친다** — 「누구에게 갔는지 아무도 모르는
    # 성공」이라 유실보다 나쁘다.
    # 🪤 검문은 반드시 **insert_message 앞**이다. 뒤에 두면 거절하면서 고아 행을
    # 남긴다(처음에 그렇게 넣었고, 그 축의 테스트가 없어서 못 잡았다).
    if to_agent:
        dupes = db().execute(
            "SELECT session, cli, task FROM agents WHERE name=? AND state != 'lost' "
            "AND COALESCE(ephemeral,0)=0 ORDER BY registered_at DESC",
            (to_agent,)).fetchall()
        if len(dupes) > 1:
            return {"ok": False, "error": "ambiguous-recipient", "name": to_agent,
                    "candidates": [{"session": d["session"], "cli": d["cli"],
                                    "task": (d["task"] or "")[:60]} for d in dupes],
                    "hint": "같은 이름의 살아있는 행이 둘 이상이다 — 어느 쪽인지 "
                            "확정되기 전에는 배달하지 않는다. `am org --name <그 이름> "
                            "--rename-to <새 이름>` 으로 한쪽을 개명해라."}
    thread = body.get("thread") or new_id("t")
    sender = verified_sender(body["from_session"], body.get("from_agent"))
    # 🔴 자기 자신에게 보내는 것을 막는다. CC 네이티브도 self-target 을 거부한다.
    # 실측 피해: 매니저의 전문 재전송(m-30c64e5e)이 자기 앞으로 라우팅돼 아무도 못 본 채
    # expired 로 끝났다 — 발신자는 보냈다고 믿고 수신자는 영영 못 받는, 최악의 무음 유실.
    # (reply 의 스레드 후속 라우팅은 따로 고쳤지만, 근본 가드가 없어 다른 경로로 재발했다.)
    if to_agent and to_agent == sender:
        return {"ok": False, "error": "self-target",
                "hint": f"'{to_agent}' 는 너 자신이다. 수신자를 다시 확인해라 "
                        f"(서브에이전트는 부모 세션 이름으로 해석된다)."}
    # decide/broadcast 는 배달이 아니라 기록 — 즉시 종결 (TTL 스팸 방지)
    record_only = to_agent == "broadcast" or body.get("type") == "decide"
    # review 는 배달 금지 — fork 부활 전용 (설계 §5, 리뷰 파일럿에서 이중 배달 실측)
    initial_state = "acknowledged" if record_only else (
        "escalated" if body.get("type") == "review" else "queued")
    mid = insert_message(
        thread=thread, from_agent=sender, from_session=body["from_session"],
        to_agent=to_agent or "broadcast", mtype=body.get("type", "consult"),
        priority=body.get("priority", "normal"), body=body.get("body", ""),
        refs=json.dumps(body.get("refs", {})),
        state=initial_state,
        meta={"revive_confirm": bool(body.get("revive_confirm"))},
        ttl_s=body.get("ttl_s", DEFAULT_TTL_S), reply_to=body.get("reply_to"))
    if record_only:
        return {"ok": True, "id": mid, "thread": thread, "ticket": None}
    ticket = None
    if body.get("priority") == "blocking":
        ticket = new_id("tk")
        db().execute("INSERT INTO tickets VALUES(?,?,?,?,?)",
                     (ticket, mid, body["from_session"], "open", now()))
        # TTL 만료 타이머 (미배달 → 발신자 notice, 설계 §4)
        db().execute("INSERT INTO timers(id,kind,msg_id,due_at) VALUES(?,?,?,?)",
                     (new_id("tm"), "ttl", mid, now() + body.get("ttl_s", DEFAULT_TTL_S)))
        # 디스패치 (설계 §5): dormant → 즉시 부활 잡, live → 메시지 단위 debounce
        recipient = db().execute(
            "SELECT * FROM agents WHERE name=? ORDER BY registered_at DESC LIMIT 1",
            (to_agent,)).fetchone()
        rec_state = recipient["state"] if recipient else "lost"
        # 🔴 '살아 있다'와 '닿는다'는 다른 축이다. codex 팀원을 dormant 로 박아 두던
        # 시절엔 모든 배달이 부활 경로로 갔고 — 비쌌지만 **닿았다**. 생사를 실제로
        # 재게 고치자 그들이 live 가 되면서 웨이크 경로로 옮겨졌는데, 초인종 주소가
        # 없는 기존 세션은 그 경로에 채널이 없어 **통째로 만료**됐다(팀B장 실측:
        # 아침까지 오가던 왕복이 배포 직후 4건 연속 만료). 내 수선이 만든 회귀다.
        # 닿을 채널이 없으면 살아 있어도 부활 경로로 보낸다 — 비싼 게 유실보다 낫다.
        is_codex = recipient is not None and (recipient["cli"] or "") == "codex"
        # 🔴 **살아 있는 codex 는 절대 부활 경로로 보내지 않는다.** 부활은 fork 라
        # 산 세션 옆에 사본을 하나 더 만든다 — 팀장이 $9.61 청구를 거부한 판단이
        # 옳았던 그 자리다. live codex 의 배달 경로는 초인종 하나뿐이고, 그게 안
        # 되면 고칠 곳은 초인종이지 결제가 아니다.
        # (09-04 에 내가 넣은 unreachable→부활 폴백은 여기서 철회한다. 그 폴백이
        #  「채널 없음」을 「유료 부활 필요」로 바꿔 놨고, 예산이 소진되면 메시지가
        #  통째로 죽었다 — 팀C 팀장 실측.)
        codex_live = is_codex and rec_state.startswith("live")
        if body.get("type") == "review" or (
                rec_state in ("dormant", "lost") and not codex_live):
            # review 는 모든 상태에서 부활 경로. lost 판정·통지는 워커 revive 가 수행
            db().execute("INSERT INTO timers(id,kind,msg_id,due_at,fired) VALUES(?,?,?,?,2)",
                         (new_id("tm"), "revive-now", mid, now()))
        else:
            db().execute("INSERT INTO timers(id,kind,msg_id,due_at) VALUES(?,?,?,?)",
                         (new_id("tm"), "debounce", mid, now() + DEBOUNCE_S))
    return {"ok": True, "id": mid, "thread": thread, "ticket": ticket}


def h_reply(body, _q):
    """reply 적재 + 티켓 해소 + lease/supersede 판정 (설계 §5)."""
    orig = db().execute("SELECT * FROM messages WHERE id=?",
                        (body["reply_to"],)).fetchone()
    if not orig:
        return {"ok": False, "error": "unknown-message"}
    # reply 경로도 발신 인가 대칭 적용 (저자 fork 리뷰 R2) — 워커 발신만 예외
    if body.get("from_session") != "__worker__":
        known = db().execute("SELECT 1 FROM agents WHERE session=?",
                             (body.get("from_session", ""),)).fetchone()
        if not known:
            return {"ok": False, "error": "unregistered-sender"}
    meta = json.loads(body.get("meta", "{}")) if isinstance(body.get("meta"), str) \
        else body.get("meta", {})
    sender = body.get("from_agent", "") if body.get("from_session") == "__worker__" \
        else verified_sender(body.get("from_session", ""), body.get("from_agent"))
    supersedes = None
    if orig["state"] == "answered" and sender == orig["to_agent"]:
        # supersede 는 "원 수신자(저자)의 늦은 답변"에만 (설계 §5).
        # 조건 없던 시절 발신자 자신의 후속 재전송이 남의 답변을 supersede 처리한 실사고.
        # 🔴 supersede 대상은 '먼저 나간 부활 응답'뿐이다. 조건 없이 최신 답장을 집던
        # 동안, 원 발신자 **자신의 글**을 가리키는 공지가 나갔다(실측 t-a9aac3f0:
        # m-957a8744 는 5818fe42 자신의 reply 인데 그걸 supersede 했다고 통지).
        # 받은 쪽은 "내 글이 왜 정정 대상이지"에서 숨은 조건을 의심했고, 한 세션이
        # 그 때문에 전송 계약층 머지를 보류하는 실비용이 났다.
        # 🔴 supersede 의 의미는 "**부활 사본**이 먼저 답했는데 본체가 늦게 답했다" 하나뿐이다.
        # 그냥 '같은 저자의 이전 답장'을 집으면, 한 세션이 스레드에 두 번 답하기만 해도
        # "저자 본체의 늦은 답변이 도착" 이라는 거짓 공지가 나간다 — 실측 2회, 그중 한 번은
        # 받은 쪽이 숨은 조건을 의심해 머지를 보류하는 실비용까지 냈다(부활은 없었다).
        # 부활 응답의 식별 축: 워커가 대리 게시하므로 from_session='__worker__' 이고
        # meta 에 responder_session 이 실린다.
        prev = db().execute(
            "SELECT id FROM messages WHERE reply_to=? AND from_agent=? "
            "AND (from_session='__worker__' "
            "     OR COALESCE(meta,'') LIKE '%responder_session%') "
            "ORDER BY created DESC LIMIT 1",
            (orig["id"], sender)).fetchone()
        supersedes = prev["id"] if prev else None
        if not supersedes:
            metric("reply.supersede_skipped", 1, orig["id"])
        metric("reply.supersede", 1, orig["id"])
    meta["supersedes"] = supersedes
    # 라우팅: 스레드의 "상대방"에게. 발신자가 원 메시지 발신자 본인이면(자기 스레드 후속)
    # 수신자는 원 수신자다 — 기계적 orig.from_agent 라우팅이 자기 자신에게 되돌아가
    # 22분간 미배달된 실사고(전문 재전송 유실)를 반영.
    recipient = orig["to_agent"] if (sender == orig["from_agent"]
                                     and body.get("from_session") != "__worker__") \
        else orig["from_agent"]
    mid = insert_message(
        thread=orig["thread"], from_agent=sender,
        from_session=body.get("from_session", ""), to_agent=recipient,
        mtype="reply", priority="normal", body=body.get("body", ""),
        meta=meta, reply_to=orig["id"], body_cap=4000)
    db().execute("UPDATE messages SET state='answered' WHERE id=?", (orig["id"],))
    db().execute("UPDATE tickets SET status='answered' WHERE msg_id=?", (orig["id"],))
    if supersedes:
        notice(orig["from_agent"],
               f"정정: {orig['id']} 에 저자 본체의 늦은 답변이 도착 "
               f"(supersedes {supersedes}) — 전문은 `am read {orig['thread']}`",
               thread=orig["thread"])
    metric("reply.ok", 1, orig["id"])
    return {"ok": True, "id": mid, "supersedes": supersedes}


def h_defer(body, _q):
    """defer = 지금 답 못 함. 재배달 예약 + 발신자 사실 통지 (조용한 소멸 금지)."""
    row = db().execute("SELECT * FROM messages WHERE id=?", (body["id"],)).fetchone()
    if not row:
        return {"ok": False, "error": "unknown-message"}
    if row["priority"] == "fyi":
        # fyi 를 30분 뒤 다시 보여줄 이유가 없다(답할 의무가 없는 등급이다).
        # 수신자에게 '정지 수단'이 되어야 하므로 여기서 종결시킨다 — 실측 신고:
        # 회신 금지 + defer 가 재배달을 예약 ⇒ 수신자가 멈출 방법이 없었다.
        db().execute("UPDATE messages SET state='delivered' WHERE id=?", (body["id"],))
        metric("defer.fyi_closed", 1, body["id"])
        return {"ok": True, "closed": True,
                "note": "fyi 라 재배달 없이 종결했다(다시 오지 않는다)."}
    db().execute("UPDATE messages SET state='deferred' WHERE id=?", (body["id"],))
    db().execute("INSERT INTO timers(id,kind,msg_id,due_at) VALUES(?,?,?,?)",
                 (new_id("tm"), "redeliver", body["id"], now() + 1800))
    # defer 는 마감을 미루는 행위다 — TTL 을 재배달 시점 뒤로 밀지 않으면
    # created+3600 이 재배달(+1800)을 앞질러 "미루기"가 곧 "만료"가 된다.
    db().execute("UPDATE messages SET ttl_s = MAX(ttl_s, ? - created) WHERE id=?",
                 (now() + 1800 + DEFAULT_TTL_S, body["id"]))
    notice(row["from_agent"], f"저자가 미룸(defer): {row['id']} — 30분 후 재배달 예약",
           thread=row["thread"])
    metric("defer", 1, body["id"])
    return {"ok": True}


def h_inbox(_body, q):
    """수신자용: 배달 대상 요약. check=1 이면 1줄 요약."""
    session = q.get("session", [""])[0]
    row = db().execute("SELECT name, role, team, reports_to FROM agents "
                       "WHERE session=?", (session,)).fetchone()
    if not row:
        return {"items": []}
    rows = db().execute(
        "SELECT * FROM messages WHERE to_agent=? AND state IN ('queued','injected') "
        "AND type != 'reply' ORDER BY cursor", (row["name"],)).fetchall()
    items = [{"id": r["id"], "thread": r["thread"], "from": r["from_agent"],
              "type": r["type"], "priority": r["priority"], "body": r["body"]}
             for r in rows]
    # 수신자 위치를 함께 준다 — 봉투가 매 배달마다 '너는 누구인가'를 알린다
    return {"items": items,
            "me": {"name": row["name"], "role": row["role"],
                   "team": row["team"], "reports_to": row["reports_to"]}}


# 수신 세션이 '받지 않았다'고 알려온 형상 (peer_message_status 영수증).
# 전부 미배달이므로 메시지 상태는 queued 그대로 둔다 — 훅 주입·부활 폴백이 살아야 한다.
PEER_REJECT_STATES = ("held", "denied", "expired", "refused", "dropped")


def h_ack(body, _q):
    """워커 배달 상태 회신: queued→injected→acknowledged. lease 는 injected 시점 부여.

    via = 'uds'(유휴 웨이크) | 'hook'(훅 주입).
    상태를 바꾸지 않는 회신이 둘 있다 —
      - wake_failed : 웨이크 레인 하나가 안 됐을 뿐. 메시지는 queued 로 남는다.
      - held/denied/expired/refused/dropped : 수신 세션이 **받지 않았다**고 회신한 것.
        특히 held 는 사람 승인 대기다. 이걸 injected 로 찍던 시절 배달 회계가 거짓
        양성이었다(worker 는 wake.ok, relay 는 injected — 실제로는 아무도 못 봄).
    injected 는 근거(evidence)와 함께 기록한다: 'receipt-delivered'(영수증 확증) 또는
    'assumed:...'(부정 영수증 부재 — accept 경로엔 영수증이 아예 없다, 실측).
    """
    st = body["state"]
    via = str(body.get("via", ""))[:16]
    mid = body["id"]
    detail = str(body.get("detail", ""))[:120]
    if st == "wake_activity":
        # 🔴 약한 증거(활동)로 깨운 건. 상태는 안 바꾸되 **워커에게 현재 상태를
        # 돌려준다** — 이게 없어서 워커가 이미 answered 된 메시지를 100초 간격으로
        # 무한 재배달했다(실측: 같은 봉투 3회). relay 가 invalid-state 로 거부만 하고
        # 아무것도 안 알려주니 워커가 배울 방법이 없었다.
        row = db().execute("SELECT state FROM messages WHERE id=?", (mid,)).fetchone()
        cur = row["state"] if row else "gone"
        db().execute("UPDATE messages SET wake_status=? WHERE id=? AND state='queued'",
                     (f"{st}:{detail}"[:120], mid))
        metric("wake.activity", 1, f"{mid} {detail[:80]}")
        return {"ok": True, "state_changed": False, "delivered": False,
                "current_state": cur,
                # 종착 상태면 워커가 캐시에서 버려야 한다(재배달 루프 차단)
                "terminal": cur in ("answered", "acknowledged", "expired",
                                    "delivered", "deferred", "gone")}
    if st in ("wake_failed", "wake_unconfirmed") or st in PEER_REJECT_STATES:
        # 전부 '안 갔다'는 회신이다. 메시지 상태는 건드리지 않는다 —
        # queued 로 남아야 훅 주입·부활 폴백이 그대로 집어간다.
        # 단 wake_status(배달 근거)는 **아직 미배달인 건에만** 쓴다. 조건 없이 쓰던 동안
        # 다른 레인(훅)으로 이미 배달된 메시지의 근거를 늦게 온 웨이크 실패가 덮어써
        # 감사 기록이 거짓말을 했다(운영 데이터 실측: injected 인데 wake_unconfirmed).
        db().execute("UPDATE messages SET wake_status=? "
                     "WHERE id=? AND state='queued'", (f"{st}:{detail}"[:120], mid))
        key = {"wake_failed": "wake.fail",
               "wake_unconfirmed": "wake.unconfirmed"}.get(st, f"wake.{st}")
        metric(key, 1, f"{mid} {detail[:80]}")
        # 🔴 실패·미확인 회신에도 현재 상태를 돌려준다. 이걸 wake_activity 에만 주던
        # 동안, 수신자가 reply·defer 를 마쳐 relay 는 종착으로 아는 메시지를 워커가
        # 5초마다 계속 밀었다(수신자 실측: 1시간+ 매분 재주입).
        cur = (db().execute("SELECT state FROM messages WHERE id=?",
                            (mid,)).fetchone() or {"state": "gone"})["state"]
        return {"ok": True, "state_changed": False, "delivered": False,
                "current_state": cur,
                "terminal": cur in ("answered", "acknowledged", "expired",
                                    "delivered", "deferred", "gone")}
    if st not in ("injected", "acknowledged", "inject_failed"):
        return {"ok": False, "error": "invalid-state"}   # answered 위조 차단
    row = db().execute("SELECT * FROM messages WHERE id=?", (mid,)).fetchone()
    if not row:
        return {"ok": False}
    changed = True
    if st == "injected":
        # injected_at 은 '주입 시각' 정본. 재큐 판정을 created 로 하던 시절
        # 배달된 메시지가 10분 뒤 무조건 queued 로 되돌려져 좀비가 됐다.
        # 훅 레인(via 없음)도 같은 문을 타므로 두 레인 모두 스탬프가 남는다 —
        # 스탬프가 없으면 관측도 못 하고 _sweep_requeue 대상에서도 빠진다.
        evidence = str(body.get("evidence", "") or ("hook" if via != "uds" else
                                                    "assumed:legacy"))[:120]
        # 🔴 fyi 는 **1회 배달로 끝난다**. 'injected' 는 종착이 아니라서 워커가 항목을
        # 캐시에 계속 들고 재웨이크하는데, fyi 는 회신 금지라 answered 로 갈 일이 없고
        # 재큐 대상도 아니다 ⇒ 영원히 갇힌다. 실측 신고: 한 세션에 **1분 간격 60회+**,
        # DB 에 injected 상태 fyi 914건. '답할 의무 없음'이 정의인데 재노출할 근거도 없다.
        landing = "delivered" if row["priority"] == "fyi" else st
        changed = bool(db().execute(
            "UPDATE messages SET state=?, injected_at=?, wake_status=?, "
            "inject_count=COALESCE(inject_count,0)+1 "
            "WHERE id=? AND state='queued'",
            (landing, now(), evidence, mid)).rowcount)
        metric(f"inject.ok.{via or 'hook'}", 1, f"{mid} {evidence}")
    else:
        db().execute("UPDATE messages SET state=? WHERE id=?", (st, mid))
    # 🪤 row 는 UPDATE **이전** 스냅샷이다. 상태 전이 여부를 row 로 판정하면 중복
    # injected ack(훅+웨이크 동시 도착, defer→재배달 왕복)이 매번 리스 타이머를 새로
    # 꽂는다 — 실측: ack 4회(실전이 2회)에 lease 타이머 4개. 타이머가 쌓이면 같은
    # 메시지가 여러 번 부활 승격 후보가 된다. 전이가 실제로 일어난 경우에만,
    # 그리고 아직 발화 안 한 리스 타이머가 없을 때만 꽂는다(debounce 와 같은 규칙).
    if st == "injected" and row["priority"] == "blocking" and changed:
        # 주입 성공 = 본체가 lease 선점 (설계 §5). debounce 는 취소.
        db().execute("UPDATE messages SET lease_holder=?, lease_expires=? WHERE id=?",
                     (row["to_agent"], now() + LEASE_S, mid))
        db().execute("UPDATE timers SET fired=1 WHERE msg_id=? AND kind='debounce'",
                     (mid,))
        pending_lease = db().execute(
            "SELECT 1 FROM timers WHERE msg_id=? AND kind='lease' AND fired=0",
            (mid,)).fetchone()
        if not pending_lease:
            db().execute("INSERT INTO timers(id,kind,msg_id,due_at) VALUES(?,?,?,?)",
                         (new_id("tm"), "lease", mid, now() + LEASE_S))
    if st == "inject_failed" and row["priority"] == "blocking":
        # 메시지 단위 debounce 타이머 (설계 §5 단일화 규칙)
        existing = db().execute(
            "SELECT 1 FROM timers WHERE msg_id=? AND kind='debounce'", (mid,)).fetchone()
        if not existing:
            db().execute("INSERT INTO timers(id,kind,msg_id,due_at) VALUES(?,?,?,?)",
                         (new_id("tm"), "debounce", mid, now() + DEBOUNCE_S))
    return {"ok": True, "state_changed": changed}


def h_wait(_body, q):
    """발신자의 재진입 폴 (설계 §3). 최대 for 초 대기."""
    ticket = q.get("ticket", [""])[0]
    wait_for = min(int(q.get("for", ["50"])[0]), 55)
    deadline = now() + wait_for
    while now() < deadline:
        row = db().execute("SELECT * FROM tickets WHERE id=?", (ticket,)).fetchone()
        if not row:
            return {"status": "unknown-ticket"}
        if row["status"] == "cancelled":
            return {"status": "cancelled"}
        if row["status"] == "answered":
            reply = db().execute(
                "SELECT * FROM messages WHERE reply_to=? AND type='reply' "
                "ORDER BY created DESC LIMIT 1", (row["msg_id"],)).fetchone()
            if reply:
                return {"status": "answered", "body": reply["body"],
                        "meta": json.loads(reply["meta"] or "{}"),
                        "thread": reply["thread"]}
        # 사이클·예산·author-lost notice 는 대기 반환값으로 — 같은 스레드 것만
        # (아무 notice 나 삼키면 다른 스레드의 통지를 소비해버린다)
        orig = db().execute("SELECT from_agent, created, thread FROM messages "
                            "WHERE id=?", (row["msg_id"],)).fetchone()
        if orig:
            note = db().execute(
                "SELECT * FROM messages WHERE to_agent=? AND type='notice' "
                "AND state='queued' AND created>=? AND thread=? "
                "ORDER BY created LIMIT 1",
                (orig["from_agent"], orig["created"], orig["thread"])).fetchone()
            if note:
                db().execute("UPDATE messages SET state='acknowledged' WHERE id=?",
                             (note["id"],))
                db().commit()
                return {"status": "notice", "body": note["body"]}
        time.sleep(1.0)
    return {"status": "pending", "hint": f"am wait {ticket} 로 재진입"}


def h_cancel(body, _q):
    db().execute("UPDATE tickets SET status='cancelled' WHERE id=?", (body["ticket"],))
    metric("ticket.cancel", 1, body["ticket"])
    return {"ok": True}


def h_poll(_body, q):
    """워커 long-poll: 배달할 메시지 + 부활 잡. 커서 기반 (설계 §2-2)."""
    home = q.get("home", ["local"])[0]
    cursor = int(q.get("cursor", ["0"])[0])
    wait_for = min(int(q.get("wait", ["25"])[0]), 55)
    deadline = now() + wait_for
    while now() < deadline:
        # 🪤 JOIN agents ON to_agent=name 은 동명 세션 수만큼 같은 메시지를 복제한다
        # (실측: agents 2행·messages 1건 → deliveries 2건). 워커는 이걸 세션마다 캐시에
        # 넣으므로 같은 봉투가 두 번 주입되고, ack 도 두 번 간다. 이름은 조회 축일 뿐
        # 배달 단위가 아니다 — 존재 검사(IN)로 바꿔 팬아웃 자체를 없앤다.
        # 어느 세션에 꽂을지는 워커가 /agent(최신 registered_at 1행)로 따로 해석한다.
        # sender_mode: 발신 **세션**의 권한 계급. 웨이크 봉투의 from-mode attest 원천이다
        # (수신자가 bypass 계급이면 attest 없이는 CC 가 무조건 hold — 번들 게이트 실측).
        # 워커가 세션마다 되묻지 않도록 여기서 조인해 실어 보낸다. __relay__ 등 세션이
        # 없는 발신자는 NULL 이고, 그러면 워커가 attest 를 생략한다(과대 주장 금지).
        deliveries = [dict(r) for r in db().execute(
            "SELECT m.*, a.permission_mode AS sender_mode FROM messages m "
            "LEFT JOIN agents a ON a.session = m.from_session "
            "WHERE m.state='queued' AND m.cursor>? "
            "AND m.to_agent IN (SELECT name FROM agents WHERE home=? AND name IS NOT NULL "
            "AND name != '') ORDER BY m.cursor",
            (cursor, home)).fetchall()]
        jobs = [dict(r) for r in db().execute(
            "SELECT * FROM timers WHERE fired=2 ORDER BY due_at").fetchall()]
        if deliveries or jobs:
            for j in jobs:
                db().execute("UPDATE timers SET fired=3 WHERE id=?", (j["id"],))
            return {"deliveries": deliveries, "revive_jobs": jobs}
        time.sleep(1.0)
    return {"deliveries": [], "revive_jobs": []}



ROLE_ORDER = {"chairman": 0, "secretary": 1, "lead": 2, "member": 3, "": 9}


def h_board(_body, q):
    """조직도 + 각자 현재 작업 + 라인 밖 발신 집계 (am board).

    강제는 없다(사용자 결정) — 대신 **보이게** 한다. 라인을 건너뛴 발신은 막지 않고
    세되, 팀장이 자기 팀의 흐름을 한 화면에서 읽을 수 있어야 규약이 규약으로 산다.
    기술 질의(consult --owner-of)는 경계를 넘는 게 정상이라 라인 밖으로 세지 않는다.
    """
    since = now() - float(q.get("since_h", ["48"])[0]) * 3600
    rows = [dict(r) for r in db().execute(
        "SELECT name, role, team, reports_to, cmux_title, state, task, recent_prompt, "
        "COALESCE(task_explicit,0) AS task_explicit, "
        "CASE WHEN last_activity IS NULL THEN NULL "
        "ELSE CAST(? - last_activity AS INTEGER) END AS idle_s "
        "FROM agents WHERE name != '' AND COALESCE(ephemeral,0)=0 "
        "AND state LIKE 'live-%'", (now(),)).fetchall()]
    line = {r["name"]: (r.get("reports_to") or "") for r in rows}
    traffic = {}
    for r in db().execute(
            "SELECT from_agent, to_agent, COUNT(*) n, "
            # 저자 질의(--owner-of)는 경계를 넘는 게 정상이라 라인 밖으로 세지 않는다
            "SUM(CASE WHEN COALESCE(refs,'') LIKE '%owner_of%' "
            "       OR COALESCE(meta,'') LIKE '%owner_of%' THEN 1 ELSE 0 END) consults "
            "FROM messages WHERE created > ? AND to_agent != 'broadcast' "
            "AND from_agent NOT IN ('__relay__','__worker__') "
            "GROUP BY from_agent, to_agent", (since,)):
        traffic[(r["from_agent"], r["to_agent"])] = (r["n"], r["consults"])
    for r in rows:
        sent = off = 0
        for (f, t), (n, cons) in traffic.items():
            if f != r["name"]:
                continue
            sent += n
            # 라인 안 = 내 보고선 / 내게 보고하는 사람 / 같은 팀
            same_team = any(x["name"] == t and x.get("team") == r.get("team")
                            for x in rows)
            in_line = (t == line.get(r["name"]) or line.get(t) == r["name"]
                       or same_team)
            if not in_line:
                off += n - cons          # 저자 질의는 라인 밖으로 안 센다
        r["sent"], r["off_line"] = sent, max(off, 0)
        r["received"] = sum(n for (f, t), (n, _) in traffic.items() if t == r["name"])
    rows.sort(key=lambda r: (ROLE_ORDER.get(r.get("role") or "", 9),
                             r.get("team") or "~", r["name"]))
    return {"org": rows, "since_h": float(q.get("since_h", ["48"])[0])}


VALID_ROLES = ("chairman", "secretary", "lead", "member")


def h_org(body, _q):
    """조직 배정 — 이름으로 지목해 role/team/reports_to 를 세운다 (am org set).

    자기 자신만 등록할 수 있게 하면 조직도를 그릴 사람이 없다. 강제가 없는 모델이라
    (사용자 결정) 이건 신원 통제가 아니라 **라벨 관리**다 — 잘못 붙으면 눈에 보이고
    누구든 고칠 수 있다. 대신 값은 검증한다: 역할 오타가 조용히 들어가면 조직도가
    거짓말을 하고, 그건 오늘 내내 고쳐 온 종류의 결함이다.
    """
    name = (body.get("name") or "").strip()
    # session 으로 지목하면 이름은 없어도 된다 — 이름이 겹쳐서 개명하려는 상황이라
    # 이름을 요구하는 건 순환이다(모호한 이름으로만 모호성을 풀라는 뜻이 된다).
    if not name and not (body.get("session") or "").strip():
        return {"ok": False, "error": "name-or-session-required"}
    role = (body.get("role") or "").strip()
    if role and role not in VALID_ROLES:
        return {"ok": False, "error": "bad-role",
                "hint": f"role 은 {'|'.join(VALID_ROLES)} 중 하나"}
    # 🔴 이름이 여럿이면 여기서도 조용히 하나를 고르면 안 된다 — h_send 와 같은 이유고,
    # 더 나쁘다: 개명은 **모호성을 푸는 도구**인데 그 도구가 어느 쪽을 고쳤는지 모르면
    # 모호성이 그대로 남는다. session 을 주면 그걸로 정확히 지목한다(탈출구).
    sess = (body.get("session") or "").strip()
    if sess:
        row = db().execute("SELECT session FROM agents WHERE session=?",
                           (sess,)).fetchone()
        if not row:
            return {"ok": False, "error": "unknown-session", "session": sess}
        cur = db().execute("SELECT name FROM agents WHERE session=?",
                           (sess,)).fetchone()
        name = cur["name"] or name
    else:
        cands = db().execute(
            "SELECT session, cli, task FROM agents WHERE name=? AND state != 'lost' "
            "ORDER BY registered_at DESC", (name,)).fetchall()
        if len(cands) > 1:
            return {"ok": False, "error": "ambiguous-agent", "name": name,
                    "candidates": [{"session": c["session"], "cli": c["cli"],
                                    "task": (c["task"] or "")[:60]} for c in cands],
                    "hint": "이 이름의 행이 둘 이상이다 — 어느 쪽인지 확정해야 한다. "
                            "`am org --session <위 session> --rename-to <새 이름>` 으로 "
                            "한쪽씩 지목해라."}
        row = cands[0] if cands else None
    if not row:
        return {"ok": False, "error": "unknown-agent", "name": name}
    # 개명 — codex 채용에 필요하다. 스캐너가 지은 codex-<id8> 를 채용자가 정한 이름으로
    # 바꿔야 지목·보고선이 사람이 읽는 이름으로 선다(claude 는 AM_NAME 주입으로 처음부터
    # 원하는 이름이라 이 경로가 필요 없다). 살아있는 남의 이름은 뺏지 못한다.
    new_name = (body.get("rename_to") or "").strip()
    if new_name:
        if _name_is_squatted(new_name, row["session"]):
            return {"ok": False, "error": "name-taken", "name": new_name,
                    "hint": "살아있는 다른 세션이 그 이름을 쓰고 있다 — 선점자 우선."}
        db().execute("UPDATE agents SET name=? WHERE session=?",
                     (new_name, row["session"]))
        metric("org.rename", 1, f"{name} -> {new_name}")
        name = new_name
    sets, vals = [], []
    for col in ("role", "team", "reports_to", "cmux_surface", "cmux_workspace"):
        v = (body.get(col) or "").strip()
        if v:
            sets.append(f"{col}=?")
            vals.append(v)
    if not sets:
        if new_name:
            cur = db().execute("SELECT name, role, team, reports_to FROM agents "
                               "WHERE session=?", (row["session"],)).fetchone()
            return {"ok": True, "agent": dict(cur), "renamed": True}
        return {"ok": False, "error": "nothing-to-set"}
    vals.append(row["session"])
    db().execute(f"UPDATE agents SET {', '.join(sets)} WHERE session=?", vals)
    metric("org.set", 1, f"{name} {body.get('role','')}/{body.get('team','')}")
    cur = db().execute("SELECT name, role, team, reports_to FROM agents "
                       "WHERE session=?", (row["session"],)).fetchone()
    return {"ok": True, "agent": dict(cur)}

def _gate_notice_once(msg_id, sender, text, thread, reason):
    """같은 미배달 건에 같은 사유의 게이트 통지는 **평생 한 번만** 낸다.

    🔴 「승인이 필요하다」는 **상태**지 사건이 아니다. 그런데 게이트는 워커가
    재시도할 때마다 호출되고, 그때마다 같은 문구를 새 메시지로 발행했다 —
    새 정보가 0인데 발신자 세션을 매번 깨운다(실측: 한 팀장이 오늘 이걸로 5회+
    깨어났다. 돈은 안 나갔고 나간 건 그의 토큰이다).
    승인이 오면 게이트가 통과하므로 이 표식은 자연히 무의미해진다.
    """
    cur = db().execute("SELECT gate_notice FROM messages WHERE id=?",
                       (msg_id,)).fetchone()
    if cur and (cur["gate_notice"] or "") == reason:
        return False
    notice(sender, text, thread=thread)
    db().execute("UPDATE messages SET gate_notice=? WHERE id=?", (reason, msg_id))
    return True


def h_gate(body, _q):
    """부활 사전 예산 게이트 (설계 §6). 워커가 스폰 직전 호출."""
    est = float(body["est_usd"])
    sender = body["sender"]
    msg_id = body.get("msg_id", "")
    msg = db().execute("SELECT thread, meta FROM messages WHERE id=?",
                       (msg_id,)).fetchone()
    thread = msg["thread"] if msg else None
    confirm = bool(json.loads(msg["meta"] or "{}").get("revive_confirm")) if msg else False
    # 티켓 생존 확인 (설계 §3 — 소비자 없는 지출 차단)
    t = db().execute("SELECT status FROM tickets WHERE msg_id=?", (msg_id,)).fetchone()
    if t and t["status"] in ("cancelled", "answered"):
        return {"allow": False, "reason": f"ticket-{t['status']}"}
    # 회장 지시(09-08): **codex 는 부활 예산을 신경쓰지 않는다.** 예산 게이트가
    # codex 수신자 앞 메시지를 통째로 죽이는 일이 실제로 있었다(팀C 팀장 실측:
    # 같은 지시를 두 번 보냈고 두 번 다 최종 상태 queued 로 만료). 예산은 유료 fork
    # 를 통제하는 장치지 팀 운영을 멈추는 장치가 아니다.
    to_row = db().execute(
        "SELECT cli FROM agents WHERE name=(SELECT to_agent FROM messages WHERE id=?) "
        "ORDER BY registered_at DESC LIMIT 1", (msg_id,)).fetchone()
    if to_row and (to_row["cli"] or "") == "codex":
        return {"allow": True, "budget_exempt": "codex"}
    if spent(f"sender:{sender}") + est > SENDER_DAILY_USD:
        _gate_notice_once(msg_id, sender,
                          f"부활 중단: 발신자 일일 예산 ${SENDER_DAILY_USD} 초과 예상",
                          thread, "sender-daily-budget")
        return {"allow": False, "reason": "sender-daily-budget"}
    if spent("global") + est > GLOBAL_DAILY_USD:
        _gate_notice_once(msg_id, sender,
                          f"부활 중단: 전역 일일 예산 ${GLOBAL_DAILY_USD} 초과 예상",
                          thread, "global-daily-budget")
        return {"allow": False, "reason": "global-daily-budget"}
    if est > AUTO_GATE_USD and not confirm:
        _gate_notice_once(
            msg_id, sender,
            f"부활 예상 ${est:.2f} > ${AUTO_GATE_USD} — 승인 대기. 되살리려면 "
            f"`--revive-confirm` 으로 다시 보내라. 안 되살릴 거면 "
            f"`am wait --cancel <ticket>` 으로 티켓을 닫아라 — 열린 티켓이 "
            f"재시도를 계속 먹인다. (이 통지는 건당 한 번만 나간다)",
            thread, "needs-confirm")
        return {"allow": False, "reason": "needs-confirm", "est_usd": est}
    return {"allow": True}


def h_spend(body, _q):
    add_spend(f"sender:{body['sender']}", float(body["usd"]))
    add_spend("global", float(body["usd"]))
    metric("revive.spend", float(body["usd"]), body.get("msg_id", ""))
    return {"ok": True}


def h_read(_body, q):
    thread = q.get("thread", [""])[0]
    rows = db().execute("SELECT * FROM messages WHERE thread=? ORDER BY cursor",
                        (thread,)).fetchall()
    return {"messages": [dict(r) for r in rows]}


def h_message(_body, q):
    """워커용 단건 조회 — 워커는 relay.db 를 직접 열지 않는다 (멀티머신)."""
    row = db().execute("SELECT * FROM messages WHERE id=?",
                       (q.get("id", [""])[0],)).fetchone()
    return {"message": dict(row) if row else None}


def h_agent(_body, q):
    # 🔴 SELECT * 금지: msg_socket(=주입 주소)이 조회 응답으로 새면 어떤 에이전트든
    # 남의 세션 주입 주소를 읽을 수 있다. 컬럼을 명시 투영한다 — 주소는
    # /agent-by-session(워커 전용) 한 곳에서만 나간다.
    row = db().execute(
        "SELECT name, session, cli, home, repo, cwd, task, paths, design, model, "
        "state, registered_at, last_seen, session_end_commit FROM agents "
        "WHERE name=? ORDER BY registered_at DESC LIMIT 1",
        (q.get("name", [""])[0],)).fetchone()
    return {"agent": dict(row) if row else None}


def h_agent_by_session(_body, q):
    """워커 전용 세션 단건 조회 — 웨이크 소켓 해석용.

    msg_socket 은 여기서만 나간다. /agents·/who 같은 조망용 응답에는 절대 싣지 않는다
    (에이전트가 읽는 목록에 다른 세션의 주입 주소를 뿌리지 않기 위함).
    """
    row = db().execute("SELECT * FROM agents WHERE session=?",
                       (q.get("session", [""])[0],)).fetchone()
    return {"agent": dict(row) if row else None}


MAX_REVIVE_ATTEMPTS = 2   # 메시지당 부활 시도 상한 — 재발화 무한 루프·영구 과금 차단
MAX_INJECT_ATTEMPTS = 2   # 메시지당 재큐 상한 — injected↔queued 무한 왕복(좀비) 차단
STALE_AGENT_S = 600       # live 보고가 이만큼 끊기면 강등 후보 (증거는 별도 요구)
LIVENESS_FRESH_S = 180    # 워커의 마지막 '성공한 열거'가 이 안이어야 강등 근거가 된다
HARD_STALE_S = 24 * 3600  # 관측자 없는 홈의 백스톱 — 하루면 어차피 죽은 세션이다
REQUEUE_AFTER_S = 600     # 주입 후 이만큼 무응답이면 배달 유실로 보고 재큐


def _sweep_requeue(conn):
    """injected 인 채 응답 없는 normal/fyi 재큐 (배달 유실 복구).

    세 가지가 동시에 맞아야 한다:
      - 기준은 created 가 아니라 injected_at. created 기준이던 시절, 방금 배달된
        메시지가 '생성 10분 경과'만으로 queued 로 되돌려져 좀비가 됐다.
      - 재큐 상한. 없으면 injected↔queued 를 영원히 왕복한다.
      - cursor 재발급. h_poll 이 cursor > ? 로 긁으므로, 워커 커서가 이미 지나간
        메시지는 재큐해도 영원히 안 나온다 (워커 재시작 전까지 복구 불가였다).

    재큐 대상은 '응답을 기다리는' 메시지뿐이다. notice·reply 는 종착지라 응답이 올 리
    없으므로 재큐하면 같은 내용을 수신자에게 반복 주입하는 소음이 된다 — 실측: 배포
    직후 재큐 5건이 전부 notice 였다(inject_count 2까지 재주입).
    """
    # 🪤 커서는 **행마다** 새로 발급해야 한다. UPDATE … SET cursor=(SELECT MAX(cursor)+1
    # FROM messages) 는 비상관 서브쿼리라 문 단위로 한 번만 평가된다 — 재큐 대상 전부가
    # 같은 값을 받는다(실측: 3건이 전부 cursor=4). 워커가 그 값까지 커서를 전진시키면
    # 같은 커서를 가진 나머지는 `cursor > ?` 에서 통째로 사라진다.
    rows = conn.execute(
        # 🔑 재큐 축은 배달 레인이 아니라 **응답 기대 여부**다. 표시됐다는 사실이
        # 응답을 보장하지 않으므로 훅 배달분도 복구 대상이 맞다(그 불변식은 테스트로
        # 못박혀 있다). 다만 fyi 는 정의상 '읽고 넘겨도 되는' 등급이라 응답이 영영
        # 안 오고, 그러면 TTL 까지 같은 내용을 반복 노출한다 — 실측: 공지 하나가
        # 10분 간격으로 두 번 배달됐다. 공지·브로드캐스트는 fyi 로 보내면 조용해진다.
        "SELECT id FROM messages "
        "WHERE state='injected' AND priority NOT IN ('blocking','fyi') "
        "AND type NOT IN ('notice','reply') "
        "AND reply_to IS NULL AND injected_at IS NOT NULL AND injected_at < ? "
        "AND COALESCE(inject_count,0) < ? ORDER BY cursor",
        (now() - REQUEUE_AFTER_S, MAX_INJECT_ATTEMPTS)).fetchall()
    for r in rows:
        conn.execute("UPDATE messages SET state='queued', cursor=? WHERE id=?",
                     (next_cursor(conn), r["id"]))
    n = len(rows)
    if n:
        conn.execute("INSERT INTO metrics VALUES(?,?,?,?)",
                     (now(), "inject.requeue", n, ""))
    return n


# 같은 발신자에게 **같은 수신자 앞** 미배달을 반복 통지하지 않는 창.
# 통지 하나가 그 세션의 웨이크 하나이고, 두 번째부터는 새 정보가 0이다.
UNREACHABLE_NOTICE_S = 3600


def _notice_cooldown_ok(conn, key, window):
    """이 키로 최근 window 안에 통지한 적이 없으면 True(그리고 시각을 찍는다)."""
    row = conn.execute("SELECT at FROM notice_cooldown WHERE key=?", (key,)).fetchone()
    if row and now() - float(row["at"]) < window:
        return False
    conn.execute("INSERT INTO notice_cooldown(key,at) VALUES(?,?) "
                 "ON CONFLICT(key) DO UPDATE SET at=excluded.at", (key, now()))
    return True


def _sweep_ttl(conn):
    """우선순위·타이머 존재와 무관한 TTL 종결 (설계 §4: 미배달 만료는 전 우선순위 규칙).

    TTL 타이머 INSERT 가 blocking 분기 안에만 있어서 normal 은 만료가 아예 없었다 —
    실측: 시스템 전 생애 expired 0건, 최고령 queued 12,557분(8.7일). 타이머 행에 기대지
    않고 messages 를 직접 스윕하므로 과거 누락분도 자동 회수된다.
    """
    rows = conn.execute(
        "SELECT id, thread, from_agent, to_agent, type, state, priority, "
        "COALESCE(ttl_renews,0) AS ttl_renews, "
        "COALESCE(inject_count,0) AS inject_count FROM messages "
        "WHERE state IN ('queued','injected','deferred') AND created + ttl_s <= ?",
        (now(),)).fetchall()
    if not rows:
        return
    # 🔴 배달된 것과 미배달을 갈라야 한다. state='injected' 는 수신자에게 실제로
    # 들어간 것이므로 '미배달 만료' 가 아니다 — 통지하면 살아 있는 세션에
    # "안 갔으니 blocking 으로 다시 보내라"는 거짓 경보가 꽂힌다(실측: 배달된 reply
    # 전량이 TTL 에 오경보를 냈다). injected 는 조용히 종결(delivered)로 닫는다.
    #
    # 🪤 그런데 **현재 state 만으로는 배달 사실을 알 수 없다.** _sweep_requeue 가
    # 무응답 injected 를 queued 로 되돌리고, h_defer 는 deferred 로 옮긴다 — 둘 다
    # 이미 수신자에게 들어간 뒤의 상태다. state 로만 가르던 동안 배달된 메시지가
    # '미배달 만료' 로 통지됐다(운영 실측 2026-08-22: m-5cd30c40 은 inject_count=1,
    # injected_at 17:20:35 인데 18:20:31 에 "수신자가 유휴/종료 상태였을 수 있다" 통지.
    # 받은 PM 이 오진을 믿고 같은 내용을 재발송한 뒤 채널 자체를 버렸다).
    # 배달 사실의 정본은 inject_count 다 — h_ack 이 근거 있는 injected 에서만 올린다.
    # 🔴 fyi 는 **깨우지 않는 것이 계약**이다. 그래서 유휴 수신자에게는 웨이크가
    # 안 일어나고, TTL 이 오면 조용히 사라진다 — 처방을 fyi 로 보내면 영영 안 닿는다
    # (실측: 팀장이 「am register 를 돌려라」를 --fyi 로 보냈고 그게 첫 만료 건이었다.
    #  처방이 처방의 부재 때문에 못 닿는 형상).
    # 수신자가 **아직 살아 있으면** 만료는 틀린 종결이다 — 만료의 목적은 사라진
    # 수신자 앞의 메시지를 닫는 것이지, 아직 안 깨어난 사람의 우편을 버리는 게 아니다.
    # 살아 있는 동안은 TTL 을 갱신해 다음 웨이크에 편승시킨다(상한 있음 — 영원히
    # 사는 큐는 그 자체가 결함이다).
    renew = [r for r in rows
             if r["priority"] == "fyi" and r["state"] == "queued"
             and not r["inject_count"]
             and (r["ttl_renews"] or 0) < FYI_MAX_RENEWS
             and conn.execute(
                 "SELECT 1 FROM agents WHERE name=? AND state LIKE 'live-%'",
                 (r["to_agent"],)).fetchone()]
    if renew:
        conn.executemany(
            "UPDATE messages SET ttl_s = ttl_s + ?, "
            "ttl_renews = COALESCE(ttl_renews,0) + 1 WHERE id=?",
            [(DEFAULT_TTL_S, r["id"]) for r in renew])
        renewed = {r["id"] for r in renew}
        rows = [r for r in rows if r["id"] not in renewed]
        if not rows:
            return
    delivered_once = [r for r in rows if r["state"] == "injected" or r["inject_count"]]
    ids = {r["id"] for r in delivered_once}
    undelivered = [r for r in rows if r["id"] not in ids]
    delivered = delivered_once
    conn.executemany("UPDATE messages SET state='expired' WHERE id=?",
                     [(r["id"],) for r in undelivered])
    conn.executemany("UPDATE messages SET state='delivered' WHERE id=?",
                     [(r["id"],) for r in delivered])
    conn.executemany("UPDATE tickets SET status='cancelled' WHERE msg_id=? "
                     "AND status='open'", [(r["id"],) for r in undelivered])
    conn.execute("INSERT INTO metrics VALUES(?,?,?,?)",
                 (now(), "expire.sweep", len(undelivered),
                  f"delivered_closed={len(delivered)}"))
    rows = undelivered   # 통지 대상은 진짜 미배달분뿐
    # 발신자 통지는 '살아 있는 발신자'에게만, 발신자당 1건으로 묶는다.
    # 죽은 발신자에게 보내면 그 notice 가 똑같은 블랙홀로 들어가 적체를 배로 늘린다
    # (실측: 적체 안에 이미 그런 고아 notice 2건이 있었다).
    per_sender = {}
    for r in rows:
        if r["type"] == "notice" or r["from_agent"] in ("__relay__", "__worker__"):
            continue   # notice 에 대한 notice 금지 (자기증식 차단)
        per_sender.setdefault(r["from_agent"], []).append(r)
    for sender, items in per_sender.items():
        live = conn.execute(
            "SELECT 1 FROM agents WHERE name=? AND state LIKE 'live-%' "
            "AND last_seen > ?", (sender, now() - STALE_AGENT_S)).fetchone()
        if not live:
            continue
        # 🔴 **같은 수신자 앞 미배달을 매번 통지하면** 발신자가 그 수만큼 깨어난다.
        # 실측: 한 팀장이 dormant 인 팀장 하나(3시간+ 무응답)에게 계속 보냈고,
        # 한 시간에 만료 통지 6건 = 웨이크 6번을 받았다. 두 번째부터는 새 정보가
        # 0이다 — 「그 수신자는 안 닿는다」를 이미 말했다.
        # 수신자별로 창을 두고, 창 안의 나머지는 조용히 만료시킨다.
        by_target = {}
        for it in items:
            by_target.setdefault(it["to_agent"], []).append(it)
        for target, group in by_target.items():
            if not _notice_cooldown_ok(conn, f"unreachable:{sender}:{target}",
                                       UNREACHABLE_NOTICE_S):
                continue
            head = group[0]
            extra = f" 외 {len(group)-1}건" if len(group) > 1 else ""
            insert_message(
                thread=head["thread"], from_agent="__relay__",
                from_session="__relay__", to_agent=sender, mtype="notice",
                priority="normal",
                body=f"미배달 만료: {head['id']}(수신자 {target}){extra} — TTL 초과. "
                     f"수신자가 유휴/종료 상태였을 수 있다. blocking 으로 다시 "
                     f"보내면 부활 응답 경로를 탄다. "
                     f"(같은 수신자 앞 만료는 {UNREACHABLE_NOTICE_S // 60}분에 "
                     f"한 번만 알린다 — 그 사이 것은 조용히 닫힌다)", conn=conn)


def _sweep_stale_agents(conn):
    """liveness 보고가 끊긴 live-* 행 강등 — **죽음의 적극적 증거가 있을 때만**.

    h_liveness 는 보고된 세션만 갱신하고 목록에서 사라진 세션을 강등하지 않는다.
    실측: 8.6일간 last_seen 이 멈춘 채 'live-active' 로 남아 h_send 의 디스패치 분기를
    오도한 행이 있었다(부활 대신 debounce 로 감).

    🪤 그런데 '보고가 없다'는 두 가지를 뜻한다 — 세션이 죽었다, 또는 **워커의 관측이
    죽었다**. 구분 없이 강등하던 시절, `claude agents --json` 한 번 실패하면 600초 뒤
    함대 전체가 dormant 가 됐다(실측 2026-08-22: 라이브 49세션이 liveness 타임아웃으로
    idle 443초 — 강등 157초 전이었다). dormant 는 배달을 가장 비싼 부활 경로로 몰기
    때문에 이 오판은 곧 과금이다.
    그래서 강등 조건에 '그 홈의 워커가 지금도 성공적으로 열거 중'을 요구한다.
    그 워커가 열거했는데 이 세션이 없었다 = 죽음의 적극적 증거.
    관측자가 아예 없는 홈(워커 미가동)은 HARD_STALE_S 백스톱으로만 정리한다 —
    하루가 지나도록 아무도 살아있다고 말해주지 않은 행은 디스패치를 오도하기만 한다.
    """
    n = conn.execute(
        "UPDATE agents SET state='dormant' WHERE state LIKE 'live-%' AND last_seen < ? "
        "AND home IN (SELECT home FROM worker_sweeps WHERE last_ok > ?)",
        (now() - STALE_AGENT_S, now() - LIVENESS_FRESH_S)).rowcount
    if n:
        conn.execute("INSERT INTO metrics VALUES(?,?,?,?)",
                     (now(), "agent.stale_demote", n, "observed"))
    hard = conn.execute(
        "UPDATE agents SET state='dormant' WHERE state LIKE 'live-%' AND last_seen < ?",
        (now() - HARD_STALE_S,)).rowcount
    if hard:
        conn.execute("INSERT INTO metrics VALUES(?,?,?,?)",
                     (now(), "agent.stale_demote", hard, "hard-backstop"))


def _fire_due(conn):
    """만기 타이머 1회분 처리.

    루프에서 떼어낸 이유는 **회귀 테스트가 실물 분기를 태우게** 하려고다 — 예전엔
    테스트가 이 분기들의 SQL 을 복사해 흉내 냈다. 복사본이 초록이어도 여기가 틀리면
    아무도 모른다(실측: defer 재배달 커서 결함이 그렇게 88건 초록 밑에 살아 있었다).
    """
    due = conn.execute(
        "SELECT * FROM timers WHERE fired=0 AND due_at<=?", (now(),)).fetchall()
    for t in due:
        msg = conn.execute("SELECT * FROM messages WHERE id=?",
                           (t["msg_id"],)).fetchone()
        if not msg or msg["state"] in ("answered", "expired"):
            conn.execute("UPDATE timers SET fired=1 WHERE id=?", (t["id"],))
            continue
        if t["kind"] == "lease" and (msg["lease_expires"] or 0) > now():
            # 🪤 리스의 정본은 messages.lease_expires 다. 타이머는 그 추종자여야 한다 —
            # 재배달로 리스가 갱신돼도 옛 due_at 은 그대로라 **즉시** 발화해 부활 잡을
            # 띄웠다(실측: defer→재배달 직후 승격 1건 = 유료). 남은 리스만큼 미루면
            # 갱신 경로가 몇 개든 이 한 곳에서 정합해진다.
            conn.execute("UPDATE timers SET due_at=? WHERE id=?",
                         (msg["lease_expires"], t["id"]))
        elif t["kind"] in ("lease", "debounce", "revive-now"):
            _escalate(conn, t, msg)
        elif t["kind"] == "redeliver":
            # 🪤 커서를 새로 발급하지 않으면 워커의 `cursor > ?` 에서 영원히 안 보인다
            # (실측: 재배달 후 폴 → deliveries 0건). defer 는 발신자에게 "30분 후
            # 재배달"을 통지까지 해 놓고 조용히 그 약속을 깨고 있었다.
            conn.execute("UPDATE messages SET state='queued', cursor=? "
                         "WHERE id=? AND state='deferred'",
                         (next_cursor(conn), msg["id"]))
            conn.execute("UPDATE timers SET fired=1 WHERE id=?", (t["id"],))
        elif t["kind"] == "ttl":
            # 상태 무관 종결 — injected 채 답 없는 메시지가 영생하지 않게
            conn.execute("UPDATE messages SET state='expired' WHERE id=?", (msg["id"],))
            conn.execute("UPDATE tickets SET status='cancelled' "
                         "WHERE msg_id=? AND status='open'", (msg["id"],))
            insert_message(
                thread=msg["thread"], from_agent="__relay__",
                from_session="__relay__", to_agent=msg["from_agent"],
                mtype="notice", priority="normal",
                body=f"만료: {msg['id']} (수신자 {msg['to_agent']}, "
                     f"최종 상태 {msg['state']})", conn=conn)
            conn.execute("UPDATE timers SET fired=1 WHERE id=?", (t["id"],))
    return len(due)


def _recover_orphans(conn):
    """수거(fired=3) 후 5분 내 미답인 부활 잡을 시도 상한 내에서 재발화."""
    orphans = conn.execute(
        "SELECT t.*, m.from_agent FROM timers t JOIN messages m "
        "ON t.msg_id=m.id WHERE t.fired=3 AND t.due_at < ? "
        "AND m.state NOT IN ('answered','expired')", (now() - 300,)).fetchall()
    for o in orphans:
        _escalate(conn, o, {"id": o["msg_id"], "from_agent": o["from_agent"]})


def timer_loop():
    """due_at 스윕. 부활 승격은 시도 상한 내에서만, TTL 은 상태 무관 종결."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    while True:
        try:
            _fire_due(conn)
            _recover_orphans(conn)
            _sweep_requeue(conn)
            _sweep_ttl(conn)
            _sweep_stale_agents(conn)
            conn.commit()
        except Exception as e:  # noqa: BLE001 — 타이머 루프는 죽지 않는다
            print(f"[timer] error: {e}", flush=True)
        time.sleep(2.0)


def _escalate(conn, t, msg):
    """부활 잡 승격 — 시도 상한 초과 시 종결 + 발신자 1회 통지."""
    if t["attempts"] >= MAX_REVIVE_ATTEMPTS:
        conn.execute("UPDATE timers SET fired=1 WHERE id=?", (t["id"],))
        row = conn.execute("SELECT thread FROM messages WHERE id=?",
                           (msg["id"],)).fetchone()
        insert_message(
            thread=row["thread"] if row else None, from_agent="__relay__",
            from_session="__relay__", to_agent=msg["from_agent"], mtype="notice",
            priority="normal",
            body=f"부활 시도 상한({MAX_REVIVE_ATTEMPTS}회) 소진: {msg['id']} — "
                 "수동 --revive 또는 문서 폴백을 권장", conn=conn)
        return
    conn.execute("UPDATE timers SET fired=2, attempts=attempts+1, due_at=? WHERE id=?",
                 (now(), t["id"]))


ROUTES = {
    ("POST", "/register"): h_register,
    ("POST", "/liveness"): h_liveness,
    ("GET", "/who"): h_who,
    ("POST", "/claim"): h_claim,
    ("POST", "/send"): h_send,
    ("POST", "/notice"): h_worker_notice,
    ("POST", "/retire"): h_retire,
    ("POST", "/reply"): h_reply,
    ("POST", "/defer"): h_defer,
    ("GET", "/inbox"): h_inbox,
    ("POST", "/ack"): h_ack,
    ("GET", "/wait"): h_wait,
    ("POST", "/cancel"): h_cancel,
    ("GET", "/poll"): h_poll,
    ("POST", "/gate"): h_gate,
    ("POST", "/spend"): h_spend,
    ("GET", "/read"): h_read,
    ("GET", "/message"): h_message,
    ("GET", "/agent"): h_agent,
    ("GET", "/agent-by-session"): h_agent_by_session,
    ("GET", "/agents"): h_agents,
    ("GET", "/board"): h_board,
    ("POST", "/org"): h_org,
}


class Handler(BaseHTTPRequestHandler):
    def _serve(self, method):
        url = urlparse(self.path)
        if method == "GET" and url.path == "/healthz":   # 무인증 — k8s probe 전용
            self._json(200, {"ok": True})
            return
        # 워커별 토큰 (설계 §2-1). 토큰 미설정(로컬 v0)이면 통과.
        _local.worker = None
        if TOKENS:
            auth = self.headers.get("Authorization", "").removeprefix("Bearer ").strip()
            match = [n for n, t in TOKENS.items() if t and t == auth]
            if not match:
                self._json(401, {"error": "unauthorized"})
                return
            # 어느 워커인지 기억한다 — msg_socket 갱신 등 '워커만' 인가에 쓴다
            _local.worker = match[0]
        fn = ROUTES.get((method, url.path))
        if not fn:
            self._json(404, {"error": "not-found"})
            return
        body = {}
        if method == "POST":
            length = int(self.headers.get("Content-Length", 0))
            if length:
                body = json.loads(self.rfile.read(length))
        try:
            result = fn(body, parse_qs(url.query))
            db().commit()
        except Exception as e:  # noqa: BLE001
            db().rollback()
            self._json(500, {"error": str(e)})
            return
        self._json(200, result)

    def do_GET(self):
        self._serve("GET")

    def do_POST(self):
        self._serve("POST")

    def _json(self, code, obj):
        data = json.dumps(obj, ensure_ascii=False).encode()
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            # 🪤 워커 로그에만 달았던 가드가 여기에도 필요했다. 클라이언트(am·워커)가
            # 타임아웃으로 먼저 끊으면 socketserver 가 스택트레이스를 뱉는다 — 그리고
            # 예전 _serve 는 그 실패를 500 응답으로 갚으려다 **또** 터졌다(실측: 격리
            # E2E 한 번에 relay 로그 트레이스백 2건). 파드 로그는 모두가 보는 화면이다.
            self.close_connection = True

    def log_message(self, *args):
        pass


def main():
    for pair in os.environ.get("HUB_WORKER_TOKENS", "").split(","):
        if ":" in pair:
            name, tok = pair.split(":", 1)
            TOKENS[name] = tok
    bind = os.environ.get("HUB_RELAY_BIND", "127.0.0.1")
    if bind != "127.0.0.1" and not TOKENS:
        raise SystemExit("HUB_RELAY_BIND 가 로컬이 아니면 HUB_WORKER_TOKENS 필수 "
                         "(무인증 네트워크 노출 금지)")
    conn = db()
    conn.executescript(SCHEMA)
    migrate(conn)
    conn.commit()
    threading.Thread(target=timer_loop, daemon=True).start()
    server = ThreadingHTTPServer((bind, PORT), Handler)
    print(f"hub-relay listening {bind}:{PORT} db={DB_PATH}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
