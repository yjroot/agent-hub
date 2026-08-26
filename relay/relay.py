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
        "state,msg_socket,registered_at,last_seen,ephemeral,permission_mode) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
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
        "state=excluded.state, last_seen=?",
        (a.get("name"), a["session"], a.get("cli", "claude"), a.get("home", "local"),
         a.get("repo", ""), a.get("cwd", ""), a.get("task", ""),
         json.dumps(a.get("paths", [])), a.get("design", ""), a.get("model", ""),
         a.get("state", "live-active"), a.get("msg_socket", ""), now(), now(),
         1 if a.get("ephemeral") else 0, a.get("permission_mode", ""), now()))
    _apply_hints(a)
    if squatted:
        return {"ok": True, "name": a["name"],
                "name_conflict": "이름을 이미 살아있는 다른 세션이 쓰고 있어 기본 이름으로 "
                                 "등록했다 (선점자 우선)"}
    return {"ok": True}


def _apply_hints(a):
    hint = (a.get("task_hint") or "").strip()
    # 🔴 하네스 주입 문구를 '세션 정체성'으로 캡처하면 안 된다. 실측 183행 중 46행(25%)이
    # '<' 로 시작했다 — <local-command-caveat>·<task-notification>·<cross-session-message>.
    # 마지막 것은 **내 웨이크 봉투**다: 남을 깨운 내 메시지가 그 세션의 정체성 라벨이 됐다.
    if hint.startswith("<") or _MARKER_RE.search(hint[:40]):
        metric("task_hint.rejected", 1, hint[:60])
        hint = ""
    if hint:
        # 비어 있을 때 + **오염된 값을 쓰고 있을 때** 갱신한다(명시 register 는 불침).
        db().execute("UPDATE agents SET task=? WHERE session=? "
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
    rows = db().execute(
        "SELECT name, cli, state, task, cwd, repo, last_seen, last_activity, "
        "COALESCE(ephemeral,0) AS ephemeral, "
        # 🔑 idle 은 **활동 축**으로 잰다. last_seen 은 워커 liveness 스윕(20s)이 매번
        # 갱신하는 도달성 축이라 live 행이 전부 0분이 된다(실측 49행 13~15초) —
        # "5분 idle 과 6시간 idle 은 판단이 다르다"고 요청받아 넣은 칸이 정보량 0이었다.
        # 미측정은 NULL 로 남긴다: 0 으로 채우면 '방금 활동'이라는 거짓말이 된다.
        "CASE WHEN last_activity IS NULL THEN NULL "
        "ELSE CAST(? - last_activity AS INTEGER) END AS idle_s FROM agents "
        "WHERE (?='' OR state=?) AND name != '' "
        "AND (? OR COALESCE(ephemeral,0)=0) "
        # 정렬도 활동 축으로 — last_seen DESC 는 전부 동률이라 사실상 임의 순서였다.
        "ORDER BY COALESCE(last_activity, 0) DESC LIMIT ?",
        (now(), state, state, 1 if show_all else 0,
         int(q.get("limit", ["40"])[0]))).fetchall()
    return {"agents": [dict(r) for r in rows]}


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
        if body.get("type") == "review" or rec_state in ("dormant", "lost"):
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
        prev = db().execute(
            "SELECT id FROM messages WHERE reply_to=? AND from_agent=? "
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
    row = db().execute("SELECT name FROM agents WHERE session=?", (session,)).fetchone()
    if not row:
        return {"items": []}
    rows = db().execute(
        "SELECT * FROM messages WHERE to_agent=? AND state IN ('queued','injected') "
        "AND type != 'reply' ORDER BY cursor", (row["name"],)).fetchall()
    items = [{"id": r["id"], "thread": r["thread"], "from": r["from_agent"],
              "type": r["type"], "priority": r["priority"], "body": r["body"]}
             for r in rows]
    return {"items": items}


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
        changed = bool(db().execute(
            "UPDATE messages SET state=?, injected_at=?, wake_status=?, "
            "inject_count=COALESCE(inject_count,0)+1 "
            "WHERE id=? AND state='queued'", (st, now(), evidence, mid)).rowcount)
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
    if spent(f"sender:{sender}") + est > SENDER_DAILY_USD:
        notice(sender, f"부활 중단: 발신자 일일 예산 ${SENDER_DAILY_USD} 초과 예상",
               thread=thread)
        return {"allow": False, "reason": "sender-daily-budget"}
    if spent("global") + est > GLOBAL_DAILY_USD:
        notice(sender, f"부활 중단: 전역 일일 예산 ${GLOBAL_DAILY_USD} 초과 예상",
               thread=thread)
        return {"allow": False, "reason": "global-daily-budget"}
    if est > AUTO_GATE_USD and not confirm:
        notice(sender, f"부활 예상 ${est:.2f} > ${AUTO_GATE_USD} — --revive-confirm 필요",
               thread=thread)
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


def _sweep_ttl(conn):
    """우선순위·타이머 존재와 무관한 TTL 종결 (설계 §4: 미배달 만료는 전 우선순위 규칙).

    TTL 타이머 INSERT 가 blocking 분기 안에만 있어서 normal 은 만료가 아예 없었다 —
    실측: 시스템 전 생애 expired 0건, 최고령 queued 12,557분(8.7일). 타이머 행에 기대지
    않고 messages 를 직접 스윕하므로 과거 누락분도 자동 회수된다.
    """
    rows = conn.execute(
        "SELECT id, thread, from_agent, to_agent, type, state, "
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
        head = items[0]
        extra = f" 외 {len(items)-1}건" if len(items) > 1 else ""
        insert_message(
            thread=head["thread"], from_agent="__relay__", from_session="__relay__",
            to_agent=sender, mtype="notice", priority="normal",
            body=f"미배달 만료: {head['id']}(수신자 {head['to_agent']}){extra} — "
                 "TTL 초과. 수신자가 유휴/종료 상태였을 수 있다. "
                 "blocking 으로 다시 보내면 부활 응답 경로를 탄다.", conn=conn)


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
