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
BODY_MAX = 500

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
CREATE INDEX IF NOT EXISTS idx_msg_to ON messages(to_agent, state);
CREATE INDEX IF NOT EXISTS idx_timers_due ON timers(fired, due_at);
"""


def now():
    return time.time()


def today():
    return time.strftime("%Y-%m-%d")


def new_id(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def metric(key, value=1.0, detail=""):
    db().execute("INSERT INTO metrics VALUES(?,?,?,?)", (now(), key, value, detail))


def insert_message(*, thread, from_agent, from_session, to_agent, mtype, priority,
                   body, refs="{}", state="queued", meta=None, ttl_s=DEFAULT_TTL_S,
                   reply_to=None, body_cap=BODY_MAX, conn=None):
    """모든 메시지 INSERT 의 단일 경로. cursor = rowid (동시 발신 경쟁 제거)."""
    c = conn or db()
    mid = new_id("m")
    if len(body) > body_cap:
        body = body[:body_cap] + " …[truncated]"
    c.execute(
        "INSERT INTO messages(id,thread,from_agent,from_session,to_agent,type,priority,"
        "body,refs,state,meta,ttl_s,reply_to,created) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (mid, thread or new_id("t"), from_agent, from_session, to_agent, mtype,
         priority, body, refs, state, json.dumps(meta or {}), ttl_s, reply_to, now()))
    c.execute("UPDATE messages SET cursor=rowid WHERE id=?", (mid,))
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

def h_register(body, _q):
    a = body
    db().execute(
        "INSERT INTO agents(name,session,cli,home,repo,cwd,task,paths,design,model,"
        "state,msg_socket,registered_at,last_seen) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(session) DO UPDATE SET "
        "home=excluded.home, cwd=COALESCE(NULLIF(excluded.cwd,''), cwd), "
        "name=COALESCE(NULLIF(excluded.name,''), name), "
        "task=COALESCE(NULLIF(excluded.task,''), task), "
        "paths=CASE WHEN excluded.paths='[]' THEN paths ELSE excluded.paths END, "
        "design=COALESCE(NULLIF(excluded.design,''), design), "
        "model=COALESCE(NULLIF(excluded.model,''), model), "
        "state=excluded.state, last_seen=?",
        (a.get("name"), a["session"], a.get("cli", "claude"), a.get("home", "local"),
         a.get("repo", ""), a.get("cwd", ""), a.get("task", ""),
         json.dumps(a.get("paths", [])), a.get("design", ""), a.get("model", ""),
         a.get("state", "live-active"), a.get("msg_socket", ""), now(), now(), now()))
    return {"ok": True}


def h_liveness(body, _q):
    """워커 보고: [{session, state}] — 판정식(§1-1)은 워커 책임, relay는 기록."""
    for item in body.get("agents", []):
        db().execute("UPDATE agents SET state=?, last_seen=? WHERE session=?",
                     (item["state"], now(), item["session"]))
    if body.get("session_ended"):
        db().execute(
            "UPDATE agents SET state='dormant', session_end_commit=? WHERE session=?",
            (body.get("commit", ""), body["session_ended"]))
    return {"ok": True}


def h_who(_body, q):
    path = q.get("path", [""])[0]
    repo = q.get("repo", [""])[0]
    rows = db().execute(
        "SELECT * FROM agents WHERE state != 'lost' ORDER BY registered_at DESC").fetchall()
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
    supersedes = None
    if orig["state"] == "answered":
        # 이미 부활 응답이 나갔는데 본체가 늦게 답한 경우 → supersede (설계 §5)
        prev = db().execute(
            "SELECT id FROM messages WHERE reply_to=? ORDER BY created DESC LIMIT 1",
            (orig["id"],)).fetchone()
        supersedes = prev["id"] if prev else None
        metric("reply.supersede", 1, orig["id"])
    meta["supersedes"] = supersedes
    sender = body.get("from_agent", "") if body.get("from_session") == "__worker__" \
        else verified_sender(body.get("from_session", ""), body.get("from_agent"))
    mid = insert_message(
        thread=orig["thread"], from_agent=sender,
        from_session=body.get("from_session", ""), to_agent=orig["from_agent"],
        mtype="reply", priority="normal", body=body.get("body", ""),
        meta=meta, reply_to=orig["id"], body_cap=4000)
    db().execute("UPDATE messages SET state='answered' WHERE id=?", (orig["id"],))
    db().execute("UPDATE tickets SET status='answered' WHERE msg_id=?", (orig["id"],))
    if supersedes:
        notice(orig["from_agent"],
               f"정정: {orig['id']} 에 저자 본체의 늦은 답변이 도착 (supersedes {supersedes})",
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


def h_ack(body, _q):
    """워커 배달 상태 회신: queued→injected→acknowledged. lease 는 injected 시점 부여."""
    st = body["state"]
    if st not in ("injected", "acknowledged", "inject_failed"):
        return {"ok": False, "error": "invalid-state"}   # answered 위조 차단
    mid = body["id"]
    row = db().execute("SELECT * FROM messages WHERE id=?", (mid,)).fetchone()
    if not row:
        return {"ok": False}
    db().execute("UPDATE messages SET state=? WHERE id=? AND state='queued'", (st, mid)) \
        if st == "injected" else \
        db().execute("UPDATE messages SET state=? WHERE id=?", (st, mid))
    if st == "injected" and row["priority"] == "blocking":
        # 주입 성공 = 본체가 lease 선점 (설계 §5). debounce 는 취소.
        db().execute("UPDATE messages SET lease_holder=?, lease_expires=? WHERE id=?",
                     (row["to_agent"], now() + LEASE_S, mid))
        db().execute("UPDATE timers SET fired=1 WHERE msg_id=? AND kind='debounce'",
                     (mid,))
        db().execute("INSERT INTO timers(id,kind,msg_id,due_at) VALUES(?,?,?,?)",
                     (new_id("tm"), "lease", mid, now() + LEASE_S))
    if st == "inject_failed" and row["priority"] == "blocking":
        # 메시지 단위 debounce 타이머 (설계 §5 단일화 규칙)
        existing = db().execute(
            "SELECT 1 FROM timers WHERE msg_id=? AND kind='debounce'", (mid,)).fetchone()
        if not existing:
            db().execute("INSERT INTO timers(id,kind,msg_id,due_at) VALUES(?,?,?,?)",
                         (new_id("tm"), "debounce", mid, now() + DEBOUNCE_S))
    return {"ok": True}


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
        deliveries = [dict(r) for r in db().execute(
            "SELECT m.* FROM messages m JOIN agents a ON m.to_agent=a.name "
            "WHERE a.home=? AND m.state='queued' AND m.cursor>? ORDER BY m.cursor",
            (home, cursor)).fetchall()]
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
    row = db().execute(
        "SELECT * FROM agents WHERE name=? ORDER BY registered_at DESC LIMIT 1",
        (q.get("name", [""])[0],)).fetchone()
    return {"agent": dict(row) if row else None}


MAX_REVIVE_ATTEMPTS = 2   # 메시지당 부활 시도 상한 — 재발화 무한 루프·영구 과금 차단


def timer_loop():
    """due_at 스윕. 부활 승격은 시도 상한 내에서만, TTL 은 상태 무관 종결."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    while True:
        try:
            due = conn.execute(
                "SELECT * FROM timers WHERE fired=0 AND due_at<=?", (now(),)).fetchall()
            for t in due:
                msg = conn.execute("SELECT * FROM messages WHERE id=?",
                                   (t["msg_id"],)).fetchone()
                if not msg or msg["state"] in ("answered", "expired"):
                    conn.execute("UPDATE timers SET fired=1 WHERE id=?", (t["id"],))
                    continue
                if t["kind"] in ("lease", "debounce", "revive-now"):
                    _escalate(conn, t, msg)
                elif t["kind"] == "redeliver":
                    conn.execute("UPDATE messages SET state='queued' WHERE id=? "
                                 "AND state='deferred'", (msg["id"],))
                    conn.execute("UPDATE timers SET fired=1 WHERE id=?", (t["id"],))
                elif t["kind"] == "ttl":
                    # 상태 무관 종결 — injected 채 답 없는 메시지가 영생하지 않게
                    conn.execute("UPDATE messages SET state='expired' WHERE id=?",
                                 (msg["id"],))
                    conn.execute("UPDATE tickets SET status='cancelled' "
                                 "WHERE msg_id=? AND status='open'", (msg["id"],))
                    insert_message(
                        thread=msg["thread"], from_agent="__relay__",
                        from_session="__relay__", to_agent=msg["from_agent"],
                        mtype="notice", priority="normal",
                        body=f"만료: {msg['id']} (수신자 {msg['to_agent']}, "
                             f"최종 상태 {msg['state']})", conn=conn)
                    conn.execute("UPDATE timers SET fired=1 WHERE id=?", (t["id"],))
            # 고아 부활 잡 복구: 수거(fired=3) 후 5분 내 미답 → 시도 상한 내 재발화
            orphans = conn.execute(
                "SELECT t.*, m.from_agent FROM timers t JOIN messages m "
                "ON t.msg_id=m.id WHERE t.fired=3 AND t.due_at < ? "
                "AND m.state NOT IN ('answered','expired')", (now() - 300,)).fetchall()
            for o in orphans:
                _escalate(conn, o, {"id": o["msg_id"], "from_agent": o["from_agent"]})
            # injected 인 채 10분 이상 미답인 normal/fyi 는 재큐 (배달 유실 복구)
            conn.execute(
                "UPDATE messages SET state='queued' WHERE state='injected' "
                "AND priority != 'blocking' AND created < ? AND reply_to IS NULL",
                (now() - 600,))
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
}


class Handler(BaseHTTPRequestHandler):
    def _serve(self, method):
        url = urlparse(self.path)
        if method == "GET" and url.path == "/healthz":   # 무인증 — k8s probe 전용
            self._json(200, {"ok": True})
            return
        # 워커별 토큰 (설계 §2-1). 토큰 미설정(로컬 v0)이면 통과.
        if TOKENS:
            auth = self.headers.get("Authorization", "").removeprefix("Bearer ").strip()
            if auth not in TOKENS.values():
                self._json(401, {"error": "unauthorized"})
                return
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
            self._json(200, result)
        except Exception as e:  # noqa: BLE001
            db().rollback()
            self._json(500, {"error": str(e)})

    def do_GET(self):
        self._serve("GET")

    def do_POST(self):
        self._serve("POST")

    def _json(self, code, obj):
        data = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

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
    conn.commit()
    threading.Thread(target=timer_loop, daemon=True).start()
    server = ThreadingHTTPServer((bind, PORT), Handler)
    print(f"hub-relay listening {bind}:{PORT} db={DB_PATH}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
