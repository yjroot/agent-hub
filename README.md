# Agent Hub (v0)

에이전트 간 메신저 — Claude Code / Codex / Hermes 세션이 서로 질의(consult)·범위 선언(claim)·저자 리뷰(review)를 주고받는 인프라.

**설계 정본**: `docs/DESIGN-agent-messenger.md` (v4.2, 리뷰 3라운드 GO)

## 구성

| 컴포넌트 | 역할 |
|---|---|
| `relay/relay.py` | 중계서버 (SoT·시계·notice 전용 발행·타이머 영속·예산 원장). :8790 |
| `worker/worker.py` | 머신별 워커 (로컬 API·long-poll·부활 엔진·liveness). 127.0.0.1:8791 |
| `cli/am` | 에이전트용 CLI (register/who/claim/ask/wait/review/inbox/read/reply/defer/decide) |
| `hooks/am_hook.py` | Claude Code 훅 4종 진입점 (전부 `"timeout": 2` 필수) |

## 기동 (v0 로컬)

```bash
python3 relay/relay.py &          # HUB_DB=~/.agent-hub/relay.db
python3 worker/worker.py &        # HUB_RELAY=http://127.0.0.1:8790
export PATH="$PWD/cli:$PATH"
```

## 세션 훅 설치 (프로젝트 .claude/settings.json)

```json
{
  "permissions": {"allow": ["Bash(am:*)"]},
  "hooks": {
    "SessionStart":     [{"hooks": [{"type": "command", "command": "python3 <repo>/hooks/am_hook.py session_start",     "timeout": 2}]}],
    "UserPromptSubmit": [{"hooks": [{"type": "command", "command": "python3 <repo>/hooks/am_hook.py user_prompt_submit", "timeout": 2}]}],
    "PostToolUse":      [{"hooks": [{"type": "command", "command": "python3 <repo>/hooks/am_hook.py post_tool_use",      "timeout": 2}]}],
    "SessionEnd":       [{"hooks": [{"type": "command", "command": "python3 <repo>/hooks/am_hook.py session_end",        "timeout": 2}]}]
  }
}
```

⚠️ `Bash(am:*)` 권한 허용이 없으면 헤드리스/자동 세션에서 모델이 reply 를 시도해도 권한에서 막힌다 (봉투 실측에서 확인).

## ⚠️ 유휴 세션 웨이크의 하드 전제

워커는 유휴(사용자 입력 대기) 세션을 Claude Code 의 UNIX 도메인 소켓으로 깨운다. **이 레인이
동작하려면 수신 쪽 설정이 아래를 만족해야 한다.** 아니면 웨이크는 조용히 실패하고 훅 주입·부활
폴백만 남는다(메시지가 유실되지는 않지만, 유휴 세션은 다음 툴 경계까지 못 받는다).

| 전제 | 값 | 확인 |
|---|---|---|
| `crossSessionInbound` | `"accept"` | `~/.claude/settings.json`. **미설정이면 bypassPermissions 세션은 '사람 승인 대기(hold)'로 파킹된다** |
| Claude Code 버전 | ≥ 2.1.224 | 그 아래는 세션 소켓 자체가 없다 (실측: 경계가 정확히 2.1.224) |
| 크로스세션 메시징 게이트 | on | 꺼져 있으면 소켓이 안 열린다 |

실측(2026-08-22, CC 2.1.239, 일회용 세션):

| 수신 설정 | 결과 | 되먹임(`peer_message_status`) |
|---|---|---|
| `accept` | 배달됨 | **없음** (영수증 자체가 안 온다) |
| `hold` | 승인 대기로 파킹 — 모델이 못 본다 | `status="held"` |
| `refuse` | 거부 | `status="expired"`, `status_detail="refused"` |

세 형상 모두 와이어는 정상 종료(clean EOF)로 보인다 — **`sendall` 성공은 배달의 증거가 아니다.**
그래서 워커는 두 신호로만 배달을 계상한다:
1. **부정 영수증 부재 + 수신 세션의 실제 활동** (`~/.claude/sessions/<pid>.json` 의 status 변화.
   실측: 배달 시 idle→busy 0.06초, hold 시 무변화)
2. `delivered` 영수증 (held 가 사람 승인으로 풀린 경우)

증거가 없으면 `wake_unconfirmed` 로 남기고 **배달로 치지 않는다**. relay 상태는 `queued` 로
유지되어 훅 주입·부활 폴백이 그대로 동작한다. 근거는 `messages.wake_status` 에 기록된다.

워커는 기동 시 실효 `crossSessionInbound` 를 읽어 `accept` 가 아니면 경고 로그를 남긴다(차단은
하지 않는다). 현재 값은 `curl -s 127.0.0.1:8791/health | jq .inbound_policy` 로 확인한다.

## 보안 경계 (웨이크 주소)

- 워커 로컬 API(`127.0.0.1:8791`)는 **워커가 관측한 세션**의 `/register` 만 받는다
  (`~/.claude/sessions` 레지스트리 / 트랜스크립트 실재 / 이미 등록된 세션).
- `msg_socket`(주입 주소)은 **요청 본문의 자가 신고를 무시**하고 워커가 레지스트리에서
  직접 확인한 값만 쓴다. relay 도 워커 토큰 보유자만 이 필드를 갱신할 수 있다.
- 배달 직전 소유권을 재확인한다 (sessionId → pid 생존 → procStart 대조 → connect).
- 살아 있는 다른 세션이 쓰는 **이름은 뺏을 수 없다** (선점자 우선. 이름을 뺏기면 그 이름 앞으로
  오는 배달이 통째로 신규 행으로 넘어간다).

## 핵심 규칙 (요약)

- 부활은 예외 없이 `--fork-session` + `--model` 명시 + 화이트리스트 도구(`--tools "Read,Grep,Glob" --strict-mcp-config`) + `-n "agent-hub-responder <thread>"`.
- lost/revive-failed = 사실 통지만, 대리 답변 금지.
- 비용: 발신자 귀속. 사전 게이트(고정항+트랜스크립트 예측, ≤$5 자동) / 발신자 $20/일 / 전역 $60/일. `--max-budget-usd 15` 는 백스톱.
- blocking 은 티켓+`am wait` 재진입 폴. **am 을 백그라운드로 돌리지 말 것.**
