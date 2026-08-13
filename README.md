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

## 핵심 규칙 (요약)

- 부활은 예외 없이 `--fork-session` + `--model` 명시 + 화이트리스트 도구(`--tools "Read,Grep,Glob" --strict-mcp-config`) + `-n "agent-hub-responder <thread>"`.
- lost/revive-failed = 사실 통지만, 대리 답변 금지.
- 비용: 발신자 귀속. 사전 게이트(고정항+트랜스크립트 예측, ≤$5 자동) / 발신자 $20/일 / 전역 $60/일. `--max-budget-usd 15` 는 백스톱.
- blocking 은 티켓+`am wait` 재진입 폴. **am 을 백그라운드로 돌리지 말 것.**
