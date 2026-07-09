# CodexCont

[English](README.md) · [中文](README_zh.md)

Continue-thinking middleware for Codex / OpenAI Responses-compatible APIs.

This project is a small Starlette proxy that sits between a coding agent and an upstream Responses endpoint. It detects a known reasoning-truncation fingerprint (`usage.output_tokens_details.reasoning_tokens == 518 * n - 2`), silently asks the model to continue thinking, and folds multiple upstream streaming responses into one coherent downstream SSE response.

```text
Coding agent  ->  CodexCont  ->  Codex / Responses API
```

> **Installing via an AI agent?** Hand it [`INSTALL-GUIDE-AGENT/AGENT.md`](INSTALL-GUIDE-AGENT/AGENT.md) — a step-by-step runbook written for an AI agent to execute on your machine.

## Disclaimer

This project explicitly bypasses the observed OpenAI Codex reasoning-truncation behavior. If your use of this middleware is considered abusive, violates service terms, increases costs unexpectedly, or causes any other adverse consequences, you are solely responsible for those consequences.

## What it does

- Streams reasoning items to the agent live.
- Buffers tentative final output (`message` and `function_call`) until the upstream terminal event reveals whether the round was truncated.
- If the round is truncated, discards the tentative output and opens a continuation round with the prior reasoning replayed.
- If the round finishes cleanly or a safety cap is reached, flushes the final round output and emits one reconstructed terminal response.
- Leaves non-matching traffic as a transparent passthrough.

The default continuation method is a hidden `phase: "commentary"` assistant message (`"Continue thinking..."`). A legacy synthetic tool-pair mode is also available.

## Requirements

- Python `>= 3.12`
- [`uv`](https://docs.astral.sh/uv/) recommended

Runtime dependencies are declared in `pyproject.toml`:

- `httpx`
- `starlette`
- `uvicorn`

## Quick start

```bash
uv sync
cp config.example.toml config.toml
uv run python run.py
```

`run.py` reads the local `config.toml`; start by copying `config.example.toml` and then adjust it as needed.

The example default server listens on `127.0.0.1:8787` and accepts POST requests at:

- `/v1/responses`

You can also run with the already-created virtual environment directly:

```bash
# Windows / Git Bash in this workspace
.venv/Scripts/python.exe run.py
```

## Point your client at the proxy

Use the proxy URL instead of the real upstream URL.

Example:

```text
http://127.0.0.1:8787/v1/responses
```

The example default configuration (`config.example.toml`, copied to `config.toml`) uses:

```toml
[upstream]
url = "https://chatgpt.com/backend-api/codex/responses"
mode = "header"
```

With `mode = "header"`, a `Responses-API-Base` request header overrides the configured `url`; when the header is absent, requests fall back to the configured Codex URL.

For example, to target a generic Responses-compatible endpoint, send:

```text
Responses-API-Base: https://api.openai.com/v1
```

The middleware appends `/responses` unless the supplied value already ends with `/responses`. This control header is stripped before forwarding upstream.

## Authentication

`config.toml` supports three auth modes. The example default is `passthrough`:

```toml
[auth]
mode = "passthrough"               # passthrough | inject | passthrough_then_inject
access_token = ""                  # sent as Authorization: Bearer <access_token>
chatgpt_account_id = ""            # sent as chatgpt-account-id when non-empty
```

Modes:

- `passthrough`: forward the caller's auth headers only; inject nothing.
- `inject`: override/set auth headers from config.
- `passthrough_then_inject`: keep caller auth when present, otherwise inject from config.

Security guard: if a request supplies `Responses-API-Base`, the middleware will not leak configured credentials to that request-supplied URL. If the current auth mode would inject configured credentials for that request, it is rejected with `400`. To use per-request upstream overrides with credentials, pass the caller's own `Authorization` and use `mode = "passthrough"` or `mode = "passthrough_then_inject"`.

Do not commit secrets. `rt.json` and `free_rt.json` are ignored by `.gitignore`, and tokens in `config.toml` should be handled carefully.

## When continuation is applied

The middleware folds only when all of the following are true:

- `[continue].enabled = true`
- request body is a JSON object
- `stream` is truthy
- reasoning is not explicitly disabled (`"reasoning": false` disables folding)
- when using `method = "tool_pair"`, the request does not declare a real tool with the same name as `[continue].continue_tool_name`

All other requests are proxied unchanged as passthrough streams.

## Continuation logic

For each upstream round:

1. Reasoning item events are forwarded live with rewritten `sequence_number` and `output_index`.
2. Message and function-call events are buffered as tentative output.
3. On the terminal event, the middleware reads `usage.output_tokens_details.reasoning_tokens`.
4. If the token count matches `518 * n - 2`, is within the configured tier window, has encrypted reasoning content, and safety caps allow it, the middleware:
   - drops the buffered tentative output,
   - appends the round's reasoning plus a continuation marker to the next request input,
   - opens another upstream streaming round.
5. Otherwise it flushes the final buffered output and emits a reconstructed terminal event.

The downstream agent sees one response, while metadata includes details about the hidden rounds.

## Stream timeout

Set `[stream].upstream_event_timeout_seconds` to cap how long one upstream round may go without a parsed SSE `data:` event. Set `[stream].upstream_round_timeout_seconds` to cap total wall-clock time for one round, even if parseable SSE events keep arriving:

```toml
[stream]
upstream_event_timeout_seconds = 300
upstream_round_timeout_seconds = 480
```

SSE comments/keepalives do not count as progress. On timeout, the middleware emits `response.incomplete` with `incomplete_details.reason = "upstream_event_timeout"` or `"upstream_round_timeout"` and does not flush unconfirmed tentative message/tool output.

## Request audit log

Enable the independent SQLite request audit database when diagnosing upstream `400` or schema compatibility issues:

```toml
[request_log]
enabled = true
path = "logs/request_audit.sqlite3"
store_body = true
max_body_bytes = 8388608
preview_chars = 240
```

The audit database stores request metadata, a compressed raw body, per-item `input[i]` rows, per-tool rows, and schema findings. It does not store `Authorization` / `Cookie` headers. The `request_input_items` table makes fields such as `input[83].arguments` directly queryable by `type`, `name`, `arguments_type`, and `arguments_json_type`.

## Compatibility normalization

Some Responses-compatible upstreams require historical call item `arguments` to
be JSON objects, while Codex clients may replay them as JSON strings. Enable the
compatibility transform for those upstreams:

```toml
[compat]
normalize_input_arguments = true
```

When enabled, only call-like `input[i]` items whose `arguments` is a string that
strictly parses to a JSON object are converted before forwarding. Invalid JSON,
non-object JSON, duplicate-key objects, already-object values, and unrelated
fields are left unchanged. The original request bytes remain in the audit DB,
and conversion/skip decisions are written to `request_compat_actions`.

## Response metadata

The final reconstructed response includes proxy metadata such as:

- `metadata.proxy_rounds`: per-round reasoning token counts and detected tier `n`.
- `metadata.proxy_billed_usage`: summed upstream token usage across hidden rounds.
- `metadata.proxy_stopped_reason`: present when a guard or error stops continuation.

Agent-facing `usage` is reconstructed to look like one response: round-1 input/cached tokens, summed reasoning tokens, and the final round's non-reasoning output.

## Tests

The test suite is self-contained and does not require `pytest`:

```bash
uv run python tests/test_middleware.py
# or
.venv/Scripts/python.exe tests/test_middleware.py
```

Current offline coverage includes:

- truncation math
- incremental SSE parsing
- fold/rewrite behavior with captured SSE fixtures
- commentary and tool-pair continuation payloads
- header transparency
- upstream URL resolution
- auth safety guard
- EOF/upstream-error/failed-terminal/stream-timeout behavior
- structured request audit logging to SQLite
- compatibility normalization of `input[i].arguments`

## Project layout

```text
middleware/
  app.py       # Starlette app and route handler
  audit.py     # independent SQLite request audit logging
  codex.py     # truncation math and continuation payload builders
  config.py    # config.toml loader and dataclasses
  creds.py     # upstream header/auth construction
  proxy.py     # fold_stream state machine
  sse.py       # incremental SSE parser/serializer
  store.py     # in-memory ID store for optional stateful repair

tests/
  test_middleware.py
  fixtures/

run.py         # uvicorn entrypoint
config.example.toml # example runtime configuration; copy to config.toml for local use
```

## Limitations

- Final answer text is buffered until the terminal round proves it is not truncated, so final-answer first-token latency can be higher than a normal stream.
- Non-streaming requests are currently passed through rather than folded.
- The truncation detector is intentionally specific to the observed `518 * n - 2` fingerprint.
- Optional `repair_followup = "stateful"` uses in-memory process-local state; it is not shared across multiple proxy instances.

## Acknowledgements

This project would not exist without the discussions in the [LINUX DO](https://linux.do) community. Special thanks to **@shinorochi** and **@dskdkj** of the LINUX DO community for jointly pinning down the truncation mechanism and GPT's thinking model, and to **@shinorochi** for proposing the better approach based on `commentary` input rather than faked tool calls.
