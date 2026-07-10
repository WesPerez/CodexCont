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
dynamic_allowed_origins = ["https://api.openai.com", "https://chatgpt.com"]
```

With `mode = "header"`, a `Responses-API-Base` request header overrides the configured `url`; when the header is absent, requests fall back to the configured Codex URL.

For example, to target a generic Responses-compatible endpoint, send:

```text
Responses-API-Base: https://api.openai.com/v1
```

The middleware appends `/responses` unless the supplied value already ends with `/responses`. This control header is stripped before forwarding upstream. Header-selected targets must match an exact entry in `dynamic_allowed_origins`; an empty list disables dynamic targets. Private or loopback resolved addresses are rejected unless `dynamic_allow_private_ips = true`.

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

Security guard: when a request supplies `Responses-API-Base`, configured access tokens, account IDs, and `[upstream.headers]` overrides are never applied. Only caller-supplied headers may reach the allowlisted dynamic target; unauthenticated and non-Authorization authentication schemes remain possible when the target supports them.

`[server].max_request_body_bytes` provides an application-layer request cap even when the service is reached without Nginx. Keep the reverse proxy capped as well; the example default is 32 MiB.

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
upstream_connect_timeout_seconds = 5
upstream_read_timeout_seconds = 330
upstream_write_timeout_seconds = 60
upstream_pool_timeout_seconds = 5
```

SSE comments/keepalives do not count as progress. On timeout, the middleware emits `response.incomplete` with `incomplete_details.reason = "upstream_event_timeout"` or `"upstream_round_timeout"` and does not flush unconfirmed tentative message/tool output. The transport timeouts also bound connection setup, response-header/raw-read waits, request upload, and connection-pool waits; first-request transport timeouts return `504`, while other connection errors return `502`.

## Request audit log

Enable the independent SQLite request audit database when diagnosing upstream `400` or schema compatibility issues:

```toml
[request_log]
enabled = true
path = "logs/request_audit.sqlite3"
store_body = true
max_body_bytes = 0
background_body_writes = true
background_max_pending_bytes = 134217728
retention_days = 7
sqlite_busy_timeout_ms = 5000
prune_interval_seconds = 0
prune_batch_size = 0
store_forwarded_body = true
store_response_body = "errors"
max_response_body_bytes = 1048576
preview_chars = 240
```

The audit database stores request metadata, a compressed raw body, per-item `input[i]` rows, per-tool rows, and schema findings. It does not store `Authorization` / `Cookie` headers. The `request_input_items` table makes fields such as `input[83].arguments` directly queryable by `type`, `name`, `arguments_type`, and `arguments_json_type`.

`max_body_bytes = 0` stores complete request bodies. Positive values retain only
that many bytes and mark larger bodies as truncated. With background writes
enabled, compression and SQLite body writes use one bounded FIFO; queue pressure
applies backpressure rather than silently dropping evidence. A single artifact
larger than the queue budget is written synchronously instead of being queued.
Failed body writes are retried once and recorded in `request_audit_failures`.

`request_audit_bodies` stores compressed body artifacts by stage:
`client_request_body`, `upstream_request_body`, `upstream_response_body`, and
`downstream_response_body`. By default, forwarded upstream request bodies are
stored, while response bodies are stored only for errors. Set
`store_response_body = "all"` temporarily when you need successful streaming SSE
prefixes as well. Rows older than `retention_days` are pruned from this audit DB
when a throttled prune pass is due. `sqlite_busy_timeout_ms` keeps diagnostic
logging from waiting indefinitely behind another SQLite user;
`prune_interval_seconds` and `prune_batch_size` bound cleanup work on the request
path. Use a short busy timeout and bounded periodic batches on production hosts.

## Compatibility normalization

Different Responses-compatible historical tool items require different
`arguments` shapes. Enable the compatibility transform when replayed history
contains older or upstream-specific shapes:

```toml
[compat]
normalize_input_arguments = true
synthesize_web_search_call_ids = true
max_output_tokens_compat = "keep"
reasoning_effort_compat = "keep"
```

When enabled, only configured `input[i]` item types are normalized. By default,
`function_call.arguments` objects are serialized to JSON strings, which is the
normal OpenAI Responses shape, while `tool_search_call.arguments` JSON strings
are strictly parsed to objects for upstreams that require that built-in-tool
history shape. Enable `synthesize_web_search_call_ids` to add stable `ws_...`
ids to historical `web_search_call` items missing `id`, because Responses
history replay requires an item id. Invalid JSON, non-object JSON, duplicate-key
objects, already-correct values, and unrelated fields are left unchanged. The
original request bytes remain in the audit DB, and conversion/skip decisions are
written to `request_compat_actions`.

`max_output_tokens` is the standard Responses field. Keep it for standard
Responses upstreams. Use `max_output_tokens_compat = "rename_to_max_tokens"`
only for an upstream that explicitly accepts the Chat Completions-style
`max_tokens` field on its Responses endpoint. Use `"drop"` for upstreams that
reject both field names; this removes `max_output_tokens` and any legacy
`max_tokens`, restoring compatibility but removing the requested output cap
before forwarding. Use `reasoning_effort_compat = "minimal_to_none"`
only for upstreams that reject `reasoning.effort = "minimal"` but accept
`"none"`.

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
