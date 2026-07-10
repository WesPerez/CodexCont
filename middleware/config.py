"""Load config.toml into a frozen Config dataclass.

One central config controls everything. Missing keys fall back to the defaults
baked in here, so a partial (or absent) config.toml still works.
"""
from __future__ import annotations

import tomllib
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ServerCfg:
    host: str = "127.0.0.1"
    port: int = 8787
    # Hard application-layer cap. 0 disables the cap; keep Nginx capped too.
    max_request_body_bytes: int = 32 * 1024 * 1024
    listen_paths: tuple[str, ...] = (
        "/backend-api/codex/responses",
        "/v1/responses",
        "/responses",
    )


@dataclass(frozen=True)
class UpstreamCfg:
    url: str = "https://chatgpt.com/backend-api/codex/responses"
    # "fixed"           = always use `url`, ignore the Responses-API-Base header.
    # "header"          = use the Responses-API-Base header if present, else `url`.
    # "header_required" = require the header; if absent, reject the request (400).
    mode: str = "fixed"
    # Optional explicit header overrides applied LAST; empty by default so the
    # proxy is a pure header passthrough and invents nothing (no User-Agent).
    headers: dict[str, str] = field(default_factory=dict)
    # Header-selected upstreams are restricted to exact origins such as
    # "https://api.openai.com". Empty disables dynamic targets (secure default).
    dynamic_allowed_origins: tuple[str, ...] = ()
    # Fixed operator-configured URLs are unaffected by this private-IP policy.
    dynamic_allow_private_ips: bool = False
    # Ignore ambient HTTP(S)_PROXY by default so URL validation and connection
    # routing use the same host policy. Enable only for a trusted egress proxy.
    trust_env: bool = False


@dataclass(frozen=True)
class AuthCfg:
    mode: str = "passthrough_then_inject"  # passthrough | inject | passthrough_then_inject
    access_token: str = ""  # injected as `Authorization: Bearer <access_token>`
    chatgpt_account_id: str = ""  # injected as `chatgpt-account-id` (Codex only; empty = omit header)


@dataclass(frozen=True)
class ContinueCfg:
    enabled: bool = True
    # Empty = fold all models; otherwise only fold models whose name starts
    # with one of these prefixes. Non-matching models remain pure passthrough.
    model_prefixes: tuple[str, ...] = ()
    truncation_step: int = 518
    max_continue: int = 8  # hard round cap after round 1 (primary runaway guard)
    min_n: int = 1  # continue only when truncation tier n >= min_n
    max_n: int = 0  # 0 = no cap; else stop forcing once n > max_n
    retry_low_reasoning_after_continue: bool = False
    # Only used after this middleware has already forced a continuation. If the
    # next round ends cleanly but reasoning is still at/below this value, retry.
    min_continue_reasoning_tokens: int = 0
    method: str = "commentary"  # continuation provocation: "commentary" (default) | "tool_pair"
    marker_text: str = "Continue thinking..."  # commentary path: assistant message text
    forward_marker: bool = False  # commentary path: emit the marker downstream so the agent
    # echoes it back next turn (cross-turn structure + prompt-cache); false = hidden/clean.
    # --- tool_pair path only (legacy; used when method = "tool_pair") ---
    continue_tool_name: str = "continue_thinking"  # synthetic tool name + collision-bypass name
    continue_output_text: str = "Please continue thinking about the query."  # function_call_output
    repair_followup: str = "off"  # tool_pair cross-turn: "off" | "stateful" (id-keyed re-insert)
    max_total_output_tokens: int = 0  # optional cumulative cap (0 = off)


@dataclass(frozen=True)
class StreamCfg:
    force_include_encrypted: bool = True
    rechunk_final_answer: bool = True
    rechunk_size: int = 8
    # Seconds to wait for the next parsed SSE data event from upstream. Comments
    # such as ":" keepalives do not count as progress. 0 disables the guard.
    upstream_event_timeout_seconds: float = 0
    # Seconds to allow one upstream round to run in wall-clock time, even if it
    # keeps emitting parseable SSE events. 0 disables the guard.
    upstream_round_timeout_seconds: float = 0
    # HTTP transport guards. The read timeout also protects passthrough streams
    # and the wait for initial response headers; fold streams keep their stricter
    # parsed-event and round-level guards above.
    upstream_connect_timeout_seconds: float = 5
    upstream_read_timeout_seconds: float = 330
    upstream_write_timeout_seconds: float = 60
    upstream_pool_timeout_seconds: float = 5
    upstream_dns_timeout_seconds: float = 5
    upstream_error_body_timeout_seconds: float = 30
    upstream_error_body_max_bytes: int = 4 * 1024 * 1024
    upstream_max_connections: int = 256
    upstream_max_keepalive_connections: int = 64


@dataclass(frozen=True)
class LogCfg:
    level: str = "info"
    dump_rounds_dir: str = ""


@dataclass(frozen=True)
class RequestLogCfg:
    enabled: bool = False
    # Independent SQLite DB used for request-body audit records. Relative paths
    # resolve against the directory containing config.toml.
    path: str = "logs/request_audit.sqlite3"
    # Store the original request body as zlib-compressed bytes. The structured
    # summary tables are still written when this is false.
    store_body: bool = True
    # Bodies above this size are stored as a compressed prefix and marked
    # truncated; 0 disables audit truncation.
    max_body_bytes: int = 0
    # Persist large body artifacts through one FIFO writer so request forwarding
    # does not wait for compression and SQLite commits in the normal case.
    background_body_writes: bool = True
    # Bound body bytes retained by the writer queue. When full, enqueue applies
    # backpressure instead of silently dropping diagnostic evidence. 0 is unlimited.
    background_max_pending_bytes: int = 128 * 1024 * 1024
    # Remove audit rows older than this many days. 0 disables automatic pruning.
    retention_days: int = 7
    # Give up quickly when another process holds the audit DB. Auditing is
    # diagnostic and must not stall the online request path for seconds.
    sqlite_busy_timeout_ms: int = 5000
    # Throttle retention work. 0 preserves the legacy every-request behavior.
    prune_interval_seconds: int = 0
    # Maximum expired parent rows removed per prune pass. 0 removes all.
    prune_batch_size: int = 0
    # Also store the exact body sent upstream after compatibility transforms.
    store_forwarded_body: bool = True
    # Response-body capture: off | errors | all. Streaming bodies are capped by
    # max_response_body_bytes and stored compressed.
    store_response_body: str = "errors"
    max_response_body_bytes: int = 1024 * 1024
    # Preview characters for short fields such as function_call.arguments.
    # Set to 0 to store only length/hash/type.
    preview_chars: int = 240


@dataclass(frozen=True)
class CompatCfg:
    # Normalize selected historical call item argument shapes before forwarding.
    # function_call targets JSON string; tool_search_call targets JSON object.
    normalize_input_arguments: bool = False
    normalize_input_argument_item_types: tuple[str, ...] = ("function_call", "tool_search_call")
    # Synthesize stable ids for historical web_search_call items that are missing
    # the Responses-required `id`. None preserves the previous behavior: follow
    # normalize_input_arguments. Set true/false to control it independently.
    synthesize_web_search_call_ids: bool | None = None
    # How to handle top-level max_output_tokens when an upstream rejects that
    # standard Responses field: keep | drop | rename_to_max_tokens.
    max_output_tokens_compat: str = ""
    # Backward-compatible alias. Prefer max_output_tokens_compat="drop".
    drop_max_output_tokens: bool = False
    # How to handle reasoning.effort for upstream variants:
    # keep | minimal_to_none.
    reasoning_effort_compat: str = ""


@dataclass(frozen=True)
class Config:
    server: ServerCfg = field(default_factory=ServerCfg)
    upstream: UpstreamCfg = field(default_factory=UpstreamCfg)
    auth: AuthCfg = field(default_factory=AuthCfg)
    cont: ContinueCfg = field(default_factory=ContinueCfg)
    stream: StreamCfg = field(default_factory=StreamCfg)
    log: LogCfg = field(default_factory=LogCfg)
    request_log: RequestLogCfg = field(default_factory=RequestLogCfg)
    compat: CompatCfg = field(default_factory=CompatCfg)
    # Directory config.toml lived in (for resolving relative paths if needed).
    root: Path = field(default_factory=lambda: Path.cwd())


def _section(data: dict[str, Any], name: str) -> dict[str, Any]:
    val = data.get(name) or {}
    if not isinstance(val, dict):
        raise ValueError(f"config section [{name}] must be a table")
    return val


def _only_known(cls: type, data: dict[str, Any]) -> dict[str, Any]:
    """Keep only keys that map to dataclass fields (ignore stray keys)."""
    known = {f.name for f in cls.__dataclass_fields__.values()}  # type: ignore[attr-defined]
    return {k: v for k, v in data.items() if k in known}


def load_config(path: str | Path) -> Config:
    path = Path(path)
    data: dict[str, Any] = {}
    if path.exists():
        data = tomllib.loads(path.read_text(encoding="utf-8"))

    server = _section(data, "server")
    upstream = _section(data, "upstream")
    auth = _section(data, "auth")
    cont = _section(data, "continue")
    stream = _section(data, "stream")
    log = _section(data, "log")
    request_log = _section(data, "request_log")
    compat = _section(data, "compat")

    # listen_paths is a list in TOML; store as tuple.
    if "listen_paths" in server and isinstance(server["listen_paths"], list):
        server = {**server, "listen_paths": tuple(server["listen_paths"])}
    if "model_prefixes" in cont and isinstance(cont["model_prefixes"], list):
        cont = {**cont, "model_prefixes": tuple(str(x) for x in cont["model_prefixes"])}
    if (
        "dynamic_allowed_origins" in upstream
        and isinstance(upstream["dynamic_allowed_origins"], list)
    ):
        if not all(isinstance(value, str) for value in upstream["dynamic_allowed_origins"]):
            raise ValueError("upstream.dynamic_allowed_origins entries must be strings")
        upstream = {
            **upstream,
            "dynamic_allowed_origins": tuple(
                str(x) for x in upstream["dynamic_allowed_origins"]
            ),
        }
    if "dynamic_allow_private_ips" in upstream and not isinstance(
        upstream["dynamic_allow_private_ips"], bool
    ):
        raise ValueError("upstream.dynamic_allow_private_ips must be a boolean")
    if "trust_env" in upstream and not isinstance(upstream["trust_env"], bool):
        raise ValueError("upstream.trust_env must be a boolean")
    if (
        "normalize_input_argument_item_types" in compat
        and isinstance(compat["normalize_input_argument_item_types"], list)
    ):
        compat = {
            **compat,
            "normalize_input_argument_item_types": tuple(
                str(x) for x in compat["normalize_input_argument_item_types"]
            ),
        }

    # [upstream.headers] is a nested table under [upstream].
    up_headers = upstream.get("headers") or {}
    upstream = {k: v for k, v in upstream.items() if k != "headers"}
    upstream["headers"] = {str(k): str(v) for k, v in up_headers.items()}

    return Config(
        server=ServerCfg(**_only_known(ServerCfg, server)),
        upstream=UpstreamCfg(**_only_known(UpstreamCfg, upstream)),
        auth=AuthCfg(**_only_known(AuthCfg, auth)),
        cont=ContinueCfg(**_only_known(ContinueCfg, cont)),
        stream=StreamCfg(**_only_known(StreamCfg, stream)),
        log=LogCfg(**_only_known(LogCfg, log)),
        request_log=RequestLogCfg(**_only_known(RequestLogCfg, request_log)),
        compat=CompatCfg(**_only_known(CompatCfg, compat)),
        root=path.resolve().parent if path.exists() else Path.cwd(),
    )


def with_root(cfg: Config, root: Path) -> Config:
    return replace(cfg, root=root)
