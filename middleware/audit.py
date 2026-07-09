"""Structured request-body audit logging.

The normal app logs intentionally stay small. This module writes a separate
SQLite database for request diagnosis: the original body can be stored
compressed, while input/tool rows make schema problems queryable without
opening a huge JSON payload.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import zlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .config import RequestLogCfg


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    if isinstance(value, str):
        return "string"
    if isinstance(value, (int, float)):
        return "number"
    return type(value).__name__


def _sha_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _preview(value: Any, limit: int) -> str:
    if limit <= 0:
        return ""
    if isinstance(value, str):
        text = value
    else:
        text = _json(value)
    return text if len(text) <= limit else text[:limit] + "...[truncated]"


def _text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (bool, int, float)):
        return str(value)
    return _json(value)


def _json_string_type(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        return _type_name(json.loads(value))
    except json.JSONDecodeError:
        return "invalid_json"


def _content_shape(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        return {"type": "string", "chars": len(value)}
    if isinstance(value, list):
        parts = []
        for i, part in enumerate(value[:30]):
            if isinstance(part, dict):
                entry: dict[str, Any] = {
                    "idx": i,
                    "type": part.get("type"),
                    "keys": sorted(str(k) for k in part.keys()),
                }
                for key in ("text", "input_text", "output_text"):
                    if isinstance(part.get(key), str):
                        entry[f"{key}_chars"] = len(part[key])
                parts.append(entry)
            else:
                parts.append({"idx": i, "type": _type_name(part)})
        return {"type": "array", "len": len(value), "parts": parts}
    if isinstance(value, dict):
        return {"type": "object", "keys": sorted(str(k) for k in value.keys())}
    return {"type": _type_name(value)}


def _tool_name(tool: dict[str, Any]) -> str | None:
    if isinstance(tool.get("name"), str):
        return tool["name"]
    fn = tool.get("function")
    if isinstance(fn, dict) and isinstance(fn.get("name"), str):
        return fn["name"]
    return None


class RequestAuditStore:
    def __init__(self, cfg: RequestLogCfg, root: Path):
        self.cfg = cfg
        db_path = Path(cfg.path)
        if not db_path.is_absolute():
            db_path = root / db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._ensure_schema()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _ensure_schema(self) -> None:
        with self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS request_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    trace_id TEXT,
                    request_id TEXT,
                    method TEXT NOT NULL,
                    path TEXT NOT NULL,
                    client_host TEXT,
                    user_agent TEXT,
                    content_type TEXT,
                    upstream_url TEXT,
                    decision TEXT,
                    model TEXT,
                    stream INTEGER,
                    body_bytes INTEGER NOT NULL,
                    body_sha256 TEXT NOT NULL,
                    raw_body_zlib BLOB,
                    raw_body_encoding TEXT,
                    raw_body_truncated INTEGER NOT NULL DEFAULT 0,
                    raw_body_original_bytes INTEGER NOT NULL,
                    raw_body_stored_bytes INTEGER NOT NULL,
                    parse_error TEXT,
                    top_level_keys_json TEXT,
                    input_count INTEGER,
                    tool_count INTEGER,
                    input_type_counts_json TEXT,
                    argument_type_counts_json TEXT,
                    upstream_status_code INTEGER,
                    downstream_status_code INTEGER,
                    response_error TEXT
                );

                CREATE TABLE IF NOT EXISTS request_input_items (
                    audit_id INTEGER NOT NULL REFERENCES request_audit(id) ON DELETE CASCADE,
                    idx INTEGER NOT NULL,
                    item_type TEXT,
                    role TEXT,
                    name TEXT,
                    call_id TEXT,
                    item_id TEXT,
                    status TEXT,
                    arguments_type TEXT,
                    arguments_json_type TEXT,
                    arguments_len INTEGER,
                    arguments_sha256 TEXT,
                    arguments_preview TEXT,
                    output_len INTEGER,
                    output_sha256 TEXT,
                    has_encrypted_content INTEGER NOT NULL DEFAULT 0,
                    content_shape_json TEXT,
                    keys_json TEXT,
                    PRIMARY KEY (audit_id, idx)
                );

                CREATE TABLE IF NOT EXISTS request_tools (
                    audit_id INTEGER NOT NULL REFERENCES request_audit(id) ON DELETE CASCADE,
                    idx INTEGER NOT NULL,
                    tool_type TEXT,
                    name TEXT,
                    keys_json TEXT,
                    raw_summary_json TEXT,
                    PRIMARY KEY (audit_id, idx)
                );

                CREATE TABLE IF NOT EXISTS request_schema_findings (
                    audit_id INTEGER NOT NULL REFERENCES request_audit(id) ON DELETE CASCADE,
                    idx INTEGER NOT NULL,
                    level TEXT NOT NULL,
                    path TEXT NOT NULL,
                    code TEXT NOT NULL,
                    message TEXT NOT NULL,
                    PRIMARY KEY (audit_id, idx)
                );

                CREATE TABLE IF NOT EXISTS request_compat_actions (
                    audit_id INTEGER NOT NULL REFERENCES request_audit(id) ON DELETE CASCADE,
                    idx INTEGER NOT NULL,
                    path TEXT NOT NULL,
                    action TEXT NOT NULL,
                    code TEXT NOT NULL,
                    message TEXT NOT NULL,
                    original_type TEXT,
                    parsed_type TEXT,
                    PRIMARY KEY (audit_id, idx)
                );

                CREATE INDEX IF NOT EXISTS idx_request_audit_created
                    ON request_audit(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_request_audit_trace
                    ON request_audit(trace_id);
                CREATE INDEX IF NOT EXISTS idx_request_audit_request
                    ON request_audit(request_id);
                CREATE INDEX IF NOT EXISTS idx_request_input_arg_type
                    ON request_input_items(arguments_type, arguments_json_type);
                CREATE INDEX IF NOT EXISTS idx_request_findings_code
                    ON request_schema_findings(code);
                CREATE INDEX IF NOT EXISTS idx_request_compat_actions_code
                    ON request_compat_actions(action, code);
                """
            )

    def record_request(
        self,
        *,
        trace_id: str,
        request_id: str,
        method: str,
        path: str,
        client_host: str | None,
        user_agent: str | None,
        content_type: str | None,
        upstream_url: str | None,
        decision: str,
        raw_body: bytes,
        body: dict[str, Any] | None,
        parse_error: str | None,
    ) -> int:
        created = _now()
        raw_prefix = raw_body[: max(0, int(self.cfg.max_body_bytes))]
        raw_body_zlib = zlib.compress(raw_prefix) if self.cfg.store_body else None
        body_truncated = int(len(raw_prefix) < len(raw_body))
        input_items = body.get("input") if isinstance(body, dict) else None
        tools = body.get("tools") if isinstance(body, dict) else None
        input_rows, findings = self._input_rows(input_items)
        findings.extend(self._top_level_findings(body))
        tool_rows = self._tool_rows(tools)
        input_type_counts = self._input_type_counts(input_items)
        argument_type_counts = self._argument_type_counts(input_rows)

        with self._lock, self._conn:
            cur = self._conn.execute(
                """
                INSERT INTO request_audit (
                    created_at, updated_at, trace_id, request_id, method, path,
                    client_host, user_agent, content_type, upstream_url, decision,
                    model, stream, body_bytes, body_sha256, raw_body_zlib,
                    raw_body_encoding, raw_body_truncated, raw_body_original_bytes,
                    raw_body_stored_bytes, parse_error, top_level_keys_json,
                    input_count, tool_count, input_type_counts_json,
                    argument_type_counts_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    created,
                    created,
                    trace_id,
                    request_id,
                    method,
                    path,
                    client_host,
                    user_agent,
                    content_type,
                    upstream_url,
                    decision,
                    _text_or_none(body.get("model")) if isinstance(body, dict) else None,
                    int(bool(body.get("stream"))) if isinstance(body, dict) else None,
                    len(raw_body),
                    _sha_bytes(raw_body),
                    raw_body_zlib,
                    "zlib+raw-prefix" if raw_body_zlib is not None else None,
                    body_truncated,
                    len(raw_body),
                    len(raw_prefix) if raw_body_zlib is not None else 0,
                    parse_error,
                    _json(sorted(str(k) for k in body.keys())) if isinstance(body, dict) else None,
                    len(input_items) if isinstance(input_items, list) else None,
                    len(tools) if isinstance(tools, list) else None,
                    _json(input_type_counts),
                    _json(argument_type_counts),
                ),
            )
            audit_id = int(cur.lastrowid)
            self._conn.executemany(
                """
                INSERT INTO request_input_items (
                    audit_id, idx, item_type, role, name, call_id, item_id, status,
                    arguments_type, arguments_json_type, arguments_len,
                    arguments_sha256, arguments_preview, output_len, output_sha256,
                    has_encrypted_content, content_shape_json, keys_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [(audit_id, *row) for row in input_rows],
            )
            self._conn.executemany(
                """
                INSERT INTO request_tools (
                    audit_id, idx, tool_type, name, keys_json, raw_summary_json
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [(audit_id, *row) for row in tool_rows],
            )
            self._insert_findings(audit_id, findings)
            return audit_id

    def update_response(
        self,
        audit_id: int,
        *,
        upstream_status_code: int | None = None,
        downstream_status_code: int | None = None,
        response_error: str | None = None,
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE request_audit
                SET updated_at = ?,
                    upstream_status_code = COALESCE(?, upstream_status_code),
                    downstream_status_code = COALESCE(?, downstream_status_code),
                    response_error = COALESCE(?, response_error)
                WHERE id = ?
                """,
                (_now(), upstream_status_code, downstream_status_code, response_error, audit_id),
            )

    def record_compat_actions(self, audit_id: int, actions: list[Any] | tuple[Any, ...]) -> None:
        if not actions:
            return
        rows = []
        for idx, action in enumerate(actions):
            rows.append(
                (
                    audit_id,
                    idx,
                    str(getattr(action, "path", "")),
                    str(getattr(action, "action", "")),
                    str(getattr(action, "code", "")),
                    str(getattr(action, "message", "")),
                    _text_or_none(getattr(action, "original_type", None)),
                    _text_or_none(getattr(action, "parsed_type", None)),
                )
            )
        with self._lock, self._conn:
            self._conn.executemany(
                """
                INSERT OR REPLACE INTO request_compat_actions (
                    audit_id, idx, path, action, code, message,
                    original_type, parsed_type
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def _input_rows(
        self, input_items: Any
    ) -> tuple[list[tuple[Any, ...]], list[tuple[str, str, str, str]]]:
        rows: list[tuple[Any, ...]] = []
        findings: list[tuple[str, str, str, str]] = []
        if input_items is None:
            return rows, findings
        if not isinstance(input_items, list):
            findings.append(("warn", "input", "input_not_array", "input is not an array"))
            return rows, findings

        for idx, item in enumerate(input_items):
            path = f"input[{idx}]"
            if not isinstance(item, dict):
                rows.append((idx, _type_name(item), None, None, None, None, None,
                             None, None, None, None, None, None, None, 0, None, None))
                findings.append(("warn", path, "input_item_not_object", "input item is not an object"))
                continue

            args = item.get("arguments")
            args_type = _type_name(args) if "arguments" in item else None
            args_json_type = _json_string_type(args)
            args_text = args if isinstance(args, str) else _json(args) if args is not None else None
            output = item.get("output")
            output_text = output if isinstance(output, str) else None
            content = item.get("content")
            rows.append((
                idx,
                _text_or_none(item.get("type")),
                _text_or_none(item.get("role")),
                _text_or_none(item.get("name")),
                _text_or_none(item.get("call_id")),
                _text_or_none(item.get("id")),
                _text_or_none(item.get("status")),
                args_type,
                args_json_type,
                len(args_text) if args_text is not None else None,
                _sha_text(args_text) if args_text is not None else None,
                _preview(args, self.cfg.preview_chars) if args is not None else None,
                len(output_text) if output_text is not None else None,
                _sha_text(output_text) if output_text is not None else None,
                int(bool(item.get("encrypted_content"))),
                _json(_content_shape(content)) if "content" in item else None,
                _json(sorted(str(k) for k in item.keys())),
            ))

            if "arguments" in item:
                arg_path = f"{path}.arguments"
                if isinstance(args, str):
                    level = "warn" if item.get("type") == "function_call" else "info"
                    findings.append((
                        level,
                        arg_path,
                        "arguments_string",
                        f"arguments is a JSON string; parsed_json_type={args_json_type}",
                    ))
                    if args_json_type == "invalid_json":
                        findings.append((
                            "warn",
                            arg_path,
                            "arguments_invalid_json",
                            "arguments is a string but cannot be parsed as JSON",
                        ))
                    elif args_json_type != "object":
                        findings.append((
                            "warn",
                            arg_path,
                            "arguments_json_not_object",
                            f"arguments string parses as {args_json_type}, not object",
                        ))
                elif item.get("type") == "function_call" and not isinstance(args, dict):
                    findings.append((
                        "warn",
                        arg_path,
                        "arguments_not_object",
                        f"function_call arguments is {args_type}, not object",
                    ))
        return rows, findings

    def _tool_rows(self, tools: Any) -> list[tuple[Any, ...]]:
        if not isinstance(tools, list):
            return []
        rows: list[tuple[Any, ...]] = []
        for idx, tool in enumerate(tools):
            if not isinstance(tool, dict):
                rows.append((idx, _type_name(tool), None, None, _json({"type": _type_name(tool)})))
                continue
            summary = {
                "type": tool.get("type"),
                "name": _tool_name(tool),
                "keys": sorted(str(k) for k in tool.keys()),
            }
            rows.append((
                idx,
                _text_or_none(tool.get("type")),
                _tool_name(tool),
                _json(sorted(str(k) for k in tool.keys())),
                _json(summary),
            ))
        return rows

    def _input_type_counts(self, input_items: Any) -> dict[str, int]:
        counts: dict[str, int] = {}
        if not isinstance(input_items, list):
            return counts
        for item in input_items:
            key = item.get("type") if isinstance(item, dict) else _type_name(item)
            key = str(key or "unknown")
            counts[key] = counts.get(key, 0) + 1
        return counts

    def _argument_type_counts(self, rows: list[tuple[Any, ...]]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in rows:
            arg_type = row[7]
            if not arg_type:
                continue
            key = str(arg_type)
            counts[key] = counts.get(key, 0) + 1
        return counts

    def _top_level_findings(
        self, body: dict[str, Any] | None
    ) -> list[tuple[str, str, str, str]]:
        if not isinstance(body, dict):
            return []
        findings: list[tuple[str, str, str, str]] = []
        if "max_output_tokens" in body:
            findings.append((
                "warn",
                "max_output_tokens",
                "top_level_max_output_tokens",
                "top-level max_output_tokens is present; some Responses upstreams reject it",
            ))
        tools = body.get("tools")
        if tools is not None and not isinstance(tools, list):
            findings.append(("warn", "tools", "tools_not_array", "tools is not an array"))
        input_items = body.get("input")
        if input_items is not None and not isinstance(input_items, list):
            findings.append(("warn", "input", "input_not_array", "input is not an array"))
        return findings

    def _insert_findings(
        self,
        audit_id: int,
        findings: list[tuple[str, str, str, str]],
    ) -> None:
        rows = [(audit_id, idx, *finding) for idx, finding in enumerate(findings)]
        self._conn.executemany(
            """
            INSERT INTO request_schema_findings (
                audit_id, idx, level, path, code, message
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
