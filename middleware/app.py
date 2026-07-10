"""Starlette app: route the agent's Responses request through the fold logic.

Only ACTS when continuation is enabled and the agent did not itself declare a
`continue_thinking` tool (collision rule). Otherwise it is a pure passthrough,
so it is safe in front of all traffic.
"""
from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import json
import logging
import math
import socket
import uuid
from collections.abc import AsyncIterator, Callable
from typing import Any

import httpx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from .audit import AuditBodyCapture, RequestAuditStore
from .codex import (
    build_round_payload,
    declares_continue_tool,
    reasoning_enabled,
    repair_followup_input,
)
from .compat import CompatAction, CompatResult, normalize_request_body
from .config import Config
from .creds import build_upstream_headers
from .proxy import fold_stream, open_passthrough, open_round
from .store import IdStore

log = logging.getLogger("middleware.app")


class _AuditBodyWriter:
    """Single-writer FIFO for compressed audit body artifacts."""

    def __init__(self, max_pending_bytes: int):
        self.max_pending_bytes = max(0, int(max_pending_bytes))
        self._pending_bytes = 0
        self._closed = False
        self._condition = asyncio.Condition()
        self._queue: asyncio.Queue[
            tuple[Callable[..., None], tuple[Any, ...], dict[str, Any], int] | None
        ] = asyncio.Queue()
        self._worker = asyncio.create_task(self._run(), name="codexcont-audit-body-writer")

    async def submit(
        self,
        func: Callable[..., None],
        *args: Any,
        pending_bytes: int = 0,
        **kwargs: Any,
    ) -> None:
        size = max(0, int(pending_bytes))
        if self.max_pending_bytes and size > self.max_pending_bytes:
            await self._execute(func, args, kwargs)
            return
        async with self._condition:
            while (
                not self._closed
                and self.max_pending_bytes
                and self._pending_bytes + size > self.max_pending_bytes
            ):
                await self._condition.wait()
            if self._closed:
                raise RuntimeError("audit body writer is closed")
            self._pending_bytes += size
        await self._queue.put((func, args, kwargs, size))

    @staticmethod
    def _write_context(
        func: Callable[..., None],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> tuple[RequestAuditStore | None, int | None, str, int]:
        store = getattr(func, "__self__", None)
        audit_id = args[0] if args and isinstance(args[0], int) else None
        operation = getattr(func, "__name__", "unknown")
        stage = str(kwargs.get("stage") or operation)
        ordinal = max(0, int(kwargs.get("ordinal") or 0))
        if operation == "record_client_body":
            stage = "client_request_body"
            ordinal = 0
        return store if isinstance(store, RequestAuditStore) else None, audit_id, stage, ordinal

    async def _execute(
        self,
        func: Callable[..., None],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        last_error: Exception | None = None
        for attempt in (1, 2):
            try:
                await asyncio.to_thread(func, *args, **kwargs)
                return
            except Exception as exc:
                last_error = exc
                if attempt == 1:
                    await asyncio.sleep(0.05)
        operation = getattr(func, "__name__", "unknown")
        log.error(
            "background audit body write failed after retry operation=%s error=%s",
            operation,
            last_error,
            exc_info=(
                type(last_error),
                last_error,
                last_error.__traceback__,
            ) if last_error is not None else None,
        )
        store, audit_id, stage, ordinal = self._write_context(func, args, kwargs)
        if store is None or audit_id is None:
            return
        try:
            await asyncio.to_thread(
                store.record_body_failure,
                audit_id,
                stage=stage,
                ordinal=ordinal,
                operation=operation,
                error=str(last_error or "unknown background audit error"),
                attempts=2,
            )
        except Exception:
            log.exception(
                "failed to persist background audit failure id=%s stage=%s ordinal=%s",
                audit_id,
                stage,
                ordinal,
            )

    async def _run(self) -> None:
        while True:
            item = await self._queue.get()
            if item is None:
                self._queue.task_done()
                return
            func, args, kwargs, size = item
            try:
                await self._execute(func, args, kwargs)
            finally:
                async with self._condition:
                    self._pending_bytes -= size
                    self._condition.notify_all()
                self._queue.task_done()

    async def aclose(self) -> None:
        async with self._condition:
            self._closed = True
            self._condition.notify_all()
        await self._queue.join()
        await self._queue.put(None)
        await self._worker


class RequestBodyTooLarge(Exception):
    def __init__(self, observed_bytes: int, max_bytes: int):
        super().__init__(f"request body exceeds {max_bytes} bytes")
        self.observed_bytes = observed_bytes
        self.max_bytes = max_bytes


class ResponseBodyTooLarge(Exception):
    pass


_RESPONSE_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-connection",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}


async def _read_request_body(request: Request, max_bytes: int) -> bytes:
    """Read a request body with an application-layer hard cap."""
    limit = int(max_bytes)
    if limit < 0:
        raise ValueError("max_request_body_bytes must be non-negative")
    content_length = request.headers.get("content-length")
    if limit and content_length:
        try:
            declared = int(content_length)
        except ValueError:
            declared = 0
        if declared > limit:
            raise RequestBodyTooLarge(declared, limit)

    chunks: list[bytes] = []
    observed = 0
    async for chunk in request.stream():
        observed += len(chunk)
        if limit and observed > limit:
            raise RequestBodyTooLarge(observed, limit)
        chunks.append(chunk)
    return b"".join(chunks)


async def _read_response_body(response: httpx.Response, max_bytes: int) -> bytes:
    limit = int(max_bytes)
    if limit < 0:
        raise ValueError("upstream_error_body_max_bytes must be non-negative")
    chunks: list[bytes] = []
    observed = 0
    async for chunk in response.aiter_bytes():
        observed += len(chunk)
        if limit and observed > limit:
            raise ResponseBodyTooLarge
        chunks.append(chunk)
    return b"".join(chunks)


def _header_pairs(headers: Any) -> list[tuple[bytes, bytes]]:
    raw = getattr(headers, "raw", None)
    if raw is not None:
        return [(bytes(name), bytes(value)) for name, value in raw]
    return [
        (str(name).encode("latin-1"), str(value).encode("latin-1"))
        for name, value in headers.items()
    ]


def _response_raw_headers(
    headers: Any,
    *,
    body_length: int | None = None,
    default_content_type: str | None = None,
    allowed_names: set[str] | None = None,
) -> list[tuple[bytes, bytes]]:
    """Forward end-to-end headers after httpx decoded the response body."""
    pairs = _header_pairs(headers)
    connection_tokens: set[str] = set()
    for name, value in pairs:
        if name.decode("latin-1").lower() == "connection":
            connection_tokens.update(
                x.strip().lower()
                for x in value.decode("latin-1").split(",")
                if x.strip()
            )

    out: list[tuple[bytes, bytes]] = []
    has_content_type = False
    for name, value in pairs:
        lname = name.decode("latin-1").lower()
        if lname in _RESPONSE_HOP_BY_HOP or lname in connection_tokens:
            continue
        if allowed_names is not None and lname not in allowed_names:
            continue
        # StreamingResponse owns framing, and httpx aiter_bytes()/aread() return
        # decoded bytes, so forwarding these values would describe the wrong body.
        if lname in {
            "content-length",
            "content-encoding",
            "content-md5",
            "content-digest",
            "digest",
            "etag",
            "content-range",
            "accept-ranges",
        }:
            continue
        if lname == "content-type":
            has_content_type = True
        out.append((lname.encode("latin-1"), value))
    if default_content_type is not None and not has_content_type:
        out.append((b"content-type", default_content_type.encode("latin-1")))
    if body_length is not None:
        out.append((b"content-length", str(body_length).encode("ascii")))
    return out


def _apply_upstream_headers(
    response: Response,
    headers: Any,
    *,
    body_length: int | None = None,
    default_content_type: str | None = None,
    allowed_names: set[str] | None = None,
) -> Response:
    response.raw_headers = _response_raw_headers(
        headers,
        body_length=body_length,
        default_content_type=default_content_type,
        allowed_names=allowed_names,
    )
    return response


def _canonical_origin(url: str) -> tuple[str, str, int] | None:
    if not url or "\\" in url or any(ord(char) <= 32 for char in url):
        return None
    try:
        parsed = httpx.URL(url)
        scheme = parsed.scheme.lower()
        host = parsed.raw_host.decode("ascii").rstrip(".").lower()
        port = parsed.port if parsed.port is not None else (443 if scheme == "https" else 80)
    except (UnicodeError, ValueError, httpx.InvalidURL):
        return None
    if (
        scheme not in {"http", "https"}
        or not host
        or bool(parsed.username)
        or bool(parsed.password)
        or parsed.query
        or parsed.fragment
        or "%" in host
        or port <= 0
        or port > 65535
    ):
        return None
    display_host = f"[{host}]" if ":" in host else host
    default_port = 443 if scheme == "https" else 80
    origin = f"{scheme}://{display_host}" + (f":{port}" if port != default_port else "")
    return origin, host, port


def _address_is_global(value: str) -> bool:
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return False
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        address = address.ipv4_mapped
    return address.is_global


def _validated_timeout(value: Any, name: str) -> float | None:
    seconds = float(value)
    if not math.isfinite(seconds) or seconds < 0:
        raise ValueError(f"{name} must be a finite non-negative number")
    return seconds if seconds > 0 else None


def _validated_nonnegative_int(value: Any, name: str) -> int:
    number = int(value)
    if number < 0:
        raise ValueError(f"{name} must be non-negative")
    return number


def _validated_positive_int(value: Any, name: str) -> int:
    number = int(value)
    if number <= 0:
        raise ValueError(f"{name} must be positive")
    return number


def _validate_config(cfg: Config) -> None:
    _validated_nonnegative_int(cfg.server.max_request_body_bytes, "max_request_body_bytes")
    for name in (
        "max_body_bytes",
        "background_max_pending_bytes",
        "retention_days",
        "sqlite_busy_timeout_ms",
        "prune_interval_seconds",
        "prune_batch_size",
        "max_response_body_bytes",
        "preview_chars",
    ):
        _validated_nonnegative_int(getattr(cfg.request_log, name), name)
    if not isinstance(cfg.request_log.background_body_writes, bool):
        raise ValueError("background_body_writes must be a boolean")
    _validated_nonnegative_int(
        cfg.stream.upstream_error_body_max_bytes,
        "upstream_error_body_max_bytes",
    )
    for name in (
        "upstream_connect_timeout_seconds",
        "upstream_read_timeout_seconds",
        "upstream_write_timeout_seconds",
        "upstream_pool_timeout_seconds",
        "upstream_dns_timeout_seconds",
        "upstream_error_body_timeout_seconds",
    ):
        _validated_timeout(getattr(cfg.stream, name), name)
    _validated_positive_int(cfg.stream.upstream_max_connections, "upstream_max_connections")
    _validated_positive_int(
        cfg.stream.upstream_max_keepalive_connections,
        "upstream_max_keepalive_connections",
    )
    if cfg.stream.upstream_max_keepalive_connections > cfg.stream.upstream_max_connections:
        raise ValueError("upstream_max_keepalive_connections cannot exceed upstream_max_connections")
    for origin in cfg.upstream.dynamic_allowed_origins:
        parsed = httpx.URL(origin)
        if _canonical_origin(origin) is None or parsed.raw_path != b"/":
            raise ValueError(f"invalid dynamic upstream origin: {origin!r}")


async def _upstream_url_error(cfg: Config, url: str, *, from_header: bool) -> str | None:
    if not from_header:
        try:
            fixed = httpx.URL(url)
        except (ValueError, httpx.InvalidURL):
            return "invalid upstream URL"
        if fixed.scheme not in {"http", "https"} or not fixed.raw_host:
            return "invalid upstream URL"
        return None

    target = _canonical_origin(url)
    if target is None:
        return "invalid upstream URL"

    origin, host, port = target
    allowed: set[str] = set()
    for configured in cfg.upstream.dynamic_allowed_origins:
        parsed = _canonical_origin(configured)
        if parsed is not None:
            allowed.add(parsed[0])
    if origin not in allowed:
        return "dynamic upstream origin is not allowed"
    if cfg.upstream.dynamic_allow_private_ips is True:
        return None
    if host == "localhost":
        return "private or loopback upstream addresses are not allowed"

    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        literal = None
    if literal is not None:
        return None if _address_is_global(host) else "private or loopback upstream addresses are not allowed"

    try:
        resolved = await asyncio.wait_for(
            asyncio.to_thread(
                socket.getaddrinfo,
                host,
                port,
                type=socket.SOCK_STREAM,
            ),
            timeout=_validated_timeout(
                cfg.stream.upstream_dns_timeout_seconds,
                "upstream_dns_timeout_seconds",
            ),
        )
    except (OSError, asyncio.TimeoutError):
        return "dynamic upstream hostname could not be resolved"
    addresses = {entry[4][0] for entry in resolved if entry[4]}
    if not addresses or any(not _address_is_global(address) for address in addresses):
        return "dynamic upstream resolved to a non-public address"
    return None


def _header_base(request: Request) -> str | None:
    """The non-blank Responses-API-Base header value, or None (case-insensitive)."""
    v = request.headers.get("responses-api-base")
    v = v.strip() if v else ""
    return v or None


def _join_responses(base: str) -> str:
    """Build the Responses endpoint from a base URL (OpenAI base_url convention:
    `<base>/responses`). Lenient: if the value already ends in `/responses`
    (a full endpoint was passed), use it as-is."""
    base = base.rstrip("/")
    return base if base.endswith("/responses") else base + "/responses"


def _resolve_upstream_url(cfg: Config, request: Request) -> str | None:
    """Target URL for this request.

    - "fixed": always the configured URL (header ignored).
    - "header": the Responses-API-Base header (case-insensitive) is treated as a
      base URL and `/responses` is appended; overrides the configured URL when
      present, else the configured URL.
    - "header_required": the header MUST be present; returns None when it is
      absent/blank so the caller can reject the request (400).

    The header is stripped before forwarding upstream (build_upstream_headers).
    """
    if cfg.upstream.mode in ("header", "header_required"):
        base = _header_base(request)
        if base:
            return _join_responses(base)
        if cfg.upstream.mode == "header_required":
            return None
    return cfg.upstream.url


def _url_is_from_header(cfg: Config, request: Request) -> bool:
    return cfg.upstream.mode in ("header", "header_required") and _header_base(request) is not None


def _model_allowed_to_fold(cfg: Config, model: Any) -> bool:
    prefixes = cfg.cont.model_prefixes
    if not prefixes:
        return True
    return isinstance(model, str) and model.startswith(prefixes)


def _request_trace_id(request: Request) -> str:
    for name in ("x-client-request-id", "x-request-id"):
        value = request.headers.get(name)
        if value and value.strip():
            return value.strip()
    return str(uuid.uuid4())


async def _passthrough(
    client: httpx.AsyncClient,
    cfg: Config,
    request: Request,
    raw: bytes,
    url: str,
    audit_id: int | None = None,
):
    """Pure proxy: forward the raw request and stream the raw response back."""
    headers = build_upstream_headers(
        request.headers.items(),
        cfg,
        allow_config_credentials=not _url_is_from_header(cfg, request),
    )
    try:
        resp = await open_passthrough(client, url, raw, headers)
    except httpx.TimeoutException:
        log.warning("upstream timeout path=%s url=%s", request.url.path, url)
        await _audit_response(
            request,
            audit_id,
            downstream_status_code=504,
            response_error="upstream timeout",
        )
        return JSONResponse({"error": "upstream timeout"}, status_code=504)
    except httpx.HTTPError:
        log.warning("upstream connection error path=%s url=%s", request.url.path, url)
        await _audit_response(
            request,
            audit_id,
            downstream_status_code=502,
            response_error="upstream connection error",
        )
        return JSONResponse({"error": "upstream connection error"}, status_code=502)
    await _audit_response(
        request,
        audit_id,
        upstream_status_code=resp.status_code,
        downstream_status_code=resp.status_code,
    )

    async def body_iter():
        try:
            async for chunk in _capture_response_stream(
                request,
                audit_id,
                resp.aiter_bytes(),
                stages=("upstream_response_body", "downstream_response_body"),
                ordinal=1,
                status_code=resp.status_code,
                content_type=resp.headers.get("content-type"),
                force_possible=True,
            ):
                yield chunk
        except httpx.TimeoutException:
            log.warning("upstream read timeout path=%s url=%s", request.url.path, url)
            await _audit_response(
                request,
                audit_id,
                response_error="upstream read timeout after response headers",
            )
            raise
        except httpx.HTTPError:
            log.warning("upstream read error path=%s url=%s", request.url.path, url)
            await _audit_response(
                request,
                audit_id,
                response_error="upstream read error after response headers",
            )
            raise
        finally:
            await resp.aclose()

    downstream = StreamingResponse(
        body_iter(),
        status_code=resp.status_code,
        media_type=None,
    )
    return _apply_upstream_headers(
        downstream,
        resp.headers,
        default_content_type="text/event-stream",
    )


def _audit_store(request: Request) -> RequestAuditStore | None:
    return getattr(request.app.state, "request_audit", None)


def _audit_body_writer(request: Request) -> _AuditBodyWriter | None:
    return getattr(request.app.state, "request_audit_body_writer", None)


async def _audit_request(
    request: Request,
    *,
    trace_id: str,
    request_id: str,
    raw: bytes,
    body: dict[str, Any] | None,
    parse_error: str | None,
    upstream_url: str | None,
    decision: str,
) -> int | None:
    store = _audit_store(request)
    if store is None:
        return None
    try:
        client_host = request.client.host if request.client else None
        writer = _audit_body_writer(request)
        audit_id = await asyncio.to_thread(
            store.record_request,
            trace_id=trace_id,
            request_id=request_id,
            method=request.method,
            path=request.url.path,
            client_host=client_host,
            user_agent=request.headers.get("user-agent"),
            content_type=request.headers.get("content-type"),
            upstream_url=upstream_url,
            decision=decision,
            raw_body=raw,
            body=body,
            parse_error=parse_error,
            store_body_inline=writer is None,
        )
        if writer is not None and store.cfg.store_body:
            await writer.submit(
                store.record_client_body,
                audit_id,
                body=raw,
                content_type=request.headers.get("content-type"),
                pending_bytes=len(raw),
            )
        return audit_id
    except Exception:
        log.exception("request audit insert failed trace=%s req=%s", trace_id, request_id)
        return None


async def _audit_response(
    request: Request,
    audit_id: int | None,
    *,
    upstream_status_code: int | None = None,
    downstream_status_code: int | None = None,
    response_error: str | None = None,
) -> None:
    if audit_id is None:
        return
    store = _audit_store(request)
    if store is None:
        return
    try:
        await asyncio.to_thread(
            store.update_response,
            audit_id,
            upstream_status_code=upstream_status_code,
            downstream_status_code=downstream_status_code,
            response_error=response_error,
        )
    except Exception:
        log.exception("request audit update failed id=%s", audit_id)


async def _audit_compat_actions(
    request: Request,
    audit_id: int | None,
    actions: tuple[CompatAction, ...],
) -> None:
    if audit_id is None or not actions:
        return
    store = _audit_store(request)
    if store is None:
        return
    try:
        await asyncio.to_thread(store.record_compat_actions, audit_id, actions)
    except Exception:
        log.exception("request compat audit failed id=%s", audit_id)


async def _audit_body(
    request: Request,
    audit_id: int | None,
    *,
    stage: str,
    body: bytes,
    content_type: str | None = None,
    ordinal: int = 0,
    max_bytes: int | None = None,
) -> None:
    if audit_id is None:
        return
    store = _audit_store(request)
    if store is None:
        return
    try:
        writer = _audit_body_writer(request)
        if writer is not None:
            await writer.submit(
                store.record_body,
                audit_id,
                stage=stage,
                body=body,
                content_type=content_type,
                ordinal=ordinal,
                max_bytes=max_bytes,
                pending_bytes=len(body),
            )
            return
        await asyncio.to_thread(
            store.record_body,
            audit_id,
            stage=stage,
            body=body,
            content_type=content_type,
            ordinal=ordinal,
            max_bytes=max_bytes,
        )
    except Exception:
        log.exception("request body audit failed id=%s stage=%s", audit_id, stage)


async def _audit_forwarded_body(
    request: Request,
    audit_id: int | None,
    body: bytes,
    *,
    ordinal: int,
) -> None:
    cfg: Config = request.app.state.cfg
    if not cfg.request_log.store_forwarded_body:
        return
    await _audit_body(
        request,
        audit_id,
        stage="upstream_request_body",
        body=body,
        content_type=request.headers.get("content-type") or "application/json",
        ordinal=ordinal,
        max_bytes=cfg.request_log.max_body_bytes,
    )


def _should_capture_response(
    request: Request,
    status_code: int | None,
    *,
    force: bool = False,
    force_possible: bool = False,
) -> bool:
    store = _audit_store(request)
    return bool(
        store
        and store.should_capture_response_body(status_code, force_possible=force_possible)
    ) or bool(store and force and store.should_record_response_body(status_code, force=True))


def _response_chunk_has_error_event(chunk: bytes) -> bool:
    return b"response.incomplete" in chunk or b"response.failed" in chunk


async def _audit_capture(
    request: Request,
    audit_id: int | None,
    *,
    stage: str,
    capture: AuditBodyCapture,
    content_type: str | None = None,
    ordinal: int = 0,
) -> None:
    if audit_id is None:
        return
    store = _audit_store(request)
    if store is None:
        return
    try:
        writer = _audit_body_writer(request)
        if writer is not None:
            await writer.submit(
                store.record_captured_body,
                audit_id,
                stage=stage,
                capture=capture,
                content_type=content_type,
                ordinal=ordinal,
                pending_bytes=capture.stored_bytes,
            )
            return
        await asyncio.to_thread(
            store.record_captured_body,
            audit_id,
            stage=stage,
            capture=capture,
            content_type=content_type,
            ordinal=ordinal,
        )
    except Exception:
        log.exception("captured body audit failed id=%s stage=%s", audit_id, stage)


async def _audit_response_body(
    request: Request,
    audit_id: int | None,
    *,
    stage: str,
    body: bytes,
    status_code: int | None,
    content_type: str | None = None,
    ordinal: int = 0,
    force: bool = False,
) -> None:
    if not _should_capture_response(request, status_code, force=force):
        return
    cfg: Config = request.app.state.cfg
    await _audit_body(
        request,
        audit_id,
        stage=stage,
        body=body,
        content_type=content_type,
        ordinal=ordinal,
        max_bytes=cfg.request_log.max_response_body_bytes,
    )


async def _capture_response_stream(
    request: Request,
    audit_id: int | None,
    source: AsyncIterator[bytes],
    *,
    stages: tuple[str, ...],
    ordinal: int,
    status_code: int | None,
    content_type: str | None,
    force: bool = False,
    force_possible: bool = False,
) -> AsyncIterator[bytes]:
    cfg: Config = request.app.state.cfg
    capture = (
        AuditBodyCapture(cfg.request_log.max_response_body_bytes)
        if _should_capture_response(
            request,
            status_code,
            force=force,
            force_possible=force_possible,
        )
        else None
    )
    try:
        async for chunk in source:
            if capture is not None:
                capture.add(chunk)
                if _response_chunk_has_error_event(chunk):
                    capture.mark_force_store()
            yield chunk
    finally:
        store = _audit_store(request)
        should_store = bool(
            capture is not None
            and store is not None
            and store.should_record_response_body(
                status_code,
                force=force or capture.force_store,
            )
        )
        if should_store and capture is not None:
            for stage in stages:
                await _audit_capture(
                    request,
                    audit_id,
                    stage=stage,
                    capture=capture,
                    content_type=content_type,
                    ordinal=ordinal,
                )


def _log_compat_result(
    result: CompatResult,
    *,
    trace_id: str,
    request_id: str,
    phase: str,
) -> None:
    if not result.actions:
        return
    normalized = [a.path for a in result.actions if a.action == "normalized"]
    skipped = [f"{a.path}:{a.code}" for a in result.actions if a.action == "skipped"]
    log.info(
        "compat normalize trace=%s req=%s phase=%s changed=%d skipped=%d "
        "normalized=%s skipped_paths=%s",
        trace_id,
        request_id,
        phase,
        result.changed_count,
        result.skipped_count,
        normalized[:20],
        skipped[:20],
    )


def _body_bytes(body: dict[str, Any]) -> bytes:
    return json.dumps(body, ensure_ascii=False).encode("utf-8")


async def handle_responses(request: Request) -> Response:
    cfg: Config = request.app.state.cfg
    client: httpx.AsyncClient = request.app.state.client

    trace_id = _request_trace_id(request)
    request_id = str(uuid.uuid4())
    try:
        raw = await _read_request_body(request, cfg.server.max_request_body_bytes)
    except RequestBodyTooLarge as exc:
        log.warning(
            "request body too large trace=%s req=%s path=%s observed=%d max=%d",
            trace_id,
            request_id,
            request.url.path,
            exc.observed_bytes,
            exc.max_bytes,
        )
        audit_id = await _audit_request(
            request,
            trace_id=trace_id,
            request_id=request_id,
            raw=b"",
            body=None,
            parse_error=f"body_too_large:{exc.observed_bytes}",
            upstream_url=None,
            decision="reject:body_too_large",
        )
        await _audit_response(
            request,
            audit_id,
            downstream_status_code=413,
            response_error="request body too large",
        )
        return JSONResponse(
            {"error": "request body too large", "max_bytes": exc.max_bytes},
            status_code=413,
        )
    try:
        body: dict[str, Any] = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        audit_id = await _audit_request(
            request,
            trace_id=trace_id,
            request_id=request_id,
            raw=raw,
            body=None,
            parse_error="invalid_json",
            upstream_url=None,
            decision="reject:invalid_json",
        )
        await _audit_response(
            request, audit_id, downstream_status_code=400, response_error="invalid JSON body"
        )
        return JSONResponse({"error": "invalid JSON body"}, status_code=400)
    if not isinstance(body, dict):
        audit_id = await _audit_request(
            request,
            trace_id=trace_id,
            request_id=request_id,
            raw=raw,
            body=None,
            parse_error="body_not_object",
            upstream_url=None,
            decision="reject:body_not_object",
        )
        await _audit_response(
            request, audit_id, downstream_status_code=400, response_error="body must be a JSON object"
        )
        return JSONResponse({"error": "body must be a JSON object"}, status_code=400)

    url = _resolve_upstream_url(cfg, request)
    if url is None:
        audit_id = await _audit_request(
            request,
            trace_id=trace_id,
            request_id=request_id,
            raw=raw,
            body=body,
            parse_error=None,
            upstream_url=None,
            decision="reject:missing_upstream_header",
        )
        await _audit_response(
            request, audit_id, downstream_status_code=400,
            response_error="Responses-API-Base header is required",
        )
        return JSONResponse(
            {"error": "Responses-API-Base header is required (upstream mode=header_required)"},
            status_code=400,
        )

    url_from_header = _url_is_from_header(cfg, request)
    url_error = await _upstream_url_error(cfg, url, from_header=url_from_header)
    if url_error is not None:
        audit_id = await _audit_request(
            request,
            trace_id=trace_id,
            request_id=request_id,
            raw=raw,
            body=body,
            parse_error=None,
            upstream_url=None,
            decision="reject:invalid_upstream_url",
        )
        await _audit_response(
            request,
            audit_id,
            downstream_status_code=400,
            response_error=url_error,
        )
        return JSONResponse({"error": url_error}, status_code=400)

    # Fold only a streaming, reasoning-enabled request that isn't a collision.
    # Everything else (non-reasoning, non-streaming, continuation disabled, or
    # the agent declaring its own continue_thinking) is a pure passthrough.
    # The collision rule only matters for the tool_pair method (we inject a tool);
    # commentary injects no tool, so a declared continue_thinking is irrelevant.
    collision = (
        cfg.cont.method == "tool_pair"
        and declares_continue_tool(body, cfg.cont.continue_tool_name)
    )
    model_allowed = _model_allowed_to_fold(cfg, body.get("model"))
    should_fold = (
        cfg.cont.enabled
        and bool(body.get("stream"))
        and reasoning_enabled(body)
        and model_allowed
        and not collision
    )
    if not should_fold:
        why = ("disabled" if not cfg.cont.enabled
               else "non-stream" if not body.get("stream")
               else "non-reasoning" if not reasoning_enabled(body)
               else "model-not-matched" if not model_allowed
               else "declares-continue_thinking")
        audit_id = await _audit_request(
            request,
            trace_id=trace_id,
            request_id=request_id,
            raw=raw,
            body=body,
            parse_error=None,
            upstream_url=url,
            decision=f"passthrough:{why}",
        )
        compat = normalize_request_body(body, cfg.compat)
        _log_compat_result(compat, trace_id=trace_id, request_id=request_id, phase="passthrough")
        await _audit_compat_actions(request, audit_id, compat.actions)
        forward_raw = _body_bytes(compat.body) if compat.changed_count else raw
        await _audit_forwarded_body(request, audit_id, forward_raw, ordinal=1)
        log.info("passthrough (%s): model=%s path=%s url=%s",
                 why, body.get("model"), request.url.path, url)
        return await _passthrough(client, cfg, request, forward_raw, url, audit_id=audit_id)

    audit_id = await _audit_request(
        request,
        trace_id=trace_id,
        request_id=request_id,
        raw=raw,
        body=body,
        parse_error=None,
        upstream_url=url,
        decision="fold",
    )
    log.info("fold start trace=%s req=%s: model=%s path=%s url=%s input_items=%d",
             trace_id, request_id, body.get("model"), request.url.path, url,
             len(body.get("input") or []))

    compat = normalize_request_body(body, cfg.compat)
    _log_compat_result(compat, trace_id=trace_id, request_id=request_id, phase="fold-request")
    await _audit_compat_actions(request, audit_id, compat.actions)
    body = compat.body

    # repair_followup="stateful": re-insert tool_pair continue pairs after recorded
    # ids (tool_pair only — commentary preserves cross-turn structure via forward_marker).
    if cfg.cont.repair_followup == "stateful" and cfg.cont.method == "tool_pair":
        body = {
            **body,
            "input": repair_followup_input(
                list(body.get("input") or []),
                request.app.state.id_store,
                tool_name=cfg.cont.continue_tool_name,
                output_text=cfg.cont.continue_output_text,
            ),
        }

    headers = build_upstream_headers(
        request.headers.items(),
        cfg,
        allow_config_credentials=not url_from_header,
    )
    payload = build_round_payload(
        body,
        input_items=list(body.get("input") or []),
        force_include_encrypted=cfg.stream.force_include_encrypted,
        drop_previous_response_id=False,  # round 1 passes it through
    )
    payload_compat = normalize_request_body(payload, cfg.compat)
    _log_compat_result(
        payload_compat,
        trace_id=trace_id,
        request_id=request_id,
        phase="fold-round-1",
    )
    payload = payload_compat.body
    payload_raw = _body_bytes(payload)
    await _audit_forwarded_body(request, audit_id, payload_raw, ordinal=1)

    async def audit_continuation_request(round_no: int, body_bytes: bytes) -> None:
        await _audit_forwarded_body(request, audit_id, body_bytes, ordinal=round_no)

    def make_upstream_response_capture(
        round_no: int,
        status_code: int | None,
        content_type: str | None,
    ) -> AuditBodyCapture | None:
        if not _should_capture_response(request, status_code, force_possible=True):
            return None
        return AuditBodyCapture(cfg.request_log.max_response_body_bytes)

    async def audit_upstream_response_capture(
        round_no: int,
        status_code: int | None,
        content_type: str | None,
        capture: AuditBodyCapture,
    ) -> None:
        store = _audit_store(request)
        if store is None or not store.should_record_response_body(
            status_code,
            force=capture.force_store,
        ):
            return
        await _audit_capture(
            request,
            audit_id,
            stage="upstream_response_body",
            capture=capture,
            content_type=content_type,
            ordinal=round_no,
        )

    async def audit_upstream_response_bytes(
        round_no: int,
        status_code: int | None,
        content_type: str | None,
        body_bytes: bytes,
        force: bool = False,
    ) -> None:
        await _audit_response_body(
            request,
            audit_id,
            stage="upstream_response_body",
            body=body_bytes,
            status_code=status_code,
            content_type=content_type,
            ordinal=round_no,
            force=force,
        )

    # Open round 1 here so a non-2xx (e.g. bad auth) is mirrored with its real
    # status code rather than buried inside a 200 SSE stream.
    try:
        resp = await open_round(client, url, payload, headers)
    except httpx.TimeoutException:
        log.warning("upstream timeout trace=%s req=%s url=%s", trace_id, request_id, url)
        await _audit_response(
            request,
            audit_id,
            downstream_status_code=504,
            response_error="upstream timeout",
        )
        return JSONResponse({"error": "upstream timeout"}, status_code=504)
    except httpx.HTTPError:
        log.warning("upstream connection error trace=%s req=%s url=%s", trace_id, request_id, url)
        await _audit_response(
            request,
            audit_id,
            downstream_status_code=502,
            response_error="upstream connection error",
        )
        return JSONResponse({"error": "upstream connection error"}, status_code=502)
    await _audit_response(
        request,
        audit_id,
        upstream_status_code=resp.status_code,
        downstream_status_code=resp.status_code if resp.status_code >= 400 else 200,
    )
    if resp.status_code >= 400:
        try:
            total_timeout = _validated_timeout(
                cfg.stream.upstream_error_body_timeout_seconds,
                "upstream_error_body_timeout_seconds",
            )
            if total_timeout is None:
                err = await _read_response_body(
                    resp,
                    cfg.stream.upstream_error_body_max_bytes,
                )
            else:
                async with asyncio.timeout(total_timeout):
                    err = await _read_response_body(
                        resp,
                        cfg.stream.upstream_error_body_max_bytes,
                    )
        except (asyncio.TimeoutError, httpx.TimeoutException):
            log.warning(
                "upstream error body timeout trace=%s req=%s status=%d",
                trace_id,
                request_id,
                resp.status_code,
            )
            await _audit_response(
                request,
                audit_id,
                downstream_status_code=504,
                response_error="upstream error body timeout",
            )
            return JSONResponse({"error": "upstream error body timeout"}, status_code=504)
        except ResponseBodyTooLarge:
            log.warning(
                "upstream error body too large trace=%s req=%s status=%d",
                trace_id,
                request_id,
                resp.status_code,
            )
            await _audit_response(
                request,
                audit_id,
                downstream_status_code=502,
                response_error="upstream error body too large",
            )
            return JSONResponse({"error": "upstream error body too large"}, status_code=502)
        except httpx.HTTPError:
            log.warning(
                "upstream error body read failed trace=%s req=%s status=%d",
                trace_id,
                request_id,
                resp.status_code,
            )
            await _audit_response(
                request,
                audit_id,
                downstream_status_code=502,
                response_error="upstream error body read failed",
            )
            return JSONResponse({"error": "upstream error body read failed"}, status_code=502)
        finally:
            await resp.aclose()

        content_type = resp.headers.get("content-type")
        await _audit_response_body(
            request,
            audit_id,
            stage="upstream_response_body",
            body=err,
            status_code=resp.status_code,
            content_type=content_type,
            ordinal=1,
        )
        await _audit_response_body(
            request,
            audit_id,
            stage="downstream_response_body",
            body=err,
            status_code=resp.status_code,
            content_type=content_type,
            ordinal=1,
        )
        downstream_error = Response(
            err,
            status_code=resp.status_code,
            media_type=None,
        )
        return _apply_upstream_headers(
            downstream_error,
            resp.headers,
            body_length=len(err),
        )

    downstream = StreamingResponse(
        _capture_response_stream(
            request,
            audit_id,
            fold_stream(
                client,
                cfg,
                body,
                headers,
                resp,
                request.app.state.id_store,
                url=url,
                trace_id=trace_id,
                request_id=request_id,
                audit_upstream_request_body=audit_continuation_request,
                make_upstream_response_capture=make_upstream_response_capture,
                audit_upstream_response_capture=audit_upstream_response_capture,
                audit_upstream_response_bytes=audit_upstream_response_bytes,
            ),
            stages=("downstream_response_body",),
            ordinal=1,
            status_code=200,
            content_type="text/event-stream",
            force_possible=True,
        ),
        media_type="text/event-stream",
    )
    # A folded response may contain hidden additional rounds, so first-round
    # rate-limit headers would be misleading. Preserve only correlation headers.
    return _apply_upstream_headers(
        downstream,
        resp.headers,
        default_content_type="text/event-stream",
        allowed_names={"x-request-id", "openai-request-id", "request-id", "traceparent"},
    )


def _httpx_timeout(value: float) -> float | None:
    return _validated_timeout(value, "HTTPX timeout")


def _make_client(cfg: Config | None = None) -> httpx.AsyncClient:
    """A client that does NOT invent a User-Agent or Accept of its own; those
    are forwarded from the agent or omitted. httpx still manages Host /
    Content-Length / Accept-Encoding / Connection (plan-allowed)."""
    cfg = cfg or Config()
    client = httpx.AsyncClient(
        timeout=httpx.Timeout(
            connect=_httpx_timeout(cfg.stream.upstream_connect_timeout_seconds),
            read=_httpx_timeout(cfg.stream.upstream_read_timeout_seconds),
            write=_httpx_timeout(cfg.stream.upstream_write_timeout_seconds),
            pool=_httpx_timeout(cfg.stream.upstream_pool_timeout_seconds),
        ),
        limits=httpx.Limits(
            max_connections=_validated_positive_int(
                cfg.stream.upstream_max_connections,
                "upstream_max_connections",
            ),
            max_keepalive_connections=_validated_positive_int(
                cfg.stream.upstream_max_keepalive_connections,
                "upstream_max_keepalive_connections",
            ),
        ),
        follow_redirects=False,
        trust_env=cfg.upstream.trust_env is True,
    )
    for h in ("user-agent", "accept"):
        if h in client.headers:
            del client.headers[h]
    return client


def create_app(cfg: Config) -> Starlette:
    _validate_config(cfg)

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette):
        app.state.cfg = cfg
        app.state.client = _make_client(cfg)
        app.state.id_store = IdStore()
        app.state.request_audit = (
            RequestAuditStore(cfg.request_log, cfg.root)
            if cfg.request_log.enabled else None
        )
        app.state.request_audit_body_writer = (
            _AuditBodyWriter(cfg.request_log.background_max_pending_bytes)
            if app.state.request_audit is not None
            and cfg.request_log.background_body_writes
            else None
        )
        try:
            yield
        finally:
            if app.state.request_audit_body_writer is not None:
                await app.state.request_audit_body_writer.aclose()
            if app.state.request_audit is not None:
                app.state.request_audit.close()
            await app.state.client.aclose()

    routes = [
        Route(path, handle_responses, methods=["POST"]) for path in cfg.server.listen_paths
    ]
    return Starlette(routes=routes, lifespan=lifespan)
