#!/usr/bin/env python3
"""Offline tests for the continue_thinking middleware.

Run: .venv/Scripts/python.exe tests/test_middleware.py
No pytest dependency — a tiny runner prints PASS/FAIL per check.
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
import tempfile
import zlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = Path(__file__).resolve().parent / "fixtures"
sys.path.insert(0, str(ROOT))

from starlette.datastructures import Headers

from middleware.app import (
    _make_client,
    _model_allowed_to_fold,
    _resolve_upstream_url,
    _url_is_from_header,
)
from middleware.audit import AuditBodyCapture, RequestAuditStore
from middleware.codex import (
    continue_call_id,
    is_truncation_pattern,
    reasoning_enabled,
    repair_followup_input,
    should_continue,
    tier_n,
)
from middleware.compat import normalize_request_body
from middleware.config import CompatCfg, RequestLogCfg, load_config
from middleware.creds import build_upstream_headers, would_inject_authorization
from middleware.proxy import fold_stream
from middleware.sse import DONE, incremental_sse
from middleware.store import IdStore


# --- helpers ----------------------------------------------------------------


def make_sse(events: list[dict]) -> bytes:
    out = b""
    for ev in events:
        out += f"event: {ev['type']}\r\n".encode()
        out += b"data: " + json.dumps(ev).encode() + b"\r\n\r\n"
    return out


async def _aiter_once(data: bytes):
    yield data


async def parse_events(data: bytes) -> list:
    evs = []
    async for e in incremental_sse(_aiter_once(data)):
        evs.append(e)
    return evs


class FakeResp:
    def __init__(self, data: bytes, status: int = 200, chunk: int = 4096):
        self._data = data
        self.status_code = status
        self.headers: dict[str, str] = {}
        self._chunk = chunk

    async def aiter_bytes(self):
        for i in range(0, len(self._data), self._chunk):
            yield self._data[i : i + self._chunk]

    async def aread(self) -> bytes:
        return self._data

    async def aclose(self) -> None:
        pass


class SlowKeepaliveResp(FakeResp):
    def __init__(self, data: bytes, delay: float = 0.01):
        super().__init__(data)
        self._delay = delay

    async def aiter_bytes(self):
        if self._data:
            yield self._data
        while True:
            await asyncio.sleep(self._delay)
            yield b": keepalive\n\n"


class SlowEventsResp(FakeResp):
    def __init__(self, data: bytes, event: dict, delay: float = 0.01):
        super().__init__(data)
        self._event = event
        self._delay = delay

    async def aiter_bytes(self):
        if self._data:
            yield self._data
        while True:
            await asyncio.sleep(self._delay)
            yield make_sse([self._event])


class FakeClient:
    """Returns the queued responses on successive send() calls; records the JSON
    body of each build_request (the per-continuation-round upstream payload)."""

    def __init__(self, responses: list[FakeResp]):
        self._responses = list(responses)
        self._i = 0
        self.payloads: list[dict] = []

    def build_request(self, *a, **k):
        content = k.get("content")
        if content is not None:
            try:
                self.payloads.append(json.loads(content))
            except (json.JSONDecodeError, TypeError):
                pass
        return ("req", a, k)

    async def send(self, req, stream=True):
        r = self._responses[self._i]
        self._i += 1
        return r


async def run_fold(cfg, base_body, first_resp, later_resps) -> list:
    client = FakeClient(later_resps)
    out = b""
    async for chunk in fold_stream(client, cfg, base_body, {}, first_resp):
        out += chunk
    return await parse_events(out)


async def run_fold_capture(cfg, base_body, first_resp, client) -> list:
    """Like run_fold but uses a caller-supplied client (to inspect client.payloads)."""
    out = b""
    async for chunk in fold_stream(client, cfg, base_body, {}, first_resp):
        out += chunk
    return await parse_events(out)


# --- test registry ----------------------------------------------------------

_RESULTS: list[tuple[str, bool, str]] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    _RESULTS.append((name, bool(cond), detail))


# --- 1. truncation math -----------------------------------------------------


def test_truncation_math():
    for n, tok in enumerate([516, 1034, 1552, 2070, 2588], start=1):
        check(f"is_truncation({tok})", is_truncation_pattern(tok))
        check(f"tier_n({tok})=={n}", tier_n(tok) == n, str(tier_n(tok)))
    for bad in (515, 517, 0, None):
        check(f"not is_truncation({bad})", not is_truncation_pattern(bad))
    # window
    check("should_continue 516 default", should_continue(516, min_n=1, max_n=0))
    check("should_continue 2588 max_n=3 blocked", not should_continue(2588, min_n=1, max_n=3))
    check("should_continue 516 min_n=2 blocked", not should_continue(516, min_n=2, max_n=0))
    check("should_continue None", not should_continue(None, min_n=1, max_n=0))


# --- 2. SSE framing robustness ---------------------------------------------


async def test_sse_framing():
    data = (FIXTURES / "codex_poc_r1.sse.txt").read_bytes()
    whole = await parse_events(data)

    # odd-sized chunks must produce identical events
    async def chunked(src: bytes, size: int):
        for i in range(0, len(src), size):
            yield src[i : i + size]

    pieces = []
    async for e in incremental_sse(chunked(data, 7)):
        pieces.append(e)

    check("sse whole-vs-chunked count", len(whole) == len(pieces), f"{len(whole)} vs {len(pieces)}")
    types_w = [e.get("type") for e in whole if isinstance(e, dict)]
    types_c = [e.get("type") for e in pieces if isinstance(e, dict)]
    check("sse whole-vs-chunked types", types_w == types_c)
    check("sse has completed", "response.completed" in types_w)
    check("sse no spurious DONE", DONE not in whole)  # Codex sends no [DONE]


# --- 3. fold rewrite on real r1 + r2 captures -------------------------------


async def test_fold_real_captures():
    cfg = load_config(ROOT / "config.toml")
    cfg = replace(cfg, cont=replace(cfg.cont, max_continue=1))  # r1 -> continue -> r2 -> stop

    r1 = FakeResp((FIXTURES / "codex_poc_r1.sse.txt").read_bytes())
    r2 = FakeResp((FIXTURES / "codex_poc_r2.sse.txt").read_bytes())
    base_body = {"model": "gpt-5.5", "input": [{"role": "user", "content": "q"}]}

    evs = await run_fold(cfg, base_body, r1, [r2])
    dict_evs = [e for e in evs if isinstance(e, dict)]
    types = [e.get("type") for e in dict_evs]

    check("fold one created", types.count("response.created") == 1)
    check("fold one in_progress", types.count("response.in_progress") == 1)
    check("fold one terminal", sum(types.count(t) for t in
          ("response.completed", "response.failed", "response.incomplete")) == 1)

    seqs = [e["sequence_number"] for e in dict_evs]
    check("fold seq monotonic 0..n", seqs == list(range(len(dict_evs))), str(seqs[:5]))

    # reasoning items forwarded at ds_oi 0 then 1
    rdone = [e for e in dict_evs if e.get("type") == "response.output_item.done"
             and (e.get("item") or {}).get("type") == "reasoning"]
    check("fold 2 reasoning items", len(rdone) == 2, str(len(rdone)))
    check("fold reasoning oi 0,1", [e["output_index"] for e in rdone] == [0, 1],
          str([e.get("output_index") for e in rdone]))

    # message flushed (r2) at ds_oi 2; r1 message discarded
    deltas = "".join(e.get("delta", "") for e in dict_evs
                     if e.get("type") == "response.output_text.delta")
    check("fold r2 answer present", "答案是" in deltas or "21" in deltas, deltas[:40])
    check("fold r1 message discarded", "最少需要取出" not in deltas)

    created = next(e for e in dict_evs if e.get("type") == "response.created")
    completed = dict_evs[-1]
    created_id = (created.get("response") or {}).get("id")
    completed_id = (completed.get("response") or {}).get("id")
    check("fold created/completed share id", created_id == completed_id,
          f"{created_id} vs {completed_id}")
    out_items = (completed.get("response") or {}).get("output") or []
    check("fold reconstructed output non-empty (3 items)", len(out_items) == 3, str(len(out_items)))
    # Agent-facing usage = single-response equivalent (NOT summed input).
    usage = (completed.get("response") or {}).get("usage") or {}
    check("fold input = round1 (4582, not summed)", usage.get("input_tokens") == 4582,
          str(usage.get("input_tokens")))
    check("fold cached = round1 (3840)",
          (usage.get("input_tokens_details") or {}).get("cached_tokens") == 3840)
    rt = (usage.get("output_tokens_details") or {}).get("reasoning_tokens")
    check("fold reasoning summed 3104", rt == 516 + 2588, str(rt))
    # output = summed reasoning + final round's non-reasoning (2947-2588=359)
    check("fold output = reasoning + final msg",
          usage.get("output_tokens") == 3104 + (2947 - 2588), str(usage.get("output_tokens")))
    check("fold total = input + output",
          usage.get("total_tokens") == 4582 + 3104 + (2947 - 2588), str(usage.get("total_tokens")))

    md = (completed.get("response") or {}).get("metadata") or {}
    check("fold proxy_rounds has 2 entries", len(md.get("proxy_rounds") or []) == 2,
          str(md.get("proxy_rounds")))
    check("fold stopped_reason max_continue", md.get("proxy_stopped_reason") == "max_continue",
          str(md.get("proxy_stopped_reason")))
    billed = md.get("proxy_billed_usage") or {}
    check("fold billed input summed 9722", billed.get("input_tokens") == 4582 + 5140,
          str(billed.get("input_tokens")))


# --- 3b. truncated tool call is discarded; clean tool call flushes ----------


def _round(rs_id, enc, reasoning_tokens_val, *, extra_items=None, msg=None):
    evs = [
        {"type": "response.created", "response": {"id": "resp_x", "status": "in_progress",
         "model": "gpt-5.5", "metadata": {}}},
        {"type": "response.in_progress", "response": {"id": "resp_x"}},
        {"type": "response.output_item.added", "output_index": 0,
         "item": {"id": rs_id, "type": "reasoning"}},
        {"type": "response.output_item.done", "output_index": 0,
         "item": {"id": rs_id, "type": "reasoning", "encrypted_content": enc}},
    ]
    oi = 1
    for it in (extra_items or []):
        evs.append({"type": "response.output_item.added", "output_index": oi, "item": it})
        if it["type"] == "function_call":
            evs.append({"type": "response.function_call_arguments.delta", "output_index": oi,
                        "item_id": it["id"], "delta": it.get("arguments", "{}")})
        evs.append({"type": "response.output_item.done", "output_index": oi, "item": it})
        oi += 1
    if msg is not None:
        evs += [
            {"type": "response.output_item.added", "output_index": oi,
             "item": {"id": "msg_x", "type": "message"}},
            {"type": "response.content_part.added", "output_index": oi, "item_id": "msg_x",
             "content_index": 0, "part": {"type": "output_text"}},
            {"type": "response.output_text.delta", "output_index": oi, "item_id": "msg_x",
             "content_index": 0, "delta": msg},
            {"type": "response.output_text.done", "output_index": oi, "item_id": "msg_x",
             "content_index": 0, "text": msg},
            {"type": "response.content_part.done", "output_index": oi, "item_id": "msg_x",
             "content_index": 0, "part": {"type": "output_text", "text": msg}},
            {"type": "response.output_item.done", "output_index": oi,
             "item": {"id": "msg_x", "type": "message",
                      "content": [{"type": "output_text", "text": msg}]}},
        ]
    evs.append({"type": "response.completed", "response": {"id": "resp_x", "status": "completed",
                "usage": {"input_tokens": 100, "output_tokens": 50, "total_tokens": 150,
                          "output_tokens_details": {"reasoning_tokens": reasoning_tokens_val}}}})
    return make_sse(evs)


async def test_truncated_tool_call_discarded():
    cfg = load_config(ROOT / "config.toml")
    base_body = {"model": "gpt-5.5", "input": [{"role": "user", "content": "q"}]}

    # Round A: truncated (516) + a real tool call. Round B: clean message.
    tool = {"id": "fc_a", "type": "function_call", "name": "shell", "call_id": "call_a",
            "arguments": "{\"cmd\":\"ls\"}"}
    rA = FakeResp(_round("rs_a", "ENC_A", 516, extra_items=[tool]))
    rB = FakeResp(_round("rs_b", "ENC_B", 999, msg="done"))

    evs = [e for e in await run_fold(cfg, base_body, rA, [rB]) if isinstance(e, dict)]
    has_fc = any((e.get("item") or {}).get("type") == "function_call" for e in evs)
    fc_args = any(e.get("type") == "response.function_call_arguments.delta" for e in evs)
    check("truncated tool call discarded (no fc item)", not has_fc)
    check("truncated tool call discarded (no fc args)", not fc_args)
    deltas = "".join(e.get("delta", "") for e in evs
                     if e.get("type") == "response.output_text.delta")
    check("clean round message flushed", deltas == "done", deltas)

    # Clean round ending in a tool call → must flush it through.
    rOnly = FakeResp(_round("rs_c", "ENC_C", 999, extra_items=[tool]))
    evs2 = [e for e in await run_fold(cfg, base_body, rOnly, []) if isinstance(e, dict)]
    has_fc2 = any((e.get("item") or {}).get("type") == "function_call" for e in evs2)
    check("clean round tool call flushed", has_fc2)


# --- commentary continuation (default) vs tool_pair --------------------------


async def test_commentary_continuation_payload():
    cfg = load_config(ROOT / "config.toml")  # method = "commentary" by default
    base_body = {"model": "gpt-5.5", "input": [{"role": "user", "content": "q"}]}
    rA = FakeResp(_round("rs_a", "ENC_A", 516, msg="trunc"))  # truncated → continue
    rB = FakeResp(_round("rs_b", "ENC_B", 999, msg="done"))   # clean → stop
    client = FakeClient([rB])
    audited_payloads: list[tuple[int, dict]] = []

    async def audit_upstream_request(round_no: int, body_bytes: bytes) -> None:
        audited_payloads.append((round_no, json.loads(body_bytes)))

    out = b""
    async for chunk in fold_stream(
        client,
        cfg,
        base_body,
        {},
        rA,
        audit_upstream_request_body=audit_upstream_request,
    ):
        out += chunk
    evs = [e for e in await parse_events(out) if isinstance(e, dict)]

    check("commentary: one continuation round opened", len(client.payloads) == 1,
          str(len(client.payloads)))
    check("commentary: continuation request audited",
          len(audited_payloads) == 1 and audited_payloads[0][0] == 2,
          str(audited_payloads))
    if audited_payloads and client.payloads:
        check("commentary: audited continuation matches forwarded payload",
              audited_payloads[0][1] == client.payloads[0],
              str(audited_payloads[0][1]))
    inp = (client.payloads[0].get("input") if client.payloads else []) or []
    last = inp[-1] if inp else {}
    check("commentary: marker is a phase:commentary assistant message",
          last.get("type") == "message" and last.get("role") == "assistant"
          and last.get("phase") == "commentary", str(last))
    check("commentary: marker text from config",
          (last.get("content") or [{}])[0].get("text") == cfg.cont.marker_text)
    check("commentary: no function_call injected in replay",
          not any(isinstance(x, dict) and x.get("type") == "function_call" for x in inp))
    check("commentary: prior reasoning replayed (encrypted)",
          any(isinstance(x, dict) and x.get("type") == "reasoning"
              and x.get("encrypted_content") for x in inp))
    # forward_marker defaults false → marker stays hidden from the downstream stream
    check("commentary: marker hidden downstream by default",
          not any((e.get("item") or {}).get("phase") == "commentary" for e in evs))


async def test_tool_pair_continuation_payload():
    base = load_config(ROOT / "config.toml")
    cfg = replace(base, cont=replace(base.cont, method="tool_pair"))
    base_body = {"model": "gpt-5.5", "input": [{"role": "user", "content": "q"}]}
    rA = FakeResp(_round("rs_a", "ENC_A", 516, msg="trunc"))
    rB = FakeResp(_round("rs_b", "ENC_B", 999, msg="done"))
    client = FakeClient([rB])
    await run_fold_capture(cfg, base_body, rA, client)

    inp = (client.payloads[0].get("input") if client.payloads else []) or []
    types = [x.get("type") for x in inp if isinstance(x, dict)]
    check("tool_pair: function_call + output injected",
          "function_call" in types and "function_call_output" in types, str(types))
    check("tool_pair: no commentary message in replay",
          not any(isinstance(x, dict) and x.get("phase") == "commentary" for x in inp))


async def test_fold_upstream_response_capture_callback():
    cfg = load_config(ROOT / "config.toml")
    base_body = {"model": "gpt-5.5", "input": [{"role": "user", "content": "q"}]}
    resp = FakeResp(_round("rs_cap", "ENC_CAP", 999, msg="done"))
    client = FakeClient([])
    captures: list[tuple[int, int | None, int, int, bool]] = []

    def make_capture(round_no: int, status_code: int | None, content_type: str | None):
        return AuditBodyCapture(12)

    async def audit_capture(
        round_no: int,
        status_code: int | None,
        content_type: str | None,
        capture: AuditBodyCapture,
    ) -> None:
        captures.append((
            round_no,
            status_code,
            capture.original_bytes,
            capture.stored_bytes,
            capture.truncated,
        ))

    out = b""
    async for chunk in fold_stream(
        client,
        cfg,
        base_body,
        {},
        resp,
        make_upstream_response_capture=make_capture,
        audit_upstream_response_capture=audit_capture,
    ):
        out += chunk
    await parse_events(out)
    check("fold captures upstream response body prefix",
          len(captures) == 1
          and captures[0][0] == 1
          and captures[0][1] == 200
          and captures[0][2] > captures[0][3] == 12
          and captures[0][4],
          str(captures))


async def test_forward_marker_emits_downstream():
    base = load_config(ROOT / "config.toml")
    cfg = replace(base, cont=replace(base.cont, method="commentary", forward_marker=True))
    base_body = {"model": "gpt-5.5", "input": [{"role": "user", "content": "q"}]}
    rA = FakeResp(_round("rs_a", "ENC_A", 516, msg="trunc"))
    rB = FakeResp(_round("rs_b", "ENC_B", 999, msg="done"))
    evs = [e for e in await run_fold(cfg, base_body, rA, [rB]) if isinstance(e, dict)]

    done = [e for e in evs if e.get("type") == "response.output_item.done"
            and (e.get("item") or {}).get("phase") == "commentary"]
    check("forward_marker: one commentary item emitted downstream", len(done) == 1,
          str(len(done)))
    delta = "".join(e.get("delta", "") for e in evs
                    if e.get("type") == "response.output_text.delta"
                    and e.get("item_id", "").startswith("msg_continue_"))
    check("forward_marker: commentary delta carries marker text",
          delta == cfg.cont.marker_text, delta)
    # reconstructed output carries the commentary item (so the agent echoes it)
    completed = evs[-1]
    out_items = (completed.get("response") or {}).get("output") or []
    phases = [it.get("phase") for it in out_items if isinstance(it, dict)]
    check("forward_marker: commentary in reconstructed output", "commentary" in phases,
          str(phases))
    # sequence numbers stay monotonic 0..n despite the injected item
    seqs = [e["sequence_number"] for e in evs]
    check("forward_marker: seq monotonic with injected marker",
          seqs == list(range(len(evs))), str(seqs[:6]))


async def test_low_reasoning_retry_after_continue():
    base = load_config(ROOT / "config.toml")
    base_body = {"model": "gpt-5.5", "input": [{"role": "user", "content": "q"}]}

    disabled = replace(
        base,
        cont=replace(
            base.cont,
            retry_low_reasoning_after_continue=False,
            min_continue_reasoning_tokens=256,
            max_continue=3,
        ),
    )
    r1 = FakeResp(_round("rs_a", "ENC_A", 516, msg="truncated"))
    r2 = FakeResp(_round("rs_b", "ENC_B", 0, msg="wrong"))
    r3 = FakeResp(_round("rs_c", "ENC_C", 300, msg="recovered"))
    client = FakeClient([r2, r3])
    evs = [e for e in await run_fold_capture(disabled, base_body, r1, client)
           if isinstance(e, dict)]
    deltas = "".join(e.get("delta", "") for e in evs
                     if e.get("type") == "response.output_text.delta")
    check("low-reasoning retry disabled stops on clean low round", deltas == "wrong", deltas)
    check("low-reasoning retry disabled opened one continuation", len(client.payloads) == 1,
          str(len(client.payloads)))

    enabled = replace(
        base,
        cont=replace(
            base.cont,
            retry_low_reasoning_after_continue=True,
            min_continue_reasoning_tokens=256,
            max_continue=3,
        ),
    )
    r1 = FakeResp(_round("rs_a", "ENC_A", 516, msg="truncated"))
    r2 = FakeResp(_round("rs_b", "ENC_B", 0, msg="wrong"))
    r3 = FakeResp(_round("rs_c", "ENC_C", 300, msg="recovered"))
    client = FakeClient([r2, r3])
    evs = [e for e in await run_fold_capture(enabled, base_body, r1, client)
           if isinstance(e, dict)]
    deltas = "".join(e.get("delta", "") for e in evs
                     if e.get("type") == "response.output_text.delta")
    check("low-reasoning retry enabled discards low round", deltas == "recovered", deltas)
    check("low-reasoning retry enabled opened two continuations", len(client.payloads) == 2,
          str(len(client.payloads)))
    md = (evs[-1].get("response") or {}).get("metadata") or {}
    decisions = [r.get("decision") for r in (md.get("proxy_rounds") or [])]
    check("low-reasoning retry decision recorded",
          "continue:low_reasoning_after_continue" in decisions, str(decisions))

    r1 = FakeResp(_round("rs_a", "ENC_A", 516, msg="truncated"))
    tool = {"id": "fc_low", "type": "function_call", "name": "shell", "call_id": "call_low",
            "arguments": "{\"cmd\":\"pwd\"}"}
    r2 = FakeResp(_round("rs_b", "ENC_B", 0, extra_items=[tool]))
    r3 = FakeResp(_round("rs_c", "ENC_C", 300, msg="should-not-open"))
    client = FakeClient([r2, r3])
    evs = [e for e in await run_fold_capture(enabled, base_body, r1, client)
           if isinstance(e, dict)]
    has_fc = any((e.get("item") or {}).get("type") == "function_call" for e in evs)
    check("low-reasoning retry does not discard tool calls", has_fc)
    check("low-reasoning retry opened no extra continuation for tool call",
          len(client.payloads) == 1, str(len(client.payloads)))


# --- 2-fix. header transparency (#2) ----------------------------------------


def test_header_transparency():
    cfg = load_config(ROOT / "config.toml")
    client = _make_client()
    check("client invents no user-agent", "user-agent" not in client.headers)
    check("client invents no accept", "accept" not in client.headers)

    agent = [
        ("Authorization", "Bearer agent"),
        ("Content-Type", "application/json"),
        ("User-Agent", "codex_cli_rs/1.0"),
        ("Host", "drop.me"),
        ("Content-Length", "123"),
        ("Accept-Encoding", "gzip"),
        ("Responses-API-Base", "https://override/responses"),
        ("X-Custom", "keep"),
    ]
    out = build_upstream_headers(agent, cfg)
    low = {k.lower(): v for k, v in out.items()}
    check("hdr keeps content-type", low.get("content-type") == "application/json")
    check("hdr keeps user-agent", low.get("user-agent") == "codex_cli_rs/1.0")
    check("hdr keeps custom", low.get("x-custom") == "keep")
    check("hdr keeps authorization", low.get("authorization") == "Bearer agent")
    for dropped in ("host", "content-length", "accept-encoding", "responses-api-base"):
        check(f"hdr drops {dropped}", dropped not in low)


# --- upstream URL resolution via Responses-API-Base header ------------------


class _Req:
    def __init__(self, headers: dict):
        self.headers = Headers(headers)


def test_upstream_url_resolution():
    base = load_config(ROOT / "config.toml")
    fixed = replace(base, upstream=replace(base.upstream, mode="fixed", url="https://cfg/responses"))
    header = replace(base, upstream=replace(base.upstream, mode="header", url="https://cfg/responses"))
    with_hdr = _Req({"Responses-API-Base": "https://override/v1"})
    no_hdr = _Req({})

    check("fixed ignores header", _resolve_upstream_url(fixed, with_hdr) == "https://cfg/responses")
    check("header appends /responses to base",
          _resolve_upstream_url(header, with_hdr) == "https://override/v1/responses")
    check("header falls back to url",
          _resolve_upstream_url(header, no_hdr) == "https://cfg/responses")
    check("header trims trailing slash + case-insensitive",
          _resolve_upstream_url(header, _Req({"responses-api-base": "https://low/v1/"})) == "https://low/v1/responses")
    check("header full endpoint left as-is",
          _resolve_upstream_url(header, _Req({"Responses-API-Base": "https://x/v1/responses"})) == "https://x/v1/responses")
    check("header blank → fallback",
          _resolve_upstream_url(header, _Req({"Responses-API-Base": "   "})) == "https://cfg/responses")

    # header_required: present → use it; absent/blank → None (caller returns 400)
    req = replace(base, upstream=replace(base.upstream, mode="header_required", url="https://cfg/responses"))
    check("required appends /responses",
          _resolve_upstream_url(req, with_hdr) == "https://override/v1/responses")
    check("required missing → None", _resolve_upstream_url(req, no_hdr) is None)
    check("required blank → None",
          _resolve_upstream_url(req, _Req({"Responses-API-Base": " "})) is None)


# --- security guard: never send config creds to a header-supplied URL --------


def test_auth_safety_guard():
    base = load_config(ROOT / "config.toml")

    def blocked(url_mode, auth_mode, token, has_hdr, has_auth):
        cfg = replace(
            base,
            upstream=replace(base.upstream, mode=url_mode),
            auth=replace(base.auth, mode=auth_mode, access_token=token),
        )
        h = {}
        if has_hdr:
            h["Responses-API-Base"] = "https://external/responses"
        if has_auth:
            h["Authorization"] = "Bearer agent"
        rq = _Req(h)
        from_hdr = _url_is_from_header(cfg, rq)
        inj = would_inject_authorization(
            cfg, agent_has_authorization=rq.headers.get("authorization") is not None
        )
        return from_hdr and inj  # the exact condition handle_responses rejects on

    # fixed url → always safe
    check("guard: fixed+inject allow", not blocked("fixed", "inject", "TOK", True, False))
    # header + passthrough → never injects → allow
    check("guard: header+passthrough allow",
          not blocked("header", "passthrough", "TOK", True, False))
    # header + inject + header present → block (even if agent has its own auth)
    check("guard: header+inject+hdr block (noauth)",
          blocked("header", "inject", "TOK", True, False))
    check("guard: header+inject+hdr block (auth)",
          blocked("header", "inject", "TOK", True, True))
    # header + inject, no header → config url → allow
    check("guard: header+inject no-hdr allow",
          not blocked("header", "inject", "TOK", False, False))
    # header + PtI + header + agent has own auth → allow (uses agent's)
    check("guard: header+PtI+hdr+auth allow",
          not blocked("header", "passthrough_then_inject", "TOK", True, True))
    # header + PtI + header + no agent auth → block (would inject config)
    check("guard: header+PtI+hdr+noauth block",
          blocked("header", "passthrough_then_inject", "TOK", True, False))
    # header_required + inject + header → block
    check("guard: required+inject+hdr block",
          blocked("header_required", "inject", "TOK", True, False))
    # empty configured token → nothing to leak → allow
    check("guard: empty token allow", not blocked("header", "inject", "", True, False))


# --- auth injection from config (#2 follow-up) ------------------------------


def test_auth_injection():
    base = load_config(ROOT / "config.toml")

    def hdrs(cfg, agent):
        return {k.lower(): v for k, v in build_upstream_headers(agent, cfg).items()}

    # passthrough_then_inject: inject token when agent sends none; empty account → no header
    cfg = replace(base, auth=replace(base.auth, mode="passthrough_then_inject",
                                     access_token="TOK", chatgpt_account_id=""))
    out = hdrs(cfg, [("x", "1")])
    check("inject token when missing", out.get("authorization") == "Bearer TOK")
    check("no account header when empty", "chatgpt-account-id" not in out)

    # passthrough_then_inject: agent's auth wins (not overridden)
    out2 = hdrs(cfg, [("Authorization", "Bearer AGENT")])
    check("fallback keeps agent auth", out2.get("authorization") == "Bearer AGENT")

    # inject: config overrides agent + adds account
    cfg2 = replace(base, auth=replace(base.auth, mode="inject",
                                      access_token="TOK", chatgpt_account_id="acct1"))
    out3 = hdrs(cfg2, [("Authorization", "Bearer AGENT")])
    check("inject overrides agent auth", out3.get("authorization") == "Bearer TOK")
    check("inject adds account", out3.get("chatgpt-account-id") == "acct1")

    # passthrough: never inject anything
    cfg3 = replace(base, auth=replace(base.auth, mode="passthrough",
                                      access_token="TOK", chatgpt_account_id="acct1"))
    out4 = hdrs(cfg3, [("x", "1")])
    check("passthrough never injects", "authorization" not in out4 and "chatgpt-account-id" not in out4)


# --- 4-fix. reasoning/stream gating (#4) ------------------------------------


def test_reasoning_gate():
    check("reasoning_enabled dict", reasoning_enabled({"reasoning": {"effort": "high"}}))
    check("reasoning_enabled absent → true", reasoning_enabled({"input": []}))
    check("reasoning_enabled null → true", reasoning_enabled({"reasoning": None}))
    check("reasoning_enabled empty dict → true", reasoning_enabled({"reasoning": {}}))
    check("reasoning_enabled explicit false → false", not reasoning_enabled({"reasoning": False}))


def test_model_prefix_gate():
    cfg = load_config(ROOT / "config.toml")
    unrestricted = replace(cfg, cont=replace(cfg.cont, model_prefixes=()))
    gpt_only = replace(cfg, cont=replace(cfg.cont, model_prefixes=("gpt-",)))

    check("model gate empty allows all", _model_allowed_to_fold(unrestricted, "claude-sonnet-4"))
    check("model gate gpt allows gpt", _model_allowed_to_fold(gpt_only, "gpt-5.5"))
    check("model gate gpt rejects non-gpt", not _model_allowed_to_fold(gpt_only, "claude-sonnet-4"))
    check("model gate gpt rejects missing model", not _model_allowed_to_fold(gpt_only, None))


# --- request audit logging --------------------------------------------------


def test_request_audit_store_records_schema():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = RequestLogCfg(
            enabled=True,
            path="audit.sqlite3",
            store_body=True,
            max_body_bytes=4096,
            preview_chars=80,
        )
        store = RequestAuditStore(cfg, Path(tmp))
        body = {
            "model": "gpt-5.5",
            "stream": True,
            "max_output_tokens": 100,
            "input": [
                {"type": "message", "role": "user", "content": "q"},
                {
                    "type": "function_call",
                    "name": "exec_command",
                    "call_id": "call_1",
                    "arguments": {"cmd": "ls"},
                },
                {
                    "type": "tool_search_call",
                    "arguments": "{\"query\":\"compat\"}",
                },
                {
                    "type": "web_search_call",
                    "status": "completed",
                    "action": {"type": "search", "query": "compat"},
                },
                {"type": "function_call_output", "call_id": "call_1", "output": "ok"},
            ],
            "tools": [{"type": "function", "name": "exec_command"}],
        }
        raw = json.dumps(body).encode()
        audit_id = store.record_request(
            trace_id="trace_a",
            request_id="req_a",
            method="POST",
            path="/v1/responses",
            client_host="127.0.0.1",
            user_agent="test",
            content_type="application/json",
            upstream_url="http://upstream/v1/responses",
            decision="fold",
            raw_body=raw,
            body=body,
            parse_error=None,
        )
        store.update_response(audit_id, upstream_status_code=502, downstream_status_code=502)
        compat = normalize_request_body(body, CompatCfg(normalize_input_arguments=True))
        store.record_compat_actions(audit_id, compat.actions)
        store.record_body(
            audit_id,
            stage="upstream_request_body",
            ordinal=1,
            body=json.dumps(compat.body, ensure_ascii=False).encode(),
            content_type="application/json",
        )
        response_capture = AuditBodyCapture(10)
        response_capture.add(b"0123456789abcdef")
        store.record_captured_body(
            audit_id,
            stage="upstream_response_body",
            ordinal=1,
            capture=response_capture,
            content_type="text/event-stream",
        )
        store.close()

        conn = sqlite3.connect(Path(tmp) / "audit.sqlite3")
        conn.row_factory = sqlite3.Row
        req = conn.execute(
            "select * from request_audit where id = ?", (audit_id,)
        ).fetchone()
        check("audit request row inserted", req is not None)
        check("audit input_count", req["input_count"] == 5, str(req["input_count"]))
        check("audit body stored compressed", req["raw_body_zlib"] is not None)
        check("audit upstream status recorded", req["upstream_status_code"] == 502)

        body_rows = conn.execute(
            """
            select stage, ordinal, content_type, body_truncated,
                   body_original_bytes, body_stored_bytes, body_zlib
            from request_audit_bodies
            where audit_id = ?
            order by stage, ordinal
            """,
            (audit_id,),
        ).fetchall()
        body_keys = {(row["stage"], row["ordinal"]) for row in body_rows}
        check("audit stores client request body artifact",
              ("client_request_body", 0) in body_keys, str(body_keys))
        check("audit stores forwarded request body artifact",
              ("upstream_request_body", 1) in body_keys, str(body_keys))
        check("audit stores upstream response body artifact",
              ("upstream_response_body", 1) in body_keys, str(body_keys))
        response_rows = [row for row in body_rows if row["stage"] == "upstream_response_body"]
        if response_rows:
            row = response_rows[0]
            check("audit response artifact truncated",
                  row["body_truncated"] == 1
                  and row["body_original_bytes"] == 16
                  and row["body_stored_bytes"] == 10,
                  str(dict(row)))
            check("audit response artifact compressed prefix",
                  zlib.decompress(row["body_zlib"]) == b"0123456789",
                  str(zlib.decompress(row["body_zlib"])))

        item = conn.execute(
            """
            select idx, item_type, name, arguments_type, arguments_json_type
            from request_input_items
            where audit_id = ? and idx = 1
            """,
            (audit_id,),
        ).fetchone()
        check("audit function_call item row", item["item_type"] == "function_call")
        check("audit arguments type object", item["arguments_type"] == "object",
              str(item["arguments_type"]))
        check("audit object has no parsed JSON type", item["arguments_json_type"] is None,
              str(item["arguments_json_type"]))

        findings = conn.execute(
            """
            select path, code from request_schema_findings
            where audit_id = ?
            order by idx
            """,
            (audit_id,),
        ).fetchall()
        pairs = {(row["path"], row["code"]) for row in findings}
        check("audit flags input arguments object",
              ("input[1].arguments", "arguments_object") in pairs, str(pairs))
        check("audit flags max_output_tokens",
              ("max_output_tokens", "top_level_max_output_tokens") in pairs, str(pairs))
        check("audit flags web_search_call missing id",
              ("input[3].id", "web_search_call_missing_id") in pairs, str(pairs))

        compat_rows = conn.execute(
            """
            select path, action, code, original_type, parsed_type
            from request_compat_actions
            where audit_id = ?
            order by idx
            """,
            (audit_id,),
        ).fetchall()
        check("audit records compat actions", len(compat_rows) == 3, str(compat_rows))
        if compat_rows:
            rows = {row["path"]: row for row in compat_rows}
            check("audit compat serializes function_call",
                  rows["input[1].arguments"]["code"] == "serialized_object"
                  and rows["input[1].arguments"]["parsed_type"] == "string",
                  str([dict(row) for row in compat_rows]))
            check("audit compat parses tool_search_call",
                  rows["input[2].arguments"]["code"] == "parsed_object"
                  and rows["input[2].arguments"]["parsed_type"] == "object",
                  str([dict(row) for row in compat_rows]))
            check("audit compat synthesizes web_search_call id",
                  rows["input[3].id"]["code"] == "synthesized_web_search_call_id"
                  and rows["input[3].id"]["parsed_type"] == "string",
                  str([dict(row) for row in compat_rows]))
        conn.close()


def test_request_audit_retention_and_response_modes():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = RequestLogCfg(
            enabled=True,
            path="audit.sqlite3",
            store_body=True,
            max_body_bytes=128,
            retention_days=7,
            store_response_body="errors",
            max_response_body_bytes=8,
        )
        db = Path(tmp) / "audit.sqlite3"
        store = RequestAuditStore(cfg, Path(tmp))
        old_id = store.record_request(
            trace_id="trace_old",
            request_id="req_old",
            method="POST",
            path="/v1/responses",
            client_host=None,
            user_agent=None,
            content_type="application/json",
            upstream_url="http://upstream/v1/responses",
            decision="fold",
            raw_body=b'{"model":"old"}',
            body={"model": "old"},
            parse_error=None,
        )
        store.close()

        old_ts = (datetime.now(UTC) - timedelta(days=8)).isoformat(timespec="milliseconds")
        conn = sqlite3.connect(db)
        conn.execute(
            "update request_audit set created_at = ?, updated_at = ? where id = ?",
            (old_ts, old_ts, old_id),
        )
        conn.commit()
        conn.close()

        store = RequestAuditStore(cfg, Path(tmp))
        new_id = store.record_request(
            trace_id="trace_new",
            request_id="req_new",
            method="POST",
            path="/v1/responses",
            client_host=None,
            user_agent=None,
            content_type="application/json",
            upstream_url="http://upstream/v1/responses",
            decision="fold",
            raw_body=b'{"model":"new"}',
            body={"model": "new"},
            parse_error=None,
        )
        check("audit response mode captures errors",
              store.should_record_response_body(502)
              and not store.should_record_response_body(200)
              and store.should_record_response_body(200, force=True)
              and store.should_capture_response_body(200, force_possible=True))
        store.close()

        conn = sqlite3.connect(db)
        old_count = conn.execute(
            "select count(*) from request_audit where id = ?", (old_id,)
        ).fetchone()[0]
        new_count = conn.execute(
            "select count(*) from request_audit where id = ?", (new_id,)
        ).fetchone()[0]
        old_body_count = conn.execute(
            "select count(*) from request_audit_bodies where audit_id = ?", (old_id,)
        ).fetchone()[0]
        conn.close()
        check("audit retention prunes old request", old_count == 0, str(old_count))
        check("audit retention keeps new request", new_count == 1, str(new_count))
        check("audit retention cascades body artifacts",
              old_body_count == 0, str(old_body_count))


def test_compat_normalizes_input_arguments_safely():
    body = {
        "model": "gpt-5.5",
        "input": [
            {
                "type": "function_call",
                "name": "exec_command",
                "arguments": "{\"cmd\":\"ls\",\"limit\":5}",
                "extra": {"keep": True},
            },
            {
                "type": "function_call",
                "name": "exec_command",
                "arguments": {"cmd": "pwd", "limit": 1},
            },
            {
                "type": "tool_search_call",
                "arguments": "{\"query\":\"computer use screenshot window\",\"limit\":5}",
            },
            {"type": "tool_search_call", "arguments": {"already": "object"}},
            {"type": "tool_search_call", "arguments": "{\"a\":1,\"a\":2}"},
            {"type": "tool_search_call", "arguments": "[1,2]"},
            {"type": "tool_search_call", "arguments": "{\"unterminated\""},
            {"type": "message", "arguments": {"do_not": "touch"}},
            {
                "type": "function_call_output",
                "output": "{\"do_not\":\"touch\"}",
                "arguments": {"also": "do_not_touch"},
            },
            {"type": "function_call", "arguments": 7},
            {"type": "tool_search_call", "arguments": 7},
            {"type": "tool_search_call", "arguments": "{\"ratio\":1.234567890123456789}"},
            {
                "type": "web_search_call",
                "status": "completed",
                "action": {"type": "search", "query": "compat"},
            },
            {
                "type": "web_search_call",
                "id": "ws_keep",
                "status": "completed",
                "action": {"type": "open_page", "url": "https://example.com"},
            },
        ],
    }

    result = normalize_request_body(body, CompatCfg(normalize_input_arguments=True))
    out = result.body

    check("compat returns new body when changed", out is not body)
    check("compat original not mutated",
          isinstance(body["input"][0]["arguments"], str), str(body["input"][0]))
    check("compat existing function_call string stays string",
          out["input"][0]["arguments"] == "{\"cmd\":\"ls\",\"limit\":5}",
          str(out["input"][0]["arguments"]))
    check("compat function_call object serialized",
          out["input"][1]["arguments"] == "{\"cmd\":\"pwd\",\"limit\":1}",
          str(out["input"][1]["arguments"]))
    check("compat tool_search_call string parsed",
          out["input"][2]["arguments"] == {
              "query": "computer use screenshot window",
              "limit": 5,
          },
          str(out["input"][2]["arguments"]))
    check("compat preserves sibling fields",
          out["input"][0]["extra"] == {"keep": True}, str(out["input"][0]))
    check("compat existing tool_search_call object reused",
          out["input"][3]["arguments"] == {"already": "object"},
          str(out["input"][3]["arguments"]))
    check("compat duplicate keys skipped",
          out["input"][4]["arguments"] == "{\"a\":1,\"a\":2}",
          str(out["input"][4]["arguments"]))
    check("compat non-object JSON skipped",
          out["input"][5]["arguments"] == "[1,2]", str(out["input"][5]["arguments"]))
    check("compat invalid JSON skipped",
          out["input"][6]["arguments"] == "{\"unterminated\"",
          str(out["input"][6]["arguments"]))
    check("compat message arguments untouched",
          out["input"][7]["arguments"] == {"do_not": "touch"},
          str(out["input"][7]["arguments"]))
    check("compat output item arguments untouched",
          out["input"][8]["arguments"] == {"also": "do_not_touch"},
          str(out["input"][8]["arguments"]))
    codes = [(a.path, a.action, a.code) for a in result.actions]
    check("compat action counts",
          result.changed_count == 3 and result.skipped_count == 6, str(codes))
    check("compat action function_call serialized",
          ("input[1].arguments", "normalized", "serialized_object") in codes, str(codes))
    check("compat action tool_search_call parsed",
          ("input[2].arguments", "normalized", "parsed_object") in codes, str(codes))
    check("compat action web_search_call id",
          ("input[12].id", "normalized", "synthesized_web_search_call_id") in codes,
          str(codes))
    check("compat action duplicate",
          ("input[4].arguments", "skipped", "duplicate_keys") in codes, str(codes))
    check("compat action non-object",
          ("input[5].arguments", "skipped", "non_object_json") in codes, str(codes))
    check("compat action invalid",
          ("input[6].arguments", "skipped", "invalid_json") in codes, str(codes))
    check("compat function_call unsupported type",
          ("input[9].arguments", "skipped", "unsupported_argument_type") in codes,
          str(codes))
    check("compat tool_search_call unsupported type",
          ("input[10].arguments", "skipped", "unsupported_argument_type") in codes,
          str(codes))
    check("compat floating point skipped",
          out["input"][11]["arguments"] == "{\"ratio\":1.234567890123456789}"
          and ("input[11].arguments", "skipped", "floating_point_number") in codes,
          str(codes))
    check("compat web_search_call id synthesized",
          isinstance(out["input"][12].get("id"), str)
          and out["input"][12]["id"].startswith("ws_12_"),
          str(out["input"][12]))
    repeat = normalize_request_body(body, CompatCfg(normalize_input_arguments=True))
    check("compat web_search_call id stable",
          repeat.body["input"][12]["id"] == out["input"][12]["id"],
          str(repeat.body["input"][12]))
    check("compat web_search_call existing id preserved",
          out["input"][13]["id"] == "ws_keep", str(out["input"][13]))

    disabled = normalize_request_body(body, CompatCfg(normalize_input_arguments=False))
    check("compat disabled returns original body", disabled.body is body)
    check("compat disabled no actions", disabled.actions == ())

    web_only = {
        "input": [
            {
                "type": "web_search_call",
                "status": "completed",
                "action": {"type": "search", "query": "compat"},
            }
        ]
    }
    web_only_result = normalize_request_body(
        web_only,
        CompatCfg(synthesize_web_search_call_ids=True),
    )
    check("compat can synthesize web_search_call id independently",
          web_only_result.body["input"][0]["id"].startswith("ws_0_")
          and web_only_result.changed_count == 1,
          str(web_only_result.body))
    web_explicit_disabled = normalize_request_body(
        web_only,
        CompatCfg(normalize_input_arguments=True, synthesize_web_search_call_ids=False),
    )
    check("compat can disable web_search_call id synthesis explicitly",
          web_explicit_disabled.body is web_only and web_explicit_disabled.actions == (),
          str(web_explicit_disabled.actions))

    max_body = {
        "model": "gpt-5.5",
        "max_output_tokens": 100,
        "input": [
            {
                "type": "message",
                "content": [{"type": "input_text", "text": "max_output_tokens stays here"}],
            }
        ],
    }
    max_result = normalize_request_body(
        max_body,
        CompatCfg(
            normalize_input_arguments=True,
            max_output_tokens_compat="rename_to_max_tokens",
        ),
    )
    check("compat renames max_output_tokens",
          "max_output_tokens" not in max_result.body
          and max_result.body.get("max_tokens") == 100,
          str(max_result.body))
    check("compat max_output_tokens original not mutated",
          max_body["max_output_tokens"] == 100, str(max_body))
    check("compat max_output_tokens records action",
          ("max_output_tokens", "normalized", "renamed_max_output_tokens_to_max_tokens")
          in [(a.path, a.action, a.code) for a in max_result.actions],
          str([(a.path, a.action, a.code) for a in max_result.actions]))

    drop_max = normalize_request_body(
        max_body,
        CompatCfg(normalize_input_arguments=True, max_output_tokens_compat="drop"),
    )
    check("compat can explicitly drop max_output_tokens",
          "max_output_tokens" not in drop_max.body and "max_tokens" not in drop_max.body,
          str(drop_max.body))

    legacy_max_body = {
        "model": "gpt-5.5",
        "max_tokens": 80,
        "input": [{"role": "user", "content": "ping"}],
    }
    drop_legacy_max = normalize_request_body(
        legacy_max_body,
        CompatCfg(max_output_tokens_compat="drop"),
    )
    check("compat drop also removes legacy max_tokens",
          "max_output_tokens" not in drop_legacy_max.body
          and "max_tokens" not in drop_legacy_max.body,
          str(drop_legacy_max.body))
    check("compat legacy max_tokens original not mutated",
          legacy_max_body["max_tokens"] == 80, str(legacy_max_body))

    both_max_body = {
        "model": "gpt-5.5",
        "max_output_tokens": 100,
        "max_tokens": 80,
        "input": [{"role": "user", "content": "ping"}],
    }
    drop_both_max = normalize_request_body(
        both_max_body,
        CompatCfg(max_output_tokens_compat="drop"),
    )
    drop_both_codes = [(a.path, a.action, a.code) for a in drop_both_max.actions]
    check("compat drop removes both max token fields",
          "max_output_tokens" not in drop_both_max.body
          and "max_tokens" not in drop_both_max.body,
          str(drop_both_max.body))
    check("compat drop records both max token fields",
          ("max_output_tokens", "normalized", "dropped_max_output_tokens") in drop_both_codes
          and ("max_tokens", "normalized", "dropped_max_tokens") in drop_both_codes,
          str(drop_both_codes))

    legacy_drop_max = normalize_request_body(
        max_body,
        CompatCfg(normalize_input_arguments=True, drop_max_output_tokens=True),
    )
    check("compat legacy drop_max_output_tokens still drops",
          "max_output_tokens" not in legacy_drop_max.body
          and "max_tokens" not in legacy_drop_max.body,
          str(legacy_drop_max.body))

    keep_max = normalize_request_body(
        max_body,
        CompatCfg(normalize_input_arguments=True),
    )
    check("compat keeps max_output_tokens when disabled",
          keep_max.body is max_body and keep_max.actions == (), str(keep_max.actions))

    minimal_reasoning = {
        "model": "gpt-5.5",
        "reasoning": {"effort": "minimal", "summary": "auto"},
        "input": [{"role": "user", "content": "ping"}],
    }
    minimal_result = normalize_request_body(
        minimal_reasoning,
        CompatCfg(reasoning_effort_compat="minimal_to_none"),
    )
    check("compat reasoning minimal becomes none",
          minimal_result.body["reasoning"] == {"effort": "none", "summary": "auto"},
          str(minimal_result.body))
    check("compat reasoning original not mutated",
          minimal_reasoning["reasoning"]["effort"] == "minimal",
          str(minimal_reasoning))
    check("compat reasoning records action",
          ("reasoning.effort", "normalized", "normalized_reasoning_effort_minimal_to_none")
          in [(a.path, a.action, a.code) for a in minimal_result.actions],
          str([(a.path, a.action, a.code) for a in minimal_result.actions]))

    keep_reasoning = normalize_request_body(
        minimal_reasoning,
        CompatCfg(),
    )
    check("compat keeps reasoning minimal by default",
          keep_reasoning.body is minimal_reasoning and keep_reasoning.actions == (),
          str(keep_reasoning.actions))

    only_function_call = normalize_request_body(
        body,
        CompatCfg(
            normalize_input_arguments=True,
            normalize_input_argument_item_types=("function_call",),
        ),
    )
    check("compat item type filter serializes function_call",
          only_function_call.body["input"][1]["arguments"] == "{\"cmd\":\"pwd\",\"limit\":1}",
          str(only_function_call.body["input"][1]["arguments"]))
    check("compat item type filter leaves tool_search_call string",
          only_function_call.body["input"][2]["arguments"]
          == "{\"query\":\"computer use screenshot window\",\"limit\":5}",
          str(only_function_call.body["input"][2]["arguments"]))


# --- 3-fix. stateful follow-up repair (#3) ----------------------------------


def test_stateful_repair():
    store = IdStore()
    store.add("rs_keep")
    inp = [
        {"role": "user", "content": "q"},
        {"type": "reasoning", "id": "rs_keep", "encrypted_content": "E1"},
        {"type": "reasoning", "id": "rs_natural", "encrypted_content": "E2"},  # not recorded
        {"type": "message", "id": "msg"},
    ]
    out = repair_followup_input(inp, store, tool_name="continue_thinking", output_text="go")

    # pair inserted right after rs_keep only
    idx = next(i for i, x in enumerate(out)
               if isinstance(x, dict) and x.get("id") == "rs_keep")
    nxt = out[idx + 1]
    nxt2 = out[idx + 2]
    cid = continue_call_id("rs_keep")
    check("stateful inserts call after recorded id",
          nxt.get("type") == "function_call" and nxt.get("call_id") == cid, str(nxt))
    check("stateful inserts output after call",
          nxt2.get("type") == "function_call_output" and nxt2.get("call_id") == cid)

    # natural-consecutive reasoning (unrecorded) gets NO splice
    nidx = next(i for i, x in enumerate(out)
                if isinstance(x, dict) and x.get("id") == "rs_natural")
    check("stateful no splice for unrecorded id",
          out[nidx + 1].get("type") == "message", str(out[nidx + 1]))

    # idempotent: re-running adds nothing
    out2 = repair_followup_input(out, store, tool_name="continue_thinking", output_text="go")
    check("stateful idempotent", len(out2) == len(out), f"{len(out)} -> {len(out2)}")


# --- 7-fix. graceful EOF → incomplete (#7) ----------------------------------


async def test_eof_incomplete():
    cfg = load_config(ROOT / "config.toml")
    base_body = {"model": "gpt-5.5", "input": [{"role": "user", "content": "q"}]}
    # A round that streams reasoning + message but NO terminal event.
    events = [
        {"type": "response.created", "response": {"id": "resp_e", "status": "in_progress"}},
        {"type": "response.in_progress", "response": {"id": "resp_e"}},
        {"type": "response.output_item.added", "output_index": 0,
         "item": {"id": "rs_e", "type": "reasoning"}},
        {"type": "response.output_item.done", "output_index": 0,
         "item": {"id": "rs_e", "type": "reasoning", "encrypted_content": "E"}},
        {"type": "response.output_item.added", "output_index": 1,
         "item": {"id": "msg_e", "type": "message"}},
        {"type": "response.output_text.delta", "output_index": 1, "item_id": "msg_e",
         "content_index": 0, "delta": "partial"},
        {"type": "response.output_item.done", "output_index": 1,
         "item": {"id": "msg_e", "type": "message"}},
        # <-- no response.completed
    ]
    evs = [e for e in await run_fold(cfg, base_body, FakeResp(make_sse(events)), [])
           if isinstance(e, dict)]
    term = evs[-1]
    check("eof terminal is incomplete", term.get("type") == "response.incomplete",
          term.get("type"))
    reason = ((term.get("response") or {}).get("incomplete_details") or {}).get("reason")
    check("eof reason upstream_eof", reason == "upstream_eof", str(reason))

    # buffered tentative output must NOT leak on EOF (only reasoning survives)
    leaked = any(e.get("type") == "response.output_text.delta" for e in evs)
    check("eof does not leak buffered message", not leaked)
    out_items = (term.get("response") or {}).get("output") or []
    check("eof output is reasoning only",
          all(it.get("type") == "reasoning" for it in out_items) and len(out_items) == 1,
          str([it.get("type") for it in out_items]))


async def test_upstream_event_timeout_incomplete():
    base = load_config(ROOT / "config.toml")
    cfg = replace(
        base,
        stream=replace(base.stream, upstream_event_timeout_seconds=0.03),
    )
    base_body = {"model": "gpt-5.5", "input": [{"role": "user", "content": "q"}]}
    events = [
        {"type": "response.created", "response": {"id": "resp_t", "status": "in_progress"}},
        {"type": "response.in_progress", "response": {"id": "resp_t"}},
        {"type": "response.output_item.added", "output_index": 0,
         "item": {"id": "rs_t", "type": "reasoning"}},
        {"type": "response.output_item.done", "output_index": 0,
         "item": {"id": "rs_t", "type": "reasoning", "encrypted_content": "E"}},
        {"type": "response.output_item.added", "output_index": 1,
         "item": {"id": "msg_t", "type": "message"}},
        {"type": "response.output_text.delta", "output_index": 1, "item_id": "msg_t",
         "content_index": 0, "delta": "partial"},
    ]

    evs = [e for e in await run_fold(
        cfg,
        base_body,
        SlowKeepaliveResp(make_sse(events), delay=0.005),
        [],
    ) if isinstance(e, dict)]
    term = evs[-1]
    check("event timeout terminal is incomplete",
          term.get("type") == "response.incomplete", term.get("type"))
    reason = ((term.get("response") or {}).get("incomplete_details") or {}).get("reason")
    check("event timeout reason upstream_event_timeout",
          reason == "upstream_event_timeout", str(reason))
    leaked = any(e.get("type") == "response.output_text.delta" for e in evs)
    check("event timeout does not leak buffered message", not leaked)
    md = (term.get("response") or {}).get("metadata") or {}
    decisions = [r.get("decision") for r in (md.get("proxy_rounds") or [])]
    check("event timeout decision recorded",
          decisions == ["upstream_event_timeout"], str(decisions))


async def test_failed_terminal_incomplete_no_buffer_flush():
    cfg = load_config(ROOT / "config.toml")
    base_body = {"model": "gpt-5.5", "input": [{"role": "user", "content": "q"}]}
    events = [
        {"type": "response.created", "response": {"id": "resp_f", "status": "in_progress"}},
        {"type": "response.in_progress", "response": {"id": "resp_f"}},
        {"type": "response.output_item.added", "output_index": 0,
         "item": {"id": "rs_f", "type": "reasoning"}},
        {"type": "response.output_item.done", "output_index": 0,
         "item": {"id": "rs_f", "type": "reasoning", "encrypted_content": "E"}},
        {"type": "response.output_item.added", "output_index": 1,
         "item": {"id": "msg_f", "type": "message"}},
        {"type": "response.content_part.added", "output_index": 1, "item_id": "msg_f",
         "content_index": 0, "part": {"type": "output_text"}},
        {"type": "response.output_text.delta", "output_index": 1, "item_id": "msg_f",
         "content_index": 0, "delta": "partial"},
        {"type": "response.output_item.done", "output_index": 1,
         "item": {"id": "msg_f", "type": "message",
                  "content": [{"type": "output_text", "text": "partial"}]}},
        {"type": "response.failed", "response": {"id": "resp_f", "status": "failed",
         "usage": {"input_tokens": 100, "output_tokens": 20, "total_tokens": 120,
                   "output_tokens_details": {"reasoning_tokens": 11}}}},
    ]

    evs = [e for e in await run_fold(cfg, base_body, FakeResp(make_sse(events)), [])
           if isinstance(e, dict)]
    types = [e.get("type") for e in evs]
    term = evs[-1]
    check("failed terminal not forwarded", "response.failed" not in types, str(types))
    check("failed terminal becomes incomplete",
          term.get("type") == "response.incomplete", term.get("type"))
    reason = ((term.get("response") or {}).get("incomplete_details") or {}).get("reason")
    check("failed terminal reason upstream_failed", reason == "upstream_failed", str(reason))
    leaked = any(e.get("type") == "response.output_text.delta" for e in evs)
    check("failed terminal does not leak buffered message", not leaked)
    out_items = (term.get("response") or {}).get("output") or []
    check("failed terminal output is reasoning only",
          all(it.get("type") == "reasoning" for it in out_items) and len(out_items) == 1,
          str([it.get("type") for it in out_items]))
    md = (term.get("response") or {}).get("metadata") or {}
    decisions = [r.get("decision") for r in (md.get("proxy_rounds") or [])]
    check("failed terminal decision recorded",
          decisions == ["upstream_failed"], str(decisions))


async def test_incomplete_terminal_no_buffer_flush():
    cfg = load_config(ROOT / "config.toml")
    base_body = {"model": "gpt-5.5", "input": [{"role": "user", "content": "q"}]}
    events = [
        {"type": "response.created", "response": {"id": "resp_i", "status": "in_progress"}},
        {"type": "response.output_item.added", "output_index": 0,
         "item": {"id": "rs_i", "type": "reasoning"}},
        {"type": "response.output_item.done", "output_index": 0,
         "item": {"id": "rs_i", "type": "reasoning", "encrypted_content": "E"}},
        {"type": "response.output_item.added", "output_index": 1,
         "item": {"id": "msg_i", "type": "message"}},
        {"type": "response.output_text.delta", "output_index": 1, "item_id": "msg_i",
         "content_index": 0, "delta": "partial"},
        {"type": "response.incomplete", "response": {"id": "resp_i", "status": "incomplete",
         "incomplete_details": {"reason": "max_output_tokens"},
         "usage": {"input_tokens": 100, "output_tokens": 20, "total_tokens": 120,
                   "output_tokens_details": {"reasoning_tokens": 11}}}},
    ]

    evs = [e for e in await run_fold(cfg, base_body, FakeResp(make_sse(events)), [])
           if isinstance(e, dict)]
    term = evs[-1]
    check("upstream incomplete remains incomplete",
          term.get("type") == "response.incomplete", term.get("type"))
    reason = ((term.get("response") or {}).get("incomplete_details") or {}).get("reason")
    check("upstream incomplete reason normalized",
          reason == "upstream_incomplete", str(reason))
    leaked = any(e.get("type") == "response.output_text.delta" for e in evs)
    check("upstream incomplete does not leak buffered message", not leaked)
    md = (term.get("response") or {}).get("metadata") or {}
    decisions = [r.get("decision") for r in (md.get("proxy_rounds") or [])]
    check("upstream incomplete decision recorded",
          decisions == ["upstream_incomplete"], str(decisions))


async def test_upstream_round_timeout_incomplete_despite_events():
    base = load_config(ROOT / "config.toml")
    cfg = replace(
        base,
        stream=replace(
            base.stream,
            upstream_event_timeout_seconds=1,
            upstream_round_timeout_seconds=0.03,
        ),
    )
    base_body = {"model": "gpt-5.5", "input": [{"role": "user", "content": "q"}]}
    events = [
        {"type": "response.created", "response": {"id": "resp_rt", "status": "in_progress"}},
        {"type": "response.in_progress", "response": {"id": "resp_rt"}},
        {"type": "response.output_item.added", "output_index": 0,
         "item": {"id": "rs_rt", "type": "reasoning"}},
        {"type": "response.output_item.done", "output_index": 0,
         "item": {"id": "rs_rt", "type": "reasoning", "encrypted_content": "E"}},
        {"type": "response.output_item.added", "output_index": 1,
         "item": {"id": "msg_rt", "type": "message"}},
        {"type": "response.output_text.delta", "output_index": 1, "item_id": "msg_rt",
         "content_index": 0, "delta": "partial"},
    ]
    keep_progress = {"type": "response.in_progress", "response": {"id": "resp_rt"}}

    evs = [e for e in await run_fold(
        cfg,
        base_body,
        SlowEventsResp(make_sse(events), keep_progress, delay=0.005),
        [],
    ) if isinstance(e, dict)]
    term = evs[-1]
    check("round timeout terminal is incomplete",
          term.get("type") == "response.incomplete", term.get("type"))
    reason = ((term.get("response") or {}).get("incomplete_details") or {}).get("reason")
    check("round timeout reason upstream_round_timeout",
          reason == "upstream_round_timeout", str(reason))
    leaked = any(e.get("type") == "response.output_text.delta" for e in evs)
    check("round timeout does not leak buffered message", not leaked)
    md = (term.get("response") or {}).get("metadata") or {}
    decisions = [r.get("decision") for r in (md.get("proxy_rounds") or [])]
    check("round timeout decision recorded",
          decisions == ["upstream_round_timeout"], str(decisions))


# --- runner -----------------------------------------------------------------


async def _main():
    test_truncation_math()
    await test_sse_framing()
    await test_fold_real_captures()
    await test_truncated_tool_call_discarded()
    await test_commentary_continuation_payload()
    await test_tool_pair_continuation_payload()
    await test_fold_upstream_response_capture_callback()
    await test_forward_marker_emits_downstream()
    await test_low_reasoning_retry_after_continue()
    test_header_transparency()
    test_upstream_url_resolution()
    test_auth_safety_guard()
    test_auth_injection()
    test_reasoning_gate()
    test_model_prefix_gate()
    test_request_audit_store_records_schema()
    test_request_audit_retention_and_response_modes()
    test_compat_normalizes_input_arguments_safely()
    test_stateful_repair()
    await test_eof_incomplete()
    await test_upstream_event_timeout_incomplete()
    await test_failed_terminal_incomplete_no_buffer_flush()
    await test_incomplete_terminal_no_buffer_flush()
    await test_upstream_round_timeout_incomplete_despite_events()


def main():
    asyncio.run(_main())
    passed = sum(1 for _, ok, _ in _RESULTS if ok)
    for name, ok, detail in _RESULTS:
        mark = "PASS" if ok else "FAIL"
        line = f"[{mark}] {name}"
        if not ok and detail:
            line += f"  -- {detail}"
        print(line)
    print(f"\n{passed}/{len(_RESULTS)} checks passed")
    sys.exit(0 if passed == len(_RESULTS) else 1)


if __name__ == "__main__":
    main()
