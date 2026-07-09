"""Request-shape compatibility transforms for upstream variants."""
from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .config import CompatCfg


@dataclass(frozen=True)
class CompatAction:
    path: str
    action: str
    code: str
    message: str
    original_type: str
    parsed_type: str | None = None


@dataclass(frozen=True)
class CompatResult:
    body: dict[str, Any]
    actions: tuple[CompatAction, ...] = ()

    @property
    def changed_count(self) -> int:
        return sum(1 for action in self.actions if action.action == "normalized")

    @property
    def skipped_count(self) -> int:
        return sum(1 for action in self.actions if action.action == "skipped")


class _DuplicateKeyError(ValueError):
    pass


class _NonStandardConstantError(ValueError):
    pass


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


def _object_without_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in pairs:
        if key in out:
            raise _DuplicateKeyError(f"duplicate JSON object key: {key}")
        out[key] = value
    return out


def _reject_constant(value: str) -> None:
    raise _NonStandardConstantError(f"non-standard JSON constant: {value}")


def _contains_decimal(value: Any) -> bool:
    if isinstance(value, Decimal):
        return True
    if isinstance(value, dict):
        return any(_contains_decimal(v) for v in value.values())
    if isinstance(value, list):
        return any(_contains_decimal(v) for v in value)
    return False


def _is_normalizable_item(item: dict[str, Any], cfg: CompatCfg) -> bool:
    item_type = item.get("type")
    return (
        isinstance(item_type, str)
        and item_type in cfg.normalize_input_argument_item_types
        and _target_argument_type(item_type) is not None
    )


def _target_argument_type(item_type: str) -> str | None:
    if item_type == "function_call":
        return "string"
    if item_type == "tool_search_call":
        return "object"
    return None


def _parse_json_object_argument(text: str) -> tuple[dict[str, Any] | None, str, str]:
    try:
        parsed = json.loads(
            text,
            object_pairs_hook=_object_without_duplicate_keys,
            parse_constant=_reject_constant,
            parse_float=Decimal,
        )
    except _DuplicateKeyError as exc:
        return None, "duplicate_keys", str(exc)
    except _NonStandardConstantError as exc:
        return None, "invalid_json", str(exc)
    except json.JSONDecodeError as exc:
        return None, "invalid_json", exc.msg
    if not isinstance(parsed, dict):
        return None, "non_object_json", f"parsed_json_type={_type_name(parsed)}"
    if _contains_decimal(parsed):
        return (
            None,
            "floating_point_number",
            "JSON object contains a floating-point number; skipped to avoid precision loss",
        )
    return parsed, "parsed_object", "parsed_json_type=object"


def _skip_action(path: str, original_type: str) -> CompatAction:
    return CompatAction(
        path=path,
        action="skipped",
        code="unsupported_argument_type",
        message=f"arguments_type={original_type}",
        original_type=original_type,
        parsed_type=None,
    )


def _stable_item_id(prefix: str, idx: int, item: dict[str, Any]) -> str:
    try:
        material = json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError):
        material = repr(sorted(str(k) for k in item.keys()))
    digest = hashlib.sha256(f"{idx}:{material}".encode("utf-8", errors="replace")).hexdigest()
    return f"{prefix}_{idx}_{digest[:24]}"


def _max_output_tokens_mode(cfg: CompatCfg) -> str:
    mode = cfg.max_output_tokens_compat.strip().lower()
    if not mode and cfg.drop_max_output_tokens:
        return "drop"
    if mode in {"keep", "drop", "rename_to_max_tokens"}:
        return mode
    return "keep"


def _synthesize_web_search_call_ids(cfg: CompatCfg) -> bool:
    if cfg.synthesize_web_search_call_ids is None:
        return bool(cfg.normalize_input_arguments)
    return bool(cfg.synthesize_web_search_call_ids)


def _reasoning_effort_mode(cfg: CompatCfg) -> str:
    mode = cfg.reasoning_effort_compat.strip().lower()
    if mode in {"keep", "minimal_to_none"}:
        return mode
    return "keep"


def normalize_request_body(body: dict[str, Any], cfg: CompatCfg) -> CompatResult:
    """Normalize selected `input[i].arguments` to upstream-specific shapes.

    `function_call.arguments` is normalized to a JSON string. `tool_search_call`
    is normalized to a JSON object. Already-correct shapes are left untouched.
    """
    max_tokens_mode = _max_output_tokens_mode(cfg)
    synthesize_web_ids = _synthesize_web_search_call_ids(cfg)
    reasoning_effort_mode = _reasoning_effort_mode(cfg)
    if (
        not cfg.normalize_input_arguments
        and max_tokens_mode == "keep"
        and not synthesize_web_ids
        and reasoning_effort_mode == "keep"
    ):
        return CompatResult(body)

    normalized_body = body
    normalized_input: list[Any] | None = None
    actions: list[CompatAction] = []

    if max_tokens_mode == "drop":
        for field in ("max_output_tokens", "max_tokens"):
            if field not in body:
                continue
            if normalized_body is body:
                normalized_body = dict(body)
            original_type = _type_name(body.get(field))
            normalized_body.pop(field, None)
            actions.append(
                CompatAction(
                    path=field,
                    action="normalized",
                    code=f"dropped_{field}",
                    message=f"dropped top-level {field} for upstream compatibility",
                    original_type=original_type,
                    parsed_type=None,
                )
            )
    elif max_tokens_mode == "rename_to_max_tokens" and "max_output_tokens" in body:
        if normalized_body is body:
            normalized_body = dict(body)
        original_type = _type_name(body.get("max_output_tokens"))
        if "max_tokens" not in normalized_body:
            normalized_body["max_tokens"] = body.get("max_output_tokens")
            action_code = "renamed_max_output_tokens_to_max_tokens"
            action_message = "renamed top-level max_output_tokens to max_tokens"
            parsed_type = "max_tokens"
        else:
            action_code = "dropped_max_output_tokens"
            action_message = "dropped top-level max_output_tokens for upstream compatibility"
            parsed_type = None
        normalized_body.pop("max_output_tokens", None)
        actions.append(
            CompatAction(
                path="max_output_tokens",
                action="normalized",
                code=action_code,
                message=action_message,
                original_type=original_type,
                parsed_type=parsed_type,
            )
        )

    reasoning = body.get("reasoning")
    if (
        reasoning_effort_mode == "minimal_to_none"
        and isinstance(reasoning, dict)
        and reasoning.get("effort") == "minimal"
    ):
        if normalized_body is body:
            normalized_body = dict(body)
        normalized_reasoning = dict(reasoning)
        normalized_reasoning["effort"] = "none"
        normalized_body["reasoning"] = normalized_reasoning
        actions.append(
            CompatAction(
                path="reasoning.effort",
                action="normalized",
                code="normalized_reasoning_effort_minimal_to_none",
                message="normalized reasoning.effort minimal to none for upstream compatibility",
                original_type="string",
                parsed_type="none",
            )
        )

    input_items = body.get("input")
    if not isinstance(input_items, list):
        return CompatResult(normalized_body, tuple(actions))

    for idx, item in enumerate(input_items):
        if not isinstance(item, dict):
            continue
        item_type = item.get("type")
        if (
            synthesize_web_ids
            and item_type == "web_search_call"
            and not str(item.get("id") or "").strip()
        ):
            if normalized_body is body:
                normalized_body = dict(body)
            if normalized_input is None:
                normalized_input = list(input_items)
                normalized_body["input"] = normalized_input
            new_item = dict(item)
            new_item["id"] = _stable_item_id("ws", idx, item)
            normalized_input[idx] = new_item
            actions.append(
                CompatAction(
                    path=f"input[{idx}].id",
                    action="normalized",
                    code="synthesized_web_search_call_id",
                    message="synthesized missing web_search_call id for Responses history replay",
                    original_type=_type_name(item.get("id")) if "id" in item else "missing",
                    parsed_type="string",
                )
            )
            item = new_item
        if not cfg.normalize_input_arguments:
            continue
        if "arguments" not in item or not _is_normalizable_item(item, cfg):
            continue

        path = f"input[{idx}].arguments"
        arguments = item.get("arguments")
        original_type = _type_name(arguments)
        target_type = _target_argument_type(str(item.get("type")))

        new_arguments: Any
        action_code: str
        action_message: str

        if target_type == "string":
            if isinstance(arguments, str):
                continue
            if not isinstance(arguments, dict):
                actions.append(_skip_action(path, original_type))
                continue
            new_arguments = json.dumps(arguments, ensure_ascii=False, separators=(",", ":"))
            action_code = "serialized_object"
            action_message = "serialized object arguments to JSON string"
        elif target_type == "object":
            if isinstance(arguments, dict):
                continue
            if not isinstance(arguments, str):
                actions.append(_skip_action(path, original_type))
                continue
            parsed, code, message = _parse_json_object_argument(arguments)
            if parsed is None:
                parsed_type = None
                if code == "non_object_json" and message.startswith("parsed_json_type="):
                    parsed_type = message.removeprefix("parsed_json_type=")
                actions.append(
                    CompatAction(
                        path=path,
                        action="skipped",
                        code=code,
                        message=message,
                        original_type=original_type,
                        parsed_type=parsed_type,
                    )
                )
                continue
            new_arguments = parsed
            action_code = code
            action_message = message
        else:
            continue

        if normalized_body is body:
            normalized_body = dict(body)
        if normalized_input is None:
            normalized_input = list(input_items)
            normalized_body["input"] = normalized_input

        new_item = dict(item)
        new_item["arguments"] = new_arguments
        normalized_input[idx] = new_item
        actions.append(
            CompatAction(
                path=path,
                action="normalized",
                code=action_code,
                message=action_message,
                original_type=original_type,
                parsed_type=target_type,
            )
        )

    return CompatResult(normalized_body, tuple(actions))
