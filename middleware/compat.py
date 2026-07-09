"""Request-shape compatibility transforms for upstream variants.

These transforms deliberately operate on a copy-on-write view of the parsed
body. The caller can keep auditing the client's original bytes while forwarding
the normalized shape to an upstream that is stricter about field types.
"""
from __future__ import annotations

import json
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


def _is_call_item(item: dict[str, Any]) -> bool:
    item_type = item.get("type")
    return isinstance(item_type, str) and item_type.endswith("_call")


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


def normalize_request_body(body: dict[str, Any], cfg: CompatCfg) -> CompatResult:
    """Normalize configured request-body compatibility issues.

    Currently supported:
    - Convert `input[i].arguments` from a JSON object encoded as a string into
      a real object for call-like input items.

    Safety rules:
    - Only `input` list items whose `type` ends in `_call` are considered.
    - Only string `arguments` values that strictly parse to a JSON object are
      converted.
    - JSON with duplicate object keys is skipped to avoid silently discarding
      information during parsing.
    - Non-object JSON, invalid JSON, already-object values, and unrelated fields
      are left untouched.
    """
    if not cfg.normalize_input_arguments:
        return CompatResult(body)

    input_items = body.get("input")
    if not isinstance(input_items, list):
        return CompatResult(body)

    normalized_body = body
    normalized_input: list[Any] | None = None
    actions: list[CompatAction] = []

    for idx, item in enumerate(input_items):
        if not isinstance(item, dict):
            continue
        if "arguments" not in item or not _is_call_item(item):
            continue

        path = f"input[{idx}].arguments"
        arguments = item.get("arguments")
        original_type = _type_name(arguments)

        if isinstance(arguments, dict):
            continue
        if not isinstance(arguments, str):
            actions.append(
                CompatAction(
                    path=path,
                    action="skipped",
                    code="unsupported_argument_type",
                    message=f"arguments_type={original_type}",
                    original_type=original_type,
                    parsed_type=None,
                )
            )
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

        if normalized_body is body:
            normalized_body = dict(body)
        if normalized_input is None:
            normalized_input = list(input_items)
            normalized_body["input"] = normalized_input

        new_item = dict(item)
        new_item["arguments"] = parsed
        normalized_input[idx] = new_item
        actions.append(
            CompatAction(
                path=path,
                action="normalized",
                code=code,
                message=message,
                original_type=original_type,
                parsed_type="object",
            )
        )

    return CompatResult(normalized_body, tuple(actions))
