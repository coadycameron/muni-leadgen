from __future__ import annotations

import json
import os
import re
import sys
import time
from typing import Any, Dict, Optional, Tuple

_CLIENT = None


def get_gemini_client():
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT

    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY in environment.")

    try:
        from google import genai
    except Exception as exc:
        raise RuntimeError("Missing google-genai SDK. Install with: pip install google-genai") from exc

    _CLIENT = genai.Client(api_key=api_key)
    return _CLIENT


def extract_json_payload(text: str) -> Any:
    raw = (text or "").strip()
    if not raw:
        raise RuntimeError("Empty response")
    try:
        return json.loads(raw)
    except Exception:
        pass

    matches = list(re.finditer(r"```(?:json)?\s*(.*?)```", raw, flags=re.DOTALL | re.IGNORECASE))
    for match in matches:
        body = (match.group(1) or "").strip()
        if not body:
            continue
        try:
            return json.loads(body)
        except Exception:
            continue

    first_obj = raw.find("{")
    first_arr = raw.find("[")
    candidates = [p for p in [first_obj, first_arr] if p >= 0]
    if not candidates:
        raise RuntimeError("No JSON payload found")
    start = min(candidates)
    snippet = raw[start:]
    for end in range(len(snippet), 0, -1):
        try:
            return json.loads(snippet[:end])
        except Exception:
            continue
    raise RuntimeError("Failed to parse JSON payload")


def _iter_candidate_search_query_lists(value: Any, depth: int = 0):
    if value is None or depth > 6:
        return
    if isinstance(value, dict):
        for key, child in value.items():
            if key in {"web_search_queries", "webSearchQueries", "search_queries", "searchQueries"} and isinstance(child, list):
                yield child
            else:
                yield from _iter_candidate_search_query_lists(child, depth + 1)
        return
    if isinstance(value, (list, tuple)):
        for child in value:
            yield from _iter_candidate_search_query_lists(child, depth + 1)
        return
    for attr in ("web_search_queries", "webSearchQueries", "search_queries", "searchQueries"):
        try:
            child = getattr(value, attr, None)
        except Exception:
            child = None
        if isinstance(child, list):
            yield child
    for attr in ("grounding_metadata", "groundingMetadata", "metadata"):
        try:
            child = getattr(value, attr, None)
        except Exception:
            child = None
        if child is not None:
            yield from _iter_candidate_search_query_lists(child, depth + 1)


def count_grounding_search_queries(resp: Any) -> int:
    try:
        candidates = getattr(resp, "candidates", None) or []
        total = 0
        seen_ids = set()
        for candidate in candidates:
            for query_list in _iter_candidate_search_query_lists(candidate):
                marker = id(query_list)
                if marker in seen_ids:
                    continue
                seen_ids.add(marker)
                total += len([q for q in query_list if str(q or "").strip()])
        return total
    except Exception:
        return 0


def resolve_structured_model(model: str, use_google_search: bool, stage: str) -> str:
    chosen = (model or "").strip()
    if not use_google_search:
        return chosen
    supported = {
        "gemini-3.1-pro-preview",
        "gemini-3-flash-preview",
        "gemini-2.5-flash",
        "gemini-2.5-flash-preview-09-2025",
    }
    if chosen in supported:
        return chosen
    fallback = os.environ.get("GEMINI_STRUCTURED_TOOLS_MODEL", "gemini-2.5-flash").strip() or "gemini-2.5-flash"
    print(
        f"Warning: stage={stage} requested model {chosen or 'UNKNOWN'} does not officially support structured output with built-in tools. "
        f"Switching to {fallback}.",
        file=sys.stderr,
    )
    return fallback


def call_gemini(
    system_prompt: str,
    user_prompt: str,
    model: str,
    use_google_search: bool,
    stage: str,
    response_json_schema: Optional[Dict[str, Any]],
    max_output_tokens: int,
    temperature: float = 0.2,
) -> Tuple[str, Any, str]:
    client = get_gemini_client()
    resolved_model = resolve_structured_model(model, use_google_search, stage)
    attempts = int(os.environ.get("GEMINI_ATTEMPTS", "4"))
    base_sleep = float(os.environ.get("GEMINI_RETRY_SLEEP", "2.0"))

    expected_top_key = "leads" if stage == "research" else "emails" if stage == "email" else None
    last_error: Optional[Exception] = None

    def _normalize(payload: Any) -> Any:
        if payload is None:
            return None
        if expected_top_key:
            if isinstance(payload, dict) and expected_top_key in payload:
                return payload
            if isinstance(payload, list):
                return {expected_top_key: payload}
            if isinstance(payload, dict) and len(payload) == 1:
                only_value = next(iter(payload.values()))
                if isinstance(only_value, list):
                    return {expected_top_key: only_value}
            return None
        return payload if isinstance(payload, (dict, list)) else None

    for attempt in range(1, attempts + 1):
        try:
            config: Dict[str, Any] = {
                "system_instruction": system_prompt,
                "temperature": temperature,
                "max_output_tokens": max_output_tokens,
            }
            if use_google_search:
                config["tools"] = [{"google_search": {}}]
            if response_json_schema is not None:
                config["response_mime_type"] = "application/json"
                config["response_json_schema"] = response_json_schema

            resp = client.models.generate_content(
                model=resolved_model,
                contents=[user_prompt],
                config=config,
            )
            raw_text = (getattr(resp, "text", None) or "").strip()
            parsed = None
            for attr in ("parsed", "parsed_output", "parsedOutput"):
                try:
                    value = getattr(resp, attr, None)
                except Exception:
                    value = None
                if value is not None:
                    parsed = _normalize(value)
                    if parsed is not None:
                        break
            if parsed is None and raw_text:
                parsed = _normalize(extract_json_payload(raw_text))

            if use_google_search:
                search_queries = count_grounding_search_queries(resp)
                print(
                    f"Gemini stage={stage} use_google_search=true grounding_search_queries={search_queries}",
                    file=sys.stderr,
                )

            if response_json_schema is not None and parsed is None:
                raise RuntimeError(f"Gemini returned an unusable structured payload for stage={stage}")

            return raw_text, parsed, resolved_model
        except Exception as exc:
            last_error = exc
            if attempt >= attempts:
                break
            time.sleep(min(base_sleep * (2 ** (attempt - 1)), 20.0))

    raise RuntimeError(f"Gemini call failed for stage={stage}: {last_error}")
