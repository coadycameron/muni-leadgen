import os
import time
from typing import Dict, List

import httpx

from leadgen_common.saturation_utils import normalize_state_code

HUBSPOT_SEARCH_ENDPOINT = "https://api.hubapi.com/crm/v3/objects/contacts/search"


def _hubspot_token() -> str:
    token = (
        os.environ.get("HUBSPOT_PRIVATE_APP_TOKEN", "")
        or os.environ.get("HUBSPOT_TOKEN", "")
        or os.environ.get("HUBSPOT_API_KEY", "")
    ).strip()
    if not token:
        raise RuntimeError("Missing HUBSPOT_PRIVATE_APP_TOKEN (or HUBSPOT_TOKEN / HUBSPOT_API_KEY)")
    return token


def _headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {_hubspot_token()}",
        "Content-Type": "application/json",
    }


def _sleep_backoff(attempt: int) -> None:
    time.sleep(min(2 ** attempt, 10))


def _state_properties() -> List[str]:
    props: List[str] = []

    env_values = [
        os.environ.get("HUBSPOT_CONTACT_STATE_CODE_PROPERTY", "").strip(),
        os.environ.get("HUBSPOT_CONTACT_STATE_NAME_PROPERTY", "").strip(),
        os.environ.get("HUBSPOT_CONTACT_STATE_PROPERTIES", "").strip(),
    ]
    for raw in env_values:
        if not raw:
            continue
        for part in raw.split(","):
            value = part.strip()
            if value and value not in props:
                props.append(value)

    # HubSpot's contact API docs explicitly document `state` as a default contact
    # property internal name. Keep this default so the lookup works even when no
    # env override is configured.
    for fallback in ["state"]:
        if fallback not in props:
            props.append(fallback)

    return props


def find_contact_state_by_email(email: str, timeout_s: float = 20.0) -> str:
    email = (email or "").strip()
    if not email:
        return ""

    state_props = _state_properties()
    if not state_props:
        return ""

    payload = {
        "filterGroups": [
            {"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]}
        ],
        "properties": ["email"] + state_props,
        "limit": 1,
    }

    with httpx.Client(timeout=timeout_s) as client:
        for attempt in range(0, 4):
            r = client.post(HUBSPOT_SEARCH_ENDPOINT, headers=_headers(), json=payload)

            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                time.sleep(float(retry_after) if retry_after else 5.0)
                continue

            if r.status_code >= 500:
                _sleep_backoff(attempt)
                continue

            if r.status_code != 200:
                return ""

            data = r.json() or {}
            results = data.get("results", []) or []
            if not results:
                return ""

            props = results[0].get("properties", {}) or {}
            for prop_name in state_props:
                state_code = normalize_state_code(props.get(prop_name))
                if state_code:
                    return state_code

            return ""

    return ""