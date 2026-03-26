# leadgen_common/hubspot_sink.py

import os
import time
from typing import Dict, List, Optional, Tuple

import httpx


HUBSPOT_CONTACTS_ENDPOINT = "https://api.hubapi.com/crm/v3/objects/contacts"
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


def find_contact_id_by_email(email: str, timeout_s: float = 20.0) -> Optional[str]:
    email = (email or "").strip()
    if not email:
        return None

    payload = {
        "filterGroups": [
            {"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]}
        ],
        "properties": ["email"],
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
                return None

            data = r.json() or {}
            results = data.get("results", []) or []
            if results:
                return results[0].get("id")

            return None

    return None


def upsert_contact(properties: Dict[str, str], timeout_s: float = 20.0) -> Tuple[str, str]:
    """
    Returns: (action, contact_id)
    action is 'created' or 'updated'
    """
    email = (properties.get("email") or "").strip()
    if not email:
        raise RuntimeError("HubSpot upsert missing email")

    # Remove empty values so we do not overwrite with blanks
    clean_props: Dict[str, str] = {}
    for k, v in (properties or {}).items():
        if v is None:
            continue
        sv = str(v).strip()
        if sv == "":
            continue
        clean_props[k] = sv

    contact_id = find_contact_id_by_email(email, timeout_s=timeout_s)
    payload = {"properties": clean_props}

    with httpx.Client(timeout=timeout_s) as client:
        if contact_id:
            url = f"{HUBSPOT_CONTACTS_ENDPOINT}/{contact_id}"
            for attempt in range(0, 4):
                r = client.patch(url, headers=_headers(), json=payload)

                if r.status_code == 429:
                    retry_after = r.headers.get("Retry-After")
                    time.sleep(float(retry_after) if retry_after else 5.0)
                    continue
                if r.status_code >= 500:
                    _sleep_backoff(attempt)
                    continue
                if 200 <= r.status_code < 300:
                    return "updated", contact_id
                raise RuntimeError(f"HubSpot PATCH failed HTTP {r.status_code}: {r.text[:500]}")

        # create
        for attempt in range(0, 4):
            r = client.post(HUBSPOT_CONTACTS_ENDPOINT, headers=_headers(), json=payload)

            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                time.sleep(float(retry_after) if retry_after else 5.0)
                continue
            if r.status_code >= 500:
                _sleep_backoff(attempt)
                continue
            if 200 <= r.status_code < 300:
                data = r.json() or {}
                return "created", str(data.get("id", "UNKNOWN"))
            raise RuntimeError(f"HubSpot POST failed HTTP {r.status_code}: {r.text[:500]}")

    raise RuntimeError("HubSpot upsert failed after retries")


def push_leads_to_hubspot(final_headers: List[str], final_rows: List[List[str]]) -> Tuple[int, int]:
    """
    Maps your 14 col schema into HubSpot properties and upserts contacts.

    Returns: (created_count, updated_count)

    Env vars:
    - HUBSPOT_UPSERT_EXISTING (default: 1) if 0, skip updates
    - HUBSPOT_TIMEOUT_S (default: 20)
    """
    upsert_existing = os.environ.get("HUBSPOT_UPSERT_EXISTING", "1").strip().lower() in {"1", "true", "yes"}
    timeout_s = float(os.environ.get("HUBSPOT_TIMEOUT_S", "20"))

    idx = {h: i for i, h in enumerate(final_headers)}

    created = 0
    updated = 0

    for row in final_rows:
        def get(name: str) -> str:
            i = idx.get(name)
            if i is None or i >= len(row):
                return ""
            return str(row[i] or "").strip()

        email = get("email")
        if not email:
            continue

        props = {
            "email": email,
            "firstname": get("first_name"),
            "lastname": get("last_name"),
            "company": get("company"),
            "jobtitle": get("contact_title"),

            # Custom props, must exist in your HubSpot portal
            "tp_ai_summary": get("tp_ai_summary"),
            "tp_ai_subject": get("tp_ai_email_subject"),
            "tp_ai_email_body": get("tp_ai_email_body"),
            "tp_ai_confidence": get("tp_ai_confidence"),
            "tp_ai_ready_for_review": get("tp_ai_ready_for_review"),

            "tp_contact_source_url": get("contact_source_url"),
            "tp_catalyst_source_url": get("catalyst_source_url"),
            "tp_corroboration_source_url": get("corroboration_source_url"),
            "tp_pattern_example_email": get("pattern_example_email"),

            "tp_target_state": get("tp_target_state"),
            "tp_target_state_source": get("tp_target_state_source"),
            "tp_target_state_confidence": get("tp_target_state_confidence"),
        }

        contact_id = find_contact_id_by_email(email, timeout_s=timeout_s)
        if contact_id and not upsert_existing:
            continue

        print(
            f"HUBSPOT_DEBUG email={email} tp_ai_ready_for_review={props.get('tp_ai_ready_for_review', '')!r}",
            flush=True,
        )
        action, _cid = upsert_contact(props, timeout_s=timeout_s)
        if action == "created":
            created += 1
        else:
            updated += 1

    return created, updated