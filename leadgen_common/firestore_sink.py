import os
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Set, Tuple

from google.api_core.exceptions import AlreadyExists
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

from leadgen_common.hubspot_state_lookup import find_contact_state_by_email
from leadgen_common.saturation_utils import compute_saturation_key, normalize_company_key, normalize_state_code


DISCOVERY_QUEUE_STATUS_QUEUED = "queued"
DISCOVERY_QUEUE_STATUS_RESERVED = "reserved"
DISCOVERY_QUEUE_STATUS_RESEARCHED_NO_FIT = "researched_no_fit"
DISCOVERY_QUEUE_STATUS_RESEARCHED_FILTERED_OUT = "researched_filtered_out"
DISCOVERY_QUEUE_STATUS_RESEARCHED_CONTACT_FOUND = "researched_contact_found"
DISCOVERY_QUEUE_STATUS_SUPPRESSED = "suppressed"
DISCOVERY_QUEUE_STATUS_EXHAUSTED = "exhausted"


def compute_discovery_candidate_key(company: str, state: str) -> str:
    company_key = normalize_company_key(company)
    if not company_key:
        return ""
    state_code = normalize_state_code(state) or "UNKNOWN"
    return f"{company_key}|{state_code}"


def _coerce_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _coerce_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _discovery_priority_score(candidate: Dict[str, object]) -> float:
    base = _coerce_float(candidate.get("discovery_confidence"), 0.0)

    if normalize_state_code(candidate.get("tp_target_state")):
        base += 0.02

    times_researched = max(0, _coerce_int(candidate.get("times_researched"), 0))
    base -= min(times_researched, 5) * 0.12

    source = str(candidate.get("candidate_source") or candidate.get("discovery_source") or "").strip().lower()
    if source == "fresh_discovery":
        base += 0.08
    elif source == "carryover":
        base += 0.02

    existing_priority = candidate.get("priority_score")
    if existing_priority is not None:
        base = max(base, _coerce_float(existing_priority, base))

    return round(base, 4)


def _get_discovery_queue_collection_name() -> str:
    return os.environ.get("FIRESTORE_DISCOVERY_QUEUE_COLLECTION", "discovery_candidates").strip()


def load_recent_company_avoid_map_from_firestore(
    lookback_days: int,
    min_recent_emails: int,
) -> Dict[str, Dict[str, object]]:
    """
    Returns recent exact company|state saturation keys from Firestore.

    Only records with a known contact state participate in saturation.
    Unknown-state records are stored, but they do not block future leads until
    HubSpot enrichment backfills the contact state.
    """
    leads_collection = os.environ.get("FIRESTORE_LEADS_COLLECTION", "qaqc_leads").strip()
    db = firestore.Client()

    try:
        days = max(1, int(lookback_days))
    except Exception:
        days = 21

    try:
        threshold = max(1, int(min_recent_emails))
    except Exception:
        threshold = 3

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    counts: Counter[str] = Counter()
    display: Dict[str, Dict[str, str]] = {}

    query = db.collection(leads_collection).where(
        filter=FieldFilter("created_at", ">=", cutoff)
    )

    for snap in query.get():
        doc = snap.to_dict() or {}

        company_raw = str(doc.get("company") or "").strip()
        company_key = normalize_company_key(company_raw)
        if not company_key:
            continue

        state_code = normalize_state_code(
            doc.get("tp_target_state_normalized") or doc.get("tp_target_state")
        )
        if not state_code:
            continue

        exact_key = compute_saturation_key(company_raw, state_code)
        if not exact_key:
            continue

        counts[exact_key] += 1
        if exact_key not in display:
            display[exact_key] = {
                "company": company_raw or company_key,
                "state": state_code,
                "saturation_key": exact_key,
            }

    out: Dict[str, Dict[str, object]] = {}
    for sat_key, count in counts.items():
        if count < threshold:
            continue

        info = display.get(sat_key, {}) or {}
        out[sat_key] = {
            "company": info.get("company", sat_key),
            "state": info.get("state", ""),
            "saturation_key": info.get("saturation_key", sat_key),
            "recent_email_count": int(count),
        }

    return out


def hydrate_recent_missing_contact_states_from_hubspot(
    lookback_days: int,
    limit: int,
) -> Dict[str, int]:
    """
    Backfill missing Firestore contact states from HubSpot before a run starts.

    This keeps saturation exact-state-only without forcing Gemini to search for
    state. Only contacts with a known enriched contact state in HubSpot are
    upgraded into company|STATE saturation keys.
    """
    leads_collection = os.environ.get("FIRESTORE_LEADS_COLLECTION", "qaqc_leads").strip()
    db = firestore.Client()

    try:
        days = max(1, int(lookback_days))
    except Exception:
        days = 90

    try:
        max_candidates = max(1, int(limit))
    except Exception:
        max_candidates = 500

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    stats = {
        "scanned": 0,
        "candidates": 0,
        "hubspot_queries": 0,
        "updated": 0,
        "still_missing": 0,
    }

    query = db.collection(leads_collection).where(
        filter=FieldFilter("created_at", ">=", cutoff)
    )

    for snap in query.get():
        stats["scanned"] += 1

        doc = snap.to_dict() or {}
        existing_state = normalize_state_code(
            doc.get("tp_target_state_normalized") or doc.get("tp_target_state")
        )
        if existing_state:
            continue

        email = str(doc.get("email_norm") or doc.get("email") or "").strip().lower()
        if not email or "@" not in email:
            continue

        stats["candidates"] += 1
        if stats["candidates"] > max_candidates:
            stats["candidates"] -= 1
            break

        stats["hubspot_queries"] += 1
        state_code = normalize_state_code(find_contact_state_by_email(email))
        if not state_code:
            continue

        company = str(doc.get("company") or "").strip()
        saturation_key = compute_saturation_key(company, state_code) if company else ""

        snap.reference.set(
            {
                "tp_target_state": state_code,
                "tp_target_state_normalized": state_code,
                "tp_target_state_source": "hubspot_contact_enriched",
                "tp_target_state_confidence": "1.00",
                "tp_saturation_key": saturation_key,
                "tp_state_backfilled_at": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )
        stats["updated"] += 1

    stats["still_missing"] = max(0, stats["candidates"] - stats["updated"])
    return stats


def upsert_discovery_candidates_to_firestore(
    companies: List[Dict[str, object]],
    run_id: str,
    source: str,
    ttl_days: int,
) -> Tuple[int, int]:
    """
    Upserts discovery candidates into Firestore.

    Doc id is company|STATE where STATE is the normalized USPS code or UNKNOWN.
    Fresh discovery refreshes expiry and can re-queue previously exhausted items.
    """
    if not companies:
        return 0, 0

    db = firestore.Client()
    collection = db.collection(_get_discovery_queue_collection_name())

    try:
        ttl = max(1, int(ttl_days))
    except Exception:
        ttl = 45

    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(days=ttl)

    upserted = 0
    created = 0

    for item in companies:
        company = str(item.get("company") or "").strip()
        if not company:
            continue

        state_code = normalize_state_code(item.get("tp_target_state")) or "UNKNOWN"
        candidate_key = compute_discovery_candidate_key(company, state_code)
        if not candidate_key:
            continue

        ref = collection.document(candidate_key)
        snap = ref.get()
        existing = snap.to_dict() or {}

        times_researched = max(0, _coerce_int(existing.get("times_researched"), 0))
        existing_status = str(existing.get("status") or "").strip() or DISCOVERY_QUEUE_STATUS_QUEUED
        status = DISCOVERY_QUEUE_STATUS_QUEUED if existing_status != DISCOVERY_QUEUE_STATUS_RESERVED else DISCOVERY_QUEUE_STATUS_RESERVED

        candidate_doc = {
            "candidate_key": candidate_key,
            "company": company,
            "company_key": normalize_company_key(company),
            "tp_target_state": state_code,
            "tp_target_state_normalized": normalize_state_code(state_code),
            "discovery_reason": str(item.get("discovery_reason") or "UNKNOWN").strip() or "UNKNOWN",
            "discovery_confidence": _coerce_float(item.get("discovery_confidence"), 0.0),
            "priority_score": _discovery_priority_score(
                {
                    **existing,
                    **item,
                    "tp_target_state": state_code,
                    "times_researched": times_researched,
                    "candidate_source": "fresh_discovery",
                }
            ),
            "status": status,
            "discovery_source": source,
            "candidate_source": source,
            "source_run_id": run_id,
            "times_researched": times_researched,
            "last_discovered_at": firestore.SERVER_TIMESTAMP,
            "expires_at": expires_at,
            "updated_at": firestore.SERVER_TIMESTAMP,
        }

        if existing_status == DISCOVERY_QUEUE_STATUS_RESERVED:
            candidate_doc["reserved_by_run_id"] = existing.get("reserved_by_run_id")
            candidate_doc["reserved_at"] = existing.get("reserved_at")
        else:
            candidate_doc["reserved_by_run_id"] = None
            candidate_doc["reserved_at"] = None

        if snap.exists:
            ref.set(candidate_doc, merge=True)
        else:
            ref.set(
                {
                    **candidate_doc,
                    "created_at": firestore.SERVER_TIMESTAMP,
                    "last_researched_at": existing.get("last_researched_at"),
                    "last_outcome": existing.get("last_outcome"),
                },
                merge=True,
            )
            created += 1

        upserted += 1

    return upserted, created


def load_queued_discovery_candidates_from_firestore(
    limit: int,
    max_research_attempts: int,
    skip_saturation_keys: Optional[Iterable[str]] = None,
) -> List[Dict[str, object]]:
    if limit <= 0:
        return []

    db = firestore.Client()
    collection = db.collection(_get_discovery_queue_collection_name())
    now = datetime.now(timezone.utc)
    skip_keys: Set[str] = {str(x or "").strip() for x in (skip_saturation_keys or []) if str(x or "").strip()}

    try:
        max_attempts = max(1, int(max_research_attempts))
    except Exception:
        max_attempts = 3

    candidates: List[Dict[str, object]] = []

    query = collection.where(filter=FieldFilter("status", "==", DISCOVERY_QUEUE_STATUS_QUEUED))
    for snap in query.get():
        doc = snap.to_dict() or {}

        expires_at = doc.get("expires_at")
        if isinstance(expires_at, datetime):
            expires_cmp = expires_at.astimezone(timezone.utc) if expires_at.tzinfo else expires_at.replace(tzinfo=timezone.utc)
            if expires_cmp < now:
                continue

        times_researched = max(0, _coerce_int(doc.get("times_researched"), 0))
        if times_researched >= max_attempts:
            continue

        company = str(doc.get("company") or "").strip()
        state_code = normalize_state_code(doc.get("tp_target_state"))
        saturation_key = compute_saturation_key(company, state_code) if state_code else ""
        if saturation_key and saturation_key in skip_keys:
            continue

        candidate = {
            "queue_doc_id": snap.id,
            "candidate_key": str(doc.get("candidate_key") or snap.id),
            "company": company,
            "tp_target_state": state_code or "UNKNOWN",
            "discovery_reason": str(doc.get("discovery_reason") or "UNKNOWN").strip() or "UNKNOWN",
            "discovery_confidence": f"{_coerce_float(doc.get('discovery_confidence'), 0.0):.2f}",
            "priority_score": _coerce_float(doc.get("priority_score"), 0.0),
            "times_researched": times_researched,
            "discovery_source": str(doc.get("discovery_source") or "carryover").strip() or "carryover",
            "candidate_source": "carryover",
            "status": str(doc.get("status") or DISCOVERY_QUEUE_STATUS_QUEUED),
            "last_discovered_at": doc.get("last_discovered_at"),
            "last_researched_at": doc.get("last_researched_at"),
        }
        candidates.append(candidate)

    candidates.sort(
        key=lambda item: (
            _discovery_priority_score(item),
            str(item.get("last_discovered_at") or ""),
            item.get("company") or "",
        ),
        reverse=True,
    )
    return candidates[:limit]


def reserve_discovery_candidates_for_run(
    candidates: List[Dict[str, object]],
    run_id: str,
) -> List[Dict[str, object]]:
    if not candidates:
        return []

    db = firestore.Client()
    collection = db.collection(_get_discovery_queue_collection_name())
    reserved: List[Dict[str, object]] = []

    for item in candidates:
        doc_id = str(item.get("queue_doc_id") or item.get("candidate_key") or "").strip()
        if not doc_id:
            continue

        ref = collection.document(doc_id)
        snap = ref.get()
        doc = snap.to_dict() or {}
        status = str(doc.get("status") or "").strip() or DISCOVERY_QUEUE_STATUS_QUEUED
        reserved_by = str(doc.get("reserved_by_run_id") or "").strip()

        if status == DISCOVERY_QUEUE_STATUS_RESERVED and reserved_by and reserved_by != run_id:
            continue
        if status not in {DISCOVERY_QUEUE_STATUS_QUEUED, DISCOVERY_QUEUE_STATUS_RESERVED}:
            continue

        ref.set(
            {
                "status": DISCOVERY_QUEUE_STATUS_RESERVED,
                "reserved_by_run_id": run_id,
                "reserved_at": firestore.SERVER_TIMESTAMP,
                "updated_at": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )

        reserved.append(
            {
                **item,
                "queue_doc_id": doc_id,
                "candidate_key": str(item.get("candidate_key") or doc.get("candidate_key") or doc_id),
                "status": DISCOVERY_QUEUE_STATUS_RESERVED,
                "reserved_by_run_id": run_id,
            }
        )

    return reserved


def release_reserved_discovery_candidates(
    candidates: List[Dict[str, object]],
    run_id: str,
) -> int:
    if not candidates:
        return 0

    db = firestore.Client()
    collection = db.collection(_get_discovery_queue_collection_name())
    released = 0

    for item in candidates:
        doc_id = str(item.get("queue_doc_id") or item.get("candidate_key") or "").strip()
        if not doc_id:
            continue

        ref = collection.document(doc_id)
        snap = ref.get()
        doc = snap.to_dict() or {}
        if str(doc.get("reserved_by_run_id") or "").strip() != run_id:
            continue

        ref.set(
            {
                "status": DISCOVERY_QUEUE_STATUS_QUEUED,
                "reserved_by_run_id": None,
                "reserved_at": None,
                "updated_at": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )
        released += 1

    return released


def mark_reserved_discovery_candidates_outcomes(
    candidates: List[Dict[str, object]],
    run_id: str,
    outcome_by_candidate_key: Dict[str, str],
) -> int:
    if not candidates:
        return 0

    db = firestore.Client()
    collection = db.collection(_get_discovery_queue_collection_name())
    updated = 0

    for item in candidates:
        doc_id = str(item.get("queue_doc_id") or item.get("candidate_key") or "").strip()
        candidate_key = str(item.get("candidate_key") or doc_id).strip()
        if not doc_id or not candidate_key:
            continue

        ref = collection.document(doc_id)
        snap = ref.get()
        doc = snap.to_dict() or {}
        if str(doc.get("reserved_by_run_id") or "").strip() != run_id:
            continue

        times_researched = max(0, _coerce_int(doc.get("times_researched"), 0)) + 1
        outcome = str(outcome_by_candidate_key.get(candidate_key) or DISCOVERY_QUEUE_STATUS_QUEUED).strip() or DISCOVERY_QUEUE_STATUS_QUEUED

        priority_source = {
            **doc,
            "times_researched": times_researched,
            "candidate_source": "carryover",
        }

        ref.set(
            {
                "status": outcome,
                "last_outcome": outcome,
                "times_researched": times_researched,
                "last_researched_at": firestore.SERVER_TIMESTAMP,
                "reserved_by_run_id": None,
                "reserved_at": None,
                "priority_score": _discovery_priority_score(priority_source),
                "updated_at": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )
        updated += 1

    return updated


def write_qaqc_leads_to_firestore(
    final_headers: List[str],
    final_rows: List[List[str]],
    run_id: str,
) -> Tuple[int, int]:
    """
    Writes leads to Firestore.

    Collections:
    - FIRESTORE_RUNS_COLLECTION (default: leadgen_runs)
    - FIRESTORE_LEADS_COLLECTION (default: qaqc_leads)

    Dedupe behavior:
    - Each lead doc id is normalized_email
    - Uses create(), so an existing doc is skipped
    """
    runs_collection = os.environ.get("FIRESTORE_RUNS_COLLECTION", "leadgen_runs").strip()
    leads_collection = os.environ.get("FIRESTORE_LEADS_COLLECTION", "qaqc_leads").strip()

    db = firestore.Client()

    run_ref = db.collection(runs_collection).document(run_id)
    run_ref.set(
        {
            "run_id": run_id,
            "created_at": firestore.SERVER_TIMESTAMP,
            "leads_attempted": len(final_rows),
        },
        merge=True,
    )

    created = 0
    skipped = 0

    for row in final_rows:
        doc = {final_headers[i]: row[i] for i in range(min(len(final_headers), len(row)))}

        email = str(doc.get("email") or "").strip()
        email_norm = email.lower()
        company = str(doc.get("company") or "").strip()

        state_code = normalize_state_code(doc.get("tp_target_state"))
        saturation_key = compute_saturation_key(company, state_code) if state_code else ""

        if state_code:
            doc["tp_target_state"] = state_code
        else:
            doc["tp_target_state"] = "UNKNOWN"

        doc["email_norm"] = email_norm
        doc["tp_target_state_normalized"] = state_code
        doc["tp_saturation_key"] = saturation_key
        doc["run_id"] = run_id
        doc["created_at"] = firestore.SERVER_TIMESTAMP

        ref = db.collection(leads_collection).document(email_norm)

        try:
            ref.create(doc)
            created += 1
        except AlreadyExists:
            skipped += 1

    run_ref.set(
        {
            "leads_created": created,
            "leads_skipped_existing": skipped,
            "finished_at": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

    return created, skipped
