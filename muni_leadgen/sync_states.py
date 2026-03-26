from __future__ import annotations

import json

from .firestore_store import FirestoreMunicipalityStore
from .hubspot_client import HubSpotClient


def main() -> None:
    store = FirestoreMunicipalityStore()
    hubspot = HubSpotClient()

    stats = {
        "scanned": 0,
        "restricted": 0,
        "reopened": 0,
        "engaged": 0,
        "still_active": 0,
        "missing_contact_doc": 0,
    }

    for municipality_key, muni_doc in store.iter_municipalities_for_sync():
        stats["scanned"] += 1
        active_email = str(muni_doc.get("active_contact_email") or muni_doc.get("engaged_contact_email") or "").strip().lower()
        if not active_email:
            continue

        latest_contact_snap = store.get_latest_contact_doc(municipality_key)
        if latest_contact_snap is None:
            stats["missing_contact_doc"] += 1
            continue

        contact_snapshot = hubspot.get_contact_outcome_snapshot(active_email)
        company_snapshot = hubspot.get_company_outcome_snapshot(municipality_key)
        outcome = hubspot.classify_sync_outcome(contact_snapshot, company_snapshot)

        if outcome == "restricted":
            store.mark_contact_terminal(
                municipality_key=municipality_key,
                contact_doc_id=latest_contact_snap.id,
                outcome="restricted",
                contact_status="restricted",
                reopen=False,
                contact_email=active_email,
            )
            stats["restricted"] += 1
            continue

        if outcome in {"no_response", "bounced", "invalid", "manual_stop"}:
            store.mark_contact_terminal(
                municipality_key=municipality_key,
                contact_doc_id=latest_contact_snap.id,
                outcome=outcome,
                contact_status=outcome,
                reopen=True,
                contact_email=active_email,
            )
            stats["reopened"] += 1
            continue

        if outcome in {"replied", "meeting_booked"}:
            store.mark_contact_terminal(
                municipality_key=municipality_key,
                contact_doc_id=latest_contact_snap.id,
                outcome=outcome,
                contact_status=outcome,
                reopen=False,
                contact_email=active_email,
            )
            stats["engaged"] += 1
            continue

        store.mark_contact_terminal(
            municipality_key=municipality_key,
            contact_doc_id=latest_contact_snap.id,
            outcome="active",
            contact_status="active_sequence",
            reopen=False,
            contact_email=active_email,
        )
        stats["still_active"] += 1

    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
