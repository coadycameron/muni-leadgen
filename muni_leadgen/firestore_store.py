from __future__ import annotations

import os
import random
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import openpyxl
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

from .models import MunicipalityRow, ResearchLead, WriterEmail
from .util import (
    choice_shuffle,
    cooldown_ready,
    future_iso,
    municipality_key,
    parse_priority,
    stable_bucket,
    truthy,
)


class FirestoreMunicipalityStore:
    def __init__(self) -> None:
        self.client = firestore.Client(project=(os.environ.get("FIRESTORE_PROJECT_ID", "") or None))
        self.collection_name = os.environ.get("FIRESTORE_MUNI_COLLECTION", "muni_master").strip() or "muni_master"
        self.runs_collection_name = os.environ.get("FIRESTORE_MUNI_RUNS_COLLECTION", "muni_runs").strip() or "muni_runs"
        self.contacts_subcollection = os.environ.get("FIRESTORE_MUNI_CONTACTS_SUBCOLLECTION", "contacts").strip() or "contacts"
        self.cooldown_days = int(os.environ.get("MUNI_COOLDOWN_DAYS", "10"))

    @property
    def collection(self):
        return self.client.collection(self.collection_name)

    def _row_value(self, row: Sequence[Any], idx: Dict[str, int], header: str) -> Any:
        col = idx.get(header)
        if col is None or col < 0 or col >= len(row):
            return None
        return row[col]

    def _build_import_payload(
        self,
        row: Sequence[Any],
        idx: Dict[str, int],
        import_source: str,
    ) -> Optional[Tuple[str, Dict[str, Any]]]:
        name = str(self._row_value(row, idx, "Municipality") or "").strip()
        state = str(self._row_value(row, idx, "State") or "").strip()
        if not name or not state:
            return None

        pop_raw = self._row_value(row, idx, "Population 2024")
        try:
            population = int(pop_raw) if pop_raw not in (None, "") else None
        except Exception:
            population = None

        explicit_priority = str(self._row_value(row, idx, "Priority") or "").strip()
        priority = parse_priority(population, explicit_priority=explicit_priority)
        key = municipality_key(name, state)

        payload = {
            "municipality_key": key,
            "municipality_name": name,
            "state": state,
            "type": str(self._row_value(row, idx, "Type") or "").strip(),
            "population_2024": population,
            "priority": priority,
            "random_bucket": stable_bucket(key),
            "open_for_research": priority == "Highest - Target",
            "lead_status": "open" if priority == "Highest - Target" else "not_target",
            "lead_gen_restrict_sync": False,
            "import_source": import_source,
            "updated_at": firestore.SERVER_TIMESTAMP,
        }
        return key, payload

    def _commit_import_chunk(self, pending: Sequence[Tuple[str, Dict[str, Any]]]) -> Tuple[int, int, int]:
        if not pending:
            return 0, 0, 0

        doc_refs = [self.collection.document(key) for key, _ in pending]
        existing_ids = {snap.id for snap in self.client.get_all(doc_refs) if snap.exists}

        batch = self.client.batch()
        created = 0
        updated = 0

        for key, payload in pending:
            doc_ref = self.collection.document(key)
            if key in existing_ids:
                batch.set(doc_ref, payload, merge=True)
                updated += 1
                continue

            new_payload = dict(payload)
            new_payload.update(
                {
                    "created_at": firestore.SERVER_TIMESTAMP,
                    "active_contact_email": None,
                    "engaged_contact_email": None,
                    "reserved_by_run_id": None,
                    "reserved_at": None,
                    "next_research_eligible_at": None,
                    "last_outcome": "",
                    "blocked_emails": [],
                    "stale_contact_count": 0,
                }
            )
            batch.set(doc_ref, new_payload, merge=True)
            created += 1

        batch.commit()
        return created, updated, len(pending)

    def import_master_list_from_xlsx(self, xlsx_path: str, sheet_name: Optional[str] = None) -> Dict[str, int]:
        wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
        ws = wb[sheet_name] if sheet_name else wb.active
        rows_iter = ws.iter_rows(values_only=True)
        headers = [str(x or "").strip() for x in next(rows_iter)]
        idx = {h: i for i, h in enumerate(headers)}

        import_source = Path(xlsx_path).name
        target_only = truthy(os.environ.get("FIRESTORE_IMPORT_ONLY_HIGHEST_TARGET", "0"))
        chunk_size = int(os.environ.get("FIRESTORE_IMPORT_BATCH_SIZE", "300"))
        progress_every = int(os.environ.get("FIRESTORE_IMPORT_PROGRESS_EVERY", "1000"))

        created = 0
        updated = 0
        skipped = 0
        written = 0
        pending: List[Tuple[str, Dict[str, Any]]] = []

        for row_number, row in enumerate(rows_iter, start=2):
            built = self._build_import_payload(row, idx, import_source)
            if built is None:
                skipped += 1
                continue

            key, payload = built
            if target_only and payload["priority"] != "Highest - Target":
                skipped += 1
                continue

            pending.append((key, payload))
            if len(pending) < chunk_size:
                continue

            chunk_created, chunk_updated, chunk_written = self._commit_import_chunk(pending)
            created += chunk_created
            updated += chunk_updated
            written += chunk_written
            pending = []

            if written % progress_every == 0:
                print(f"Imported {written} rows so far at worksheet row {row_number}...")

        if pending:
            chunk_created, chunk_updated, chunk_written = self._commit_import_chunk(pending)
            created += chunk_created
            updated += chunk_updated
            written += chunk_written

        print(f"Import complete. written={written} created={created} updated={updated} skipped={skipped}")
        return {"written": written, "created": created, "updated": updated, "skipped": skipped}

    def _is_doc_eligible(self, doc: Dict[str, Any]) -> bool:
        if doc.get("priority") != "Highest - Target":
            return False
        if truthy(doc.get("lead_gen_restrict_sync")):
            return False
        if not truthy(doc.get("open_for_research")):
            return False
        if doc.get("reserved_by_run_id"):
            return False
        if doc.get("active_contact_email"):
            return False
        return cooldown_ready(doc.get("next_research_eligible_at"))

    def _reserve_doc(self, doc_ref, run_id: str) -> bool:
        transaction = self.client.transaction()

        @firestore.transactional
        def _txn(txn):
            snap = doc_ref.get(transaction=txn)
            if not snap.exists:
                return False
            data = snap.to_dict() or {}
            if not self._is_doc_eligible(data):
                return False
            txn.update(
                doc_ref,
                {
                    "reserved_by_run_id": run_id,
                    "reserved_at": firestore.SERVER_TIMESTAMP,
                    "lead_status": "reserved",
                    "open_for_research": False,
                    "updated_at": firestore.SERVER_TIMESTAMP,
                },
            )
            return True

        return bool(_txn(transaction))

    def reserve_random_target_municipalities(self, batch_size: int, run_id: str) -> List[MunicipalityRow]:
        bucket_tries = int(os.environ.get("MUNI_SELECTION_BUCKETS", "16"))
        selected: List[MunicipalityRow] = []
        seen_keys = set()

        buckets = list(range(1000))
        random.shuffle(buckets)

        for bucket in buckets[:bucket_tries]:
            if len(selected) >= batch_size:
                break
            query = (
                self.collection
                .where(filter=FieldFilter("priority", "==", "Highest - Target"))
                .where(filter=FieldFilter("open_for_research", "==", True))
                .where(filter=FieldFilter("random_bucket", "==", bucket))
            )
            docs = choice_shuffle([snap for snap in query.get()])
            for snap in docs:
                if len(selected) >= batch_size:
                    break
                if snap.id in seen_keys:
                    continue
                data = snap.to_dict() or {}
                if not self._is_doc_eligible(data):
                    continue
                if not self._reserve_doc(snap.reference, run_id):
                    continue
                seen_keys.add(snap.id)
                selected.append(
                    MunicipalityRow(
                        municipality_name=str(data.get("municipality_name") or "").strip(),
                        state=str(data.get("state") or "").strip(),
                        type=str(data.get("type") or "").strip(),
                        population_2024=data.get("population_2024"),
                        priority=str(data.get("priority") or "").strip(),
                        municipality_key=snap.id,
                        blocked_emails=[str(x).strip().lower() for x in list(data.get("blocked_emails") or []) if str(x).strip()],
                    )
                )

        if len(selected) < batch_size:
            query = (
                self.collection
                .where(filter=FieldFilter("priority", "==", "Highest - Target"))
                .where(filter=FieldFilter("open_for_research", "==", True))
            )
            docs = choice_shuffle([snap for snap in query.get()])
            for snap in docs:
                if len(selected) >= batch_size:
                    break
                if snap.id in seen_keys:
                    continue
                data = snap.to_dict() or {}
                if not self._is_doc_eligible(data):
                    continue
                if not self._reserve_doc(snap.reference, run_id):
                    continue
                seen_keys.add(snap.id)
                selected.append(
                    MunicipalityRow(
                        municipality_name=str(data.get("municipality_name") or "").strip(),
                        state=str(data.get("state") or "").strip(),
                        type=str(data.get("type") or "").strip(),
                        population_2024=data.get("population_2024"),
                        priority=str(data.get("priority") or "").strip(),
                        municipality_key=snap.id,
                        blocked_emails=[str(x).strip().lower() for x in list(data.get("blocked_emails") or []) if str(x).strip()],
                    )
                )

        self.client.collection(self.runs_collection_name).document(run_id).set(
            {
                "run_id": run_id,
                "selected_municipality_keys": [m.municipality_key for m in selected],
                "selected_count": len(selected),
                "created_at": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )
        return selected

    def release_unworked_reservations(self, municipality_keys: Sequence[str], run_id: str) -> int:
        released = 0
        for key in municipality_keys:
            doc_ref = self.collection.document(key)
            snap = doc_ref.get()
            if not snap.exists:
                continue
            data = snap.to_dict() or {}
            if str(data.get("reserved_by_run_id") or "").strip() != run_id:
                continue
            doc_ref.set(
                {
                    "reserved_by_run_id": None,
                    "reserved_at": None,
                    "open_for_research": True,
                    "lead_status": "open",
                    "updated_at": firestore.SERVER_TIMESTAMP,
                },
                merge=True,
            )
            released += 1
        return released

    def finalize_run(
        self,
        selected_rows: Sequence[MunicipalityRow],
        research_leads_by_key: Dict[str, ResearchLead],
        writer_emails_by_key: Dict[str, WriterEmail],
        verification_status_by_email: Dict[str, str],
        run_id: str,
        research_model: str,
        writer_model: str,
    ) -> List[Dict[str, Any]]:
        finalized: List[Dict[str, Any]] = []
        selected_by_key = {row.municipality_key: row for row in selected_rows}

        for key, row in selected_by_key.items():
            doc_ref = self.collection.document(key)
            research = research_leads_by_key.get(key)
            email = writer_emails_by_key.get(key)

            if research is None or email is None:
                doc_ref.set(
                    {
                        "reserved_by_run_id": None,
                        "reserved_at": None,
                        "open_for_research": True,
                        "lead_status": "open",
                        "last_outcome": "no_kept_contact",
                        "updated_at": firestore.SERVER_TIMESTAMP,
                    },
                    merge=True,
                )
                continue

            contact_email = research.contact_email.strip().lower()
            verification_status = verification_status_by_email.get(contact_email, "")
            contact_ref = doc_ref.collection(self.contacts_subcollection).document()

            contact_payload = {
                "run_id": run_id,
                "municipality_key": key,
                "municipality_name": row.municipality_name,
                "state": row.state,
                "contact_full_name": research.contact_full_name,
                "contact_preferred_name": research.contact_preferred_name,
                "contact_title": research.contact_title,
                "contact_email": contact_email,
                "personalization_tier": research.personalization_tier,
                "personalization_anchor_text": research.personalization_anchor_text,
                "current_method_or_workflow": research.current_method_or_workflow,
                "verified_context_facts": list(research.verified_context_facts),
                "writer_caution": research.writer_caution,
                "contact_source_url": research.contact_source_url,
                "catalyst_source_url": research.catalyst_source_url,
                "corroboration_source_url": research.corroboration_source_url,
                "research_confidence": research.research_confidence,
                "subject_line": email.subject_line,
                "email_body": email.email_body,
                "email_verification_status": verification_status,
                "contact_status": "active_sequence",
                "sequence_outcome": "active",
                "stale": False,
                "research_model": research_model,
                "writer_model": writer_model,
                "created_at": firestore.SERVER_TIMESTAMP,
                "updated_at": firestore.SERVER_TIMESTAMP,
            }
            contact_ref.set(contact_payload, merge=True)
            doc_ref.set(
                {
                    "reserved_by_run_id": None,
                    "reserved_at": None,
                    "open_for_research": False,
                    "lead_status": "active_contact",
                    "active_contact_email": contact_email,
                    "engaged_contact_email": None,
                    "last_outcome": "contact_pushed",
                    "last_run_id": run_id,
                    "updated_at": firestore.SERVER_TIMESTAMP,
                },
                merge=True,
            )
            finalized.append({**contact_payload, "contact_doc_id": contact_ref.id})
        return finalized

    def iter_municipalities_for_sync(self) -> Iterable[Tuple[str, Dict[str, Any]]]:
        for snap in self.collection.stream():
            data = snap.to_dict() or {}
            if not data.get("active_contact_email") and not data.get("engaged_contact_email"):
                continue
            yield snap.id, data

    def get_latest_contact_doc(self, municipality_key: str):
        docs = (
            self.collection.document(municipality_key)
            .collection(self.contacts_subcollection)
            .order_by("created_at", direction=firestore.Query.DESCENDING)
            .limit(1)
            .get()
        )
        return docs[0] if docs else None

    def mark_contact_terminal(
        self,
        municipality_key: str,
        contact_doc_id: str,
        outcome: str,
        contact_status: str,
        reopen: bool,
        contact_email: str,
    ) -> None:
        doc_ref = self.collection.document(municipality_key)
        contact_ref = doc_ref.collection(self.contacts_subcollection).document(contact_doc_id)
        updates = {
            "sequence_outcome": outcome,
            "contact_status": contact_status,
            "stale": reopen,
            "updated_at": firestore.SERVER_TIMESTAMP,
        }
        if reopen:
            updates["stale_reason"] = outcome
        contact_ref.set(updates, merge=True)

        muni_updates: Dict[str, Any] = {
            "reserved_by_run_id": None,
            "reserved_at": None,
            "updated_at": firestore.SERVER_TIMESTAMP,
            "last_outcome": outcome,
        }
        if reopen:
            muni_updates.update(
                {
                    "open_for_research": True,
                    "lead_status": "open",
                    "active_contact_email": None,
                    "engaged_contact_email": None,
                    "blocked_emails": firestore.ArrayUnion([contact_email]),
                    "stale_contact_count": firestore.Increment(1),
                    "next_research_eligible_at": future_iso(self.cooldown_days),
                }
            )
        elif outcome in {"replied", "meeting_booked"}:
            muni_updates.update(
                {
                    "open_for_research": False,
                    "lead_status": "engaged",
                    "engaged_contact_email": contact_email,
                    "active_contact_email": contact_email,
                }
            )
        elif outcome == "restricted":
            muni_updates.update(
                {
                    "open_for_research": False,
                    "lead_status": "restricted",
                    "lead_gen_restrict_sync": True,
                }
            )
        else:
            muni_updates.update(
                {
                    "open_for_research": False,
                    "lead_status": "active_contact",
                    "active_contact_email": contact_email,
                }
            )
        doc_ref.set(muni_updates, merge=True)