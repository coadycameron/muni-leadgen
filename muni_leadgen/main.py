from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Tuple

from leadgen_common.email_verification_waterfall import filter_rows_by_email_verification_waterfall
from leadgen_common.hubspot_dedupe import filter_new_leads_against_hubspot
from leadgen_common.sheets_sink import append_leads_to_sheet

from .firestore_store import FirestoreMunicipalityStore
from .gemini_utils import call_gemini
from .hubspot_client import HubSpotClient
from .models import MunicipalityRow, ResearchLead, WriterEmail
from .transformers import build_sheet_headers, build_sheet_rows, build_writer_input_payload, filter_research_leads
from .util import make_run_id, safe_json_dumps


PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"
SCHEMAS_DIR = Path(__file__).resolve().parent.parent / "schemas"


RESEARCH_RETRY_SUFFIX = (
    "\n\nRetry instructions\n"
    "- Keep Google Search enabled and verify each municipality using live web results.\n"
    "- Return only direct named contacts with direct email addresses.\n"
    "- Every kept lead must include non-blank contact_source_url, catalyst_source_url, and corroboration_source_url.\n"
    "- Use distinct evidence pages when possible. Avoid returning the same URL for all three fields.\n"
    "- If a municipality does not have enough evidence for a strong lead, omit it rather than guessing."
)


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _build_research_user_prompt(base_prompt: str, selected_rows: List[MunicipalityRow], extra_instructions: str = "") -> str:
    payload = {"INPUT_ROWS": [row.to_input_row() for row in selected_rows]}
    suffix = extra_instructions.strip()
    if suffix:
        return f"{base_prompt}\n{suffix}\n\nINPUT_ROWS.json\n{safe_json_dumps(payload)}"
    return f"{base_prompt}\n\nINPUT_ROWS.json\n{safe_json_dumps(payload)}"


def _build_writer_user_prompt(base_prompt: str, writer_input_payload: Dict) -> str:
    return f"{base_prompt}\n\nWRITER_INPUT.json\n{safe_json_dumps(writer_input_payload)}"


def _run_research_batch(
    selected_rows: List[MunicipalityRow],
    research_system_prompt: str,
    research_user_prompt: str,
    research_schema: Dict,
) -> Tuple[str, List[ResearchLead], Dict[str, ResearchLead], Dict[str, str], str, int]:
    max_attempts = max(1, int(os.environ.get("MUNI_RESEARCH_BATCH_ATTEMPTS", "2")))
    raw_attempts: List[str] = []
    best_raw = ""
    best_model = os.environ.get("GEMINI_MODEL_RESEARCH", "gemini-2.5-flash")
    best_leads: List[ResearchLead] = []
    best_kept: Dict[str, ResearchLead] = {}
    best_dropped: Dict[str, str] = {}
    attempts_used = 0

    for attempt in range(1, max_attempts + 1):
        extra = RESEARCH_RETRY_SUFFIX if attempt > 1 else ""
        research_user = _build_research_user_prompt(research_user_prompt, selected_rows, extra_instructions=extra)
        raw_research, research_payload, research_model = call_gemini(
            system_prompt=research_system_prompt,
            user_prompt=research_user,
            model=os.environ.get("GEMINI_MODEL_RESEARCH", "gemini-2.5-flash"),
            use_google_search=True,
            stage="research",
            response_json_schema=research_schema,
            max_output_tokens=int(os.environ.get("GEMINI_MAX_OUTPUT_TOKENS_RESEARCH", "6000")),
            temperature=float(os.environ.get("GEMINI_TEMPERATURE_RESEARCH", "0.2")),
        )
        attempts_used = attempt
        raw_attempts.append(f"===== RESEARCH ATTEMPT {attempt} =====\n{raw_research}")

        raw_research_leads = [ResearchLead.from_dict(x) for x in list((research_payload or {}).get("leads", []) or [])]
        kept_research_leads, dropped_research_reasons = filter_research_leads(raw_research_leads, selected_rows)

        best_raw = raw_research
        best_model = research_model
        best_leads = raw_research_leads
        best_kept = kept_research_leads
        best_dropped = dropped_research_reasons

        if kept_research_leads:
            break

    return "\n\n".join(raw_attempts), best_leads, best_kept, best_dropped, best_model, attempts_used


def main() -> None:
    run_id = make_run_id("muni")
    batch_size = int(os.environ.get("MUNI_BATCH_SIZE", "10"))
    store = FirestoreMunicipalityStore()
    selected_rows = store.reserve_random_target_municipalities(batch_size=batch_size, run_id=run_id)

    if not selected_rows:
        print("No eligible municipalities available.")
        return

    research_system_prompt = _read_text(PROMPTS_DIR / "research_system_prompt_final.txt")
    research_user_prompt = _read_text(PROMPTS_DIR / "research_user_prompt_final.txt")
    writer_system_prompt = _read_text(PROMPTS_DIR / "municipal_email_system_prompt_final_v7.txt")
    writer_user_prompt = _read_text(PROMPTS_DIR / "municipal_email_user_prompt_final_v7.txt")
    research_schema = _read_json(SCHEMAS_DIR / "research_structuredoutput_schema.json")
    writer_schema = _read_json(SCHEMAS_DIR / "municipal_writer_structuredoutput_schema.json")

    raw_research_log, raw_research_leads, kept_research_leads, dropped_research_reasons, research_model, research_attempts_used = _run_research_batch(
        selected_rows=selected_rows,
        research_system_prompt=research_system_prompt,
        research_user_prompt=research_user_prompt,
        research_schema=research_schema,
    )

    out_raw_research = Path(os.environ.get("OUT_RAW_RESEARCH", "muni_raw_research.txt"))
    out_raw_research.write_text(raw_research_log, encoding="utf-8")

    dedupe_rows = [[lead.contact_email] for lead in kept_research_leads.values()]
    dedupe_kept_rows, existing_emails = filter_new_leads_against_hubspot(dedupe_rows, email_col_index=0)
    allowed_emails = {str(row[0]).strip().lower() for row in dedupe_kept_rows if row}
    kept_research_leads = {
        k: v
        for k, v in kept_research_leads.items()
        if v.contact_email.strip().lower() in allowed_emails
    }
    for key, lead in list(kept_research_leads.items()):
        if lead.contact_email.strip().lower() in existing_emails:
            dropped_research_reasons[key] = "existing_in_hubspot"

    verification_input_rows = [[lead.contact_email] for lead in kept_research_leads.values()]
    verified_rows, removed_map, verification_audit = filter_rows_by_email_verification_waterfall(verification_input_rows, email_col_index=0)
    verified_emails = {str(row[0]).strip().lower() for row in verified_rows if row}
    kept_research_leads = {
        k: v
        for k, v in kept_research_leads.items()
        if v.contact_email.strip().lower() in verified_emails
    }
    for key, lead in list(kept_research_leads.items()):
        email_lower = lead.contact_email.strip().lower()
        if email_lower in removed_map:
            dropped_research_reasons[key] = removed_map[email_lower]

    writer_input_payload = build_writer_input_payload(kept_research_leads, selected_rows)
    out_writer_input = Path(os.environ.get("OUT_WRITER_INPUT_JSON", "WRITER_INPUT.generated.json"))
    out_writer_input.write_text(safe_json_dumps(writer_input_payload), encoding="utf-8")

    writer_emails_by_key: Dict[str, WriterEmail] = {}
    writer_model = os.environ.get("GEMINI_MODEL_EMAIL", "gemini-2.5-flash")
    raw_email = ""
    if writer_input_payload.get("leads"):
        writer_user = _build_writer_user_prompt(writer_user_prompt, writer_input_payload)
        raw_email, email_payload, writer_model = call_gemini(
            system_prompt=writer_system_prompt,
            user_prompt=writer_user,
            model=writer_model,
            use_google_search=False,
            stage="email",
            response_json_schema=writer_schema,
            max_output_tokens=int(os.environ.get("GEMINI_MAX_OUTPUT_TOKENS_EMAIL", "4000")),
            temperature=float(os.environ.get("GEMINI_TEMPERATURE_EMAIL", "0.2")),
        )
        writer_emails_by_key = {
            email.input_row_key: email
            for email in [WriterEmail.from_dict(x) for x in list((email_payload or {}).get("emails", []) or [])]
        }

    out_raw_email = Path(os.environ.get("OUT_RAW_EMAIL", "muni_raw_email.txt"))
    out_raw_email.write_text(raw_email, encoding="utf-8")

    verification_status_by_email = {}
    for email_addr, audit in verification_audit.items():
        verification_status_by_email[email_addr] = str(audit.get("final_decision_reason") or audit.get("final_decision") or "")

    finalized_leads = store.finalize_run(
        selected_rows=selected_rows,
        research_leads_by_key=kept_research_leads,
        writer_emails_by_key=writer_emails_by_key,
        verification_status_by_email=verification_status_by_email,
        run_id=run_id,
        research_model=research_model,
        writer_model=writer_model,
    )

    hubspot = HubSpotClient()
    created = 0
    updated = 0
    writer_version = Path(PROMPTS_DIR / "municipal_email_system_prompt_final_v7.txt").name
    research_version = Path(PROMPTS_DIR / "research_system_prompt_final.txt").name

    for lead in finalized_leads:
        company_id = hubspot.upsert_company(
            municipality_name=str(lead.get("municipality_name") or ""),
            state=str(lead.get("state") or ""),
            muni_key=str(lead.get("municipality_key") or ""),
            priority="Highest - Target",
            status="active_contact",
        )
        action, contact_id = hubspot.upsert_contact_from_finalized_lead(lead, writer_version=writer_version, research_version=research_version)
        if contact_id:
            lead["hubspot_contact_id"] = contact_id
        if company_id and contact_id:
            hubspot.associate_contact_to_company(contact_id=contact_id, company_id=company_id)
        if action == "created":
            created += 1
        elif action == "updated":
            updated += 1

    sheet_headers = build_sheet_headers()
    sheet_rows = build_sheet_rows(finalized_leads)
    sheet_appended = 0
    sheet_skipped = 0
    if sheet_rows and os.environ.get("SHEETS_SPREADSHEET_ID", "").strip():
        sheet_appended, sheet_skipped = append_leads_to_sheet(sheet_headers, sheet_rows, run_id=run_id)

    released = store.release_unworked_reservations(
        municipality_keys=[row.municipality_key for row in selected_rows if row.municipality_key not in {lead.get("municipality_key") for lead in finalized_leads}],
        run_id=run_id,
    )

    summary = {
        "run_id": run_id,
        "selected_count": len(selected_rows),
        "research_attempts_used": research_attempts_used,
        "research_returned": len(raw_research_leads),
        "research_kept_after_quality_filters": len(kept_research_leads),
        "writer_generated": len(writer_emails_by_key),
        "finalized_count": len(finalized_leads),
        "hubspot_created": created,
        "hubspot_updated": updated,
        "sheet_appended": sheet_appended,
        "sheet_skipped": sheet_skipped,
        "released_without_contact": released,
        "dropped_research_reasons": dropped_research_reasons,
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
