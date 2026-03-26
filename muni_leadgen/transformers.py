from __future__ import annotations

from typing import Dict, Iterable, List, Tuple

from .models import MunicipalityRow, ResearchLead, WriterLead
from .util import municipality_key, normalize_email, split_name


def index_selected_rows(selected_rows: Iterable[MunicipalityRow]) -> Dict[str, MunicipalityRow]:
    return {row.municipality_key: row for row in selected_rows}


def filter_research_leads(
    research_leads: Iterable[ResearchLead],
    selected_rows: Iterable[MunicipalityRow],
) -> Tuple[Dict[str, ResearchLead], Dict[str, str]]:
    selected_by_key = index_selected_rows(selected_rows)
    kept: Dict[str, ResearchLead] = {}
    dropped: Dict[str, str] = {}

    for lead in research_leads:
        key = lead.input_row_key.strip()
        if not key or key not in selected_by_key:
            continue

        blocked_emails = {normalize_email(e) for e in selected_by_key[key].blocked_emails}
        email = normalize_email(lead.contact_email)

        if not email or "@" not in email:
            dropped[key] = "missing_or_invalid_email"
            continue
        if email in blocked_emails:
            dropped[key] = "blocked_stale_email"
            continue
        if lead.research_confidence < 0.85:
            dropped[key] = "confidence_below_threshold"
            continue

        kept[key] = lead

    return kept, dropped


def build_writer_input_payload(
    kept_research_leads: Dict[str, ResearchLead],
    selected_rows: Iterable[MunicipalityRow],
) -> Dict[str, List[dict]]:
    selected_by_key = index_selected_rows(selected_rows)
    out: List[dict] = []
    for key, lead in kept_research_leads.items():
        muni = selected_by_key[key]
        writer_lead = WriterLead(
            input_row_key=key,
            municipality_name=muni.municipality_name,
            state=muni.state,
            contact_preferred_name=lead.contact_preferred_name,
            contact_title=lead.contact_title,
            personalization_tier=lead.personalization_tier,
            personalization_anchor_text=lead.personalization_anchor_text,
            current_method_or_workflow=lead.current_method_or_workflow,
            verified_context_facts=list(lead.verified_context_facts),
            writer_caution=lead.writer_caution,
        )
        out.append(writer_lead.to_dict())
    return {"leads": out}


def build_sheet_headers() -> List[str]:
    return [
        "email",
        "first_name",
        "last_name",
        "municipality_name",
        "state",
        "contact_title",
        "contact_preferred_name",
        "personalization_tier",
        "personalization_anchor_text",
        "current_method_or_workflow",
        "verified_context_facts",
        "writer_caution",
        "contact_source_url",
        "catalyst_source_url",
        "corroboration_source_url",
        "research_confidence",
        "subject_line",
        "email_body",
        "input_row_key",
        "ai_muni_lead",
    ]


def build_sheet_rows(finalized_leads: Iterable[dict]) -> List[List[str]]:
    rows: List[List[str]] = []
    for lead in finalized_leads:
        first_name, last_name = split_name(str(lead.get("contact_full_name") or ""))
        rows.append(
            [
                str(lead.get("contact_email") or "").strip().lower(),
                first_name,
                last_name,
                str(lead.get("municipality_name") or ""),
                str(lead.get("state") or ""),
                str(lead.get("contact_title") or ""),
                str(lead.get("contact_preferred_name") or ""),
                str(lead.get("personalization_tier") or ""),
                str(lead.get("personalization_anchor_text") or ""),
                str(lead.get("current_method_or_workflow") or ""),
                " | ".join(list(lead.get("verified_context_facts") or [])),
                str(lead.get("writer_caution") or ""),
                str(lead.get("contact_source_url") or ""),
                str(lead.get("catalyst_source_url") or ""),
                str(lead.get("corroboration_source_url") or ""),
                str(lead.get("research_confidence") or ""),
                str(lead.get("subject_line") or ""),
                str(lead.get("email_body") or ""),
                str(lead.get("municipality_key") or ""),
                "true",
            ]
        )
    return rows
