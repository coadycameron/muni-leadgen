from __future__ import annotations

from typing import Dict, Iterable, List, Tuple
from urllib.parse import urlparse

from .models import MunicipalityRow, ResearchLead, WriterLead
from .util import normalize_email, split_name


MIN_RESEARCH_CONFIDENCE = 0.85


def index_selected_rows(selected_rows: Iterable[MunicipalityRow]) -> Dict[str, MunicipalityRow]:
    return {row.municipality_key: row for row in selected_rows}


def _normalize_url(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    normalized_path = parsed.path or "/"
    normalized = f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{normalized_path}"
    if parsed.query:
        normalized = f"{normalized}?{parsed.query}"
    return normalized


def _url_domain(value: str) -> str:
    parsed = urlparse(value)
    return parsed.netloc.lower()


def _is_homepage_url(value: str) -> bool:
    parsed = urlparse(value)
    path = (parsed.path or "/").strip()
    return path in {"", "/"}


def _validate_source_urls(lead: ResearchLead) -> str:
    urls = [
        _normalize_url(lead.contact_source_url),
        _normalize_url(lead.catalyst_source_url),
        _normalize_url(lead.corroboration_source_url),
    ]
    if any(not url for url in urls):
        return "missing_or_invalid_source_url"

    unique_urls = set(urls)
    if len(unique_urls) < 2:
        return "insufficient_distinct_source_pages"

    unique_domains = {_url_domain(url) for url in urls if _url_domain(url)}
    all_homepages = all(_is_homepage_url(url) for url in urls)
    if all_homepages and len(unique_domains) < 2:
        return "homepage_only_sources"

    return ""


def _choose_better_lead(current: ResearchLead, candidate: ResearchLead) -> ResearchLead:
    if candidate.research_confidence > current.research_confidence:
        return candidate
    if candidate.research_confidence < current.research_confidence:
        return current

    current_urls = {
        _normalize_url(current.contact_source_url),
        _normalize_url(current.catalyst_source_url),
        _normalize_url(current.corroboration_source_url),
    }
    candidate_urls = {
        _normalize_url(candidate.contact_source_url),
        _normalize_url(candidate.catalyst_source_url),
        _normalize_url(candidate.corroboration_source_url),
    }
    if len(candidate_urls) > len(current_urls):
        return candidate
    return current


def filter_research_leads(
    research_leads: Iterable[ResearchLead],
    selected_rows: Iterable[MunicipalityRow],
) -> Tuple[Dict[str, ResearchLead], Dict[str, str]]:
    selected_by_key = index_selected_rows(selected_rows)
    kept: Dict[str, ResearchLead] = {}
    dropped: Dict[str, str] = {}

    for lead in research_leads:
        key = lead.input_row_key.strip()
        if not key:
            continue
        if key not in selected_by_key:
            dropped[key] = "unknown_municipality_key"
            continue

        blocked_emails = {normalize_email(e) for e in selected_by_key[key].blocked_emails}
        email = normalize_email(lead.contact_email)

        if not email or "@" not in email:
            dropped[key] = "missing_or_invalid_email"
            continue
        if email in blocked_emails:
            dropped[key] = "blocked_stale_email"
            continue
        if lead.research_confidence < MIN_RESEARCH_CONFIDENCE:
            dropped[key] = "confidence_below_threshold"
            continue

        url_issue = _validate_source_urls(lead)
        if url_issue:
            dropped[key] = url_issue
            continue

        if key in kept:
            kept[key] = _choose_better_lead(kept[key], lead)
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
