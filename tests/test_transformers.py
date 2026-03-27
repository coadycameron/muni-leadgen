from muni_leadgen.models import MunicipalityRow, ResearchLead
from muni_leadgen.transformers import build_writer_input_payload, filter_research_leads


def _lead(**overrides):
    payload = {
        "input_row_key": "Testville|Colorado",
        "contact_full_name": "Pat Roads",
        "contact_preferred_name": "Pat",
        "contact_title": "Public Works Director",
        "contact_email": "pat@example.org",
        "personalization_tier": "PLAN",
        "personalization_anchor_text": "2024 overlay program",
        "current_method_or_workflow": "UNKNOWN",
        "verified_context_facts": ["Town has a paving budget."],
        "writer_caution": "Do not imply a published condition rating workflow.",
        "contact_source_url": "https://example.org/contact/pat-roads",
        "catalyst_source_url": "https://example.org/cip/2024-overlay",
        "corroboration_source_url": "https://publicworks.example.org/streets",
        "research_confidence": 0.95,
    }
    payload.update(overrides)
    return ResearchLead(**payload)


def test_filter_research_leads_blocks_stale_email():
    selected = [
        MunicipalityRow(
            municipality_name="Testville",
            state="Colorado",
            municipality_key="Testville|Colorado",
            blocked_emails=["old@example.org"],
        )
    ]
    kept, dropped = filter_research_leads([_lead(contact_email="old@example.org")], selected)
    assert kept == {}
    assert dropped["Testville|Colorado"] == "blocked_stale_email"


def test_filter_research_leads_requires_distinct_source_pages():
    selected = [
        MunicipalityRow(
            municipality_name="Testville",
            state="Colorado",
            municipality_key="Testville|Colorado",
        )
    ]
    kept, dropped = filter_research_leads(
        [
            _lead(
                contact_source_url="https://example.org/roads",
                catalyst_source_url="https://example.org/roads",
                corroboration_source_url="https://example.org/roads",
            )
        ],
        selected,
    )
    assert kept == {}
    assert dropped["Testville|Colorado"] == "insufficient_distinct_source_pages"


def test_filter_research_leads_rejects_single_domain_homepages():
    selected = [
        MunicipalityRow(
            municipality_name="Testville",
            state="Colorado",
            municipality_key="Testville|Colorado",
        )
    ]
    kept, dropped = filter_research_leads(
        [
            _lead(
                contact_source_url="https://example.org/",
                catalyst_source_url="https://example.org/",
                corroboration_source_url="https://example.org/",
            )
        ],
        selected,
    )
    assert kept == {}
    assert dropped["Testville|Colorado"] == "insufficient_distinct_source_pages"


def test_filter_research_leads_keeps_best_duplicate_by_confidence():
    selected = [
        MunicipalityRow(
            municipality_name="Testville",
            state="Colorado",
            municipality_key="Testville|Colorado",
        )
    ]
    lower = _lead(research_confidence=0.86, contact_email="low@example.org")
    higher = _lead(research_confidence=0.93, contact_email="high@example.org")
    kept, dropped = filter_research_leads([lower, higher], selected)
    assert kept["Testville|Colorado"].contact_email == "high@example.org"
    assert dropped == {}


def test_build_writer_input_payload_contains_new_schema_fields():
    selected = [
        MunicipalityRow(
            municipality_name="Testville",
            state="Colorado",
            municipality_key="Testville|Colorado",
        )
    ]
    payload = build_writer_input_payload({"Testville|Colorado": _lead()}, selected)
    lead = payload["leads"][0]
    assert lead["municipality_name"] == "Testville"
    assert lead["state"] == "Colorado"
    assert lead["contact_full_name"] == "Pat Roads"
    assert lead["contact_email"] == "pat@example.org"
    assert lead["contact_preferred_name"] == "Pat"
    assert lead["personalization_tier"] == "PLAN"
    assert lead["contact_source_url"] == "https://example.org/contact/pat-roads"
    assert lead["research_confidence"] == 0.95
    assert "direct responsibility" in lead["contact_fit_reason"]


def test_build_writer_input_payload_uses_explicit_contact_fit_reason_when_present():
    selected = [
        MunicipalityRow(
            municipality_name="Testville",
            state="Colorado",
            municipality_key="Testville|Colorado",
        )
    ]
    payload = build_writer_input_payload(
        {
            "Testville|Colorado": _lead(
                contact_fit_reason="Named public works lead with direct oversight of municipal road planning."
            )
        },
        selected,
    )
    lead = payload["leads"][0]
    assert lead["contact_fit_reason"] == "Named public works lead with direct oversight of municipal road planning."
