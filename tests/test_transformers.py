from muni_leadgen.models import MunicipalityRow, ResearchLead
from muni_leadgen.transformers import build_writer_input_payload, filter_research_leads


def test_filter_research_leads_blocks_stale_email():
    selected = [
        MunicipalityRow(
            municipality_name="Testville",
            state="Colorado",
            municipality_key="Testville|Colorado",
            blocked_emails=["old@example.org"],
        )
    ]
    lead = ResearchLead(
        input_row_key="Testville|Colorado",
        contact_full_name="Pat Roads",
        contact_preferred_name="Pat",
        contact_title="Public Works Director",
        contact_email="old@example.org",
        personalization_tier="GENERAL",
        personalization_anchor_text="2024 street maintenance program",
        current_method_or_workflow="UNKNOWN",
        verified_context_facts=["Street maintenance is handled in-house."],
        writer_caution="No published method found.",
        contact_source_url="https://example.org/contact",
        catalyst_source_url="https://example.org/cip",
        corroboration_source_url="https://example.org/pw",
        research_confidence=0.91,
    )
    kept, dropped = filter_research_leads([lead], selected)
    assert kept == {}
    assert dropped["Testville|Colorado"] == "blocked_stale_email"


def test_build_writer_input_payload_contains_expected_fields():
    selected = [
        MunicipalityRow(
            municipality_name="Testville",
            state="Colorado",
            municipality_key="Testville|Colorado",
        )
    ]
    lead = ResearchLead(
        input_row_key="Testville|Colorado",
        contact_full_name="Pat Roads",
        contact_preferred_name="Pat",
        contact_title="Public Works Director",
        contact_email="pat@example.org",
        personalization_tier="PLAN",
        personalization_anchor_text="2024 overlay program",
        current_method_or_workflow="UNKNOWN",
        verified_context_facts=["Town has a paving budget."],
        writer_caution="Do not imply a published condition rating workflow.",
        contact_source_url="https://example.org/contact",
        catalyst_source_url="https://example.org/cip",
        corroboration_source_url="https://example.org/pw",
        research_confidence=0.95,
    )
    payload = build_writer_input_payload({"Testville|Colorado": lead}, selected)
    assert payload["leads"][0]["municipality_name"] == "Testville"
    assert payload["leads"][0]["contact_preferred_name"] == "Pat"
    assert payload["leads"][0]["personalization_tier"] == "PLAN"
