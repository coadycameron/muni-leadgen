from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class MunicipalityRow:
    municipality_name: str
    state: str
    type: str = ""
    population_2024: Optional[int] = None
    priority: str = ""
    municipality_key: str = ""
    blocked_emails: List[str] = field(default_factory=list)

    def to_input_row(self) -> Dict[str, Any]:
        row = {
            "input_row_key": self.municipality_key,
            "Municipality": self.municipality_name,
            "State": self.state,
            "Type": self.type,
            "Population 2024": self.population_2024,
        }
        if self.blocked_emails:
            row["blocked_emails"] = list(self.blocked_emails)
        return row


@dataclass
class ResearchLead:
    input_row_key: str
    contact_full_name: str
    contact_preferred_name: str
    contact_title: str
    contact_email: str
    personalization_tier: str
    personalization_anchor_text: str
    current_method_or_workflow: str
    verified_context_facts: List[str]
    writer_caution: str
    contact_source_url: str
    catalyst_source_url: str
    corroboration_source_url: str
    research_confidence: float
    contact_fit_reason: str = ""

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "ResearchLead":
        return cls(
            input_row_key=str(payload.get("input_row_key") or "").strip(),
            contact_full_name=str(payload.get("contact_full_name") or "").strip(),
            contact_preferred_name=str(payload.get("contact_preferred_name") or "").strip(),
            contact_title=str(payload.get("contact_title") or "").strip(),
            contact_email=str(payload.get("contact_email") or "").strip(),
            personalization_tier=str(payload.get("personalization_tier") or "").strip(),
            personalization_anchor_text=str(payload.get("personalization_anchor_text") or "").strip(),
            current_method_or_workflow=str(payload.get("current_method_or_workflow") or "").strip(),
            verified_context_facts=[str(x).strip() for x in list(payload.get("verified_context_facts") or []) if str(x).strip()],
            writer_caution=str(payload.get("writer_caution") or "").strip(),
            contact_source_url=str(payload.get("contact_source_url") or "").strip(),
            catalyst_source_url=str(payload.get("catalyst_source_url") or "").strip(),
            corroboration_source_url=str(payload.get("corroboration_source_url") or "").strip(),
            research_confidence=float(payload.get("research_confidence") or 0.0),
            contact_fit_reason=str(payload.get("contact_fit_reason") or "").strip(),
        )


@dataclass
class WriterLead:
    input_row_key: str
    municipality_name: str
    state: str
    contact_full_name: str
    contact_preferred_name: str
    contact_title: str
    contact_email: str
    contact_fit_reason: str
    personalization_tier: str
    personalization_anchor_text: str
    verified_context_facts: List[str]
    current_method_or_workflow: str
    writer_caution: str
    contact_source_url: str
    catalyst_source_url: str
    corroboration_source_url: str
    research_confidence: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "input_row_key": self.input_row_key,
            "municipality_name": self.municipality_name,
            "state": self.state,
            "contact_full_name": self.contact_full_name,
            "contact_preferred_name": self.contact_preferred_name,
            "contact_title": self.contact_title,
            "contact_email": self.contact_email,
            "contact_fit_reason": self.contact_fit_reason,
            "personalization_tier": self.personalization_tier,
            "personalization_anchor_text": self.personalization_anchor_text,
            "verified_context_facts": list(self.verified_context_facts),
            "current_method_or_workflow": self.current_method_or_workflow,
            "writer_caution": self.writer_caution,
            "contact_source_url": self.contact_source_url,
            "catalyst_source_url": self.catalyst_source_url,
            "corroboration_source_url": self.corroboration_source_url,
            "research_confidence": self.research_confidence,
        }


@dataclass
class WriterEmail:
    input_row_key: str
    subject_line: str
    email_body: str

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "WriterEmail":
        return cls(
            input_row_key=str(payload.get("input_row_key") or "").strip(),
            subject_line=str(payload.get("subject_line") or "").strip(),
            email_body=str(payload.get("email_body") or "").strip(),
        )
