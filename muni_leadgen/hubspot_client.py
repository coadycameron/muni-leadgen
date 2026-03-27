from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx

from .util import split_name, truthy


class HubSpotClient:
    def __init__(self) -> None:
        token = (
            os.environ.get("HUBSPOT_PRIVATE_APP_TOKEN", "")
            or os.environ.get("HUBSPOT_TOKEN", "")
            or os.environ.get("HUBSPOT_API_KEY", "")
        ).strip()
        if not token:
            raise RuntimeError("Missing HUBSPOT_PRIVATE_APP_TOKEN (or HUBSPOT_TOKEN / HUBSPOT_API_KEY)")
        self.base_url = os.environ.get("HUBSPOT_BASE_URL", "https://api.hubapi.com").rstrip("/")
        self.timeout_s = float(os.environ.get("HUBSPOT_TIMEOUT_S", "20"))
        self.max_retries = int(os.environ.get("HUBSPOT_MAX_RETRIES", "4"))
        self.headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        self.contact_flag_property = os.environ.get("HUBSPOT_MUNI_CONTACT_FLAG_PROPERTY", "aimunilead").strip() or "ai-muni-lead"
        self.company_flag_property = os.environ.get("HUBSPOT_MUNI_COMPANY_FLAG_PROPERTY", "aimuniaccount").strip() or "ai-muni-account"
        self.muni_key_property = os.environ.get("HUBSPOT_MUNI_KEY_PROPERTY", "tp_muni_key").strip() or "tp_muni_key"
        self.priority_property = os.environ.get("HUBSPOT_MUNI_PRIORITY_PROPERTY", "tp_muni_priority").strip() or "tp_muni_priority"
        self.status_property = os.environ.get("HUBSPOT_MUNI_STATUS_PROPERTY", "tp_muni_status").strip() or "tp_muni_status"
        self.sequence_outcome_property = os.environ.get("HUBSPOT_MUNI_SEQUENCE_OUTCOME_PROPERTY", "tp_muni_sequence_outcome").strip() or "tp_muni_sequence_outcome"
        self.contact_status_property = os.environ.get("HUBSPOT_MUNI_CONTACT_STATUS_PROPERTY", "tp_muni_contact_status").strip() or "tp_muni_contact_status"
        self.personalization_tier_property = os.environ.get("HUBSPOT_MUNI_PERSONALIZATION_TIER_PROPERTY", "tp_muni_personalization_tier").strip() or "tp_muni_personalization_tier"
        self.anchor_property = os.environ.get("HUBSPOT_MUNI_ANCHOR_PROPERTY", "tp_muni_anchor").strip() or "tp_muni_anchor"
        self.contact_source_url_property = os.environ.get("HUBSPOT_MUNI_CONTACT_SOURCE_URL_PROPERTY", "tp_muni_contact_source_url").strip() or "tp_muni_contact_source_url"
        self.catalyst_source_url_property = os.environ.get("HUBSPOT_MUNI_CATALYST_SOURCE_URL_PROPERTY", "tp_muni_catalyst_source_url").strip() or "tp_muni_catalyst_source_url"
        self.corroboration_source_url_property = os.environ.get("HUBSPOT_MUNI_CORROBORATION_SOURCE_URL_PROPERTY", "tp_muni_corroboration_source_url").strip() or "tp_muni_corroboration_source_url"
        self.writer_version_property = os.environ.get("HUBSPOT_MUNI_WRITER_VERSION_PROPERTY", "tp_muni_writer_version").strip() or "tp_muni_writer_version"
        self.research_version_property = os.environ.get("HUBSPOT_MUNI_RESEARCH_VERSION_PROPERTY", "tp_muni_research_version").strip() or "tp_muni_research_version"
        self.contact_restrict_property = os.environ.get("HUBSPOT_CONTACT_RESTRICT_SYNC_PROPERTY", "leadgenrestrictsync").strip() or "leadgenrestrictsync"
        self.company_restrict_property = os.environ.get("HUBSPOT_COMPANY_RESTRICT_PROPERTY", "companyleadgenrestrictboolean").strip() or "companyleadgenrestrictboolean"
        self.external_bounce_property = os.environ.get("HUBSPOT_EXTERNAL_BOUNCE_PROPERTY", "external_bounce").strip() or "external_bounce"
        
    def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                with httpx.Client(timeout=self.timeout_s) as client:
                    response = client.request(method, url, headers=self.headers, **kwargs)
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    time.sleep(float(retry_after) if retry_after else min(2 ** attempt, 10.0))
                    continue
                if response.status_code >= 500:
                    time.sleep(min(2 ** attempt, 10.0))
                    continue
                return response
            except (httpx.TimeoutException, httpx.RemoteProtocolError) as exc:
                last_exc = exc
                time.sleep(min(2 ** attempt, 10.0))
        raise RuntimeError(f"HubSpot request failed after retries: {last_exc}")

    def search_contact_by_email(self, email: str, properties: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        payload = {
            "filterGroups": [{"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]}],
            "properties": properties or ["email"],
            "limit": 1,
        }
        response = self._request("POST", f"{self.base_url}/crm/v3/objects/contacts/search", json=payload)
        if response.status_code != 200:
            return None
        results = (response.json() or {}).get("results", []) or []
        return results[0] if results else None

    def search_company_by_muni_key(self, muni_key: str, properties: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        payload = {
            "filterGroups": [{"filters": [{"propertyName": self.muni_key_property, "operator": "EQ", "value": muni_key}]}],
            "properties": properties or ["name", self.muni_key_property, self.company_restrict_property],
            "limit": 1,
        }
        response = self._request("POST", f"{self.base_url}/crm/v3/objects/companies/search", json=payload)
        if response.status_code != 200:
            return None
        results = (response.json() or {}).get("results", []) or []
        return results[0] if results else None

    def upsert_company(self, municipality_name: str, state: str, muni_key: str, priority: str, status: str) -> Optional[str]:
        company = self.search_company_by_muni_key(muni_key, properties=["name", self.muni_key_property])
        props = {
            "name": municipality_name,
            self.company_flag_property: "true",
            self.muni_key_property: muni_key,
            self.priority_property: priority,
            self.status_property: status,
            "state": state,
        }
        if company:
            company_id = company.get("id")
            response = self._request("PATCH", f"{self.base_url}/crm/v3/objects/companies/{company_id}", json={"properties": props})
            if response.status_code in {200, 204}:
                return str(company_id)
            return None
        response = self._request("POST", f"{self.base_url}/crm/v3/objects/companies", json={"properties": props})
        if response.status_code not in {200, 201}:
            return None
        return str((response.json() or {}).get("id") or "")

    def upsert_contact_from_finalized_lead(self, lead: Dict[str, Any], writer_version: str, research_version: str) -> Tuple[str, Optional[str]]:
        email = str(lead.get("contact_email") or "").strip().lower()
        first_name, last_name = split_name(str(lead.get("contact_full_name") or ""))
        props = {
            "email": email,
            "firstname": first_name,
            "lastname": last_name,
            "jobtitle": str(lead.get("contact_title") or ""),
            "company": str(lead.get("municipality_name") or ""),
            "state": str(lead.get("state") or ""),
            self.contact_flag_property: "true",
            self.muni_key_property: str(lead.get("municipality_key") or ""),
            self.priority_property: "Highest - Target",
            self.status_property: "active_contact",
            self.contact_status_property: "active_sequence",
            self.sequence_outcome_property: "active",
            self.personalization_tier_property: str(lead.get("personalization_tier") or ""),
            self.anchor_property: str(lead.get("personalization_anchor_text") or ""),
            self.contact_source_url_property: str(lead.get("contact_source_url") or ""),
            self.catalyst_source_url_property: str(lead.get("catalyst_source_url") or ""),
            self.corroboration_source_url_property: str(lead.get("corroboration_source_url") or ""),
            self.writer_version_property: writer_version,
            self.research_version_property: research_version,
            "tp_ai_subject": str(lead.get("subject_line") or ""),
            "tp_ai_email_body": str(lead.get("email_body") or ""),
            "tp_ai_confidence": str(lead.get("research_confidence") or ""),
        }
        existing = self.search_contact_by_email(
            email,
            properties=["email", self.muni_key_property],
        )
        if existing:
            contact_id = str(existing.get("id") or "")
            response = self._request("PATCH", f"{self.base_url}/crm/v3/objects/contacts/{contact_id}", json={"properties": props})
            if response.status_code in {200, 204}:
                return "updated", contact_id
            return "error", contact_id
        response = self._request("POST", f"{self.base_url}/crm/v3/objects/contacts", json={"properties": props})
        if response.status_code not in {200, 201}:
            return "error", None
        return "created", str((response.json() or {}).get("id") or "")

    def associate_contact_to_company(self, contact_id: str, company_id: str) -> bool:
        if not contact_id or not company_id:
            return False
        endpoint = f"{self.base_url}/crm/v3/objects/contacts/{contact_id}/associations/companies/{company_id}/contact_to_company"
        response = self._request("PUT", endpoint)
        return response.status_code in {200, 201, 204}

    def get_contact_outcome_snapshot(self, email: str) -> Optional[Dict[str, Any]]:
        props = [
            "email",
            self.contact_flag_property,
            self.muni_key_property,
            self.sequence_outcome_property,
            self.contact_status_property,
            self.contact_restrict_property,
            self.external_bounce_property,
        ]
        result = self.search_contact_by_email(email, properties=props)
        if not result:
            return None
        out = {"id": str(result.get("id") or ""), "properties": result.get("properties", {}) or {}}
        return out

    def get_company_outcome_snapshot(self, muni_key: str) -> Optional[Dict[str, Any]]:
        props = [self.muni_key_property, self.company_restrict_property, self.status_property]
        result = self.search_company_by_muni_key(muni_key, properties=props)
        if not result:
            return None
        return {"id": str(result.get("id") or ""), "properties": result.get("properties", {}) or {}}

    def classify_sync_outcome(self, contact_snapshot: Optional[Dict[str, Any]], company_snapshot: Optional[Dict[str, Any]]) -> str:
        company_props = (company_snapshot or {}).get("properties", {}) or {}
        contact_props = (contact_snapshot or {}).get("properties", {}) or {}

        if truthy(company_props.get(self.company_restrict_property)) or truthy(contact_props.get(self.contact_restrict_property)):
            return "restricted"
        if truthy(contact_props.get(self.external_bounce_property)):
            return "bounced"

        sequence_outcome = str(contact_props.get(self.sequence_outcome_property) or "").strip().lower()
        contact_status = str(contact_props.get(self.contact_status_property) or "").strip().lower()

        if sequence_outcome in {"replied", "meeting_booked"}:
            return sequence_outcome
        if sequence_outcome in {"no_response", "bounced", "invalid", "manual_stop", "restricted"}:
            return sequence_outcome
        if contact_status in {"replied", "meeting_booked", "no_response", "bounced", "invalid", "restricted"}:
            return contact_status
        return "active"