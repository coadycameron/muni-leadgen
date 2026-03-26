import os
import time
from typing import Dict, Iterable, List, Set, Tuple, Optional

import httpx


def normalize_email(s: str) -> str:
    return (s or "").strip().lower()


def _truthy(v: str) -> bool:
    s = (v or "").strip().lower()
    return s in {"1", "true", "yes", "y", "t", "on"}


def _email_domain(email_norm: str) -> str:
    if not email_norm or "@" not in email_norm:
        return ""
    return email_norm.split("@", 1)[1].strip().lower()


def _normalize_company_key(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


class HubSpotDedupe:
    """
    Dedupe by primary email using HubSpot CRM v3 batch read with idProperty=email.

    Also supports a company gate:
    - If a matching HubSpot company exists AND its company restriction property is TRUE,
      discard the lead before downstream verification/sending.

    Env vars:
    - HUBSPOT_PRIVATE_APP_TOKEN or HUBSPOT_API_KEY (required)
    - HUBSPOT_BASE_URL (optional, default https://api.hubapi.com)
    - HUBSPOT_TIMEOUT_S (optional, default 20)
    - HUBSPOT_MAX_RETRIES (optional, default 3)
    - HUBSPOT_COMPANY_RESTRICT_PROPERTY (optional, default "company-lead-gen-restrict")
    """

    def __init__(self) -> None:
        token = (os.environ.get("HUBSPOT_PRIVATE_APP_TOKEN", "") or os.environ.get("HUBSPOT_API_KEY", "")).strip()
        if not token:
            raise RuntimeError("Missing HUBSPOT_PRIVATE_APP_TOKEN (or HUBSPOT_API_KEY) in environment.")

        self.base_url = os.environ.get("HUBSPOT_BASE_URL", "https://api.hubapi.com").rstrip("/")
        self.timeout_s = float(os.environ.get("HUBSPOT_TIMEOUT_S", "20"))
        self.max_retries = int(os.environ.get("HUBSPOT_MAX_RETRIES", "3"))
        self.company_restrict_property = os.environ.get(
            "HUBSPOT_COMPANY_RESTRICT_PROPERTY",
            "company-lead-gen-restrict",
        ).strip() or "company-lead-gen-restrict"

        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def _request_with_retry(self, client: httpx.Client, method: str, url: str, **kwargs) -> httpx.Response:
        last_exc: Optional[Exception] = None

        for attempt in range(1, self.max_retries + 1):
            try:
                r = client.request(method, url, headers=self.headers, **kwargs)

                if r.status_code == 429:
                    retry_after = r.headers.get("Retry-After")
                    sleep_s = float(retry_after) if retry_after else 5.0
                    time.sleep(min(sleep_s, 30.0))
                    continue

                return r

            except (httpx.TimeoutException, httpx.RemoteProtocolError) as e:
                last_exc = e
                time.sleep(min(2.0 ** attempt, 10.0))

        raise RuntimeError(f"HubSpot request failed after {self.max_retries} retries: {last_exc}")

    def existing_primary_emails(self, emails: Iterable[str]) -> Set[str]:
        """
        Returns the subset of emails that already exist as HubSpot contacts by primary email.

        Uses:
        POST /crm/v3/objects/contacts/batch/read with idProperty=email
        Batch size: 100 max per request
        """
        emails_norm = [normalize_email(e) for e in emails if e and "@" in e]
        emails_norm = list(dict.fromkeys(emails_norm))
        if not emails_norm:
            return set()

        endpoint = f"{self.base_url}/crm/v3/objects/contacts/batch/read"
        existing: Set[str] = set()

        with httpx.Client(timeout=self.timeout_s) as client:
            for i in range(0, len(emails_norm), 100):
                chunk = emails_norm[i : i + 100]
                payload = {
                    "properties": ["email"],
                    "idProperty": "email",
                    "inputs": [{"id": e} for e in chunk],
                }

                r = self._request_with_retry(client, "POST", endpoint, json=payload)
                if r.status_code >= 400:
                    continue

                data = r.json()
                for obj in data.get("results", []):
                    props = obj.get("properties", {}) or {}
                    e = normalize_email(props.get("email", ""))
                    if e:
                        existing.add(e)

        return existing

    def _search_companies(
        self,
        client: httpx.Client,
        filter_groups: List[Dict],
        properties: List[str],
        limit: int = 5,
    ) -> List[Dict]:
        endpoint = f"{self.base_url}/crm/v3/objects/companies/search"
        payload = {
            "filterGroups": filter_groups,
            "properties": properties,
            "limit": int(limit),
        }
        r = self._request_with_retry(client, "POST", endpoint, json=payload)
        if r.status_code >= 400:
            return []
        data = r.json() or {}
        return list(data.get("results", []) or [])

    def is_company_restricted(self, company_name: str, email_domain: str, restrict_property: Optional[str] = None) -> bool:
        """
        Returns True only if:
        - a matching HubSpot company exists (by domain or name search), AND
        - its restrict_property evaluates truthy.

        Matching order:
        1) domain EQ email_domain (if present)
        2) name CONTAINS_TOKEN company_name (if present)
        """
        prop = (restrict_property or self.company_restrict_property or "").strip()
        if not prop:
            prop = "company-lead-gen-restrict"

        cname = (company_name or "").strip()
        dom = (email_domain or "").strip().lower()

        if not cname and not dom:
            return False

        wanted_props = ["name", "domain", prop]

        with httpx.Client(timeout=self.timeout_s) as client:
            if dom and "." in dom:
                results = self._search_companies(
                    client=client,
                    filter_groups=[{"filters": [{"propertyName": "domain", "operator": "EQ", "value": dom}]}],
                    properties=wanted_props,
                    limit=5,
                )
                if results:
                    for obj in results:
                        props = obj.get("properties", {}) or {}
                        if _truthy(str(props.get(prop, "") or "")):
                            return True
                    return False

            if cname:
                results = self._search_companies(
                    client=client,
                    filter_groups=[{"filters": [{"propertyName": "name", "operator": "CONTAINS_TOKEN", "value": cname}]}],
                    properties=wanted_props,
                    limit=5,
                )
                if results:
                    for obj in results:
                        props = obj.get("properties", {}) or {}
                        if _truthy(str(props.get(prop, "") or "")):
                            return True
                    return False

        return False


def filter_new_leads_against_hubspot(
    rows: List[List[str]],
    email_col_index: int = 0,
) -> Tuple[List[List[str]], Set[str]]:
    """
    Filters lead rows by removing any row whose email already exists in HubSpot.

    Returns:
    - kept_rows
    - removed_emails
    """
    deduper = HubSpotDedupe()
    emails = [r[email_col_index] for r in rows if len(r) > email_col_index]
    existing = deduper.existing_primary_emails(emails)

    kept: List[List[str]] = []
    removed: Set[str] = set()

    for r in rows:
        if len(r) <= email_col_index:
            continue
        e = normalize_email(r[email_col_index])
        if e and e in existing:
            removed.add(e)
            continue
        kept.append(r)

    return kept, removed


def filter_rows_by_hubspot_company_restrict(
    rows: List[List[str]],
    email_col_index: int = 0,
    company_col_index: int = 3,
    restrict_property: Optional[str] = None,
) -> Tuple[List[List[str]], Set[str], Set[str]]:
    """
    Filters lead rows by removing any row where:
    - a matching HubSpot company exists, AND
    - the company restriction property is TRUE.

    Matching uses:
    - email domain first (company.domain EQ <email_domain>), then
    - company name fallback (company.name CONTAINS_TOKEN <company_name>)

    Returns:
    - kept_rows
    - removed_emails
    - removed_companies (original company strings)
    """
    deduper = HubSpotDedupe()
    prop = (restrict_property or deduper.company_restrict_property or "").strip() or "company-lead-gen-restrict"

    cache: Dict[Tuple[str, str], bool] = {}

    kept: List[List[str]] = []
    removed_emails: Set[str] = set()
    removed_companies: Set[str] = set()

    for r in rows:
        if len(r) <= max(email_col_index, company_col_index):
            continue

        e = normalize_email(r[email_col_index])
        dom = _email_domain(e)
        c = str(r[company_col_index] or "").strip()
        ckey = _normalize_company_key(c)

        key = (dom, ckey)
        if key not in cache:
            cache[key] = deduper.is_company_restricted(company_name=c, email_domain=dom, restrict_property=prop)

        if cache[key]:
            if e:
                removed_emails.add(e)
            if c:
                removed_companies.add(c)
            continue

        kept.append(r)

    return kept, removed_emails, removed_companies