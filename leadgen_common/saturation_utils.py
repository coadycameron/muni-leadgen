import re

US_STATE_NAME_TO_CODE = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "district of columbia": "DC",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
}

US_STATE_CODES = set(US_STATE_NAME_TO_CODE.values())


def normalize_company_key(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def normalize_state_code(s: str) -> str:
    raw = str(s or "").strip()
    if not raw:
        return ""
    if raw.upper() == "UNKNOWN":
        return ""

    compact_upper = " ".join(raw.upper().split())
    if re.fullmatch(r"[A-Z]{2}", compact_upper) and compact_upper in US_STATE_CODES:
        return compact_upper

    compact_lower = " ".join(raw.lower().split())
    return US_STATE_NAME_TO_CODE.get(compact_lower, "")


def compute_saturation_key(company: str, state: str) -> str:
    company_key = normalize_company_key(company)
    if not company_key:
        return ""

    state_code = normalize_state_code(state)
    if state_code:
        return f"{company_key}|{state_code}"
    return company_key