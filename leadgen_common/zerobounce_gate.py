# leadgen_common/zerobounce_gate.py

import os
import time
from typing import Dict, Optional, Tuple

import httpx

from leadgen_common.email_validation_cache import EmailValidationCache, normalize_email


def _norm(s) -> str:
    return str(s or "").strip().lower().replace(" ", "_")


class ZeroBounceClient:
    """
    Validation endpoint:
    GET https://api.zerobounce.net/v2/validate?api_key=...&email=...&ip_address=...
    """

    def __init__(self) -> None:
        self.api_key = os.environ.get("ZEROBOUNCE_API_KEY", "").strip()
        if not self.api_key:
            raise RuntimeError("Missing ZEROBOUNCE_API_KEY")

        self.base_url = os.environ.get("ZEROBOUNCE_BASE_URL", "https://api.zerobounce.net/v2").rstrip("/")
        self.timeout_s = float(os.environ.get("ZEROBOUNCE_TIMEOUT_S", "20"))
        self.max_retries = int(os.environ.get("ZEROBOUNCE_MAX_RETRIES", "3"))
        self.ip_address = os.environ.get("ZEROBOUNCE_IP_ADDRESS", "").strip()

        self.cache_enabled = os.environ.get("FIRESTORE_EMAIL_CACHE_ENABLED", "1").strip().lower() in {"1", "true", "yes"}
        self.cache = EmailValidationCache() if self.cache_enabled else None

    def validate(self, email: str) -> Dict:
        email_norm = normalize_email(email)
        url = f"{self.base_url}/validate"
        params = {"api_key": self.api_key, "email": email_norm, "ip_address": self.ip_address}

        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                with httpx.Client(timeout=self.timeout_s) as client:
                    r = client.get(url, params=params)
                if r.status_code == 429:
                    time.sleep(min(2 ** attempt, 30.0))
                    continue
                r.raise_for_status()
                return r.json() or {}
            except Exception as e:
                last_exc = e
                time.sleep(min(2 ** attempt, 10.0))

        raise RuntimeError(f"ZeroBounce validate failed after retries: {last_exc}")


def zb_verdict(zb_json: Dict) -> Tuple[str, str]:
    status = _norm(zb_json.get("status", ""))
    sub = _norm(zb_json.get("sub_status", ""))

    hard_bad_sub = {"toxic", "disposable", "global_suppression", "possible_traps", "possible_trap"}

    if sub in hard_bad_sub:
        return "INVALID", f"zb_sub_{sub}"

    if status == "valid":
        return "VALID", "zb_valid"

    if status in {"invalid", "spamtrap", "abuse", "do_not_mail"}:
        return "INVALID", f"zb_{status}:{sub or 'none'}"

    if status in {"catch_all", "catchall", "unknown"}:
        return "RISKY", f"zb_{status}:{sub or 'none'}"

    return "RISKY", f"zb_status_{status or 'unknown'}:{sub or 'none'}"