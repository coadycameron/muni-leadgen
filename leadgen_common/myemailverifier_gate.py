import os
import random
import time
from typing import Dict, Optional, Tuple

import httpx

from leadgen_common.email_validation_cache import EmailValidationCache, normalize_email


def _norm(s) -> str:
    return str(s or "").strip().lower()


def _local_part(email_norm: str) -> str:
    if "@" not in email_norm:
        return ""
    return email_norm.split("@", 1)[0].strip().lower()


def _split_tokens(s: str) -> set:
    tokens = set()
    buf = ""
    for ch in s:
        if ch.isalnum():
            buf += ch.lower()
        else:
            if buf:
                tokens.add(buf)
                buf = ""
    if buf:
        tokens.add(buf)
    return tokens


def _is_blocked_role(email_norm: str) -> bool:
    lp = _local_part(email_norm)
    toks = _split_tokens(lp)
    block = [
        x.strip().lower()
        for x in os.environ.get("ROLE_INBOX_BLOCKLIST", "info,estimating,estimate,estimates,bid,bids,sales").split(",")
        if x.strip()
    ]
    for b in block:
        if b in toks:
            return True
        if b and b in lp:
            return True
    return False


def _is_allowlisted_role(email_norm: str) -> bool:
    lp = _local_part(email_norm)
    allow = [x.strip().lower() for x in os.environ.get("ROLE_INBOX_ALLOWLIST", "qc,qualitycontrol,ops").split(",") if x.strip()]
    return lp in set(allow)


class MyEmailVerifierClient:
    """
    Endpoints per vendor docs:
    - GET https://client.myemailverifier.com/verifier/validate_single/[email]/API_KEY
    - GET https://client.myemailverifier.com/verifier/getcredits/API_KEY
    """

    def __init__(self) -> None:
        self.api_key = os.environ.get("MYEMAILVERIFIER_API_KEY", "").strip()
        if not self.api_key:
            raise RuntimeError("Missing MYEMAILVERIFIER_API_KEY")

        self.base_url = os.environ.get("MYEMAILVERIFIER_BASE_URL", "https://client.myemailverifier.com").rstrip("/")

        self.connect_timeout_s = float(os.environ.get("MYEMAILVERIFIER_CONNECT_TIMEOUT_S", "5"))
        self.read_timeout_s = float(os.environ.get("MYEMAILVERIFIER_READ_TIMEOUT_S", os.environ.get("MYEMAILVERIFIER_TIMEOUT_S", "20")))
        self.max_retries = int(os.environ.get("MYEMAILVERIFIER_MAX_RETRIES", "3"))

        self.cache_enabled = os.environ.get("FIRESTORE_EMAIL_CACHE_ENABLED", "1").strip().lower() in {"1", "true", "yes"}
        self.cache = EmailValidationCache() if self.cache_enabled else None

        self._timeout = httpx.Timeout(
            connect=self.connect_timeout_s,
            read=self.read_timeout_s,
            write=10.0,
            pool=5.0,
        )
        self._client = httpx.Client(
            timeout=self._timeout,
            headers={"User-Agent": "lead-gen-agent/1.0"},
        )

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass

    def _get_json_with_retry(self, url: str) -> Dict:
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = self._client.get(url)

                if r.status_code == 429:
                    time.sleep(min((2 ** attempt) + random.random(), 30.0))
                    continue

                if r.status_code in {500, 502, 503, 504}:
                    time.sleep(min((2 ** attempt) + random.random(), 10.0))
                    continue

                r.raise_for_status()
                return r.json() or {}

            except (httpx.ReadTimeout, httpx.ConnectTimeout) as e:
                last_exc = e
                time.sleep(min((2 ** attempt) + random.random(), 10.0))
            except Exception as e:
                last_exc = e
                time.sleep(min((2 ** attempt) + random.random(), 10.0))

        raise RuntimeError(f"MyEmailVerifier request failed after retries: {last_exc}")

    def validate_single(self, email: str) -> Dict:
        email_norm = normalize_email(email)
        url = f"{self.base_url}/verifier/validate_single/{email_norm}/{self.api_key}"
        return self._get_json_with_retry(url)

    def get_credits_balance(self) -> int:
        url = f"{self.base_url}/verifier/getcredits/{self.api_key}"
        data = self._get_json_with_retry(url)
        raw = data.get("credits", 0)
        try:
            return int(float(str(raw).strip() or "0"))
        except Exception:
            raise RuntimeError(f"Unexpected MyEmailVerifier credits payload: {data}")


def mev_verdict(email_norm: str, mev_json: Dict) -> Tuple[str, str]:
    status = _norm(mev_json.get("Status", ""))
    disposable = _norm(mev_json.get("Disposable_Domain", "false")) == "true"
    free_domain = _norm(mev_json.get("Free_Domain", "false")) == "true"
    role_based = _norm(mev_json.get("Role_Based", "false")) == "true"
    catch_all = _norm(mev_json.get("catch_all", "false")) == "true"
    greylisted = _norm(mev_json.get("Greylisted", "false")) == "true"

    if disposable:
        return "INVALID", "mev_disposable"

    if free_domain and os.environ.get("ALLOW_FREE_DOMAINS", "0").strip().lower() not in {"1", "true", "yes"}:
        return "INVALID", "mev_free_domain"

    if role_based and _is_blocked_role(email_norm):
        return "INVALID", "mev_blocked_role"

    if status == "valid":
        if catch_all:
            return "RISKY", "mev_valid_but_catch_all"
        if greylisted:
            return "RISKY", "mev_valid_but_greylisted"
        if role_based and not _is_allowlisted_role(email_norm):
            return "RISKY", "mev_role_not_allowlisted"
        return "VALID", "mev_valid"

    if status == "invalid":
        return "INVALID", "mev_invalid"

    if status in {"unknown", "catch_all", "catch all", "grey-listed", "grey_listed"}:
        return "RISKY", f"mev_{status.replace(' ', '_')}"

    return "RISKY", f"mev_status_{status or 'unknown'}"