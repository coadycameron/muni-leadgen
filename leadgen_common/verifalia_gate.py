# leadgen_common/verifalia_gate.py

import os
import time
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import httpx

from leadgen_common.email_validation_cache import EmailValidationCache, normalize_email


def _norm(s) -> str:
    return str(s or "").strip().lower().replace(" ", "_").replace("-", "_")

class VerifaliaCreditsDepletedError(RuntimeError):
    pass

class VerifaliaClient:
    """
    Verifalia integration:
    - credits precheck via GET /credits/balance
    - email verification via POST /email-validations (+ polling when 202)

    Cache behavior:
    - Checks tp_email_cache_v1 first
    - If cache has zb_status == VALID or vf_verdict == VALID and not expired, skip Verifalia
    - Writes Verifalia results back into tp_email_cache_v1 with a 365 day expiry

    Accept rule (defaults):
    - Keep only entries where status == Success and classification == Deliverable
    - Reject disposable email addresses
    - Reject role accounts if VERIFALIA_REJECT_ROLE_ACCOUNTS=1
    """

    def _request_with_retry(self, client: httpx.Client, method: str, url: str, **kwargs) -> httpx.Response:
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = client.request(method, url, **kwargs)

                if r.status_code == 429:
                    retry_after = r.headers.get("Retry-After")
                    sleep_s = float(retry_after) if retry_after else min(2 ** attempt, 10)
                    time.sleep(min(sleep_s, 30.0))
                    continue

                if r.status_code >= 500:
                    time.sleep(min(2 ** attempt, 10))
                    continue

                return r
            except (httpx.TimeoutException, httpx.RemoteProtocolError) as e:
                last_exc = e
                time.sleep(min(2 ** attempt, 10))

        raise RuntimeError(f"Verifalia request failed after retries: {last_exc}")

    def __init__(self) -> None:
        self.username = os.environ.get("VERIFALIA_USERNAME", "").strip()
        self.password = os.environ.get("VERIFALIA_PASSWORD", "").strip()
        if not self.username or not self.password:
            raise RuntimeError("Missing VERIFALIA_USERNAME or VERIFALIA_PASSWORD")
        self.skip_when_credits_depleted = os.environ.get("VERIFALIA_SKIP_WHEN_CREDITS_DEPLETED", "0").strip().lower() in {"1", "true", "yes"}

        self.base_url = (
            os.environ.get("VERIFALIA_BASE_URL", "").strip()
            or os.environ.get("VERIFALIA_API_BASE", "").strip()
            or "https://api-1.verifalia.com/v2.7"
        ).rstrip("/")
        self.timeout_s = float(os.environ.get("VERIFALIA_TIMEOUT_S", "30"))
        self.max_retries = int(os.environ.get("VERIFALIA_MAX_RETRIES", "3"))

        self.wait_time_ms = int(os.environ.get("VERIFALIA_WAIT_TIME_MS", "30000"))
        self.max_poll_seconds = int(os.environ.get("VERIFALIA_MAX_POLL_SECONDS", "120"))

        self.quality = os.environ.get("VERIFALIA_QUALITY", "Standard").strip()
        self.batch_size = int(os.environ.get("VERIFALIA_BATCH_SIZE", "2"))

        self.accept_statuses = {_norm(x) for x in os.environ.get("VERIFALIA_ACCEPT_STATUSES", "Success").split(",") if x.strip()}
        self.accept_classifications = {_norm(x) for x in os.environ.get("VERIFALIA_ACCEPT_CLASSIFICATIONS", "Deliverable").split(",") if x.strip()}

        self.reject_disposable = os.environ.get("VERIFALIA_REJECT_DISPOSABLE", "1").strip().lower() in {"1", "true", "yes"}
        self.reject_role_accounts = os.environ.get("VERIFALIA_REJECT_ROLE_ACCOUNTS", "0").strip().lower() in {"1", "true", "yes"}

        self.fail_open = os.environ.get("VERIFALIA_FAIL_OPEN", "0").strip().lower() in {"1", "true", "yes"}
        self.credits_check_fail_open = os.environ.get("VERIFALIA_CREDITS_CHECK_FAIL_OPEN", "0").strip().lower() in {"1", "true", "yes"}

        self.cache_enabled = os.environ.get("FIRESTORE_EMAIL_CACHE_ENABLED", "1").strip().lower() in {"1", "true", "yes"}
        self.cache = EmailValidationCache() if self.cache_enabled else None

        self._cache: Dict[str, Tuple[bool, str]] = {}

    def _auth(self) -> httpx.BasicAuth:
        return httpx.BasicAuth(self.username, self.password)

    def get_credits_balance(self) -> Tuple[float, float, Optional[str]]:
        url = f"{self.base_url}/credits/balance"
        with httpx.Client(timeout=self.timeout_s, auth=self._auth()) as client:
            r = self._request_with_retry(client, "GET", url)
            if r.status_code == 403:
                raise RuntimeError("Credits balance forbidden. Missing credits:read permission.")
            if r.status_code == 401:
                raise RuntimeError("Unauthorized. Check Verifalia credentials.")
            r.raise_for_status()
            data = r.json() or {}
            credit_packs = float(data.get("creditPacks", 0) or 0)
            free_credits = float(data.get("freeCredits", 0) or 0)
            reset_in = data.get("freeCreditsResetIn", None)
            return credit_packs, free_credits, reset_in

    def ensure_min_credits_or_exit(self, min_required: float) -> None:
        try:
            credit_packs, free_credits, reset_in = self.get_credits_balance()
        except Exception as e:
            if self.credits_check_fail_open:
                print(f"Warning: Verifalia credits check failed but continuing (fail open). Error: {e}")
                return
            raise SystemExit(f"Stopping early. Verifalia credits check failed: {e}")

        available = credit_packs + free_credits
        if available < float(min_required):
            raise SystemExit(
                f"Stopping early. Verifalia credits too low. "
                f"available={available:.3f} required={float(min_required):.3f} "
                f"freeCreditsResetIn={reset_in or 'UNKNOWN'}"
            )

    def _post_job(self, emails: List[str]) -> Tuple[int, Dict, Optional[str]]:
        url = f"{self.base_url}/email-validations"
        params = {"waitTime": str(self.wait_time_ms)}
        payload = {
            "name": os.environ.get("VERIFALIA_JOB_NAME", "qaqc-leadgen").strip(),
            "quality": self.quality,
            "deduplication": "Off",
            "entries": [{"inputData": e} for e in emails],
        }

        with httpx.Client(timeout=self.timeout_s, auth=self._auth()) as client:
            r = self._request_with_retry(
                client,
                "POST",
                url,
                params=params,
                json=payload,
                headers={"Content-Type": "application/json"},
            )

        location = r.headers.get("Location")

        if r.status_code in {200, 202}:
            try:
                return r.status_code, (r.json() or {}), location
            except Exception:
                return r.status_code, {}, location

        if r.status_code == 402:
            raise VerifaliaCreditsDepletedError("Verifalia returned 402 (credits depleted).")

        r.raise_for_status()
        return r.status_code, {}, location

    def _extract_job_id(self, job: Dict, location: Optional[str]) -> Optional[str]:
        ov = job.get("overview") if isinstance(job, dict) else None
        if isinstance(ov, dict) and ov.get("id"):
            return str(ov["id"])

        if location:
            path = urlparse(location).path.rstrip("/")
            if path:
                return path.split("/")[-1]
        return None

    def _get_job_snapshot(self, job_id: str) -> Tuple[int, Dict]:
        url = f"{self.base_url}/email-validations/{job_id}"
        params = {"waitTime": str(self.wait_time_ms)}
        with httpx.Client(timeout=self.timeout_s, auth=self._auth()) as client:
            r = self._request_with_retry(client, "GET", url, params=params)
            if r.status_code in {200, 202}:
                try:
                    return r.status_code, (r.json() or {})
                except Exception:
                    return r.status_code, {}
            if r.status_code == 402:
                raise VerifaliaCreditsDepletedError("Verifalia returned 402 while polling (credits depleted).")
            r.raise_for_status()
            return r.status_code, {}

    def _wait_for_completion(self, job_id: str) -> Dict:
        deadline = time.time() + self.max_poll_seconds
        last: Dict = {}

        while time.time() < deadline:
            code, snap = self._get_job_snapshot(job_id)
            last = snap

            if code == 200:
                ov = snap.get("overview") if isinstance(snap, dict) else None
                status = str((ov or {}).get("status", "") or "")
                if status.lower() == "completed":
                    return snap
                time.sleep(2.0)
                continue

            time.sleep(3.0)

        raise RuntimeError(f"Verifalia job polling timed out. last_snapshot_overview={last.get('overview')}")

    def _extract_entries(self, snap: Dict) -> List[Dict]:
        entries = snap.get("entries") if isinstance(snap, dict) else None
        if isinstance(entries, dict) and isinstance(entries.get("data"), list):
            return entries["data"]
        if isinstance(entries, list):
            return entries
        return []

    def verify_emails(self, emails: List[str]) -> Dict[str, Tuple[bool, str]]:
        emails_norm = []
        for e in emails:
            en = normalize_email(e)
            if en and "@" in en:
                emails_norm.append(en)
        emails_norm = list(dict.fromkeys(emails_norm))

        out: Dict[str, Tuple[bool, str]] = {}

        # In memory cache
        for e in emails_norm:
            if e in self._cache:
                out[e] = self._cache[e]

        # Firestore cache short circuit
        if self.cache_enabled and self.cache is not None:
            docs = self.cache.get_many([e for e in emails_norm if e not in out])
            for e, doc in docs.items():
                if not self.cache.is_fresh(doc):
                    continue
                if self.cache.is_zb_valid(doc) or self.cache.is_vf_valid(doc) or self.cache.is_mev_valid(doc):
                    res = (True, "cache_valid")
                    self._cache[e] = res
                    out[e] = res

        pending = [e for e in emails_norm if e not in out]
        if not pending:
            return out

        for i in range(0, len(pending), self.batch_size):
            chunk = pending[i : i + self.batch_size]

            try:
                status_code, job, location = self._post_job(chunk)

                if status_code == 202:
                    job_id = self._extract_job_id(job, location)
                    if not job_id:
                        if self.fail_open:
                            for e in chunk:
                                res = (True, "fail_open_no_job_id")
                                self._cache[e] = res
                                out[e] = res
                            continue
                        raise RuntimeError("Verifalia returned 202 but no job id could be determined.")
                    job = self._wait_for_completion(job_id)

            except VerifaliaCreditsDepletedError:
                to_handle = chunk + pending[i + self.batch_size :]

                for e in to_handle:
                    if e in out:
                        continue

                    out[e] = (False, "credits_depleted_unverified")
                    self._cache[e] = out[e]  # in-memory only

                return out

            entries = self._extract_entries(job)

            # Map by inputData
            seen_in_response: Set[str] = set()
            for entry in entries:
                key = normalize_email(str(entry.get("inputData", "") or "")) or normalize_email(str(entry.get("emailAddress", "") or ""))
                if not key:
                    continue
                seen_in_response.add(key)

                status_raw = str(entry.get("status", "") or "")
                classification_raw = str(entry.get("classification", "") or "")

                status = _norm(status_raw)
                classification = _norm(classification_raw)

                is_disposable = bool(entry.get("isDisposableEmailAddress", False))
                is_role = bool(entry.get("isRoleAccount", False))

                verdict = "INVALID"
                reason = ""

                if self.reject_disposable and is_disposable:
                    verdict = "INVALID"
                    reason = "disposable"
                    res = (False, "disposable")
                elif self.reject_role_accounts and is_role:
                    verdict = "INVALID"
                    reason = "role_account"
                    res = (False, "role_account")
                elif status in self.accept_statuses and (not self.accept_classifications or classification in self.accept_classifications):
                    verdict = "VALID"
                    reason = "verifalia_accept"
                    res = (True, "valid")
                else:
                    verdict = "INVALID"
                    reason = f"rejected:{classification or 'unknown'}:{status or 'unknown'}"
                    res = (False, reason)

                self._cache[key] = res
                out[key] = res

                if self.cache_enabled and self.cache is not None:
                    self.cache.upsert_verifalia_result(
                        email_norm=key,
                        verdict=verdict,
                        status_raw=status_raw,
                        classification_raw=classification_raw,
                        is_disposable=is_disposable,
                        is_role=is_role,
                        quality=self.quality,
                        reason=reason,
                    )

            # Missing results
            for e in chunk:
                if e in out:
                    continue
                res = (True, "fail_open_missing_result") if self.fail_open else (False, "missing_result")
                self._cache[e] = res
                out[e] = res

                if self.cache_enabled and self.cache is not None:
                    self.cache.upsert_verifalia_result(
                        email_norm=e,
                        verdict="VALID" if res[0] else "INVALID",
                        status_raw="",
                        classification_raw="",
                        is_disposable=False,
                        is_role=False,
                        quality=self.quality,
                        reason=res[1],
                    )

        return out


def filter_rows_by_email_verifier(
    rows: List[List[str]],
    email_col_index: int = 0,
) -> Tuple[List[List[str]], Dict[str, str]]:
    client = VerifaliaClient()

    emails: List[str] = []
    for r in rows:
        if len(r) <= email_col_index:
            continue
        e = normalize_email(r[email_col_index])
        if e and "@" in e:
            emails.append(e)

    results = client.verify_emails(emails)

    kept: List[List[str]] = []
    removed: Dict[str, str] = {}
    seen: Set[str] = set()

    for r in rows:
        if len(r) <= email_col_index:
            continue

        e = normalize_email(r[email_col_index])
        if not e or "@" not in e:
            removed[e or ""] = "invalid"
            continue

        if e in seen:
            continue
        seen.add(e)

        keep, reason = results.get(e, (False, "no_result"))
        if not keep:
            removed[e] = reason
            continue

        kept.append(r)

    return kept, removed