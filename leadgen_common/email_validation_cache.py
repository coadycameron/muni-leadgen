# leadgen_common/email_validation_cache.py

import hashlib
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional

from google.cloud import firestore


def normalize_email(s) -> str:
    return str(s or "").strip().lower()


def _doc_id_for_email(email_norm: str) -> str:
    # Matches your tp_email_cache_v1 doc ids (sha256 hex, 64 chars)
    return hashlib.sha256(email_norm.encode("utf-8")).hexdigest()


def _norm_status(s) -> str:
    return str(s or "").strip().upper()


def _safe_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "y"}


class EmailValidationCache:
    """
    Firestore email validation cache.

    Default collection: tp_email_cache_v1
    TTL field: tp_email_cache_expires_at

    This version adds MyEmailVerifier fields:
    - mev_checked_at
    - mev_verdict (VALID|INVALID|RISKY)
    - mev_status
    - mev_is_catch_all
    - mev_is_disposable_domain
    - mev_is_free_domain
    - mev_is_role_based
    - mev_is_greylisted
    - mev_diagnosis
    - mev_reason

    And makes ZeroBounce verdict explicit:
    - zb_checked_at
    - zb_verdict (VALID|INVALID|RISKY)
    - zb_status (raw status from API if you want)
    - zb_sub_status
    - zb_reason
    """

    def __init__(self) -> None:
        self.collection = os.environ.get("FIRESTORE_EMAIL_CACHE_COLLECTION", "tp_email_cache_v1").strip()
        self.ttl_days = int(os.environ.get("FIRESTORE_EMAIL_CACHE_TTL_DAYS", "365"))

        self.db = firestore.Client()

    def _expires_at(self) -> datetime:
        return datetime.now(timezone.utc) + timedelta(days=self.ttl_days)

    def get_many(self, emails: Iterable[str]) -> Dict[str, Dict]:
        emails_norm = []
        for e in emails:
            en = normalize_email(e)
            if en and "@" in en:
                emails_norm.append(en)
        emails_norm = list(dict.fromkeys(emails_norm))
        if not emails_norm:
            return {}

        refs = [self.db.collection(self.collection).document(_doc_id_for_email(e)) for e in emails_norm]
        out: Dict[str, Dict] = {}

        for snap in self.db.get_all(refs):
            if not snap.exists:
                continue
            doc = snap.to_dict() or {}
            email_norm = normalize_email(str(doc.get("email") or "")) or ""
            if not email_norm:
                continue
            out[email_norm] = doc

        return out

    def is_fresh(self, doc: Dict) -> bool:
        exp = doc.get("tp_email_cache_expires_at")
        if not exp:
            return False
        try:
            exp_dt = exp if isinstance(exp, datetime) else None
            if exp_dt is None:
                return False
            if exp_dt.tzinfo is None:
                exp_dt = exp_dt.replace(tzinfo=timezone.utc)
            return exp_dt > datetime.now(timezone.utc)
        except Exception:
            return False

    def is_zb_valid(self, doc: Dict) -> bool:
        # Prefer explicit verdict
        if _norm_status(str(doc.get("zb_verdict") or "")) == "VALID":
            return True
        # Fallback to legacy field
        return _norm_status(str(doc.get("zb_status") or "")) == "VALID"

    def has_vf_result(self, doc: Dict) -> bool:
        return bool(
            doc.get("vf_checked_at")
            or doc.get("vf_verdict")
            or doc.get("vf_status")
            or doc.get("vf_classification")
            or doc.get("vf_reason")
        )

    def is_vf_valid(self, doc: Dict) -> bool:
        if _norm_status(str(doc.get("vf_verdict") or "")) == "VALID":
            return True

        vf_status = str(doc.get("vf_status") or "")
        vf_class = str(doc.get("vf_classification") or "")
        if vf_status and vf_class:
            return _norm_status(vf_status) == "SUCCESS" and _norm_status(vf_class) == "DELIVERABLE"

        return False

    def has_mev_result(self, doc: Dict) -> bool:
        return bool(
            doc.get("mev_checked_at")
            or doc.get("mev_verdict")
            or doc.get("mev_status")
            or doc.get("mev_reason")
        )

    def is_mev_valid(self, doc: Dict) -> bool:
        return _norm_status(str(doc.get("mev_verdict") or "")) == "VALID"

    def upsert_verifalia_result(
        self,
        email_norm: str,
        verdict: str,
        status_raw: str,
        classification_raw: str,
        is_disposable: bool,
        is_role: bool,
        quality: str,
        reason: str,
    ) -> None:
        e = normalize_email(email_norm)
        if not e or "@" not in e:
            return

        ref = self.db.collection(self.collection).document(_doc_id_for_email(e))

        payload = {
            "email": e,
            "updated_at": firestore.SERVER_TIMESTAMP,
            "tp_email_cache_expires_at": self._expires_at(),
            "vf_checked_at": firestore.SERVER_TIMESTAMP,
            "vf_verdict": _norm_status(verdict),
            "vf_status": (status_raw or "").strip(),
            "vf_classification": (classification_raw or "").strip(),
            "vf_is_disposable_email_address": bool(is_disposable),
            "vf_is_role_account": bool(is_role),
            "vf_quality": (quality or "").strip(),
            "vf_reason": (reason or "").strip(),
        }

        ref.set(payload, merge=True)

    def upsert_myemailverifier_result(
        self,
        email_norm: str,
        verdict: str,
        status_raw: str,
        catch_all: bool,
        is_disposable: bool,
        is_free: bool,
        is_role: bool,
        is_greylisted: bool,
        diagnosis: str,
        reason: str,
    ) -> None:
        """
        Store MyEmailVerifier results in the same tp_email_cache_v1 doc.

        verdict should be: VALID | INVALID | RISKY
        status_raw should be the vendor Status value (typically valid|invalid|unknown|catch_all|greylisted)
        """
        e = normalize_email(email_norm)
        if not e or "@" not in e:
            return

        ref = self.db.collection(self.collection).document(_doc_id_for_email(e))

        payload = {
            "email": e,
            "updated_at": firestore.SERVER_TIMESTAMP,
            "tp_email_cache_expires_at": self._expires_at(),
            "mev_checked_at": firestore.SERVER_TIMESTAMP,
            "mev_verdict": _norm_status(verdict),
            "mev_status": (status_raw or "").strip(),
            "mev_is_catch_all": bool(catch_all),
            "mev_is_disposable_domain": bool(is_disposable),
            "mev_is_free_domain": bool(is_free),
            "mev_is_role_based": bool(is_role),
            "mev_is_greylisted": bool(is_greylisted),
            "mev_diagnosis": (diagnosis or "").strip(),
            "mev_reason": (reason or "").strip(),
        }

        ref.set(payload, merge=True)

    def upsert_zerobounce_result(
        self,
        email_norm: str,
        verdict: str,
        status_raw: str,
        sub_status_raw: str,
        reason: str,
        raw: Optional[Dict] = None,
    ) -> None:
        """
        Store ZeroBounce results. Keep both a normalized verdict and the raw status fields.

        verdict should be: VALID | INVALID | RISKY
        status_raw is the ZB "status" field
        sub_status_raw is the ZB "sub_status" field
        """
        e = normalize_email(email_norm)
        if not e or "@" not in e:
            return

        ref = self.db.collection(self.collection).document(_doc_id_for_email(e))

        payload: Dict[str, Any] = {
            "email": e,
            "updated_at": firestore.SERVER_TIMESTAMP,
            "tp_email_cache_expires_at": self._expires_at(),
            "zb_checked_at": firestore.SERVER_TIMESTAMP,
            "zb_verdict": _norm_status(verdict),
            # keep legacy field for compatibility if anything reads zb_status today
            "zb_status": (status_raw or "").strip(),
            "zb_sub_status": (sub_status_raw or "").strip(),
            "zb_reason": (reason or "").strip(),
        }

        if raw is not None and os.environ.get("ZEROBOUNCE_STORE_RAW", "0").strip().lower() in {"1", "true", "yes"}:
            payload["zb_raw"] = raw

        ref.set(payload, merge=True)