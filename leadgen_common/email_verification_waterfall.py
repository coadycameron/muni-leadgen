import os
import random
import time
from typing import Dict, List, Set, Tuple

from leadgen_common.daily_verifier_budget import DailyBudgetExhaustedError, DailyVerifierBudget
from leadgen_common.email_validation_cache import EmailValidationCache, normalize_email
from leadgen_common.myemailverifier_gate import MyEmailVerifierClient, mev_verdict
from leadgen_common.verifalia_gate import VerifaliaClient
from leadgen_common.zerobounce_gate import ZeroBounceClient, zb_verdict


class AllVerifiersExhaustedError(RuntimeError):
    pass


def _is_allowlisted_role_localpart(email_norm: str) -> bool:
    lp = _local_part(email_norm)
    allow = [x.strip().lower() for x in os.environ.get("ROLE_INBOX_ALLOWLIST", "qc,qualitycontrol,ops").split(",") if x.strip()]
    return lp in set(allow)


def _cached_zb_verdict(doc: Dict) -> str:
    # returns VALID | INVALID | RISKY | ""
    v = str(doc.get("zb_verdict") or "").strip().upper()
    if v in {"VALID", "INVALID", "RISKY"}:
        return v
    # fallback to raw zb_status
    s = str(doc.get("zb_status") or "").strip().lower()
    if s == "valid":
        return "VALID"
    if s in {"invalid", "spamtrap", "abuse", "do_not_mail"}:
        return "INVALID"
    if s in {"catch_all", "catchall", "unknown"}:
        return "RISKY"
    return ""


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
    block = [x.strip().lower() for x in os.environ.get("ROLE_INBOX_BLOCKLIST", "info,estimating,estimate,estimates,bid,bids,sales").split(",") if x.strip()]
    for b in block:
        if b in toks:
            return True
        if b and b in lp:
            return True
    return False


def _cache_mev_fields(doc: Dict) -> bool:
    return bool(doc.get("mev_status_raw") or doc.get("mev_verdict") or doc.get("mev_status"))


def _cache_zb_fields(doc: Dict) -> bool:
    return bool(doc.get("zb_status_raw") or doc.get("zb_status") or doc.get("zb_verdict"))


def _cache_vf_fields(doc: Dict) -> bool:
    return bool(
        doc.get("vf_checked_at")
        or doc.get("vf_verdict")
        or doc.get("vf_status")
        or doc.get("vf_classification")
        or doc.get("vf_reason")
    )


def _cached_vf_result(doc: Dict) -> Tuple[bool, str]:
    verdict = str(doc.get("vf_verdict") or "").strip().upper()
    reason = str(doc.get("vf_reason") or "").strip()
    if verdict == "VALID":
        return True, reason or "cache_vf_valid"
    if verdict == "INVALID":
        return False, reason or "cache_vf_invalid"

    status = _norm(doc.get("vf_status", "")).upper()
    classification = _norm(doc.get("vf_classification", "")).upper()
    if status and classification:
        if status == "SUCCESS" and classification == "DELIVERABLE":
            return True, reason or "cache_vf_valid"
        return False, reason or f"rejected:{classification.lower()}:{status.lower()}"

    return False, reason or "cache_vf_unknown"


def _should_retry_mev_unknown() -> bool:
    return os.environ.get("MEV_RETRY_ON_UNKNOWN", "0").strip().lower() in {"1", "true", "yes"}


def _is_mev_catch_all_risky(email_norm: str, mev_json_map: Dict[str, Dict], mev_verdict_map: Dict[str, Tuple[str, str]]) -> bool:
    verdict, reason = mev_verdict_map.get(email_norm, ("", ""))
    if verdict != "RISKY":
        return False

    reason_norm = _norm(reason).replace("-", "_")
    if "catch_all" in reason_norm or "catchall" in reason_norm:
        return True

    mj = mev_json_map.get(email_norm) or {}
    status_norm = _norm(mj.get("Status", "")).replace("-", "_")
    diagnosis_norm = _norm(mj.get("Diagnosis", "")).replace("-", "_")

    if "catch_all" in status_norm or "catchall" in status_norm:
        return True
    if "catch_all" in diagnosis_norm or "catchall" in diagnosis_norm:
        return True

    return False


def _init_audit_entry(audit: Dict[str, Dict[str, str]], email_norm: str) -> Dict[str, str]:
    entry = audit.get(email_norm)
    if entry is None:
        entry = {
            "email": email_norm,
            "decision": "UNKNOWN",
            "final_reason": "",
            "final_verifier": "",
            "verifiers_used": "",
            "mev_verdict": "",
            "mev_reason": "",
            "verifalia_result": "",
            "verifalia_reason": "",
            "zerobounce_verdict": "",
            "zerobounce_reason": "",
        }
        audit[email_norm] = entry
    return entry


def _append_verifier(entry: Dict[str, str], verifier_name: str) -> None:
    if not verifier_name:
        return
    raw = entry.get("verifiers_used", "").strip()
    parts = [x for x in raw.split(",") if x] if raw else []
    if verifier_name not in parts:
        parts.append(verifier_name)
    entry["verifiers_used"] = ",".join(parts)


def _mark_removed(
    removed: Dict[str, str],
    audit: Dict[str, Dict[str, str]],
    email_norm: str,
    reason: str,
    final_verifier: str,
) -> None:
    removed[email_norm] = reason
    entry = _init_audit_entry(audit, email_norm)
    entry["decision"] = "REMOVED"
    entry["final_reason"] = reason
    entry["final_verifier"] = final_verifier


def _mark_kept(
    audit: Dict[str, Dict[str, str]],
    email_norm: str,
    reason: str,
    final_verifier: str,
) -> None:
    entry = _init_audit_entry(audit, email_norm)
    entry["decision"] = "KEPT"
    entry["final_reason"] = reason
    entry["final_verifier"] = final_verifier


def filter_rows_by_email_verification_waterfall(
    rows: List[List[str]],
    email_col_index: int = 0,
) -> Tuple[List[List[str]], Dict[str, str], Dict[str, Dict[str, str]]]:
    """
    Policy:
    - MyEmailVerifier is primary gate.
    - Verifalia runs when possible to compare results over time.
    - ZeroBounce runs only when needed, with a hard per-run budget.

    Keep rules:
    - If MEV verdict VALID and not blocked role, keep.
    - If MEV verdict RISKY, call ZeroBounce if budget. Keep only if ZB verdict VALID.
    - If MEV verdict VALID but Verifalia rejects, call ZeroBounce if budget. Keep only if ZB verdict VALID.
    - Block role inboxes containing tokens in ROLE_INBOX_BLOCKLIST regardless of provider.

    Returns:
    - kept rows
    - removed map: email -> final reason
    - verification audit: email -> detailed verifier trail and final decision
    """

    cache_enabled = os.environ.get("FIRESTORE_EMAIL_CACHE_ENABLED", "1").strip().lower() in {"1", "true", "yes"}
    cache = EmailValidationCache() if cache_enabled else None

    skip_mev_for_this_run = os.environ.get("SKIP_MEV_THIS_RUN", "0").strip().lower() in {"1", "true", "yes"}
    mev = None
    if not skip_mev_for_this_run:
        mev = MyEmailVerifierClient()

    mev_daily_budget = DailyVerifierBudget(
        provider="myemailverifier",
        daily_limit=int(os.environ.get("MYEMAILVERIFIER_DAILY_BUDGET", "100")),
    )

    try:
        run_verifalia = os.environ.get("RUN_VERIFALIA_COMPARE", "1").strip().lower() in {"1", "true", "yes"}
        verifalia: VerifaliaClient = None  # type: ignore
        if run_verifalia:
            try:
                verifalia = VerifaliaClient()
            except Exception:
                verifalia = None  # type: ignore

        zb_budget = int(os.environ.get("ZEROBOUNCE_MAX_CALLS_PER_RUN", "10"))
        zb_used = 0
        zb: ZeroBounceClient = None  # type: ignore
        try:
            zb = ZeroBounceClient()
        except Exception:
            zb = None  # type: ignore

        emails: List[str] = []
        for r in rows:
            raw = r[email_col_index] if len(r) > email_col_index else ""
            e = normalize_email(raw)
            if e and "@" in e:
                emails.append(e)

        emails = list(dict.fromkeys(emails))

        cached_docs: Dict[str, Dict] = {}
        if cache is not None:
            cached_docs = cache.get_many(emails)

        verification_audit: Dict[str, Dict[str, str]] = {}
        mev_json_map: Dict[str, Dict] = {}
        mev_verdict_map: Dict[str, Tuple[str, str]] = {}
        removed: Dict[str, str] = {}
        budget_exhausted = False
        mev_budget_open = not skip_mev_for_this_run

        for e in emails:
            entry = _init_audit_entry(verification_audit, e)

            if _is_blocked_role(e):
                _append_verifier(entry, "local_policy")
                _mark_removed(removed, verification_audit, e, "blocked_role", "local_policy")
                continue

            doc = cached_docs.get(e) if cache is not None else None
            if doc and cache is not None and cache.is_fresh(doc) and _cache_mev_fields(doc):
                _append_verifier(entry, "myemailverifier_cache")

                is_role_cached = bool(doc.get("mev_is_role_based"))
                if is_role_cached and not _is_allowlisted_role_localpart(e):
                    entry["mev_verdict"] = str(doc.get("mev_verdict") or "").strip().upper() or "UNKNOWN"
                    entry["mev_reason"] = str(doc.get("mev_reason") or "").strip() or "cache_mev_role_not_allowlisted"
                    _mark_removed(removed, verification_audit, e, "role_not_allowlisted_cached", "myemailverifier_cache")
                    continue

                mev_status_raw = str(doc.get("mev_status_raw") or doc.get("mev_status") or "")
                mev_json_map[e] = {"Status": mev_status_raw}

                verdict = str(doc.get("mev_verdict") or "").strip().upper()
                reason = str(doc.get("mev_reason") or "").strip()

                if verdict in {"VALID", "INVALID", "RISKY"}:
                    mev_verdict_map[e] = (verdict, reason or "cache_mev")
                    entry["mev_verdict"] = verdict
                    entry["mev_reason"] = reason or "cache_mev"
                else:
                    mev_verdict_map[e] = ("RISKY", "cache_mev_unknown")
                    entry["mev_verdict"] = "RISKY"
                    entry["mev_reason"] = "cache_mev_unknown"
                continue

            if not mev_budget_open or mev is None:
                budget_exhausted = True
                continue

            try:
                mev_daily_budget.try_claim(1)
                _append_verifier(entry, "myemailverifier")
                mj = mev.validate_single(e)
            except DailyBudgetExhaustedError:
                budget_exhausted = True
                mev_budget_open = False
                continue
            except Exception as exc:
                _append_verifier(entry, "myemailverifier")
                _mark_removed(removed, verification_audit, e, f"mev_error_{type(exc).__name__}", "myemailverifier")
                continue

            if _should_retry_mev_unknown():
                status0 = _norm(mj.get("Status", ""))
                if status0 == "unknown":
                    sleep_s = float(os.environ.get("MEV_UNKNOWN_RETRY_SLEEP_S", "1.0"))
                    time.sleep(sleep_s + random.random())
                    try:
                        mev_daily_budget.try_claim(1)
                        mj2 = mev.validate_single(e)
                        if _norm(mj2.get("Status", "")) != "unknown":
                            mj = mj2
                    except DailyBudgetExhaustedError:
                        budget_exhausted = True
                        mev_budget_open = False
                    except Exception:
                        pass

            mev_json_map[e] = mj
            v, reason = mev_verdict(e, mj)
            mev_verdict_map[e] = (v, reason)

            entry["mev_verdict"] = v
            entry["mev_reason"] = reason

            is_role_based = _norm(mj.get("Role_Based", "false")) == "true"
            if is_role_based and not _is_allowlisted_role_localpart(e):
                _mark_removed(removed, verification_audit, e, "role_not_allowlisted", "myemailverifier")
                continue

            if cache is not None:
                cache.upsert_myemailverifier_result(
                    email_norm=e,
                    verdict=v,
                    status_raw=str(mj.get("Status", "") or ""),
                    catch_all=_norm(mj.get("catch_all", "false")) == "true",
                    is_disposable=_norm(mj.get("Disposable_Domain", "false")) == "true",
                    is_free=_norm(mj.get("Free_Domain", "false")) == "true",
                    is_role=_norm(mj.get("Role_Based", "false")) == "true",
                    is_greylisted=_norm(mj.get("Greylisted", "false")) == "true",
                    diagnosis=str(mj.get("Diagnosis", "") or ""),
                    reason=reason,
                )

        vf_map: Dict[str, Tuple[bool, str]] = {}
        if budget_exhausted:
            for e in emails:
                if e in removed or e in mev_verdict_map:
                    continue
                doc = cached_docs.get(e) if cache is not None else None
                if not doc or cache is None or not cache.is_fresh(doc) or not _cache_vf_fields(doc):
                    continue

                vf_keep, vf_reason = _cached_vf_result(doc)
                vf_map[e] = (vf_keep, vf_reason)

                entry = _init_audit_entry(verification_audit, e)
                _append_verifier(entry, "verifalia_cache")
                entry["verifalia_result"] = "VALID" if vf_keep else "INVALID"
                entry["verifalia_reason"] = vf_reason

        if budget_exhausted and verifalia is not None:
            compare_max = int(os.environ.get("VERIFALIA_COMPARE_MAX_PER_RUN", "25"))
            compare_targets: List[str] = []

            for e in emails:
                if e in removed or e in mev_verdict_map or e in vf_map:
                    continue
                compare_targets.append(e)
                if len(compare_targets) >= compare_max:
                    break

            if compare_targets:
                for e in compare_targets:
                    entry = _init_audit_entry(verification_audit, e)
                    _append_verifier(entry, "verifalia")

                try:
                    fresh_vf_map = verifalia.verify_emails(compare_targets)
                    for e in compare_targets:
                        entry = _init_audit_entry(verification_audit, e)
                        if e in fresh_vf_map:
                            vf_keep, vf_reason = fresh_vf_map[e]
                            vf_map[e] = (vf_keep, vf_reason)
                            entry["verifalia_result"] = "VALID" if vf_keep else "INVALID"
                            entry["verifalia_reason"] = vf_reason
                        else:
                            entry["verifalia_result"] = "UNKNOWN"
                            entry["verifalia_reason"] = "missing_batch_result"
                except Exception as exc:
                    for e in compare_targets:
                        entry = _init_audit_entry(verification_audit, e)
                        entry["verifalia_result"] = "ERROR"
                        entry["verifalia_reason"] = f"verifalia_error_{type(exc).__name__}"
                    raise

        unresolved_after_budget_gate: List[str] = []
        if budget_exhausted:
            for e in emails:
                if e in removed or e in mev_verdict_map:
                    continue

                if e in vf_map:
                    _, vf_reason = vf_map[e]
                    if vf_reason != "credits_depleted_unverified":
                        continue

                unresolved_after_budget_gate.append(e)

        if unresolved_after_budget_gate:
            raise AllVerifiersExhaustedError(
                "All verifier capacity exhausted before all emails could be verified. "
                f"unverified_count={len(unresolved_after_budget_gate)} "
                f"sample_unverified={','.join(unresolved_after_budget_gate[:25])}"
            )

        for e in emails:
            entry = _init_audit_entry(verification_audit, e)

            if e in removed:
                continue

            allowlisted_role = _is_allowlisted_role_localpart(e)

            if e not in mev_verdict_map:
                if e in vf_map:
                    vf_keep, vf_reason = vf_map[e]
                    final_verifier = "verifalia_cache" if "verifalia_cache" in entry.get("verifiers_used", "") else "verifalia"
                    if vf_keep:
                        _mark_kept(verification_audit, e, "verifalia_valid_after_mev_budget_exhausted", final_verifier)
                    else:
                        _mark_removed(removed, verification_audit, e, vf_reason, final_verifier)
                    continue

                if budget_exhausted:
                    _append_verifier(entry, "myemailverifier_budget_gate")
                    _mark_removed(removed, verification_audit, e, "mev_daily_budget_exhausted", "myemailverifier_budget_gate")
                    continue

                _mark_removed(removed, verification_audit, e, "missing_mev_result", "myemailverifier")
                continue

            v, v_reason = mev_verdict_map.get(e, ("RISKY", "missing"))

            if allowlisted_role:
                if v == "VALID":
                    _mark_kept(verification_audit, e, "allowlisted_role_mev_valid", "myemailverifier")
                    continue
                _mark_removed(removed, verification_audit, e, v_reason, "myemailverifier")
                continue

            if _is_mev_catch_all_risky(e, mev_json_map, mev_verdict_map):
                _mark_removed(removed, verification_audit, e, v_reason, "myemailverifier")
                continue

            needs_zb = v == "RISKY"

            if needs_zb:
                if zb is None:
                    _mark_removed(removed, verification_audit, e, "zerobounce_unavailable", "zerobounce")
                    continue

                if zb_used >= zb_budget:
                    _mark_removed(removed, verification_audit, e, "zerobounce_budget_exhausted", "zerobounce")
                    continue

                doc = cached_docs.get(e) if cache is not None else None
                if doc and cache is not None and cache.is_fresh(doc) and _cache_zb_fields(doc):
                    cached = _cached_zb_verdict(doc)
                    if cached:
                        _append_verifier(entry, "zerobounce_cache")
                        entry["zerobounce_verdict"] = cached
                        entry["zerobounce_reason"] = f"zb_cached_{cached.lower()}"

                        if cached == "VALID":
                            _mark_kept(verification_audit, e, "zerobounce_cached_valid_after_mev_risky", "zerobounce_cache")
                            continue

                        _mark_removed(removed, verification_audit, e, f"zb_cached_{cached.lower()}", "zerobounce_cache")
                        continue

                _append_verifier(entry, "zerobounce")
                zj = zb.validate(e)
                zb_used += 1
                zv, zreason = zb_verdict(zj)

                entry["zerobounce_verdict"] = zv
                entry["zerobounce_reason"] = zreason

                if cache is not None:
                    cache.upsert_zerobounce_result(
                        email_norm=e,
                        verdict=zv,
                        status_raw=str(zj.get("status", "") or ""),
                        sub_status_raw=str(zj.get("sub_status", "") or ""),
                        reason=zreason,
                    )

                if zv == "VALID":
                    _mark_kept(verification_audit, e, "zerobounce_valid_after_mev_risky", "zerobounce")
                    continue

                _mark_removed(removed, verification_audit, e, zreason, "zerobounce")
                continue

            if v == "VALID":
                _mark_kept(verification_audit, e, "mev_valid", "myemailverifier")
                continue

            _mark_removed(removed, verification_audit, e, v_reason, "myemailverifier")

        kept: List[List[str]] = []
        seen: Set[str] = set()

        for r in rows:
            raw = r[email_col_index] if len(r) > email_col_index else ""
            e = normalize_email(raw)
            if not e or "@" not in e:
                continue
            if e in seen:
                continue
            seen.add(e)

            if e in removed:
                continue

            kept.append(r)

        return kept, removed, verification_audit

    finally:
        try:
            mev.close()
        except Exception:
            pass