##leadgen_common/daily_verifier_budget.py

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from google.cloud import firestore


class DailyBudgetExhaustedError(RuntimeError):
    pass


@dataclass(frozen=True)
class DailyBudgetState:
    provider: str
    day_key: str
    limit: int
    used: int

    @property
    def remaining(self) -> int:
        return max(0, self.limit - self.used)


class DailyVerifierBudget:
    def __init__(self, provider: str, daily_limit: int | None = None) -> None:
        self.provider = str(provider or "").strip().lower()
        if not self.provider:
            raise RuntimeError("provider is required")

        self.daily_limit = int(
            daily_limit if daily_limit is not None else os.environ.get("MYEMAILVERIFIER_DAILY_BUDGET", "100")
        )
        self.collection = os.environ.get(
            "FIRESTORE_VERIFIER_BUDGET_COLLECTION",
            "leadgen_verifier_daily_budget_v1",
        ).strip()
        self.timezone_name = os.environ.get("VERIFIER_BUDGET_TIMEZONE", "America/Halifax").strip() or "America/Halifax"
        self.db = firestore.Client()

    def _utc_now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _local_now(self) -> datetime:
        return self._utc_now().astimezone(ZoneInfo(self.timezone_name))

    def day_key(self) -> str:
        return self._local_now().strftime("%Y-%m-%d")

    def _doc_ref(self, day_key: str):
        return self.db.collection(self.collection).document(f"{self.provider}_{day_key}")

    def get_state(self) -> DailyBudgetState:
        day_key = self.day_key()
        snap = self._doc_ref(day_key).get()
        used = 0
        if snap.exists:
            try:
                used = int((snap.to_dict() or {}).get("used", 0) or 0)
            except Exception:
                used = 0
        return DailyBudgetState(
            provider=self.provider,
            day_key=day_key,
            limit=self.daily_limit,
            used=used,
        )

    def remaining(self) -> int:
        return self.get_state().remaining

    def ensure_remaining_or_raise(self, credits_needed: int = 1) -> None:
        state = self.get_state()
        if state.remaining < int(credits_needed):
            raise DailyBudgetExhaustedError(
                f"{self.provider} daily budget exhausted for {state.day_key}. "
                f"used={state.used} limit={state.limit} remaining={state.remaining}"
            )

    def try_claim(self, credits_needed: int = 1) -> DailyBudgetState:
        needed = int(credits_needed)
        if needed <= 0:
            return self.get_state()

        day_key = self.day_key()
        now = self._utc_now()
        ref = self._doc_ref(day_key)
        transaction = self.db.transaction()
        provider = self.provider
        limit = self.daily_limit
        timezone_name = self.timezone_name

        @firestore.transactional
        def _claim_in_txn(txn, doc_ref):
            snap = doc_ref.get(transaction=txn)
            data = snap.to_dict() or {}
            current_used = int(data.get("used", 0) or 0)
            if current_used + needed > limit:
                return {
                    "claimed": False,
                    "used": current_used,
                }

            new_used = current_used + needed
            txn.set(
                doc_ref,
                {
                    "provider": provider,
                    "day_key": day_key,
                    "timezone": timezone_name,
                    "limit": limit,
                    "used": new_used,
                    "updated_at": firestore.SERVER_TIMESTAMP,
                    "last_claimed_at_utc": now,
                },
                merge=True,
            )
            return {
                "claimed": True,
                "used": new_used,
            }

        result = _claim_in_txn(transaction, ref)
        state = DailyBudgetState(
            provider=provider,
            day_key=day_key,
            limit=limit,
            used=int(result.get("used", 0) or 0),
        )
        if not bool(result.get("claimed")):
            raise DailyBudgetExhaustedError(
                f"{provider} daily budget exhausted for {day_key}. "
                f"used={state.used} limit={limit} remaining={state.remaining}"
            )
        return state