from __future__ import annotations

import hashlib
import json
import os
import random
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().replace(microsecond=0).isoformat()


def make_run_id(prefix: str = "muni") -> str:
    return f"{prefix}_{int(time.time())}"


def municipality_key(name: str, state: str) -> str:
    clean_name = " ".join((name or "").strip().split())
    clean_state = " ".join((state or "").strip().split())
    return f"{clean_name}|{clean_state}"


def stable_bucket(key: str, modulo: int = 1000) -> int:
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % modulo


def split_name(full_name: str) -> Tuple[str, str]:
    parts = [p for p in (full_name or "").strip().split() if p]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def normalize_email(value: str) -> str:
    return (value or "").strip().lower()


def truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "t", "on"}


def parse_priority(population: Optional[int], explicit_priority: str = "") -> str:
    if explicit_priority:
        return explicit_priority
    try:
        pop = int(population or 0)
    except Exception:
        pop = 0
    if pop < 100:
        return "Low - Too Small"
    if pop < 500:
        return "Medium - Small"
    if pop < 2000:
        return "High - Small"
    if pop < 50000:
        return "Highest - Target"
    if pop < 75000:
        return "High - Big"
    if pop < 200000:
        return "Medium - Big"
    if pop < 10000000:
        return "Low - Too Big"
    return "CHECK"


def safe_json_dumps(payload: Any) -> str:
    return json.dumps(payload, indent=2, ensure_ascii=False)


def choice_shuffle(items: Iterable[Any]) -> List[Any]:
    out = list(items)
    random.shuffle(out)
    return out


def cooldown_ready(next_research_eligible_at: Optional[Any]) -> bool:
    if next_research_eligible_at in (None, "", 0):
        return True
    if hasattr(next_research_eligible_at, "timestamp"):
        dt = next_research_eligible_at
    else:
        try:
            dt = datetime.fromisoformat(str(next_research_eligible_at).replace("Z", "+00:00"))
        except Exception:
            return True
    return dt <= utc_now()


def future_iso(days: int) -> str:
    return (utc_now() + timedelta(days=days)).replace(microsecond=0).isoformat()
