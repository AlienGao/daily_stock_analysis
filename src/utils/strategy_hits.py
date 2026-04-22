# -*- coding: utf-8 -*-
"""Count matched_trading skills on AnalysisResult (and similar)."""
from __future__ import annotations

from typing import Any, List


def count_matched_skills(result: Any) -> int:
    ms = getattr(result, "matched_skills", None)
    if not ms or not isinstance(ms, (list, tuple)):
        return 0
    return len(ms)


def matched_skill_ids_preview(result: Any, limit: int = 8) -> List[str]:
    out: List[str] = []
    ms = getattr(result, "matched_skills", None)
    if not ms or not isinstance(ms, (list, tuple)):
        return out
    for item in ms[: max(0, limit)]:
        if isinstance(item, dict):
            sid = (item.get("id") or item.get("name") or "").strip()
            if sid:
                out.append(sid)
    return out
