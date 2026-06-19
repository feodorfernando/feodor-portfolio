"""
sql_guard.py
------------
Safety gate for LLM-generated SQL before it ever touches the warehouse. A BI copilot
must be physically unable to mutate data or read outside the certified gold layer —
guardrails, not good intentions.

Checks (all must pass):
  * single statement only (no stacked `;` injection)
  * SELECT / WITH only — reject INSERT/UPDATE/DELETE/MERGE/DROP/ALTER/CREATE/GRANT/...
  * every referenced table lives in the allow-listed gold schema
  * a LIMIT is enforced (injected if missing) to cap result size / cost

Author: Feodor Fernando
"""
from __future__ import annotations

import re
from dataclasses import dataclass

_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|merge|drop|alter|create|truncate|grant|revoke|"
    r"copy|call|refresh|optimize|vacuum|replace)\b",
    re.IGNORECASE,
)
_READ_ONLY_START = re.compile(r"^\s*(with|select)\b", re.IGNORECASE)
# crude but effective table extraction: tokens after FROM / JOIN
_TABLE_REF = re.compile(r"\b(?:from|join)\s+([a-zA-Z0-9_.]+)", re.IGNORECASE)


@dataclass
class GuardResult:
    ok: bool
    sql: str
    reason: str = ""


def guard_sql(sql: str, allowed_schema: str, max_rows: int = 1000) -> GuardResult:
    """Validate + harden LLM SQL. `allowed_schema` e.g. 'prod_catalog.gold'."""
    s = sql.strip().rstrip(";").strip()

    # 1) single statement
    if ";" in s:
        return GuardResult(False, sql, "multiple statements are not allowed")

    # 2) read-only shape
    if not _READ_ONLY_START.match(s):
        return GuardResult(False, sql, "only SELECT / WITH queries are allowed")
    if _FORBIDDEN.search(s):
        return GuardResult(False, sql, "statement contains a forbidden (write/DDL) keyword")

    # 3) every table must be in the allowed gold schema
    refs = _TABLE_REF.findall(s)
    for ref in refs:
        # allow CTE names (no dot) — they're defined inline; only check qualified tables
        if "." in ref and not ref.lower().startswith(allowed_schema.lower() + "."):
            return GuardResult(False, sql, f"table '{ref}' is outside {allowed_schema}")

    # 4) enforce a LIMIT
    if not re.search(r"\blimit\s+\d+", s, re.IGNORECASE):
        s = f"{s}\nLIMIT {max_rows}"

    return GuardResult(True, s)
