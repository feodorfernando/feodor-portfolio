"""
data_quality_agent.py
---------------------
LLM-assisted data quality. Profiles a dataset, asks Claude to PROPOSE validation
rules from the profile + a few sample rows, then executes those rules
deterministically in pandas and reports violations.

The split is deliberate: the LLM is used for the *judgement* part (what rules
make sense for this column given its name, type, and value distribution) but the
*enforcement* is plain, auditable Python — so a hallucinated rule can't silently
pass bad data.

Claude returns rules via tool-use (structured JSON), so no fragile text parsing.

Env:
    ANTHROPIC_API_KEY   required

Usage:
    python src/data_quality_agent.py --csv data/customers.csv

Author: Feodor Fernando
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

import pandas as pd
from anthropic import Anthropic

RULE_MODEL = "claude-sonnet-4-6"

# Tool schema Claude must fill: a list of typed, machine-executable rules.
RULE_TOOL = {
    "name": "emit_data_quality_rules",
    "description": "Return the data quality rules you recommend for this dataset.",
    "input_schema": {
        "type": "object",
        "properties": {
            "rules": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "column": {"type": "string"},
                        "check": {
                            "type": "string",
                            "enum": [
                                "not_null",
                                "unique",
                                "min",
                                "max",
                                "in_set",
                                "regex",
                                "max_null_fraction",
                            ],
                        },
                        "value": {
                            "description": "Threshold/pattern/allowed-set for the check (omit for not_null/unique).",
                        },
                        "rationale": {"type": "string"},
                    },
                    "required": ["column", "check", "rationale"],
                },
            }
        },
        "required": ["rules"],
    },
}

SYSTEM_PROMPT = (
    "You are a senior data engineer defining data quality expectations. Given a "
    "column profile and sample rows, propose a focused set of rules that catch "
    "realistic data problems (nulls where there should be none, out-of-range "
    "numerics, invalid categoricals, malformed identifiers/emails). Prefer a few "
    "high-value rules over many noisy ones. Use the emit_data_quality_rules tool."
)


def profile_dataframe(df: pd.DataFrame, sample_n: int = 5) -> dict:
    """Compute a compact, LLM-friendly profile of each column."""
    profile = {"row_count": int(len(df)), "columns": []}
    for col in df.columns:
        s = df[col]
        col_info = {
            "name": col,
            "dtype": str(s.dtype),
            "null_fraction": round(float(s.isna().mean()), 4),
            "n_unique": int(s.nunique(dropna=True)),
        }
        if pd.api.types.is_numeric_dtype(s):
            col_info["min"] = _num(s.min())
            col_info["max"] = _num(s.max())
            col_info["mean"] = _num(s.mean())
        else:
            top = s.dropna().astype(str).value_counts().head(5)
            col_info["top_values"] = {k: int(v) for k, v in top.items()}
        profile["columns"].append(col_info)

    profile["sample_rows"] = (
        df.head(sample_n).astype(str).to_dict(orient="records")
    )
    return profile


def _num(x):
    try:
        return round(float(x), 4)
    except (TypeError, ValueError):
        return None


def propose_rules(profile: dict) -> list[dict]:
    """Ask Claude for rules; return the structured list from the tool call."""
    client = Anthropic()
    resp = client.messages.create(
        model=RULE_MODEL,
        max_tokens=2048,
        system=SYSTEM_PROMPT,
        tools=[RULE_TOOL],
        tool_choice={"type": "tool", "name": "emit_data_quality_rules"},
        messages=[
            {
                "role": "user",
                "content": f"Dataset profile:\n```json\n{json.dumps(profile, indent=2)}\n```",
            }
        ],
    )
    for block in resp.content:
        if block.type == "tool_use" and block.name == "emit_data_quality_rules":
            return block.input["rules"]
    raise RuntimeError("model did not return rules via the tool")


# --- deterministic rule execution ----------------------------------------- #
def run_rule(df: pd.DataFrame, rule: dict) -> dict:
    col, check, val = rule["column"], rule["check"], rule.get("value")
    result = {"column": col, "check": check, "value": val, "rationale": rule.get("rationale")}

    if col not in df.columns:
        return {**result, "status": "ERROR", "detail": "column not found"}

    s = df[col]
    if check == "not_null":
        violations = int(s.isna().sum())
    elif check == "unique":
        violations = int(s.duplicated(keep=False).sum())
    elif check == "max_null_fraction":
        frac = float(s.isna().mean())
        violations = 0 if frac <= float(val) else 1
        result["observed"] = round(frac, 4)
    elif check == "min":
        violations = int((pd.to_numeric(s, errors="coerce") < float(val)).sum())
    elif check == "max":
        violations = int((pd.to_numeric(s, errors="coerce") > float(val)).sum())
    elif check == "in_set":
        allowed = set(val if isinstance(val, list) else [val])
        violations = int((~s.dropna().astype(str).isin({str(a) for a in allowed})).sum())
    elif check == "regex":
        pat = re.compile(str(val))
        violations = int(s.dropna().astype(str).apply(lambda x: pat.fullmatch(x) is None).sum())
    else:
        return {**result, "status": "ERROR", "detail": f"unknown check {check}"}

    result["violations"] = violations
    result["status"] = "PASS" if violations == 0 else "FAIL"
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    args = parser.parse_args()

    if not os.getenv("ANTHROPIC_API_KEY"):
        sys.exit("ANTHROPIC_API_KEY not set.")

    df = pd.read_csv(args.csv)
    profile = profile_dataframe(df)
    print(f"profiled {profile['row_count']} rows, {len(profile['columns'])} columns")

    rules = propose_rules(profile)
    print(f"Claude proposed {len(rules)} rules. Executing...\n")

    results = [run_rule(df, r) for r in rules]
    failed = 0
    for r in results:
        icon = {"PASS": "✅", "FAIL": "❌", "ERROR": "⚠️"}[r["status"]]
        extra = f" ({r['violations']} violations)" if r.get("violations") else ""
        print(f"{icon} {r['column']:20} {r['check']:18}{extra}")
        print(f"     ↳ {r['rationale']}")
        failed += r["status"] != "PASS"

    print(f"\n{len(results) - failed}/{len(results)} checks passed.")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
