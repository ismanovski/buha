import os
import uuid
from typing import Dict, Any, List, Optional
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

_client: Optional[Client] = None


def get_client() -> Client:
    global _client
    if _client is None:
        _client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _client


# ============ REGELN ============

def add_rule(rule: Dict[str, Any], scope: str = "spk") -> str:
    sb = get_client()
    rule_id = rule.get("id") or str(uuid.uuid4())
    row = {
        "id": rule_id,
        "scope": scope,
        "column_name": rule.get("column", ""),
        "condition": rule.get("condition", ""),
        "match_value": rule.get("value", ""),
        "second_column": rule.get("second_column") or None,
        "second_condition": rule.get("second_condition") or None,
        "second_value": rule.get("second_value") or None,
        "result_hint": rule.get("result_hint", ""),
        "result_description": rule.get("result_description", ""),
        "active": rule.get("active", True),
    }
    sb.table("rules").upsert(row).execute()
    return rule_id


def get_all_rules(scope: str = "spk") -> List[Dict[str, Any]]:
    sb = get_client()
    result = sb.table("rules").select("*").eq("scope", scope).eq("active", True).execute()
    rows = result.data or []
    for row in rows:
        row["column"] = row.pop("column_name", "")
        row["value"] = row.pop("match_value", "")
    return rows


def delete_rule(rule_id: str, scope: str = "spk") -> bool:
    sb = get_client()
    sb.table("rules").update({"active": False}).eq("id", rule_id).eq("scope", scope).execute()
    return True


def update_rule(rule_id: str, rule_data: Dict[str, Any], scope: str = "spk") -> bool:
    sb = get_client()
    row = {
        "column_name": rule_data.get("column", ""),
        "condition": rule_data.get("condition", ""),
        "match_value": rule_data.get("value", ""),
        "second_column": rule_data.get("second_column") or None,
        "second_condition": rule_data.get("second_condition") or None,
        "second_value": rule_data.get("second_value") or None,
        "result_hint": rule_data.get("result_hint", ""),
        "result_description": rule_data.get("result_description", ""),
        "active": rule_data.get("active", True),
    }
    sb.table("rules").update(row).eq("id", rule_id).eq("scope", scope).execute()
    return True


# ============ KORREKTIONEN ============

def save_correction(key: str, hinweis: str, beschreibung: str = "", scope: str = "spk") -> None:
    sb = get_client()
    row = {"scope": scope, "key": key, "hinweis": hinweis, "beschreibung": beschreibung}
    sb.table("corrections").upsert(row, on_conflict="scope,key").execute()


def get_correction(key: str, scope: str = "spk") -> Optional[Dict[str, str]]:
    sb = get_client()
    result = (
        sb.table("corrections")
        .select("hinweis,beschreibung")
        .eq("scope", scope)
        .eq("key", key)
        .limit(1)
        .execute()
    )
    if result.data:
        return result.data[0]
    return None


def save_all_corrections(corrections_dict: Dict[str, Dict[str, str]], scope: str = "spk") -> None:
    sb = get_client()
    rows = [
        {
            "scope": scope,
            "key": k,
            "hinweis": v.get("hinweis", ""),
            "beschreibung": v.get("beschreibung", ""),
        }
        for k, v in corrections_dict.items()
    ]
    if rows:
        sb.table("corrections").upsert(rows, on_conflict="scope,key").execute()


# ============ LEGACY SHIMS ============

def load_memory(scope: str = "spk") -> Dict[str, Any]:
    sb = get_client()
    rules = get_all_rules(scope)
    corr_result = (
        sb.table("corrections")
        .select("key,hinweis,beschreibung")
        .eq("scope", scope)
        .execute()
    )
    corrections = {
        r["key"]: {"hinweis": r["hinweis"], "beschreibung": r["beschreibung"]}
        for r in (corr_result.data or [])
    }
    return {"entries": [], "rules": rules, "corrections": corrections}


def add_entry(entry: Dict[str, Any], scope: str = "spk") -> None:
    pass  # Legacy – nicht mehr verwendet


def find_by_kundennummer(kundennummer: str) -> Optional[Dict[str, Any]]:
    return None


def find_by_company(company: str) -> Optional[Dict[str, Any]]:
    return None

