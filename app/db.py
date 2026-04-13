import json
from pathlib import Path
from typing import Dict, Any, List
import uuid
import os

DB_DIR = Path(os.getenv("DATA_DIR", "data"))
DB_DIR.mkdir(parents=True, exist_ok=True)

# Für SPK und CoBa werden getrennte JSON-Dateien verwendet.
# SPK:  data/memory.json
# CoBa: data/memory_coba.json

def _get_db_file(scope: str = "spk") -> Path:
    suffix = "" if scope == "spk" else f"_{scope}"
    return DB_DIR / f"memory{suffix}.json"


def _ensure_db(scope: str = "spk"):
    db_file = _get_db_file(scope)
    if not db_file.exists():
        initial_data = {
            "entries": [],
            "rules": [],
            "corrections": {}
        }
        db_file.write_text(json.dumps(initial_data, ensure_ascii=False, indent=2), encoding="utf-8")


_ensure_db()


def load_memory(scope: str = "spk") -> Dict[str, Any]:
    """Lädt die komplette Datenbank für einen Bereich (z. B. spk oder coba)."""
    _ensure_db(scope)
    db_file = _get_db_file(scope)
    return json.loads(db_file.read_text(encoding="utf-8"))


def save_memory(data: Dict[str, Any], scope: str = "spk") -> None:
    """Speichert die komplette Datenbank für einen Bereich."""
    db_file = _get_db_file(scope)
    db_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def add_entry(entry: Dict[str, Any], scope: str = "spk") -> None:
    """Fügt einen Eintrag zur Historie hinzu (Legacy)."""
    data = load_memory(scope)
    data["entries"].append(entry)
    save_memory(data, scope)


# ============ REGELN (Neue Funktionen) ============

def add_rule(rule: Dict[str, Any], scope: str = "spk") -> str:
    """Speichert eine neue Regel und gibt die ID zurück."""
    data = load_memory(scope)
    if "rules" not in data:
        data["rules"] = []
    rule_id = rule.get("id") or str(uuid.uuid4())
    rule["id"] = rule_id
    data["rules"].append(rule)
    save_memory(data, scope)
    return rule_id


def get_all_rules(scope: str = "spk") -> List[Dict[str, Any]]:
    """Gibt alle aktiven Regeln für einen Bereich zurück."""
    data = load_memory(scope)
    return [r for r in data.get("rules", []) if r.get("active", True)]


def delete_rule(rule_id: str, scope: str = "spk") -> bool:
    """Deaktiviert eine Regel."""
    data = load_memory(scope)
    for rule in data.get("rules", []):
        if rule.get("id") == rule_id:
            rule["active"] = False
            save_memory(data, scope)
            return True
    return False


def update_rule(rule_id: str, rule_data: Dict[str, Any], scope: str = "spk") -> bool:
    """Aktualisiert eine Regel."""
    data = load_memory(scope)
    for rule in data.get("rules", []):
        if rule.get("id") == rule_id:
            rule.update(rule_data)
            save_memory(data, scope)
            return True
    return False


# ============ KORREKTIONEN (Benutzer-Eingaben) ============

def save_correction(key: str, hinweis: str, beschreibung: str = "", scope: str = "spk") -> None:
    """Speichert einen Hinweis (+ optional Beschreibung) für einen Schlüssel."""
    data = load_memory(scope)
    if "corrections" not in data:
        data["corrections"] = {}
    data["corrections"][key] = {"hinweis": hinweis, "beschreibung": beschreibung}
    save_memory(data, scope)


def get_correction(key: str, scope: str = "spk") -> Dict[str, str] | None:
    """Holt einen gespeicherten Hinweis und Beschreibung für einen Schlüssel."""
    data = load_memory(scope)
    return data.get("corrections", {}).get(key)


def save_all_corrections(corrections_dict: Dict[str, Dict[str, str]], scope: str = "spk") -> None:
    """Speichert mehrere Korrekturen auf einmal (key -> {hinweis, beschreibung})."""
    data = load_memory(scope)
    if "corrections" not in data:
        data["corrections"] = {}
    for key, item in corrections_dict.items():
        data["corrections"][key] = {
            "hinweis": item.get("hinweis", ""),
            "beschreibung": item.get("beschreibung", "")
        }
    save_memory(data, scope)


# ============ LEGACY Funktionen für Kompatibilität ============

def find_by_kundennummer(kundennummer: str) -> Dict[str, Any] | None:
    data = load_memory()
    for e in data.get("entries", []):
        if e.get("kundennummer") == kundennummer:
            return e
    return None


def find_by_company(company: str) -> Dict[str, Any] | None:
    if not company:
        return None
    company_low = company.lower().strip()
    data = load_memory()
    best = None
    for e in data.get("entries", []):
        c = (e.get("company") or "").lower()
        if company_low and c and company_low in c:
            return e
    # fallback substring search
    for e in data.get("entries", []):
        c = (e.get("company") or "").lower()
        if c and c in company_low:
            return e
    return None

