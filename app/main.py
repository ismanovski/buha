from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import os
import json
import mammoth

from .parser import parse_csv_sparkasse
from .db import (
    add_entry, load_memory, find_by_kundennummer, find_by_company,
    add_rule, get_all_rules, delete_rule, update_rule,
    save_correction, get_correction, save_all_corrections,
    get_client
)
from .models import Correction, Rule, Transaction

app = FastAPI(title="Sparkasse-Kontierungs-Tool")
app.mount("/static", StaticFiles(directory="static"), name="static")

DATA_DIR = os.getenv("DATA_DIR", "data")


@app.post("/parse")
async def parse_csv(file: UploadFile = File(...)):
    """Lädt und parsed eine SPK-CSV-Datei"""
    return await _parse_csv_for_scope(file, scope="spk")


@app.post("/coba/parse")
async def parse_csv_coba(file: UploadFile = File(...)):
    """Lädt und parsed eine CoBa-CSV-Datei"""
    return await _parse_csv_for_scope(file, scope="coba")


@app.post("/save-corrections")
async def save_corrections(data: dict):
    """Speichert alle Bearbeitungen auf einmal (SPK)"""
    return _save_corrections_for_scope(data, scope="spk")


@app.post("/coba/save-corrections")
async def save_corrections_coba(data: dict):
    """Speichert alle Bearbeitungen auf einmal (CoBa)"""
    return _save_corrections_for_scope(data, scope="coba")


@app.post("/save-correction")
async def save_single_correction(correction: dict):
    """Speichert eine einzelne Korrektur (SPK)"""
    return _save_single_correction_for_scope(correction, scope="spk")


@app.post("/coba/save-correction")
async def save_single_correction_coba(correction: dict):
    """Speichert eine einzelne Korrektur (CoBa)"""
    return _save_single_correction_for_scope(correction, scope="coba")


# ============ REGELN ENDPOINTS ============

@app.post("/rules")
async def create_rule(rule: Rule):
    """Erstellt eine neue Regel (SPK)"""
    return _create_rule_for_scope(rule, scope="spk")


@app.post("/coba/rules")
async def create_rule_coba(rule: Rule):
    """Erstellt eine neue Regel (CoBa)"""
    return _create_rule_for_scope(rule, scope="coba")


@app.get("/rules")
async def list_rules():
    """Gibt alle aktiven Regeln zurück (SPK)"""
    return _list_rules_for_scope(scope="spk")


@app.get("/coba/rules")
async def list_rules_coba():
    """Gibt alle aktiven Regeln zurück (CoBa)"""
    return _list_rules_for_scope(scope="coba")


@app.put("/rules/{rule_id}")
async def update_single_rule(rule_id: str, rule: Rule):
    """Aktualisiert eine Regel (SPK)"""
    return _update_rule_for_scope(rule_id, rule, scope="spk")


@app.put("/coba/rules/{rule_id}")
async def update_single_rule_coba(rule_id: str, rule: Rule):
    """Aktualisiert eine Regel (CoBa)"""
    return _update_rule_for_scope(rule_id, rule, scope="coba")


@app.delete("/rules/{rule_id}")
async def remove_rule(rule_id: str):
    """Löscht eine Regel (SPK)"""
    return _remove_rule_for_scope(rule_id, scope="spk")


@app.delete("/coba/rules/{rule_id}")
async def remove_rule_coba(rule_id: str):
    """Löscht eine Regel (CoBa)"""
    return _remove_rule_for_scope(rule_id, scope="coba")


# ============ LEGACY ENDPOINTS ============

@app.post("/correct")
async def save_correction_legacy(correction: Correction):
    add_entry(correction.dict())
    return {"status": "gespeichert", "entry": correction.dict()}


@app.get("/memory")
def memory():
    return load_memory(scope="spk")


@app.get("/coba/memory")
def memory_coba():
    return load_memory(scope="coba")


@app.get("/handbuch")
def handbuch():
    html = open("static/handbuch.html", "r", encoding="utf-8").read()
    return HTMLResponse(html)


HANDBUCH_DOCX_PATH = "static/Handbuch Buchhaltung.docx"


@app.get("/handbuch/content")
def handbuch_content():
    sb = get_client()
    result = sb.table("handbuch").select("*").eq("id", 1).limit(1).execute()
    if result.data:
        row = result.data[0]
        return JSONResponse({"html": row.get("html", ""), "delta": row.get("delta"), "annotations": row.get("annotations", "")})
    # Fallback: docx einlesen und als HTML liefern
    if os.path.exists(HANDBUCH_DOCX_PATH):
        with open(HANDBUCH_DOCX_PATH, "rb") as f:
            doc_result = mammoth.convert_to_html(f)
        return JSONResponse({"html": doc_result.value, "delta": None, "annotations": ""})
    return JSONResponse({"html": "", "delta": None, "annotations": ""})


@app.post("/handbuch/save")
async def handbuch_save(payload: dict):
    sb = get_client()
    sb.table("handbuch").upsert({"id": 1, "html": payload.get("html", ""), "delta": payload.get("delta"), "annotations": payload.get("annotations", "")}).execute()
    return {"status": "gespeichert"}


@app.get("/re-buchung")
def re_buchung_index():
    html = open("static/re_buchung.html", "r", encoding="utf-8").read()
    return HTMLResponse(html)


@app.get("/re-buchung/neu")
def re_buchung_new():
    html = open("static/re_buchung_neu.html", "r", encoding="utf-8").read()
    return HTMLResponse(html)


@app.get("/re-buchung/verwalten")
def re_buchung_manage():
    html = open("static/re_buchung_verwalten.html", "r", encoding="utf-8").read()
    return HTMLResponse(html)


@app.get("/re/api/projects")
def re_api_list_projects():
    sb = get_client()
    result = sb.table("re_projects").select("id,name,pdf_data_url,stamps,created_at,updated_at").order("updated_at", desc=True).execute()
    projects = [
        {"id": r["id"], "name": r["name"], "pdfDataUrl": r["pdf_data_url"],
         "stamps": r["stamps"] or [], "createdAt": r["created_at"], "updatedAt": r["updated_at"]}
        for r in (result.data or [])
    ]
    return {"projects": projects}


@app.get("/re/api/projects/{project_id}")
def re_api_get_project(project_id: str):
    sb = get_client()
    result = sb.table("re_projects").select("*").eq("id", project_id).limit(1).execute()
    if result.data:
        r = result.data[0]
        return {"id": r["id"], "name": r["name"], "pdfDataUrl": r["pdf_data_url"],
                "stamps": r["stamps"] or [], "createdAt": r["created_at"], "updatedAt": r["updated_at"]}
    raise HTTPException(status_code=404, detail="Projekt nicht gefunden")


@app.post("/re/api/projects")
async def re_api_save_project(payload: dict):
    project_id = str(payload.get("id", "") or "").strip()
    if not project_id:
        raise HTTPException(status_code=400, detail="Projekt-ID fehlt")

    sb = get_client()
    now_iso = _now_iso()
    existing_result = sb.table("re_projects").select("created_at").eq("id", project_id).limit(1).execute()
    created_at = payload.get("createdAt") or (existing_result.data[0]["created_at"] if existing_result.data else now_iso)

    row = {
        "id": project_id,
        "name": payload.get("name", "RE Buchung"),
        "pdf_data_url": payload.get("pdfDataUrl", ""),
        "stamps": payload.get("stamps", []),
        "created_at": created_at,
        "updated_at": now_iso,
    }
    sb.table("re_projects").upsert(row).execute()
    return {"id": project_id, "name": row["name"], "pdfDataUrl": row["pdf_data_url"],
            "stamps": row["stamps"], "createdAt": row["created_at"], "updatedAt": row["updated_at"]}


@app.delete("/re/api/projects/{project_id}")
def re_api_delete_project(project_id: str):
    sb = get_client()
    sb.table("re_projects").delete().eq("id", project_id).execute()
    return {"status": "gelöscht"}


@app.get("/re/api/suggestions")
def re_api_get_suggestions():
    sb = get_client()
    result = sb.table("re_suggestions").select("suggestions").eq("id", 1).limit(1).execute()
    if result.data:
        return {"suggestions": result.data[0]["suggestions"] or []}
    return {"suggestions": []}


@app.post("/re/api/suggestions")
async def re_api_save_suggestions(payload: dict):
    suggestions = payload.get("suggestions", [])
    if not isinstance(suggestions, list):
        raise HTTPException(status_code=400, detail="Suggestions müssen eine Liste sein")
    sb = get_client()
    sb.table("re_suggestions").upsert({"id": 1, "suggestions": suggestions}).execute()
    return {"status": "gespeichert", "count": len(suggestions)}


@app.get("/")
def index():
    html = open("static/index.html", "r", encoding="utf-8").read()
    return HTMLResponse(html)


@app.get("/coba")
def coba_index():
    html = open("static/coba.html", "r", encoding="utf-8").read()
    return HTMLResponse(html)


# ============ HILFSFUNKTIONEN ============

def _save_corrections_for_scope(data: dict, scope: str):
    corrections = data.get("corrections", {})
    save_all_corrections(corrections, scope=scope)
    return {"status": "gespeichert", "count": len(corrections)}


def _save_single_correction_for_scope(correction: dict, scope: str):
    key = correction.get("beguenstigter") or correction.get("buchungstext") or correction.get("auftragskonto")
    if not key:
        raise HTTPException(status_code=400, detail="Beguenstigter, Buchungstext oder Auftragskonto benötigt")
    hinweis = correction.get("hinweis", "")
    beschreibung = correction.get("beschreibung", "")
    save_correction(key, hinweis, beschreibung, scope=scope)
    return {"status": "gespeichert", "entry": correction}


def _create_rule_for_scope(rule: Rule, scope: str):
    rule_id = add_rule(rule.dict(), scope=scope)
    return {"status": "erstellt", "id": rule_id}


def _list_rules_for_scope(scope: str):
    return {"rules": get_all_rules(scope=scope)}


def _update_rule_for_scope(rule_id: str, rule: Rule, scope: str):
    if update_rule(rule_id, rule.dict(), scope=scope):
        return {"status": "aktualisiert"}
    raise HTTPException(status_code=404, detail="Regel nicht gefunden")


def _remove_rule_for_scope(rule_id: str, scope: str):
    if delete_rule(rule_id, scope=scope):
        return {"status": "gelöscht"}
    raise HTTPException(status_code=404, detail="Regel nicht gefunden")


async def _parse_csv_for_scope(file: UploadFile, scope: str):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Nur CSV-Dateien erlaubt")

    csv_bytes = await file.read()
    parsed = parse_csv_sparkasse(csv_bytes)

    if parsed.get("error"):
        return JSONResponse({"error": parsed["error"]}, status_code=400)

    rules = get_all_rules(scope=scope)

    for tx in parsed.get("transactions", []):
        tx_key = tx.get("beguenstigter", "")
        for rule in rules:
            if _apply_rule(tx, rule):
                tx["hinweis"] = rule["result_hint"]
                tx["beschreibung"] = rule.get("result_description", "")
                break

        saved = get_correction(tx_key, scope=scope)
        if not saved and not tx_key:
            tx_key_fallback = tx.get("buchungstext", "")
            saved = get_correction(tx_key_fallback, scope=scope)
        if saved:
            tx["hinweis"] = saved.get("hinweis", tx.get("hinweis", ""))
            tx["beschreibung"] = saved.get("beschreibung", tx.get("beschreibung", ""))

    return JSONResponse({
        "csv_fingerprint": parsed["csv_fingerprint"],
        "transactions": parsed["transactions"],
        "row_count": parsed["row_count"],
        "rules": rules
    })


def _matches_rule_condition(transaction: dict, column: str | None, condition: str | None, value: str | None) -> bool:
    """Prüft eine einzelne Regelbedingung gegen eine Transaktion."""
    if not column or not condition or value is None:
        return False

    tx_value = str(transaction.get(column, "") or "").lower().strip()
    search_value = str(value or "").lower().strip()

    if condition == "equals":
        return tx_value == search_value
    if condition == "contains":
        return search_value in tx_value

    return False


def _apply_rule(transaction: dict, rule: dict) -> bool:
    """Prüft, ob eine Regel auf eine Transaktion passt, optional mit zweiter UND-Bedingung."""
    if not _matches_rule_condition(transaction, rule.get("column"), rule.get("condition"), rule.get("value", "")):
        return False

    second_column = rule.get("second_column")
    second_condition = rule.get("second_condition")
    second_value = rule.get("second_value")

    if second_column and second_condition and str(second_value or "").strip():
        return _matches_rule_condition(transaction, second_column, second_condition, second_value)

    return True


def _now_iso():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()
