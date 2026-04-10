from typing import List, Optional
from pydantic import BaseModel


class Transaction(BaseModel):
    """Eine Transaktion aus der CSV"""
    auftragskonto: str
    buchungstag: str
    valutadatum: str
    buchungstext: str
    verwendungszweck: str
    beguenstigter: str
    betrag: str
    hinweis: str = ""


class Rule(BaseModel):
    """Eine feste Regel für automatische Hinweise mit optionaler zweiter UND-Bedingung"""
    id: Optional[str] = None
    column: str  # z.B. "buchungstext", "verwendungszweck"
    condition: str  # "equals" oder "contains"
    value: str  # Wert zum Abgleichen
    second_column: Optional[str] = None
    second_condition: Optional[str] = None
    second_value: Optional[str] = None
    result_hint: str  # Hinweis, der eingesetzt werden soll
    result_description: str = ""  # Beschreibung, die eingesetzt werden soll
    active: bool = True


class Correction(BaseModel):
    """Eine Bearbeitung eines Hinweises durch den Nutzer"""
    csv_fingerprint: str
    auftragskonto: str  # Spalte A zur Verknüpfung
    beguenstigter: str
    hinweis: str
    original_hinweis: str = ""


class FieldItem(BaseModel):
    """Legacy - für Kompatibilität"""
    source: str
    value: str
    match: Optional[str]


class ParseResult(BaseModel):
    """Legacy - für Kompatibilität"""
    pdf_fingerprint: str
    kundennummer: List[FieldItem]
    kontierung: List[FieldItem]
    hinweis: List[FieldItem]
    raw_text_snippet: str
