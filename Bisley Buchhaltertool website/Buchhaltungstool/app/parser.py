import os
import csv
import hashlib
from typing import Dict, Any, List
from io import StringIO


def parse_csv_sparkasse(csv_bytes: bytes) -> Dict[str, Any]:
    """
    Parst eine Bank-CSV-Datei mit folgenden Spalten:
    - A: Auftragskonto
    - B: Buchungstag
    - C: Valutadatum
    - D: Buchungstext
    - E: Verwendungszweck
    - ...: weitere Spalten...
    - L: Begünstigter/Zahlungspflichtiger
    - O: Betrag
    - Q: Umsatzstatus (nur "gebucht" wird übernommen, "vorgemerkt" wird ignoriert)
    """
    try:
        # Dekodierung mit UTF-8 oder Fallback
        try:
            csv_text = csv_bytes.decode('utf-8')
        except UnicodeDecodeError:
            csv_text = csv_bytes.decode('iso-8859-1')
        
        # CSV mit Semikolon-Separator parsen
        reader = csv.reader(StringIO(csv_text), delimiter=';')
        rows = list(reader)
        
        if len(rows) < 2:
            return {"error": "CSV ist leer oder hat keine Daten"}
        
        # Header extrahieren (erste Zeile)
        headers = [h.strip().strip('"') for h in rows[0]]
        
        # Transaktionen parsen
        transactions = []
        for row in rows[1:]:
            if not row or all(not cell.strip() for cell in row):
                continue
            
            # Sichere Spaltenzugriffe
            def get_col(idx, default=""):
                return row[idx].strip().strip('"') if idx < len(row) else default

            status_q = get_col(16).lower()  # Spalte Q
            if status_q:
                if "vorgemerkt" in status_q:
                    continue
                if "gebucht" not in status_q:
                    continue
            
            # Spalten nach Index (0-basiert):
            # A=0, B=1, C=2, D=3, E=4, ..., L=11, O=14, Q=16
            transaction = {
                "auftragskonto": get_col(0),      # Spalte A
                "buchungstag": get_col(1),        # Spalte B
                "valutadatum": get_col(2),        # Spalte C
                "buchungstext": get_col(3),       # Spalte D
                "verwendungszweck": get_col(4),   # Spalte E
                "beguenstigter": get_col(11),     # Spalte L
                "betrag": get_col(14),            # Spalte O
                "hinweis": ""                     # Wird vom Nutzer bearbeitet
            }
            transactions.append(transaction)
        
        # Fingerprint für diese CSV erstellen (basiert auf Inhalt)
        csv_hash = hashlib.sha256(csv_bytes).hexdigest()[:16]
        
        return {
            "csv_fingerprint": csv_hash,
            "transactions": transactions,
            "row_count": len(transactions),
            "error": None
        }
    
    except Exception as e:
        return {
            "error": f"CSV-Parsing-Fehler: {str(e)}",
            "transactions": []
        }


def _normalize_line(line: str) -> str:
    import re
    return re.sub(r"\s+", " ", line.strip(), flags=re.UNICODE)


def _extract_matches(patterns, text):
    import re
    found = []
    for name, pat in patterns.items():
        for m in re.finditer(pat, text, flags=re.IGNORECASE):
            value = m.group(1).strip() if m.groups() else m.group(0).strip()
            found.append({"source": name, "value": value, "match": m.group(0).strip()})
    return found


def _extract_company(text: str) -> str:
    import re
    for label in ["Verwendungszweck", "Buchungstext", "Empfänger", "Zahlungsempfänger"]:
        m = re.search(r"" + label + r"[:\s]+(.+)", text, flags=re.IGNORECASE)
        if m:
            line = m.group(1).strip().split("\n")[0]
            if len(line) > 3:
                return _normalize_line(line)

    candidates = []
    for line in text.splitlines():
        n = _normalize_line(line)
        if not n or len(n) < 6:
            continue
        if re.search(r"\b(IBAN|BIC|Konto|BLZ|Saldo|Seite|Sparkasse)\b", n, re.IGNORECASE):
            continue
        if re.search(r"[0-9]{2,}", n) and len(re.sub(r"[^A-Za-z]", "", n)) < 4:
            continue
        candidates.append(n)
        if len(candidates) >= 3:
            break

    return candidates[0] if candidates else ""


def _normalize_line(line: str) -> str:
    return re.sub(r"\s+", " ", line.strip(), flags=re.UNICODE)


def _extract_matches(patterns, text):
    found = []
    for name, pat in patterns.items():
        for m in re.finditer(pat, text, flags=re.IGNORECASE):
            value = m.group(1).strip() if m.groups() else m.group(0).strip()
            found.append({"source": name, "value": value, "match": m.group(0).strip()})
    return found


def _extract_company(text: str) -> str:
    for label in ["Verwendungszweck", "Buchungstext", "Empfänger", "Zahlungsempfänger"]:
        m = re.search(r"" + label + r"[:\s]+(.+)", text, flags=re.IGNORECASE)
        if m:
            line = m.group(1).strip().split("\n")[0]
            if len(line) > 3:
                return _normalize_line(line)

    candidates = []
    for line in text.splitlines():
        n = _normalize_line(line)
        if not n or len(n) < 6:
            continue
        if re.search(r"\b(IBAN|BIC|Konto|BLZ|Saldo|Seite|Sparkasse)\b", n, re.IGNORECASE):
            continue
        if re.search(r"[0-9]{2,}", n) and len(re.sub(r"[^A-Za-z]", "", n)) < 4:
            continue
        candidates.append(n)
        if len(candidates) >= 3:
            break

    return candidates[0] if candidates else ""


def _extract_transactions(pdf_bytes: bytes) -> List[Dict[str, str]]:
    rows = []
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")

    for page in doc:
        pix = page.get_pixmap(dpi=300)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        try:
            data = _ocr_image_to_data(img)
        except pytesseract.pytesseract.TesseractNotFoundError:
            return []

        line_map = {}
        for i, txt in enumerate(data['text']):
            if not txt.strip():
                continue
            key = (data['block_num'][i], data['par_num'][i], data['line_num'][i])
            if key not in line_map:
                line_map[key] = {'top': data['top'][i], 'height': data['height'][i], 'words': []}
            line_map[key]['words'].append(txt)
            line_map[key]['height'] = max(line_map[key]['height'], data['height'][i])

        lines = []
        for line in line_map.values():
            text_line = ' '.join(line['words']).strip()
            lines.append({'top': line['top'], 'height': line['height'], 'text': text_line})

        if not lines:
            continue

        lines.sort(key=lambda e: e['top'])

        current = None
        for entry in lines:
            datum = re.search(r"\b(\d{2}\.\d{2}\.\d{4})\b", entry['text'])
            betrag = re.search(r"([+-]?\d{1,3}(?:[\.]\d{3})*,\d{2})\s*(EUR|€)?", entry['text'])
            hinweis = '#Error#'
            if re.search(r"s\.?\s*avis", entry['text'], flags=re.IGNORECASE):
                hinweis = 's.avis'
            elif re.search(r"s\.?\s*anlage", entry['text'], flags=re.IGNORECASE):
                hinweis = 's.anlage'
            elif re.search(r"[0-9]{4,5}\/[0-9]{1,3}", entry['text']):
                hinweis = re.search(r"([0-9]{4,5}\/[0-9]{1,3})", entry['text']).group(1)

            if datum and betrag:
                if current:
                    rows.append(current)
                current = {
                    'zahler': '',
                    'verwendungszweck': entry['text'],
                    'datum': datum.group(1),
                    'betrag': betrag.group(1),
                    'hinweis': hinweis,
                    'kundennummer': '?'
                }
            else:
                if current:
                    if current['verwendungszweck']:
                        current['verwendungszweck'] += ' '
                    current['verwendungszweck'] += entry['text']
                    if not current['datum'] and datum:
                        current['datum'] = datum.group(1)
                    if not current['betrag'] and betrag:
                        current['betrag'] = betrag.group(1)
                    if current['hinweis'] == '#Error#' and hinweis != '#Error#':
                        current['hinweis'] = hinweis

        if current:
            rows.append(current)

    # Fallback: falls keine Zeilen gefunden werden
    if not rows:
        text = _safe_text_from_pdf_bytes(pdf_bytes)
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            datum = re.search(r"\b(\d{2}\.\d{2}\.\d{4})\b", line)
            betrag = re.search(r"([+-]?\d{1,3}(?:[\.]\d{3})*,\d{2})\s*(EUR|€)?", line)
            if datum and betrag:
                hinweis = 's.avis' if re.search(r"s\.?\s*avis", line, flags=re.IGNORECASE) else ('s.anlage' if re.search(r"s\.?\s*anlage", line, flags=re.IGNORECASE) else '#Error#')
                rows.append({
                    'zahler': '',
                    'verwendungszweck': line,
                    'datum': datum.group(1),
                    'betrag': betrag.group(1),
                    'hinweis': hinweis,
                    'kundennummer': '?'
                })

    return rows


def parse_sparkasse(pdf_bytes: bytes) -> Dict[str, Any]:
    text = _safe_text_from_pdf_bytes(pdf_bytes)
    shrink = text[:2000].replace("\n", " ")

    patterns = {
        "kundennummer": r"(?:Kundennummer|Kundennr|Kdnr|KDNR)[:\s]*([0-9A-Za-z\-\/]+)",
        "kundennummer_simple": r"\b(KD[0-9]{3,8}|[0-9]{5,12})\b",
        "savis": r"\b(s\.?\s*avis)\b",
        "sanlage": r"\b(s\.?\s*anlage)\b",
        "kontierung": r"\b([0-9]{4,5}\/[0-9]{1,3})\b",
    }

    hig = _extract_matches(patterns, text)

    kundennummer = [item for item in hig if item["source"] in ("kundennummer", "kundennummer_simple")]
    kontierung = [item for item in hig if item["source"] == "kontierung"]
    hinweis = [item for item in hig if item["source"] in ("savis", "sanlage")]

    if not kundennummer:
        kundennummer = [{"source": "kundennummer", "value": "?", "match": None}]
    if not kontierung:
        kontierung = [{"source": "kontierung", "value": "?", "match": None}]
    if not hinweis:
        hinweis = [{"source": "hinweis", "value": "#Error#", "match": None}]

    fingerprint = hashlib.sha256(pdf_bytes).hexdigest()
    company = _extract_company(text)

    transactions = _extract_transactions(pdf_bytes)

    return {
        "pdf_fingerprint": fingerprint,
        "company": company,
        "kundennummer": kundennummer,
        "kontierung": kontierung,
        "hinweis": hinweis,
        "transactions": transactions,
        "raw_text_snippet": shrink,
        "all_text": text,
    }
