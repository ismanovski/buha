# BISLEY BUHA Tool Website

Eine Webanwendung zur automatischen Kontierung von Sparkassen-Kontoauszügen mit Lernfunktion.

## Features

- **Bank Kontierung SPK**: Automatische Analyse von PDF-Kontoauszügen, Erkennung von Transaktionen, Kundennummern und Kontierungen.
- **Mahnlauf**: Bald verfügbar.
- **Rechnungskontrolle**: Bald verfügbar.
- **Digitaler Stempel**: Bald verfügbar.
- **BUHA Handbuch**: Bald verfügbar.

## Installation

1. Stelle sicher, dass Python 3.8+ installiert ist.
2. Installiere die Abhängigkeiten:
   ```
   pip install -r requirements.txt
   ```
3. Für OCR (optionale Verbesserung bei gescannten PDFs):
   - Installiere Tesseract: https://github.com/tesseract-ocr/tesseract
   - Füge es zur PATH-Variable hinzu.

## Start

1. Navigiere zum Projektordner.
2. Starte den Server:
   ```
   python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8001
   ```
3. Öffne im Browser: `http://127.0.0.1:8001/`

## Nutzung

1. Klicke auf "Bank Kontierung SPK".
2. Lade eine PDF-Datei hoch.
3. Analysiere den Auszug.
4. Bearbeite die Kundennummern in der Tabelle.
5. Speichere einzelne oder alle Einträge.
6. Exportiere als CSV oder drucke die Tabelle.

## API

- `POST /parse`: PDF analysieren
- `POST /correct`: Korrektur speichern
- `GET /memory`: Historie abrufen

## Daten

- Historische Zuordnungen werden in `data/memory.json` gespeichert.
- Exporte werden als CSV heruntergeladen.

## Deployment

Für Render.com oder ähnliche:
- `uvicorn app.main:app --host 0.0.0.0 --port $PORT`

## Support

Bei Problemen: Stelle sicher, dass Tesseract installiert ist, falls OCR benötigt wird.