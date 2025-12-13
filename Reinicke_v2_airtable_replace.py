#!/usr/bin/env python3
"""
Scraper für https://www.immobilien-duis.de/immobilien/
Extrahiert Immobilienangebote und synct mit Airtable

Basierend auf streil-immo Scraper v1.7
"""

import os
import re
import sys
import csv
import json
import time
import hashlib
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Optional

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("[ERROR] Fehlende Module. Bitte installieren:")
    print("  pip install requests beautifulsoup4 lxml")
    sys.exit(1)

# ===========================================================================
# KONFIGURATION
# ===========================================================================

BASE = "https://www.immobilien-duis.de"
LIST_URL = f"{BASE}/immobilien/"

# Airtable
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN", "")
AIRTABLE_BASE = os.getenv("AIRTABLE_BASE", "")
AIRTABLE_TABLE_ID = os.getenv("AIRTABLE_TABLE_ID", "")

# Rate Limiting
REQUEST_DELAY = 1.5

# ===========================================================================
# REGEX PATTERNS
# ===========================================================================

RE_OBJEKTNR = re.compile(r"ID[:\s\-]+(\S+)", re.IGNORECASE)
RE_PLZ_ORT = re.compile(r"\b(\d{5})\s+([A-ZÄÖÜ][a-zäöüß\-\s/]+)")
RE_PRICE = re.compile(r"([\d.,]+)\s*€")

# ===========================================================================
# STOPWORDS
# ===========================================================================

STOP_STRINGS = [
    "Cookie", "Datenschutz", "Impressum", "Sie haben Fragen",
    "kontakt@", "Tel:", "Fax:", "E-Mail:", "www.", "http",
    "© ", "JavaScript", "Alle Rechte", "Rufen Sie uns an"
]

# ===========================================================================
# HELPER FUNCTIONS
# ===========================================================================

def _norm(s: str) -> str:
    """Normalisiere String"""
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _clean_desc_lines(lines: List[str]) -> List[str]:
    """Bereinige Beschreibungszeilen"""
    cleaned = []
    seen = set()
    
    for line in lines:
        line = _norm(line)
        if not line or len(line) < 10:
            continue
        
        # Filtere Stopwords
        if any(stop in line for stop in STOP_STRINGS):
            continue
        
        # Dedupliziere
        line_lower = line.lower()
        if line_lower in seen:
            continue
        seen.add(line_lower)
        cleaned.append(line)
    
    return cleaned

def soup_get(url: str, delay: float = REQUEST_DELAY) -> BeautifulSoup:
    """Hole HTML und parse mit BeautifulSoup"""
    time.sleep(delay)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

# ===========================================================================
# AIRTABLE FUNCTIONS
# ===========================================================================

def airtable_table_segment() -> str:
    """Gibt base/table Segment für Airtable API zurück"""
    if not AIRTABLE_BASE or not AIRTABLE_TABLE_ID:
        return ""
    return f"{AIRTABLE_BASE}/{AIRTABLE_TABLE_ID}"

def airtable_headers() -> dict:
    """Airtable API Headers"""
    return {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json"
    }

def airtable_list_all() -> tuple:
    """Liste alle Records aus Airtable"""
    url = f"https://api.airtable.com/v0/{airtable_table_segment()}"
    headers = airtable_headers()
    
    all_records = []
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        
        all_records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
        time.sleep(0.2)
    
    ids = [rec["id"] for rec in all_records]
    fields = [rec.get("fields", {}) for rec in all_records]
    return ids, fields

def airtable_existing_fields() -> set:
    """Ermittle existierende Felder"""
    _, all_fields = airtable_list_all()
    if not all_fields:
        return set()
    return set(all_fields[0].keys())

def airtable_batch_create(records: List[dict]):
    """Erstelle Records in Batches"""
    url = f"https://api.airtable.com/v0/{airtable_table_segment()}"
    headers = airtable_headers()
    
    for i in range(0, len(records), 10):
        batch = records[i:i+10]
        payload = {"records": [{"fields": r} for r in batch]}
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        time.sleep(0.2)

def airtable_batch_update(updates: List[dict]):
    """Update Records in Batches"""
    url = f"https://api.airtable.com/v0/{airtable_table_segment()}"
    headers = airtable_headers()
    
    for i in range(0, len(updates), 10):
        batch = updates[i:i+10]
        payload = {"records": batch}
        r = requests.patch(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        time.sleep(0.2)

def airtable_batch_delete(record_ids: List[str]):
    """Lösche Records in Batches"""
    url = f"https://api.airtable.com/v0/{airtable_table_segment()}"
    headers = airtable_headers()
    
    for i in range(0, len(record_ids), 10):
        batch = record_ids[i:i+10]
        params = {"records[]": batch}
        r = requests.delete(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        time.sleep(0.2)

def sanitize_record_for_airtable(record: dict, allowed_fields: set) -> dict:
    """Bereinige Record für Airtable"""
    if not allowed_fields:
        return record
    return {k: v for k, v in record.items() if k in allowed_fields or not allowed_fields}

# ===========================================================================
# EXTRACTION FUNCTIONS
# ===========================================================================

def extract_price(soup: BeautifulSoup, page_text: str) -> str:
    """Extrahiere Preis"""
    # Suche nach Preis-Pattern
    for pattern in [
        r"Kaufpreis[:\s]+€?\s*([\d.,]+)\s*€?",
        r"Kaltmiete[:\s]+€?\s*([\d.,]+)\s*€?",
        r"Miete[:\s]+€?\s*([\d.,]+)\s*€?",
        r"Preis[:\s]+€?\s*([\d.,]+)\s*€?"
    ]:
        m = re.search(pattern, page_text, re.IGNORECASE)
        if m:
            preis_str = m.group(1).replace(".", "").replace(",", ".")
            try:
                preis_num = float(preis_str)
                if preis_num > 100:  # Plausibilitätsprüfung
                    return f"€{int(preis_num):,}".replace(",", ".")
            except:
                pass
    
    return ""

def parse_price_to_number(preis_str: str) -> Optional[float]:
    """Konvertiere Preis-String zu Nummer"""
    if not preis_str:
        return None
    
    # Entferne alles außer Zahlen, Punkt und Komma
    clean = re.sub(r"[^0-9.,]", "", preis_str)
    clean = clean.replace(".", "").replace(",", ".")
    
    try:
        return float(clean)
    except:
        return None

def extract_plz_ort(text: str, title: str = "") -> str:
    """Extrahiere PLZ und Ort"""
    blacklist = [
        "mietvertrag", "kaufvertrag", "zimmer", "kaufen", "mieten",
        "haus", "wohnung", "objekt", "immobilie", "verfügbar",
        "zu", "mit", "der", "die", "das", "den", "verkaufen"
    ]
    
    # Pattern 1: "Lage" Abschnitt
    lage_match = re.search(r"Lage\s+(.+?)(?=\n[A-Z][a-z]+\s|\n\n|$)", text, re.IGNORECASE | re.DOTALL)
    if lage_match:
        lage_text = lage_match.group(1)
        # Suche PLZ + Ort in Lage
        m = RE_PLZ_ORT.search(lage_text)
        if m:
            plz, ort = m.groups()
            ort = ort.strip()
            if ort.lower() not in blacklist:
                return f"{plz} {ort}"
    
    # Pattern 2: Standard PLZ + Ort
    m = RE_PLZ_ORT.search(text)
    if m:
        plz, ort = m.groups()
        ort = ort.strip()
        if ort.lower() not in blacklist:
            return f"{plz} {ort}"
    
    # Pattern 3: Aus Titel "in ORTSNAME"
    if title:
        m = re.search(r"\bin\s+([A-ZÄÖÜ][a-zäöüß]+(?:\s+[a-zäöüß]+)?)", title)
        if m:
            ort = m.group(1)
            if ort.lower() not in blacklist:
                # Suche PLZ im Text
                plz_match = re.search(r"\b(\d{5})\b", text[:1000])
                if plz_match:
                    return f"{plz_match.group(1)} {ort}"
                return ort
    
    return ""

def extract_description(soup: BeautifulSoup, structured_data: dict, page_text: str) -> str:
    """Extrahiere Beschreibung"""
    lines = []
    
    # Strukturierte Daten
    strukturiert = []
    for key in ["Objekttyp", "Vermarktungsart", "Wohnfläche", "Grundstücksgröße", "Zimmer", "Baujahr"]:
        if structured_data.get(key):
            strukturiert.append(f"{key}: {structured_data[key]}")
    
    if strukturiert:
        lines.append("=== OBJEKTDATEN ===")
        lines.extend(strukturiert)
        lines.append("")
    
    # Freitext-Beschreibung
    desc_lines = []
    
    # Strategie 1: Suche nach "Beschreibung" oder "Lage" oder "Ausstattung" Abschnitten
    for header in ["Beschreibung", "Lage", "Ausstattung", "Sonstige Angaben"]:
        pattern = rf"{header}\s+(.+?)(?=\n[A-Z][a-z]+\s+[A-Z]|\n\n[A-Z]|$)"
        m = re.search(pattern, page_text, re.IGNORECASE | re.DOTALL)
        if m:
            text = _norm(m.group(1))
            # Teile in Absätze
            paragraphs = [p.strip() for p in text.split("\n") if p.strip()]
            for para in paragraphs[:10]:  # Max 10 Absätze pro Abschnitt
                if len(para) > 50 and not any(skip in para for skip in STOP_STRINGS):
                    desc_lines.append(f"\n{header}")
                    desc_lines.append(para)
                    break
    
    # Strategie 2: Alle längeren Paragraphen
    if not desc_lines:
        for p in soup.find_all("p"):
            text = _norm(p.get_text(" ", strip=True))
            if text and len(text) > 100:
                if not any(skip in text for skip in STOP_STRINGS):
                    desc_lines.append(text)
    
    desc_lines = _clean_desc_lines(desc_lines)
    
    if desc_lines:
        lines.append("=== BESCHREIBUNG ===")
        lines.extend(desc_lines)
    
    if lines:
        return "\n\n".join(lines)[:12000]
    return ""

# ===========================================================================
# SCRAPING FUNCTIONS
# ===========================================================================

def collect_detail_links() -> List[str]:
    """Sammle alle Detailseiten-Links"""
    print(f"[LIST] Hole {LIST_URL}")
    soup = soup_get(LIST_URL)
    
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/immodetail/" in href:
            full_url = urljoin(BASE, href)
            if full_url not in links:
                links.append(full_url)
    
    print(f"[LIST] Gefunden: {len(links)} Immobilien")
    return links

def parse_detail(detail_url: str) -> dict:
    """Parse Detailseite"""
    soup = soup_get(detail_url)
    page_text = soup.get_text("\n", strip=True)
    
    # Titel
    h1 = soup.find("h1")
    title = _norm(h1.get_text(strip=True)) if h1 else ""
    
    # Objektnummer
    m_obj = RE_OBJEKTNR.search(page_text)
    objektnummer = m_obj.group(1).strip() if m_obj else ""
    
    # Preis
    preis = extract_price(soup, page_text)
    
    # PLZ/Ort
    ort = extract_plz_ort(page_text, title)
    
    # Bild-URL
    image_url = ""
    for img in soup.find_all("img"):
        src = img.get("src", "")
        if src and "onoffice.de" in src and "logo" not in src.lower():
            image_url = src if src.startswith("http") else urljoin(BASE, src)
            break
    
    # Vermarktungsart
    vermarktungsart = "Kaufen"
    if re.search(r"\b(zu\s+vermieten|miete|zur\s+miete|kaltmiete)\b", page_text, re.IGNORECASE):
        vermarktungsart = "Mieten"
    
    # Objekttyp
    objekttyp = ""
    for pattern in [r"Objekttyp[:\s]+([^\n]+)", r"Objektart[:\s]+([^\n]+)"]:
        m = re.search(pattern, page_text, re.IGNORECASE)
        if m:
            objekttyp = _norm(m.group(1))
            break
    
    # Wohnfläche
    wohnflaeche = ""
    m = re.search(r"Wohnfläche[:\s]+ca\.\s*([\d.,]+)\s*m²", page_text, re.IGNORECASE)
    if m:
        wohnflaeche = f"ca. {m.group(1)} m²"
    
    # Grundstück
    grundstueck = ""
    m = re.search(r"Grundstücksgröße[:\s]+ca\.\s*([\d.,]+)\s*m²", page_text, re.IGNORECASE)
    if m:
        grundstueck = f"ca. {m.group(1)} m²"
    
    # Zimmer
    zimmer = ""
    m = re.search(r"(?:Anzahl\s+)?Zimmer[:\s]+([\d.,]+)", page_text, re.IGNORECASE)
    if m:
        zimmer = m.group(1)
    
    # Baujahr
    baujahr = ""
    m = re.search(r"Baujahr[:\s]+(\d{4})", page_text, re.IGNORECASE)
    if m:
        baujahr = m.group(1)
    
    # Strukturierte Daten
    structured_data = {
        "Objekttyp": objekttyp,
        "Vermarktungsart": vermarktungsart,
        "Wohnfläche": wohnflaeche,
        "Grundstücksgröße": grundstueck,
        "Zimmer": zimmer,
        "Baujahr": baujahr,
    }
    
    # Beschreibung
    description = extract_description(soup, structured_data, page_text)
    
    return {
        "Titel": title,
        "URL": detail_url,
        "Beschreibung": description,
        "Objektnummer": objektnummer,
        "Kategorie": vermarktungsart,
        "Preis": preis,
        "Ort": ort,
        "Bild_URL": image_url,
    }

def make_record(row: dict) -> dict:
    """Erstelle Airtable-Record"""
    preis_value = parse_price_to_number(row["Preis"])
    return {
        "Titel": row["Titel"],
        "Kategorie": row["Kategorie"],
        "Webseite": row["URL"],
        "Objektnummer": row["Objektnummer"],
        "Beschreibung": row["Beschreibung"],
        "Bild": row["Bild_URL"],
        "Preis": preis_value,
        "Standort": row["Ort"],
    }

def unique_key(fields: dict) -> str:
    """Eindeutiger Key für Record"""
    obj = (fields.get("Objektnummer") or "").strip()
    if obj:
        return f"obj:{obj}"
    url = (fields.get("Webseite") or "").strip()
    if url:
        return f"url:{url}"
    return f"hash:{hash(json.dumps(fields, sort_keys=True))}"

# ===========================================================================
# MAIN
# ===========================================================================

def run():
    """Hauptfunktion"""
    print("[DUIS] Starte Scraper für immobilien-duis.de")
    
    # Sammle Links
    detail_links = collect_detail_links()
    
    if not detail_links:
        print("[WARN] Keine Links gefunden!")
        return
    
    # Scrape Details
    all_rows = []
    for i, url in enumerate(detail_links, 1):
        try:
            print(f"[SCRAPE] {i}/{len(detail_links)} | {url}")
            row = parse_detail(url)
            record = make_record(row)
            
            # Zeige Vorschau
            print(f"  → {record['Kategorie']:8} | {record['Titel'][:60]} | {record.get('Standort', 'N/A')}")
            
            all_rows.append(record)
        except Exception as e:
            print(f"[ERROR] Fehler bei {url}: {e}")
            continue
    
    if not all_rows:
        print("[WARN] Keine Datensätze gefunden.")
        return
    
    # Speichere CSV
    csv_file = "duis_immobilien.csv"
    cols = ["Titel", "Kategorie", "Webseite", "Objektnummer", "Beschreibung", "Bild", "Preis", "Standort"]
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(all_rows)
    print(f"\n[CSV] Gespeichert: {csv_file} ({len(all_rows)} Zeilen)")
    
    # Airtable Sync
    if AIRTABLE_TOKEN and AIRTABLE_BASE and airtable_table_segment():
        print("\n[AIRTABLE] Starte Synchronisation...")
        
        allowed = airtable_existing_fields()
        all_ids, all_fields = airtable_list_all()
        
        existing = {}
        for rec_id, f in zip(all_ids, all_fields):
            k = unique_key(f)
            existing[k] = (rec_id, f)
        
        desired = {}
        for r in all_rows:
            k = unique_key(r)
            if k in desired:
                if len(r.get("Beschreibung", "")) > len(desired[k].get("Beschreibung", "")):
                    desired[k] = sanitize_record_for_airtable(r, allowed)
            else:
                desired[k] = sanitize_record_for_airtable(r, allowed)
        
        to_create, to_update, keep = [], [], set()
        for k, fields in desired.items():
            if k in existing:
                rec_id, old = existing[k]
                diff = {fld: val for fld, val in fields.items() if old.get(fld) != val}
                if diff:
                    to_update.append({"id": rec_id, "fields": diff})
                keep.add(k)
            else:
                to_create.append(fields)
        
        to_delete_ids = [rec_id for k, (rec_id, _) in existing.items() if k not in keep]
        
        print(f"\n[SYNC] Gesamt → create: {len(to_create)}, update: {len(to_update)}, delete: {len(to_delete_ids)}")
        
        if to_create:
            print(f"[Airtable] Erstelle {len(to_create)} neue Records...")
            airtable_batch_create(to_create)
        if to_update:
            print(f"[Airtable] Aktualisiere {len(to_update)} Records...")
            airtable_batch_update(to_update)
        if to_delete_ids:
            print(f"[Airtable] Lösche {len(to_delete_ids)} Records...")
            airtable_batch_delete(to_delete_ids)
        
        print("[Airtable] Synchronisation abgeschlossen.\n")
    else:
        print("[Airtable] ENV nicht gesetzt – Upload übersprungen.")

if __name__ == "__main__":
    run()
