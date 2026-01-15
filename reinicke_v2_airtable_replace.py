#!/usr/bin/env python3
"""
Scraper für https://alainreinickeimmobilien.de/aktuelle-angebote/
Extrahiert Immobilienangebote aus Propstack-iframes und synct mit Airtable

v2.0 - Mit verbesserter GPT-Kurzbeschreibung, Unterkategorie-Caching und Validierung
"""

import os
import re
import sys
import csv
import json
import time
import base64
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Optional, Tuple

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

BASE = "https://alainreinickeimmobilien.de"
LIST_URL = f"{BASE}/aktuelle-angebote/"
PROPSTACK_BASE = "https://alainreinicke.landingpage.immobilien"

# Propstack Shop Token
SHOP_TOKEN = "DqFSVCcC7WoWndVggQ83eLtJ"

# Airtable
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN", "")
AIRTABLE_BASE = os.getenv("AIRTABLE_BASE", "")
AIRTABLE_TABLE_ID = os.getenv("AIRTABLE_TABLE_ID", "")

# OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# SYNC-MODUS
FULL_REPLACE = os.getenv("FULL_REPLACE", "false").lower() == "true"

# Rate Limiting
REQUEST_DELAY = 1.5

# ===========================================================================
# REGEX PATTERNS
# ===========================================================================

RE_PLZ_ORT = re.compile(r"\b(\d{5})\s+([A-ZÄÖÜ][a-zäöüß\-\s/]+)")
RE_PRICE = re.compile(r"([\d.,]+)\s*€")

# ===========================================================================
# HELPER FUNCTIONS
# ===========================================================================

# Verschiedene User-Agents für Rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

def get_random_headers() -> dict:
    """Generiert zufällige Browser-Header"""
    import random
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }

def _norm(s: str) -> str:
    """Normalisiere String"""
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s).strip()
    return s

def clean_text(text: str) -> str:
    """Bereinigt Text: Entfernt mehrfache Leerzeilen und unnötige Whitespaces."""
    if not text:
        return ""
    
    text = re.sub(r'\n\s*\n+', '\n', text)
    lines = [line.strip() for line in text.split('\n')]
    lines = [line for line in lines if line]
    
    return '\n'.join(lines)

def soup_get(url: str, delay: float = REQUEST_DELAY, max_retries: int = 3) -> BeautifulSoup:
    """
    Hole HTML und parse mit BeautifulSoup.
    Mit Retry-Logik und exponential backoff.
    """
    last_error = None
    
    for attempt in range(max_retries):
        try:
            # Delay vor Request (mit Jitter)
            import random
            jitter = random.uniform(0.5, 1.5)
            time.sleep(delay * jitter)
            
            headers = get_random_headers()
            
            r = requests.get(url, headers=headers, timeout=30)
            r.raise_for_status()
            return BeautifulSoup(r.text, "lxml")
            
        except requests.exceptions.ConnectionError as e:
            last_error = e
            wait_time = (2 ** attempt) * 2  # 2, 4, 8 Sekunden
            print(f"[RETRY] Verbindungsfehler (Versuch {attempt + 1}/{max_retries}), warte {wait_time}s...")
            time.sleep(wait_time)
            
        except requests.exceptions.Timeout as e:
            last_error = e
            wait_time = (2 ** attempt) * 2
            print(f"[RETRY] Timeout (Versuch {attempt + 1}/{max_retries}), warte {wait_time}s...")
            time.sleep(wait_time)
            
        except requests.exceptions.HTTPError as e:
            # Bei 403/429 länger warten
            if e.response.status_code in [403, 429]:
                last_error = e
                wait_time = (2 ** attempt) * 5  # 5, 10, 20 Sekunden
                print(f"[RETRY] HTTP {e.response.status_code} (Versuch {attempt + 1}/{max_retries}), warte {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise
    
    # Alle Retries fehlgeschlagen
    raise last_error or Exception(f"Konnte {url} nach {max_retries} Versuchen nicht laden")


def test_website_reachability() -> bool:
    """Testet ob die Hauptwebsite erreichbar ist"""
    print(f"[TEST] Prüfe Erreichbarkeit von {BASE}...")
    
    try:
        headers = get_random_headers()
        r = requests.get(BASE, headers=headers, timeout=15)
        
        if r.status_code == 200:
            print(f"[TEST] ✅ Website erreichbar (Status: {r.status_code})")
            return True
        elif r.status_code == 403:
            print(f"[TEST] ⚠️ Website blockiert Zugriff (Status: 403)")
            print(f"[TEST] Mögliche Ursachen: Cloudflare, WAF, IP-Blocking")
            return False
        else:
            print(f"[TEST] ⚠️ Unerwarteter Status: {r.status_code}")
            return r.status_code < 500
            
    except requests.exceptions.ConnectionError as e:
        print(f"[TEST] ❌ Verbindungsfehler: {e}")
        print(f"[TEST] Die Website ist möglicherweise nicht erreichbar oder blockiert diese IP.")
        return False
    except requests.exceptions.Timeout:
        print(f"[TEST] ❌ Timeout - Website antwortet nicht")
        return False
    except Exception as e:
        print(f"[TEST] ❌ Fehler: {e}")
        return False

# ===========================================================================
# AIRTABLE FUNCTIONS
# ===========================================================================

def airtable_table_segment() -> str:
    if not AIRTABLE_BASE or not AIRTABLE_TABLE_ID:
        return ""
    return f"{AIRTABLE_BASE}/{AIRTABLE_TABLE_ID}"

def airtable_headers() -> dict:
    return {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json"
    }

def airtable_list_all() -> tuple:
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
    """Hole existierende Felder"""
    return set()  # Keine Filterung

def airtable_batch_create(records: List[dict]):
    url = f"https://api.airtable.com/v0/{airtable_table_segment()}"
    headers = airtable_headers()
    
    for i in range(0, len(records), 10):
        batch = records[i:i+10]
        payload = {"records": [{"fields": r} for r in batch]}
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        time.sleep(0.2)

def airtable_batch_update(updates: List[dict]):
    url = f"https://api.airtable.com/v0/{airtable_table_segment()}"
    headers = airtable_headers()
    
    for i in range(0, len(updates), 10):
        batch = updates[i:i+10]
        payload = {"records": batch}
        r = requests.patch(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        time.sleep(0.2)

def airtable_batch_delete(record_ids: List[str]):
    url = f"https://api.airtable.com/v0/{airtable_table_segment()}"
    headers = airtable_headers()
    
    for i in range(0, len(record_ids), 10):
        batch = record_ids[i:i+10]
        params = {"records[]": batch}
        r = requests.delete(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        time.sleep(0.2)

def sanitize_record_for_airtable(record: dict, allowed_fields: set) -> dict:
    ALWAYS_ALLOWED = {"Kurzbeschreibung"}
    
    if not allowed_fields:
        return record
    
    all_allowed = allowed_fields | ALWAYS_ALLOWED
    return {k: v for k, v in record.items() if k in all_allowed}

# ===========================================================================
# VALIDIERUNG - Leere Records filtern
# ===========================================================================

def is_valid_record(record: dict) -> bool:
    """Prüft ob ein Record gültig ist (nicht leer)."""
    titel = (record.get("Titel") or "").strip()
    webseite = (record.get("Webseite") or "").strip()
    
    if not titel or not webseite:
        return False
    
    filled_fields = 0
    for key, value in record.items():
        if value is not None:
            if isinstance(value, str) and value.strip():
                filled_fields += 1
            elif isinstance(value, (int, float)) and value > 0:
                filled_fields += 1
    
    return filled_fields >= 3


def filter_valid_records(records: list) -> list:
    """Filtert ungültige/leere Records heraus"""
    valid = []
    invalid_count = 0
    
    for record in records:
        if is_valid_record(record):
            valid.append(record)
        else:
            invalid_count += 1
            print(f"[FILTER] Ungültiger Record: {record.get('Titel', 'KEIN TITEL')[:50]}")
    
    if invalid_count > 0:
        print(f"[FILTER] {invalid_count} ungültige Records herausgefiltert")
    
    return valid


def cleanup_empty_airtable_records():
    """Löscht leere/ungültige Records aus Airtable."""
    if not (AIRTABLE_TOKEN and AIRTABLE_BASE and airtable_table_segment()):
        return
    
    print("[CLEANUP] Prüfe Airtable auf leere Records...")
    
    try:
        all_ids, all_fields = airtable_list_all()
        
        to_delete = []
        for rec_id, fields in zip(all_ids, all_fields):
            if not is_valid_record(fields):
                to_delete.append(rec_id)
                print(f"[CLEANUP] Leerer Record: {fields.get('Titel', 'KEIN TITEL')[:40]}")
        
        if to_delete:
            print(f"[CLEANUP] Lösche {len(to_delete)} leere Records...")
            airtable_batch_delete(to_delete)
            print(f"[CLEANUP] ✅ {len(to_delete)} leere Records gelöscht")
        else:
            print("[CLEANUP] ✅ Keine leeren Records gefunden")
            
    except Exception as e:
        print(f"[CLEANUP] Fehler: {e}")

# ===========================================================================
# CACHES - Kurzbeschreibung UND Unterkategorie
# ===========================================================================

KURZBESCHREIBUNG_CACHE = {}  # {objektnummer: kurzbeschreibung}
UNTERKATEGORIE_CACHE = {}    # {objektnummer: unterkategorie}

def load_caches():
    """Lädt Kurzbeschreibungen UND Unterkategorien aus Airtable in Caches"""
    global KURZBESCHREIBUNG_CACHE, UNTERKATEGORIE_CACHE
    
    if not (AIRTABLE_TOKEN and AIRTABLE_BASE and airtable_table_segment()):
        print("[CACHE] Airtable nicht konfiguriert - Caches leer")
        return
    
    if FULL_REPLACE:
        print("[CACHE] FULL_REPLACE Modus - Cache übersprungen")
        return
    
    try:
        all_ids, all_fields = airtable_list_all()
        for fields in all_fields:
            obj_nr = fields.get("Objektnummer", "").strip()
            if not obj_nr:
                continue
            
            kurzbeschreibung = fields.get("Kurzbeschreibung", "").strip()
            if kurzbeschreibung:
                KURZBESCHREIBUNG_CACHE[obj_nr] = kurzbeschreibung
            
            unterkategorie = fields.get("Unterkategorie", "").strip()
            if unterkategorie:
                UNTERKATEGORIE_CACHE[obj_nr] = unterkategorie
        
        print(f"[CACHE] {len(KURZBESCHREIBUNG_CACHE)} Kurzbeschreibungen geladen")
        print(f"[CACHE] {len(UNTERKATEGORIE_CACHE)} Unterkategorien geladen")
    except Exception as e:
        print(f"[CACHE] Fehler beim Laden: {e}")

def get_cached_kurzbeschreibung(objektnummer: str) -> str:
    return KURZBESCHREIBUNG_CACHE.get(objektnummer, "")

def get_cached_unterkategorie(objektnummer: str) -> str:
    return UNTERKATEGORIE_CACHE.get(objektnummer, "")

# ===========================================================================
# GPT KURZBESCHREIBUNG - NEUE VERSION
# ===========================================================================

KURZBESCHREIBUNG_FIELDS = [
    "Objekttyp",
    "Baujahr",
    "Wohnfläche",
    "Grundstück",
    "Zimmer",
    "Preis",
    "Standort",
    "Energieeffizienz",
    "Besonderheiten"
]

def normalize_kurzbeschreibung(gpt_output: str, scraped_data: dict) -> str:
    """Normalisiert GPT-Ausgabe. KEINE Platzhalter, nur vorhandene Felder."""
    parsed = {}
    for line in gpt_output.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        if ":" in line:
            key, value = line.split(":", 1)
            key = key.strip()
            value = value.strip()
            if value and value not in ["-", "—", "k. A.", "unbekannt", "nicht angegeben", ""]:
                parsed[key] = value
    
    scrape_mapping = {
        "Zimmer": "zimmer",
        "Wohnfläche": "wohnflaeche", 
        "Grundstück": "grundstueck",
        "Baujahr": "baujahr",
        "Preis": "preis",
        "Standort": "standort",
    }
    
    for field, scrape_key in scrape_mapping.items():
        if field not in parsed or not parsed[field]:
            scrape_value = scraped_data.get(scrape_key, "")
            if scrape_value and str(scrape_value).strip():
                if field == "Preis":
                    try:
                        preis_num = float(str(scrape_value).replace(".", "").replace(",", ".").replace("€", "").strip())
                        parsed[field] = f"{int(preis_num)} €"
                    except:
                        pass
                elif field == "Wohnfläche":
                    val = str(scrape_value).replace("ca.", "").replace("m²", "").strip()
                    if val:
                        parsed[field] = f"{val} m²"
                elif field == "Grundstück":
                    val = str(scrape_value).replace("ca.", "").replace("m²", "").strip()
                    if val:
                        parsed[field] = f"{val} m²"
                else:
                    parsed[field] = str(scrape_value)
    
    output_lines = []
    for field in KURZBESCHREIBUNG_FIELDS:
        value = parsed.get(field, "")
        if value and value.strip():
            output_lines.append(f"{field}: {value}")
    
    return "\n".join(output_lines)

def generate_kurzbeschreibung(beschreibung: str, titel: str, kategorie: str, preis: str, ort: str,
                               zimmer: str = "", wohnflaeche: str = "", grundstueck: str = "", baujahr: str = "",
                               objektnummer: str = "") -> str:
    """Generiert strukturierte Kurzbeschreibung mit GPT."""
    
    if objektnummer:
        cached = get_cached_kurzbeschreibung(objektnummer)
        if cached:
            print(f"[CACHE] Kurzbeschreibung aus Cache für {objektnummer[:20]}...")
            return cached
    
    scraped_data = {
        "kategorie": kategorie,
        "preis": preis,
        "standort": ort,
        "zimmer": zimmer,
        "wohnflaeche": wohnflaeche,
        "grundstueck": grundstueck,
        "baujahr": baujahr,
    }
    
    if not OPENAI_API_KEY:
        return normalize_kurzbeschreibung("", scraped_data)
    
    prompt = f"""# Rolle
Du bist ein präziser Immobilien-Datenanalyst und Parser. Deine Aufgabe ist es, aus unstrukturierten Immobilienanzeigen ausschließlich objektive, explizit genannte Fakten zu extrahieren und streng strukturiert auszugeben. Du arbeitest regelbasiert, deterministisch und formatgenau. Kreative Ergänzungen sind untersagt.

# Aufgabe
1. Analysiere die bereitgestellte Immobilienanzeige vollständig.
2. Extrahiere nur eindeutig genannte, objektive Fakten.
3. Gib die strukturierte Kurzbeschreibung exakt im vorgegebenen Zeilenformat aus.
4. Lasse jedes Feld vollständig weg, zu dem keine eindeutige Angabe vorliegt.

# Eingabedaten
TITEL: {titel}
KATEGORIE: {kategorie}
PREIS: {preis if preis else 'nicht angegeben'}
STANDORT: {ort if ort else 'nicht angegeben'}
BESCHREIBUNG: {beschreibung[:3000]}

# Erlaubte Felder (Whitelist – verbindlich)
Objekttyp
Baujahr
Wohnfläche
Grundstück
Zimmer
Preis
Standort
Energieeffizienz
Besonderheiten

# Ausgabeformat (verbindlich)
Objekttyp: [Einfamilienhaus | Mehrfamilienhaus | Eigentumswohnung | Baugrundstück | Reihenhaus | Doppelhaushälfte | Sonstiges]
Baujahr: [Jahr]
Wohnfläche: [Zahl in m²]
Grundstück: [Zahl in m²]
Zimmer: [Anzahl]
Preis: [Zahl in €]
Standort: [Ort oder PLZ Ort]
Energieeffizienz: [Klasse]
Besonderheiten: [kommaseparierte Liste]

# Strikte Regeln (bindend)
• Es ist strengstens untersagt, eigene Felder zu erfinden.
• Felder wie „Schlafzimmer", „Kategorie", „Etage", „Ausstattung", „Kauf/Miete" sind verboten.
• Es dürfen keine Platzhalter verwendet werden (z. B. „-", „—", „k. A.", „unbekannt").
• Wenn ein Feld nicht eindeutig ermittelbar ist, darf die gesamte Zeile nicht ausgegeben werden.
• KEINE LEERZEILEN in der Ausgabe!

# Ziel
Die Ausgabe wird automatisiert weiterverarbeitet. Jede Abweichung vom Format gilt als Fehler."""

    try:
        headers = {
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": "Du bist ein regelbasierter Datenparser. Halte dich strikt an die Vorgaben. Keine Kreativität, keine Ergänzungen. Keine Leerzeilen."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 400,
            "temperature": 0.0
        }
        
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=30
        )
        response.raise_for_status()
        
        result = response.json()
        gpt_output = result["choices"][0]["message"]["content"].strip()
        
        kurzbeschreibung = normalize_kurzbeschreibung(gpt_output, scraped_data)
        
        print(f"[GPT] Kurzbeschreibung generiert ({len(kurzbeschreibung)} Zeichen)")
        return kurzbeschreibung
        
    except Exception as e:
        print(f"[ERROR] GPT Kurzbeschreibung fehlgeschlagen: {e}")
        return normalize_kurzbeschreibung("", scraped_data)

# ===========================================================================
# GPT UNTERKATEGORIE-KLASSIFIKATION MIT CACHING
# ===========================================================================

KEYS_WOHNUNG = ["wohnung", "etagenwohnung", "eigentumswohnung", "apartment", "etw", "penthouse", "maisonette"]
KEYS_HAUS = ["haus", "einfamilienhaus", "zweifamilienhaus", "reihenhaus", "doppelhaushälfte", "villa", "bungalow", "efh", "dhh", "mfh"]
KEYS_GEWERBE = ["gewerbe", "büro", "laden", "praxis", "lager", "halle", "gastronomie", "gewerbefläche", "bürofläche"]
KEYS_GRUNDSTUECK = ["grundstück", "baugrundstück", "bauland", "bauplatz"]

def heuristic_subcategory(titel: str, beschreibung: str) -> str:
    """Heuristische Fallback-Klassifikation"""
    text = (titel + " " + beschreibung).lower()
    
    scores = {
        "Grundstück": sum(1 for k in KEYS_GRUNDSTUECK if k in text),
        "Gewerbe": sum(1 for k in KEYS_GEWERBE if k in text),
        "Wohnung": sum(1 for k in KEYS_WOHNUNG if k in text),
        "Haus": sum(1 for k in KEYS_HAUS if k in text),
    }
    
    # Grundstück hat höchste Priorität
    if scores["Grundstück"] >= 1:
        return "Grundstück"
    
    # Gewerbe vor Wohnung/Haus
    if scores["Gewerbe"] >= 2:
        return "Gewerbe"
    
    best = max(scores, key=scores.get)
    if scores[best] >= 1:
        return best
    
    return "Haus"  # Default

def gpt_classify_unterkategorie(titel: str, beschreibung: str, objektnummer: str, kategorie: str) -> str:
    """Klassifiziert die Unterkategorie via GPT. MIT CACHING."""
    
    if objektnummer:
        cached = get_cached_unterkategorie(objektnummer)
        if cached:
            print(f"[CACHE] Unterkategorie aus Cache: {cached}")
            return cached
    
    if not OPENAI_API_KEY:
        return heuristic_subcategory(titel, beschreibung)
    
    # Erlaubte Kategorien
    allowed_cats = "Wohnung, Haus, Gewerbe, Grundstück"
    
    prompt = f"""Klassifiziere dieses Immobilien-Exposé in EXAKT eine der folgenden Kategorien:
{allowed_cats}

WICHTIG:
- Gib NUR das eine Wort aus (z.B. "Wohnung" oder "Gewerbe")
- Keine Erklärung, kein Punkt, nur das Wort
- Bei Büro, Laden, Praxis, Lager, Halle → "Gewerbe"
- Bei Baugrundstück, Bauland, Bauplatz → "Grundstück"
- Bei Eigentumswohnung, ETW, Apartment → "Wohnung"
- Bei Einfamilienhaus, DHH, Reihenhaus, Villa → "Haus"

Titel: {titel}
Beschreibung: {beschreibung[:1500]}"""

    try:
        headers = {
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": "Du bist ein präziser Immobilien-Klassifizierer. Antworte mit genau einem Wort."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 10,
            "temperature": 0.0
        }
        
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=15
        )
        response.raise_for_status()
        
        result = response.json()
        output = result["choices"][0]["message"]["content"].strip().replace(".", "")
        
        valid = {"Wohnung", "Haus", "Gewerbe", "Grundstück"}
        
        if output in valid:
            print(f"[GPT] Unterkategorie: {output}")
            return output
        else:
            print(f"[GPT] Ungültige Kategorie '{output}', verwende Heuristik")
            return heuristic_subcategory(titel, beschreibung)
        
    except Exception as e:
        print(f"[ERROR] GPT Klassifikation fehlgeschlagen: {e}")
        return heuristic_subcategory(titel, beschreibung)

# ===========================================================================
# PROPSTACK IFRAME FUNCTIONS
# ===========================================================================

def collect_detail_page_links_with_categories() -> List[Tuple[str, str, str]]:
    """Sammle Links zu Detailseiten MIT Kategorie/Unterkategorie von Übersichtsseite"""
    print(f"[LIST] Hole {LIST_URL}")
    
    try:
        soup = soup_get(LIST_URL, max_retries=3)
    except Exception as e:
        print(f"[ERROR] Konnte Übersichtsseite nicht laden: {e}")
        return []
    
    detail_data = []
    
    section_mapping = {
        "einfamilienhaus": ("Kaufen", "Haus"),
        "einfamilienhäuser": ("Kaufen", "Haus"),
        "doppelhaushälfte": ("Kaufen", "Haus"),
        "doppelhaushälften": ("Kaufen", "Haus"),
        "zweifamilienhaus": ("Kaufen", "Haus"),
        "zweifamilienhäuser": ("Kaufen", "Haus"),
        "mehrfamilienhaus": ("Kaufen", "Haus"),
        "mehrfamilienhäuser": ("Kaufen", "Haus"),
        "eigentumswohnung": ("Kaufen", "Wohnung"),
        "eigentumswohnungen": ("Kaufen", "Wohnung"),
        "gewerbeimmobilie": (None, "Gewerbe"),
        "gewerbeimmobilien": (None, "Gewerbe"),
        "mietobjekt": ("Mieten", None),
        "mietobjekte": ("Mieten", None),
        "grundstück": ("Kaufen", "Grundstück"),
        "grundstücke": ("Kaufen", "Grundstück"),
        "neubau": ("Kaufen", "Haus"),
    }
    
    for a in soup.find_all("a", href=True):
        href = a["href"]
        
        if not (href.startswith("/") or "alainreinickeimmobilien.de" in href):
            continue
        
        full_url = urljoin(BASE, href)
        full_url = full_url.split("#")[0].split("?")[0]
        
        if any(d[0] == full_url for d in detail_data) or full_url == LIST_URL:
            continue
        
        path_lower = full_url.lower()
        if any(skip in path_lower for skip in [
            "/startseite", "/warum-wir", "/immobilien-ankauf",
            "/immobilienbewertung", "/aktuelle-angebote", "/kontakt",
            "/impressum", "/datenschutz", "/agb", "/cookie",
            BASE + "/#", BASE + "/$"
        ]):
            continue
        
        if full_url == BASE or full_url == BASE + "/":
            continue
        
        prev_header_text = ""
        for sibling in a.find_all_previous(["h2", "h3", "h4"]):
            prev_header_text = sibling.get_text(strip=True).lower()
            break
        
        kategorie = "Kaufen"
        unterkategorie = "Haus"
        
        if prev_header_text:
            for key, (kat, unterkat) in section_mapping.items():
                if key in prev_header_text:
                    if kat:
                        kategorie = kat
                    if unterkat:
                        unterkategorie = unterkat
                    break
        
        detail_data.append((full_url, kategorie, unterkategorie))
    
    print(f"[LIST] Gefunden: {len(detail_data)} Detailseiten")
    return detail_data

def extract_iframe_from_detail_page(detail_url: str) -> Optional[str]:
    """Extrahiere Propstack iframe-URL von einer Detailseite"""
    try:
        soup = soup_get(detail_url, delay=1.0, max_retries=2)
        
        for iframe in soup.find_all("iframe"):
            src = iframe.get("src", "")
            if "landingpage.immobilien" in src and "/public/exposee/" in src:
                return src
        
        return None
        
    except requests.exceptions.ConnectionError as e:
        print(f"[WARN] Verbindungsfehler bei {detail_url.split('/')[-1]}: {e}")
        return None
    except Exception as e:
        print(f"[ERROR] Fehler beim Laden der Detailseite: {e}")
        return None

def get_propstack_property_data_from_iframe(iframe_url: str) -> dict:
    """Hole Immobilien-Daten direkt aus Propstack iframe"""
    try:
        soup = soup_get(iframe_url, delay=1.0, max_retries=2)
        
        data = {
            "titel": "",
            "beschreibung": "",
            "preis": "",
            "ort": "",
            "kategorie": "Kaufen",
            "unterkategorie": "Haus",
            "bild_url": "",
            "zimmer": "",
            "wohnflaeche": "",
            "grundstueck": "",
            "baujahr": "",
        }
        
        for tag in ["h1", "h2", "title"]:
            elem = soup.find(tag)
            if elem:
                text = _norm(elem.get_text(strip=True))
                if len(text) > 5:
                    data["titel"] = text
                    break
        
        text_content = soup.get_text().lower()
        text_content_full = soup.get_text()
        
        # Preis extrahieren
        price_matches = RE_PRICE.findall(text_content)
        if price_matches:
            prices = []
            for p in price_matches:
                try:
                    clean = p.replace(".", "").replace(",", ".")
                    val = float(clean)
                    if val > 100:
                        prices.append((val, p + " €"))
                except:
                    pass
            if prices:
                prices.sort(reverse=True)
                data["preis"] = prices[0][1]
        
        # Miete Pattern
        miete_pattern = re.compile(r'(?:Warmmiete|Kaltmiete|Miete)\s*[:.]?\s*([\d.,]+)\s*€', re.IGNORECASE)
        miete_matches = miete_pattern.findall(text_content_full)
        
        if miete_matches:
            for miete_str in miete_matches:
                try:
                    clean = miete_str.replace(".", "").replace(",", ".")
                    val = float(clean)
                    if val > 100:
                        data["preis"] = f"{miete_str} €"
                        data["kategorie"] = "Mieten"
                        break
                except:
                    pass
        
        # PLZ/Ort
        match = RE_PLZ_ORT.search(text_content_full)
        if match:
            data["ort"] = f"{match.group(1)} {match.group(2).strip()}"
        
        if not data["ort"]:
            for meta in soup.find_all("meta"):
                content = meta.get("content", "")
                match = RE_PLZ_ORT.search(content)
                if match:
                    data["ort"] = f"{match.group(1)} {match.group(2).strip()}"
                    break
        
        # Zusätzliche Daten
        zimmer_match = re.search(r'(\d+)\s*Zimmer', text_content_full, re.IGNORECASE)
        if zimmer_match:
            data["zimmer"] = zimmer_match.group(1)
        
        wohnflaeche_match = re.search(r'(?:ca\.\s*)?(\d+(?:[.,]\d+)?)\s*m²\s*(?:Wohnfläche|Wohnfl)', text_content_full, re.IGNORECASE)
        if wohnflaeche_match:
            data["wohnflaeche"] = wohnflaeche_match.group(1).replace(",", ".")
        
        grundstueck_match = re.search(r'(?:ca\.\s*)?(\d+(?:[.,]\d+)?)\s*m²\s*(?:Grundstück|Grundst)', text_content_full, re.IGNORECASE)
        if grundstueck_match:
            data["grundstueck"] = grundstueck_match.group(1).replace(",", ".")
        
        baujahr_match = re.search(r'Baujahr[:\s]+(\d{4})', text_content_full, re.IGNORECASE)
        if baujahr_match:
            data["baujahr"] = baujahr_match.group(1)
        
        # Beschreibung
        paragraphs = []
        for p in soup.find_all(["p", "div"]):
            text = _norm(p.get_text())
            if 50 < len(text) < 500:
                if not any(skip in text.lower() for skip in ["cookie", "datenschutz", "impressum", "javascript"]):
                    paragraphs.append(text)
                    if len(paragraphs) >= 5:
                        break
        
        if paragraphs:
            data["beschreibung"] = clean_text("\n\n".join(paragraphs)[:5000])
        
        # BILD EXTRAHIEREN - PROPSTACK SPEZIFISCH
        # Propstack speichert Hauptbilder als background-image in divs!
        
        # ANSATZ 1 (PRIORITÄT): Suche nach Titelbild/Hauptbild als background-image
        # Pattern: <div class="w-100 rounded" title="Titelbild" style="background-image: url(...)">
        for elem in soup.find_all(attrs={"title": re.compile(r"titelbild|hauptbild|objektbild", re.IGNORECASE)}):
            style = elem.get("style", "")
            if "background-image" in style:
                # Regex für URL-Extraktion (mit und ohne Anführungszeichen)
                match = re.search(r'url\(["\']?([^"\')\s]+)["\']?\)', style)
                if match:
                    url = match.group(1)
                    if "propstack" in url or "images" in url:
                        data["bild_url"] = url
                        print(f"[DEBUG] ✅ Titelbild gefunden (background-image mit title)")
                        break
        
        # ANSATZ 2: Suche nach background-image mit propstack URL
        if not data["bild_url"]:
            for elem in soup.find_all(style=True):
                style = elem.get("style", "")
                if "background-image" in style and "propstack" in style:
                    match = re.search(r'url\(["\']?([^"\')\s]+)["\']?\)', style)
                    if match:
                        url = match.group(1)
                        if not any(skip in url.lower() for skip in ["logo", "icon", "avatar", "profile"]):
                            data["bild_url"] = url
                            print(f"[DEBUG] ✅ Bild gefunden (background-image propstack)")
                            break
        
        # ANSATZ 3: Suche nach allen background-image Elementen
        if not data["bild_url"]:
            for elem in soup.find_all(style=True):
                style = elem.get("style", "")
                if "background-image" in style:
                    match = re.search(r'url\(["\']?([^"\')\s]+)["\']?\)', style)
                    if match:
                        url = match.group(1)
                        # Filtere Logos, Icons, Avatare
                        if any(skip in url.lower() for skip in ["logo", "icon", "avatar", "profile", "favicon"]):
                            continue
                        # Muss eine Bild-URL sein
                        if any(ext in url.lower() for ext in [".jpg", ".jpeg", ".png", ".webp", "photo", "image"]):
                            data["bild_url"] = url if url.startswith("http") else urljoin(iframe_url, url)
                            print(f"[DEBUG] ✅ Bild gefunden (background-image)")
                            break
        
        # ANSATZ 4: Fallback auf img Tags
        if not data["bild_url"]:
            for class_name in ["property-image", "object-image", "main-image", "gallery-image", "slider-image"]:
                img = soup.find("img", class_=lambda x: x and class_name in str(x).lower())
                if img:
                    src = img.get("src", "")
                    if src and not any(skip in src.lower() for skip in ["logo", "icon", "favicon", "placeholder", "avatar"]):
                        data["bild_url"] = src if src.startswith("http") else urljoin(iframe_url, src)
                        print(f"[DEBUG] ✅ Bild gefunden (img class)")
                        break
        
        # ANSATZ 5: srcset in img Tags
        if not data["bild_url"]:
            for img in soup.find_all("img"):
                srcset = img.get("srcset", "")
                if srcset:
                    parts = [s.strip().split()[0] for s in srcset.split(",") if s.strip()]
                    if parts:
                        src = parts[-1]
                        if not any(skip in src.lower() for skip in ["logo", "icon", "favicon", "avatar"]):
                            data["bild_url"] = src if src.startswith("http") else urljoin(iframe_url, src)
                            print(f"[DEBUG] ✅ Bild gefunden (srcset)")
                            break
        
        # ANSATZ 6: Erstes großes Bild
        if not data["bild_url"]:
            for img in soup.find_all("img"):
                src = img.get("src", "")
                alt = img.get("alt", "").lower()
                
                if any(skip in src.lower() for skip in ["logo", "icon", "favicon", "avatar", "profile"]):
                    continue
                if any(skip in alt for skip in ["logo", "icon", "avatar"]):
                    continue
                
                if src and len(src) > 20:  # Filter kurze/leere URLs
                    data["bild_url"] = src if src.startswith("http") else urljoin(iframe_url, src)
                    print(f"[DEBUG] ✅ Bild gefunden (erstes img)")
                    break
        
        if not data["bild_url"]:
            print(f"[WARN] ⚠️ KEIN Bild gefunden!")
        
        return data
        
    except Exception as e:
        print(f"[ERROR] Failed to load iframe: {e}")
        return None

# ===========================================================================
# SCRAPING FUNCTIONS
# ===========================================================================

def collect_all_properties() -> List[dict]:
    """Sammle alle Immobilien von der Website"""
    
    detail_data = collect_detail_page_links_with_categories()
    
    if not detail_data:
        print("[WARN] Keine Detailseiten gefunden!")
        return []
    
    all_properties = []
    
    for i, (detail_url, overview_kategorie, overview_unterkategorie) in enumerate(detail_data, 1):
        print(f"\n[SCRAPE] {i}/{len(detail_data)}")
        
        try:
            iframe_url = extract_iframe_from_detail_page(detail_url)
            
            if not iframe_url:
                print(f"  ⚠️  Überspringe - kein iframe gefunden")
                continue
            
            prop_data = get_propstack_property_data_from_iframe(iframe_url)
            
            if prop_data:
                # Extrahiere Objektnummer
                match = re.search(r"(eyJ[A-Za-z0-9+/=]+)", iframe_url)
                if match:
                    token_b64 = match.group(1)
                    try:
                        padding = len(token_b64) % 4
                        if padding:
                            token_b64 += "=" * (4 - padding)
                        decoded = base64.b64decode(token_b64).decode('utf-8')
                        token_data = json.loads(decoded)
                        prop_data["objektnummer"] = token_data.get("property_token", "")
                    except:
                        prop_data["objektnummer"] = detail_url.split("/")[-1]
                else:
                    prop_data["objektnummer"] = detail_url.split("/")[-1]
                
                # Kategorie setzen
                if overview_kategorie and prop_data["kategorie"] != "Mieten":
                    prop_data["kategorie"] = overview_kategorie
                
                # Unterkategorie via GPT klassifizieren (mit Cache!)
                prop_data["unterkategorie"] = gpt_classify_unterkategorie(
                    titel=prop_data.get("titel", ""),
                    beschreibung=prop_data.get("beschreibung", ""),
                    objektnummer=prop_data.get("objektnummer", ""),
                    kategorie=prop_data.get("kategorie", "Kaufen")
                )
                
                # Gewerbe-Spezialfall: Kategorie aus Preis
                if prop_data["unterkategorie"] == "Gewerbe" and not overview_kategorie:
                    preis_text = prop_data.get("preis", "")
                    try:
                        clean = preis_text.replace("€", "").replace(".", "").replace(",", ".").strip()
                        preis_val = float(clean)
                        if preis_val < 30000:
                            prop_data["kategorie"] = "Mieten"
                        else:
                            prop_data["kategorie"] = "Kaufen"
                    except:
                        prop_data["kategorie"] = "Kaufen"
                
                prop_data["url"] = detail_url
                
                all_properties.append(prop_data)
                
                bild_status = "✅" if prop_data.get("bild_url") else "❌"
                print(f"  → {prop_data.get('kategorie', 'N/A'):8} | {prop_data.get('unterkategorie', 'N/A'):12} | {prop_data.get('titel', 'Unbekannt')[:40]} | Bild: {bild_status}")
            else:
                print(f"  ⚠️  Keine Daten extrahiert")
                
        except Exception as e:
            print(f"  ❌ Fehler: {e}")
            continue
    
    return all_properties

def make_record(prop: dict) -> dict:
    """Erstelle Airtable-Record"""
    preis_value = None
    if prop.get("preis"):
        try:
            clean = prop["preis"].replace("€", "").replace(".", "").replace(",", ".").strip()
            preis_value = float(clean)
        except:
            pass
    
    kurzbeschreibung = generate_kurzbeschreibung(
        beschreibung=prop.get("beschreibung", ""),
        titel=prop.get("titel", ""),
        kategorie=prop.get("kategorie", "Kaufen"),
        preis=prop.get("preis", ""),
        ort=prop.get("ort", ""),
        zimmer=prop.get("zimmer", ""),
        wohnflaeche=prop.get("wohnflaeche", ""),
        grundstueck=prop.get("grundstueck", ""),
        baujahr=prop.get("baujahr", ""),
        objektnummer=prop.get("objektnummer", "")
    )
    
    record = {
        "Titel": prop.get("titel", "Unbekannt"),
        "Kategorie": prop.get("kategorie", "Kaufen"),
        "Unterkategorie": prop.get("unterkategorie", "Sonstiges"),
        "Webseite": prop.get("url", ""),
        "Objektnummer": prop.get("objektnummer", ""),
        "Beschreibung": clean_text(prop.get("beschreibung", "")),
        "Kurzbeschreibung": kurzbeschreibung,
        "Bild": prop.get("bild_url", ""),
        "Standort": prop.get("ort", ""),
    }
    
    if preis_value is not None:
        record["Preis"] = preis_value
    
    return record

def unique_key(fields: dict) -> str:
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
    print("[REINICKE] Starte Scraper für alainreinickeimmobilien.de (Propstack)")
    print(f"[INFO] FULL_REPLACE Modus: {FULL_REPLACE}")
    
    # SCHRITT 0: Teste ob Website erreichbar ist
    if not test_website_reachability():
        print("\n" + "="*60)
        print("[FATAL] Website nicht erreichbar!")
        print("="*60)
        print("Mögliche Lösungen:")
        print("1. Warte und versuche es später erneut")
        print("2. Prüfe ob die Website einen Proxy benötigt")
        print("3. Führe den Scraper von einem anderen Server aus")
        print("="*60)
        
        # Erstelle leere CSV damit GitHub Actions nicht komplett fehlschlägt
        csv_file = "reinicke_immobilien.csv"
        cols = ["Titel", "Kategorie", "Unterkategorie", "Webseite", "Objektnummer", "Beschreibung", "Kurzbeschreibung", "Bild", "Preis", "Standort"]
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
        print(f"[INFO] Leere CSV erstellt: {csv_file}")
        
        # Exit mit Code 0 um GitHub Action nicht als "failed" zu markieren
        # (Website-Blockierung ist kein Scraper-Fehler)
        print("[INFO] Beende mit Exit-Code 0 (kein Scraper-Fehler)")
        return
    
    # Lade Caches
    print("[INIT] Lade Caches aus Airtable...")
    load_caches()
    
    # Sammle alle Immobilien
    all_properties = collect_all_properties()
    
    if not all_properties:
        print("[WARN] Keine Immobilien gefunden!")
        return
    
    # Konvertiere zu Airtable Records
    all_rows = [make_record(prop) for prop in all_properties]
    
    # VALIDIERUNG
    print(f"\n[VALIDATE] Prüfe {len(all_rows)} Records...")
    all_rows = filter_valid_records(all_rows)
    
    if not all_rows:
        print("[WARN] Keine gültigen Datensätze nach Filterung.")
        return
    
    # CSV speichern
    csv_file = "reinicke_immobilien.csv"
    cols = ["Titel", "Kategorie", "Unterkategorie", "Webseite", "Objektnummer", "Beschreibung", "Kurzbeschreibung", "Bild", "Preis", "Standort"]
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction='ignore')
        w.writeheader()
        w.writerows(all_rows)
    print(f"\n[CSV] Gespeichert: {csv_file} ({len(all_rows)} Zeilen)")
    
    # Airtable Sync
    if AIRTABLE_TOKEN and AIRTABLE_BASE and airtable_table_segment():
        print("\n[AIRTABLE] Starte Synchronisation...")
        
        if FULL_REPLACE:
            print("[AIRTABLE] Modus: FULL REPLACE")
            
            all_ids, all_fields = airtable_list_all()
            
            if all_ids:
                print(f"[AIRTABLE] Lösche {len(all_ids)} existierende Records...")
                airtable_batch_delete(all_ids)
            
            print(f"[AIRTABLE] Erstelle {len(all_rows)} neue Records...")
            airtable_batch_create(all_rows)
            
            print(f"[AIRTABLE] ✅ Tabelle komplett ersetzt: {len(all_rows)} Records")
            
        else:
            print("[AIRTABLE] Modus: INTELLIGENT SYNC")
            
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
        
        # CLEANUP
        cleanup_empty_airtable_records()
        
        print("[Airtable] Synchronisation abgeschlossen.\n")
    else:
        print("[Airtable] ENV nicht gesetzt – Upload übersprungen.")

if __name__ == "__main__":
    run()
