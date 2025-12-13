#!/usr/bin/env python3
"""
Scraper für https://alainreinickeimmobilien.de/aktuelle-angebote/
Extrahiert Immobilienangebote aus Propstack-iframes und synct mit Airtable

Besonderheit: Website nutzt Propstack Landingpage-System mit iframes
Shop-Token: DqFSVCcC7WoWndVggQ83eLtJ
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

# Propstack Shop Token (fest für Reinicke)
SHOP_TOKEN = "DqFSVCcC7WoWndVggQ83eLtJ"

# Airtable
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN", "")
AIRTABLE_BASE = os.getenv("AIRTABLE_BASE", "")
AIRTABLE_TABLE_ID = os.getenv("AIRTABLE_TABLE_ID", "")

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

def _norm(s: str) -> str:
    """Normalisiere String"""
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s).strip()
    return s

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
# PROPSTACK IFRAME FUNCTIONS
# ===========================================================================

def collect_detail_page_links() -> List[str]:
    """Sammle Links zu Detailseiten von der Übersichtsseite"""
    print(f"[LIST] Hole {LIST_URL}")
    soup = soup_get(LIST_URL)
    
    detail_links = []
    
    # Suche nach Links zu Immobilien-Detailseiten
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True).lower()
        
        # Filter: Links die zu Immobilien-Details führen
        # Typische Muster: /grundstueck-in-..., /einfamilienhaus-..., etc.
        # Oder: Links mit "Exposé" Text
        if any(pattern in href.lower() for pattern in [
            "/grundstueck",
            "/einfamilienhaus",
            "/zweifamilienhaus",
            "/mehrfamilienhaus",
            "/wohnung",
            "/haus",
            "/villa",
            "/doppelhaus"
        ]) or "exposé" in text or "expose" in text:
            
            # Mache URL absolut
            full_url = urljoin(BASE, href)
            
            # Entferne Anker und Query-Parameter
            full_url = full_url.split("#")[0].split("?")[0]
            
            # Dedupliziere
            if full_url not in detail_links and full_url != LIST_URL:
                detail_links.append(full_url)
                print(f"[DEBUG] Found detail page: {full_url.split('/')[-1]}")
    
    print(f"\n[LIST] Gefunden: {len(detail_links)} Detailseiten")
    return detail_links

def extract_iframe_from_detail_page(detail_url: str) -> Optional[str]:
    """Extrahiere Propstack iframe-URL von einer Detailseite"""
    print(f"[DETAIL] Lade {detail_url.split('/')[-1]}")
    
    try:
        soup = soup_get(detail_url, delay=1.0)
        
        # Suche nach Propstack iframe
        for iframe in soup.find_all("iframe"):
            src = iframe.get("src", "")
            if "landingpage.immobilien" in src and "/public/exposee/" in src:
                print(f"[DEBUG] iframe gefunden!")
                return src
        
        print(f"[WARN] Kein Propstack iframe gefunden")
        return None
        
    except Exception as e:
        print(f"[ERROR] Fehler beim Laden der Detailseite: {e}")
        return None

def get_propstack_property_data_from_iframe(iframe_url: str) -> dict:
    """Hole Immobilien-Daten direkt aus Propstack iframe"""
    print(f"[PROPSTACK] Lade iframe...")
    
    try:
        soup = soup_get(iframe_url, delay=1.0)
        
        # Extrahiere Daten aus der Propstack-Seite
        data = {
            "titel": "",
            "beschreibung": "",
            "preis": "",
            "ort": "",
            "kategorie": "Kaufen",
            "bild_url": "",
        }
        
        # Titel - suche h1, h2 oder title
        for tag in ["h1", "h2", "title"]:
            elem = soup.find(tag)
            if elem:
                text = _norm(elem.get_text(strip=True))
                if len(text) > 5:
                    data["titel"] = text
                    break
        
        # Preis - suche nach Preis-Pattern
        text_content = soup.get_text()
        price_matches = RE_PRICE.findall(text_content)
        if price_matches:
            # Nehme den höchsten Preis (wahrscheinlich Kaufpreis)
            prices = []
            for p in price_matches:
                try:
                    clean = p.replace(".", "").replace(",", ".")
                    val = float(clean)
                    if val > 1000:  # Filter kleine Zahlen
                        prices.append(p + " €")
                except:
                    pass
            if prices:
                data["preis"] = prices[0]
        
        # PLZ/Ort
        match = RE_PLZ_ORT.search(text_content)
        if match:
            data["ort"] = f"{match.group(1)} {match.group(2).strip()}"
        
        # Beschreibung - sammle Textabschnitte
        paragraphs = []
        for p in soup.find_all(["p", "div"]):
            text = _norm(p.get_text())
            if 50 < len(text) < 500:
                # Filter unwichtige Texte
                if not any(skip in text.lower() for skip in ["cookie", "datenschutz", "impressum", "javascript"]):
                    paragraphs.append(text)
                    if len(paragraphs) >= 5:
                        break
        
        if paragraphs:
            data["beschreibung"] = "\n\n".join(paragraphs)[:5000]
        
        # Bild - suche erstes größeres Bild
        for img in soup.find_all("img"):
            src = img.get("src", "")
            alt = img.get("alt", "").lower()
            
            # Ignoriere Logos, Icons
            if any(skip in src.lower() for skip in ["logo", "icon", "favicon"]):
                continue
            if any(skip in alt for skip in ["logo", "icon"]):
                continue
            
            # Ignoriere sehr kleine Bilder
            width = img.get("width", "")
            try:
                if width and int(width) < 100:
                    continue
            except:
                pass
            
            if src:
                data["bild_url"] = src if src.startswith("http") else urljoin(iframe_url, src)
                break
        
        # Kategorie aus Text erkennen
        if any(word in text_content.lower() for word in ["miete", "vermietet", "zu vermieten", "mietpreis"]):
            data["kategorie"] = "Mieten"
        
        return data
        
    except Exception as e:
        print(f"[ERROR] Failed to load iframe: {e}")
        return None

# ===========================================================================
# SCRAPING FUNCTIONS
# ===========================================================================

def collect_all_properties() -> List[dict]:
    """Sammle alle Immobilien von der Website"""
    
    # Schritt 1: Sammle Links zu Detailseiten
    detail_links = collect_detail_page_links()
    
    if not detail_links:
        print("[WARN] Keine Detailseiten gefunden!")
        print("[INFO] Prüfe ob die Website-Struktur sich geändert hat")
        return []
    
    # Schritt 2: Für jede Detailseite, extrahiere iframe und hole Daten
    all_properties = []
    
    for i, detail_url in enumerate(detail_links, 1):
        print(f"\n[SCRAPE] {i}/{len(detail_links)}")
        
        try:
            # Finde iframe auf der Detailseite
            iframe_url = extract_iframe_from_detail_page(detail_url)
            
            if not iframe_url:
                print(f"  ⚠️  Überspringe - kein iframe gefunden")
                continue
            
            # Lade Daten aus dem iframe
            prop_data = get_propstack_property_data_from_iframe(iframe_url)
            
            if prop_data:
                # Extrahiere Objektnummer aus iframe-URL
                import re
                match = re.search(r"(eyJ[A-Za-z0-9+/=]+)", iframe_url)
                if match:
                    token_b64 = match.group(1)
                    try:
                        # Dekodiere Token
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
                
                # URL der Detailseite
                prop_data["url"] = detail_url
                
                all_properties.append(prop_data)
                
                # Zeige Vorschau
                print(f"  → {prop_data.get('kategorie', 'N/A'):8} | {prop_data.get('titel', 'Unbekannt')[:50]} | {prop_data.get('ort', 'N/A')} | Preis: {prop_data.get('preis', 'N/A')}")
            else:
                print(f"  ⚠️  Keine Daten extrahiert")
                
        except Exception as e:
            print(f"  ❌ Fehler: {e}")
            continue
    
    return all_properties

def make_record(prop: dict) -> dict:
    """Erstelle Airtable-Record"""
    # Konvertiere Preis
    preis_value = None
    if prop.get("preis"):
        try:
            clean = prop["preis"].replace("€", "").replace(".", "").replace(",", ".").strip()
            preis_value = float(clean)
        except:
            pass
    
    record = {
        "Titel": prop.get("titel", "Unbekannt"),
        "Kategorie": prop.get("kategorie", "Kaufen"),
        "Webseite": prop.get("url", ""),
        "Objektnummer": prop.get("objektnummer", ""),
        "Beschreibung": prop.get("beschreibung", ""),
        "Bild": prop.get("bild_url", ""),
        "Standort": prop.get("ort", ""),
    }
    
    if preis_value is not None:
        record["Preis"] = preis_value
    
    return record

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

def unique_key(fields: dict) -> str:
    obj = (fields.get("Objektnummer") or "").strip()
    if obj:
        return f"obj:{obj}"
    url = (fields.get("Webseite") or "").strip()
    if url:
        return f"url:{url}"
    return f"hash:{hash(json.dumps(fields, sort_keys=True))}"

def sanitize_record_for_airtable(record: dict, allowed_fields: set) -> dict:
    if not allowed_fields:
        return record
    return {k: v for k, v in record.items() if k in allowed_fields}

def airtable_existing_fields() -> set:
    _, all_fields = airtable_list_all()
    if not all_fields:
        return set()
    return set(all_fields[0].keys())

# ===========================================================================
# MAIN
# ===========================================================================

def run():
    print("[REINICKE] Starte Scraper für alainreinickeimmobilien.de (Propstack)")
    
    # Sammle alle Immobilien
    all_properties = collect_all_properties()
    
    if not all_properties:
        print("[WARN] Keine Immobilien gefunden!")
        return
    
    # Konvertiere zu Airtable Records
    all_rows = [make_record(prop) for prop in all_properties]
    
    # CSV speichern
    csv_file = "reinicke_immobilien.csv"
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
