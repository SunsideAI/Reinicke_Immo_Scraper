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

# SYNC-MODUS
# True  = Lösche ALLES in Airtable und ersetze mit neuen Daten
# False = Update/Create/Delete nur geänderte Records (intelligent)
FULL_REPLACE = os.getenv("FULL_REPLACE", "true").lower() == "true"

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

def collect_detail_page_links_with_categories() -> List[Tuple[str, str, str]]:
    """Sammle Links zu Detailseiten MIT Kategorie/Unterkategorie von Übersichtsseite"""
    print(f"[LIST] Hole {LIST_URL}")
    
    # Wichtig: Encoding explizit setzen
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    r = requests.get(LIST_URL, headers=headers, timeout=30)
    r.encoding = 'utf-8'  # Explizit UTF-8 setzen
    soup = BeautifulSoup(r.text, "lxml")
    
    detail_data = []  # Liste von (url, kategorie, unterkategorie)
    
    # Mapping: Überschrift → (Kategorie, Unterkategorie)
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
        "gewerbeimmobilie": (None, "Gewerbe"),  # Kategorie aus Preis
        "gewerbeimmobilien": (None, "Gewerbe"),
        "mietobjekt": ("Mieten", None),  # Unterkategorie aus Titel
        "mietobjekte": ("Mieten", None),
        "grundstück": ("Kaufen", "Grundstück"),
        "grundstücke": ("Kaufen", "Grundstück"),
        "neubau": ("Kaufen", "Haus"),
    }
    
    # Sammle alle Immobilien-Links
    # NEUE LOGIK: Nehme ALLE internen Links AUSSER Navigations-Seiten
    for a in soup.find_all("a", href=True):
        href = a["href"]
        
        # Nur interne Links
        if not (href.startswith("/") or "alainreinickeimmobilien.de" in href):
            continue
        
        # Mache URL absolut
        full_url = urljoin(BASE, href)
        full_url = full_url.split("#")[0].split("?")[0]
        
        # Dedupliziere
        if any(d[0] == full_url for d in detail_data) or full_url == LIST_URL:
            continue
        
        # BLACKLIST: Überspringe Navigations-Seiten
        path_lower = full_url.lower()
        if any(skip in path_lower for skip in [
            "/startseite",
            "/warum-wir", 
            "/immobilien-ankauf",
            "/immobilienbewertung",
            "/aktuelle-angebote",
            "/kontakt",
            "/impressum",
            "/datenschutz",
            "/agb",
            "/cookie",
            BASE + "/#",
            BASE + "/$"
        ]):
            continue
        
        # Ignoriere root URL
        if full_url == BASE or full_url == BASE + "/":
            continue
        
        # Finde vorherige Überschrift (h2, h3, h4)
        prev_header_text = ""
        for sibling in a.find_all_previous(["h2", "h3", "h4"]):
            prev_header_text = sibling.get_text(strip=True).lower()
            break
        
        # Bestimme Kategorie/Unterkategorie aus Überschrift
        kategorie = "Kaufen"  # Default
        unterkategorie = "Haus"  # Default
        
        if prev_header_text:
            for key, (kat, unterkat) in section_mapping.items():
                if key in prev_header_text:
                    if kat:
                        kategorie = kat
                    if unterkat:
                        unterkategorie = unterkat
                    break
        
        detail_data.append((full_url, kategorie, unterkategorie))
        slug = full_url.split("/")[-1]
        print(f"[DEBUG] {slug[:40]:<40} → {kategorie:8} / {unterkategorie}")
    
    print(f"\n[LIST] Gefunden: {len(detail_data)} Detailseiten")
    return detail_data

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
            "kategorie": "Kaufen",  # Default, wird von Übersichtsseite überschrieben
            "unterkategorie": "Haus",  # Default, wird von Übersichtsseite überschrieben
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
        
        # Unterkategorie aus Titel oder Text extrahieren
        text_content = soup.get_text().lower()
        titel_lower = data["titel"].lower()
        
        # Einfache Kategorien wie auf der Website
        if any(kw in titel_lower or kw in text_content for kw in ["grundstück", "baugrundstück", "bauland"]):
            data["unterkategorie"] = "Grundstück"
        elif any(kw in titel_lower or kw in text_content for kw in ["gewerbe", "halle", "büro", "laden", "praxis"]):
            data["unterkategorie"] = "Gewerbe"
        elif any(kw in titel_lower or kw in text_content for kw in ["wohnung", "etw", "eigentumswohnung"]):
            data["unterkategorie"] = "Wohnung"
        elif any(kw in titel_lower or kw in text_content for kw in [
            "haus", "einfamilienhaus", "efh", "zweifamilienhaus", "2fh", "mehrfamilienhaus", "mfh",
            "doppelhaushälfte", "dhh", "reihenhaus", "villa", "bungalow"
        ]):
            data["unterkategorie"] = "Haus"
        else:
            # Fallback: Versuche aus Titel zu erraten
            data["unterkategorie"] = "Haus"  # Default
        
        # Preis - suche nach Preis-Pattern
        price_matches = RE_PRICE.findall(text_content)
        if price_matches:
            # Nehme den höchsten Preis (wahrscheinlich Kaufpreis)
            prices = []
            for p in price_matches:
                try:
                    clean = p.replace(".", "").replace(",", ".")
                    val = float(clean)
                    if val > 100:  # Filter kleine Zahlen (Zimmeranzahl etc.)
                        prices.append((val, p + " €"))
                except:
                    pass
            if prices:
                # Sortiere und nimm höchsten
                prices.sort(reverse=True)
                data["preis"] = prices[0][1]
        
        # SPEZIALFALL: Mietobjekte - Warmmiete/Kaltmiete explizit suchen
        import re
        miete_pattern = re.compile(r'(?:Warmmiete|Kaltmiete|Miete)\s*[:.]?\s*([\d.,]+)\s*€', re.IGNORECASE)
        miete_matches = miete_pattern.findall(text_content_full)
        
        if miete_matches:
            # Warmmiete bevorzugen, dann Kaltmiete
            for miete_str in miete_matches:
                try:
                    clean = miete_str.replace(".", "").replace(",", ".")
                    val = float(clean)
                    if val > 100:
                        # Formatiere Preis
                        data["preis"] = f"{miete_str.replace('.', '').replace(',', '.')} €"
                        # Setze Kategorie auf Mieten wenn Miete gefunden
                        data["kategorie"] = "Mieten"
                        print(f"[DEBUG] Warmmiete/Kaltmiete gefunden: {data['preis']}")
                        break
                except:
                    pass
        
        # PLZ/Ort - mit mehreren Ansätzen
        text_content_full = soup.get_text()
        
        # Ansatz 1: Standard PLZ-Pattern
        match = RE_PLZ_ORT.search(text_content_full)
        if match:
            data["ort"] = f"{match.group(1)} {match.group(2).strip()}"
        
        # Ansatz 2: Suche in Meta-Tags oder speziellen Feldern
        if not data["ort"]:
            for meta in soup.find_all("meta"):
                content = meta.get("content", "")
                match = RE_PLZ_ORT.search(content)
                if match:
                    data["ort"] = f"{match.group(1)} {match.group(2).strip()}"
                    break
        
        # Ansatz 3: Suche nach "in STADT" Pattern
        if not data["ort"]:
            import re
            match = re.search(r'\bin\s+(\d{5})\s+([A-ZÄÖÜ][a-zäöüß\-]+)', text_content_full)
            if match:
                data["ort"] = f"{match.group(1)} {match.group(2)}"
        
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
        
        # BILD - VERBESSERTE EXTRAKTION
        # Strategie: Mehrere Ansätze ausprobieren bis ein Bild gefunden wird
        
        # Ansatz 1: Suche nach img mit bestimmten Klassen
        for class_name in ["property-image", "object-image", "main-image", "gallery-image", "slider-image"]:
            img = soup.find("img", class_=lambda x: x and class_name in str(x).lower())
            if img:
                src = img.get("src", "")
                if src and not any(skip in src.lower() for skip in ["logo", "icon", "favicon", "placeholder"]):
                    data["bild_url"] = src if src.startswith("http") else urljoin(iframe_url, src)
                    print(f"[DEBUG] Bild gefunden (Klasse: {class_name})")
                    break
        
        # Ansatz 2: Suche in srcset (oft höhere Auflösungen)
        if not data["bild_url"]:
            for img in soup.find_all("img"):
                srcset = img.get("srcset", "")
                if srcset:
                    # Parse srcset und nimm größtes Bild
                    parts = [s.strip().split()[0] for s in srcset.split(",") if s.strip()]
                    if parts:
                        src = parts[-1]  # Größte Auflösung
                        if not any(skip in src.lower() for skip in ["logo", "icon", "favicon"]):
                            data["bild_url"] = src if src.startswith("http") else urljoin(iframe_url, src)
                            print(f"[DEBUG] Bild gefunden (srcset)")
                            break
        
        # Ansatz 3: Erstes großes Bild (width > 200 oder ohne width)
        if not data["bild_url"]:
            for img in soup.find_all("img"):
                src = img.get("src", "")
                alt = img.get("alt", "").lower()
                width = img.get("width", "")
                
                # Ignoriere Logos, Icons
                if any(skip in src.lower() for skip in ["logo", "icon", "favicon"]):
                    continue
                if any(skip in alt for skip in ["logo", "icon"]):
                    continue
                
                # Prüfe Größe
                is_large = True
                if width:
                    try:
                        if int(width) < 200:
                            is_large = False
                    except:
                        pass
                
                if src and is_large:
                    data["bild_url"] = src if src.startswith("http") else urljoin(iframe_url, src)
                    print(f"[DEBUG] Bild gefunden (erstes großes img)")
                    break
        
        # Ansatz 4: Suche in background-image CSS
        if not data["bild_url"]:
            for elem in soup.find_all(style=True):
                style = elem.get("style", "")
                if "background-image" in style:
                    import re
                    match = re.search(r'url\(["\']?([^"\']+)["\']?\)', style)
                    if match:
                        url = match.group(1)
                        if not any(skip in url.lower() for skip in ["logo", "icon"]):
                            data["bild_url"] = url if url.startswith("http") else urljoin(iframe_url, url)
                            print(f"[DEBUG] Bild gefunden (background-image)")
                            break
        
        # Ansatz 5: Einfach das erste img-Tag (Fallback)
        if not data["bild_url"]:
            img = soup.find("img")
            if img:
                src = img.get("src", "")
                if src:
                    data["bild_url"] = src if src.startswith("http") else urljoin(iframe_url, src)
                    print(f"[DEBUG] Bild gefunden (Fallback: erstes img)")
        
        if not data["bild_url"]:
            print(f"[WARN] ⚠️  KEIN Bild gefunden!")
        
        # Kategorie/Unterkategorie werden von Übersichtsseite überschrieben
        # Keine Erkennung mehr hier nötig
        
        return data
        
    except Exception as e:
        print(f"[ERROR] Failed to load iframe: {e}")
        return None

# ===========================================================================
# SCRAPING FUNCTIONS
# ===========================================================================

def collect_all_properties() -> List[dict]:
    """Sammle alle Immobilien von der Website"""
    
    # Schritt 1: Sammle Links zu Detailseiten MIT Kategorie-Info
    detail_data = collect_detail_page_links_with_categories()
    
    if not detail_data:
        print("[WARN] Keine Detailseiten gefunden!")
        print("[INFO] Prüfe ob die Website-Struktur sich geändert hat")
        return []
    
    # Schritt 2: Für jede Detailseite, extrahiere iframe und hole Daten
    all_properties = []
    
    for i, (detail_url, overview_kategorie, overview_unterkategorie) in enumerate(detail_data, 1):
        print(f"\n[SCRAPE] {i}/{len(detail_data)}")
        
        try:
            # Finde iframe auf der Detailseite
            iframe_url = extract_iframe_from_detail_page(detail_url)
            
            if not iframe_url:
                print(f"  ⚠️  Überspringe - kein iframe gefunden")
                continue
            
            # Lade Daten aus dem iframe
            prop_data = get_propstack_property_data_from_iframe(iframe_url)
            
            if prop_data:
                # Überschreibe Kategorie/Unterkategorie mit Daten von Übersichtsseite
                # AUSSER wenn Warmmiete/Kaltmiete im iframe gefunden wurde (dann ist es sicher Mieten)
                if overview_kategorie and prop_data["kategorie"] != "Mieten":
                    prop_data["kategorie"] = overview_kategorie
                elif prop_data["kategorie"] == "Mieten":
                    # Warmmiete wurde gefunden - behalte "Mieten"
                    pass
                    
                if overview_unterkategorie:
                    prop_data["unterkategorie"] = overview_unterkategorie
                
                # Spezialfall: Gewerbe - Kategorie aus Preis ableiten
                if prop_data["unterkategorie"] == "Gewerbe" and not overview_kategorie:
                    # Extrahiere Preis-Wert
                    preis_text = prop_data.get("preis", "")
                    try:
                        clean = preis_text.replace("€", "").replace(".", "").replace(",", ".").strip()
                        preis_val = float(clean)
                        if preis_val < 30000:
                            prop_data["kategorie"] = "Mieten"
                        else:
                            prop_data["kategorie"] = "Kaufen"
                    except:
                        prop_data["kategorie"] = "Kaufen"  # Default
                
                # Spezialfall: Mietobjekte - Unterkategorie aus Titel ableiten
                if prop_data["kategorie"] == "Mieten" and not overview_unterkategorie:
                    titel_lower = prop_data.get("titel", "").lower()
                    if "wohnung" in titel_lower:
                        prop_data["unterkategorie"] = "Wohnung"
                    elif "haus" in titel_lower:
                        prop_data["unterkategorie"] = "Haus"
                    elif "gewerbe" in titel_lower or "büro" in titel_lower:
                        prop_data["unterkategorie"] = "Gewerbe"
                    else:
                        prop_data["unterkategorie"] = "Wohnung"  # Default für Mietobjekte
                
                # Extrahiere Objektnummer aus iframe-URL
                import re
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
                
                # URL der Detailseite
                prop_data["url"] = detail_url
                
                all_properties.append(prop_data)
                
                # Zeige Vorschau
                bild_status = "✅" if prop_data.get("bild_url") else "❌"
                print(f"  → {prop_data.get('kategorie', 'N/A'):8} | {prop_data.get('unterkategorie', 'N/A'):20} | {prop_data.get('titel', 'Unbekannt')[:40]} | Bild: {bild_status}")
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
        "Unterkategorie": prop.get("unterkategorie", "Sonstiges"),
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
    """Hole existierende Felder - DEAKTIVIERT um alle Felder zuzulassen"""
    # WICHTIG: Wenn diese Funktion ein leeres Set zurückgibt,
    # werden ALLE Felder an Airtable gesendet (nicht gefiltert)
    # Dies ist nötig wenn neue Felder (Unterkategorie, Standort) noch leer sind
    return set()  # Leeres Set = keine Filterung

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
    cols = ["Titel", "Kategorie", "Unterkategorie", "Webseite", "Objektnummer", "Beschreibung", "Bild", "Preis", "Standort"]
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(all_rows)
    print(f"\n[CSV] Gespeichert: {csv_file} ({len(all_rows)} Zeilen)")
    
    # Airtable Sync
    if AIRTABLE_TOKEN and AIRTABLE_BASE and airtable_table_segment():
        print("\n[AIRTABLE] Starte Synchronisation...")
        
        if FULL_REPLACE:
            print("[AIRTABLE] Modus: FULL REPLACE - Lösche alles und ersetze")
            
            # Hole alle existierenden Records
            all_ids, all_fields = airtable_list_all()
            
            # Lösche ALLES
            if all_ids:
                print(f"[AIRTABLE] Lösche {len(all_ids)} existierende Records...")
                airtable_batch_delete(all_ids)
            
            # Erstelle ALLES neu
            print(f"[AIRTABLE] Erstelle {len(all_rows)} neue Records...")
            airtable_batch_create(all_rows)
            
            print(f"[AIRTABLE] ✅ Tabelle komplett ersetzt: {len(all_rows)} Records")
            
        else:
            print("[AIRTABLE] Modus: INTELLIGENT SYNC - Update nur Änderungen")
            
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
