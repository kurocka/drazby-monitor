"""
Playwright-based scrapers for:
1. Obchodny Vestnik (justice.gov.sk) - auction chapters with full detail scraping
2. Drazby.sk - enriching the 18 free-tier auctions from rendered SPA
"""
import json
import logging
import re
import time
from datetime import datetime, timedelta
from models import get_db, upsert_auction

logger = logging.getLogger(__name__)

DRAZBY_BASE = "https://www.drazby.sk"
OV_BASE = "https://obchodnyvestnik.justice.gov.sk/ObchodnyVestnik"

REGION_MAP = {
    "BA": "Bratislavský", "TT": "Trnavský", "TN": "Trenčiansky",
    "NR": "Nitriansky", "ZA": "Žilinský", "BB": "Banskobystrický",
    "PO": "Prešovský", "KE": "Košický"
}

# OV chapter codes for auctions
OV_AUCTION_CHAPTERS = [
    "OV_D",       # Dražby – dobrovoľní dražobníci
    "OV_D_SD",    # Dražby – správcovia dane
    "OV_Ex",      # Exekúcie – súdni exekútori
    "OV_PM",      # Predaj majetku
]


def _get_browser(playwright):
    """Create a headless Chromium browser."""
    return playwright.chromium.launch(
        headless=True,
        args=['--no-sandbox', '--disable-dev-shm-usage']
    )


# ─────────────────────────────────────────────
# OV (Obchodný vestník) Playwright scraper
# ─────────────────────────────────────────────

def sync_ov_playwright():
    """
    Scrape Obchodny Vestnik auction announcements via Playwright.
    Uses the search form to filter by auction chapters, then scrapes detail pages.
    """
    from playwright.sync_api import sync_playwright

    conn = get_db()
    log_id = conn.execute(
        "INSERT INTO sync_log (source, sync_type) VALUES ('obchodny_vestnik', 'playwright')"
    ).lastrowid
    conn.commit()

    total_fetched = 0
    total_new = 0

    try:
        with sync_playwright() as p:
            browser = _get_browser(p)
            context = browser.new_context(
                viewport={'width': 1280, 'height': 900},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = context.new_page()

            for chapter in OV_AUCTION_CHAPTERS:
                logger.info(f"Scraping OV chapter: {chapter}")
                try:
                    result = _scrape_ov_chapter(page, conn, chapter)
                    total_fetched += result['fetched']
                    total_new += result['new']
                    logger.info(f"  {chapter}: fetched={result['fetched']}, new={result['new']}")
                except Exception as e:
                    logger.error(f"Error scraping OV chapter {chapter}: {e}")

            browser.close()

        conn.execute("""
            UPDATE sync_log SET finished_at=datetime('now'), records_fetched=?,
            records_new=?, status='success' WHERE id=?
        """, (total_fetched, total_new, log_id))
        conn.commit()
        logger.info(f"OV Playwright sync complete: fetched={total_fetched}, new={total_new}")

    except Exception as e:
        conn.execute("""
            UPDATE sync_log SET finished_at=datetime('now'), status='error',
            error=? WHERE id=?
        """, (str(e)[:500], log_id))
        conn.commit()
        logger.error(f"OV Playwright sync error: {e}")
        raise
    finally:
        conn.close()

    return total_fetched, total_new


def _scrape_ov_chapter(page, conn, chapter_code):
    """Scrape all announcements from one OV chapter for current year."""
    fetched = 0
    new = 0

    # Navigate to search page
    page.goto(f"{OV_BASE}/Formular/FormulareZverejnene.aspx", timeout=30000, wait_until="networkidle")
    page.wait_for_timeout(2000)

    # Select chapter and year
    page.select_option('select[name*="ddlKapitola"]', value=chapter_code)
    page.select_option('select[name*="ddlRocnik"]', value=str(datetime.now().year))

    # Click search
    page.click('input[value*="Vyhľadať"]')
    page.wait_for_timeout(5000)

    # Set 100 results per page
    try:
        count_sel = page.query_selector('select[name*="CountOnPage"]')
        if count_sel:
            count_sel.select_option(value='100')
            page.wait_for_timeout(5000)
    except Exception:
        pass

    # Process all pages
    max_pages = 20  # Safety limit
    current_page = 0

    while current_page < max_pages:
        current_page += 1

        # Get FormularDetail links and their row data
        grid = page.query_selector('[id*="gvFormularZoznam"]')
        if not grid:
            break

        rows = grid.query_selector_all('tr')
        if len(rows) <= 1:  # Only header
            break

        # Extract listing data from grid rows
        listing_items = []
        for row in rows[1:]:  # Skip header
            cells = row.query_selector_all('td')
            if len(cells) < 5:
                continue

            # Find the detail link in this row
            detail_link = row.query_selector('a[href*="FormularDetail.aspx?IdFormular"]')
            if not detail_link:
                continue

            href = detail_link.get_attribute('href') or ''
            id_match = re.search(r'IdFormular=(\d+)', href)
            if not id_match:
                continue

            form_id = id_match.group(1)

            # Check if we already have this one with good data
            existing = conn.execute(
                "SELECT id, description FROM auctions WHERE id=?", (f"ov_{form_id}",)
            ).fetchone()
            if existing and existing['description'] and len(existing['description']) > 100:
                fetched += 1
                continue

            # Extract basic info from grid row
            row_text = row.inner_text()
            parts = row_text.split('\t')
            typ_podania = parts[1].strip() if len(parts) > 1 else ""
            datum = parts[2].strip() if len(parts) > 2 else ""
            kapitola = parts[3].strip() if len(parts) > 3 else ""
            subjekt = parts[4].strip() if len(parts) > 4 else ""

            # Skip non-auction filings (notifications, process documents)
            typ_lower = typ_podania.lower()
            skip_types = ['upovedomeni', 'výzv', 'uzneseni', 'rozhodnut', 'zápisnic']
            if any(st in typ_lower for st in skip_types):
                fetched += 1
                continue

            listing_items.append({
                'form_id': form_id,
                'typ_podania': typ_podania,
                'datum': datum,
                'kapitola': kapitola,
                'subjekt': subjekt
            })

        logger.info(f"  Page {current_page}: {len(listing_items)} items to process")

        # Scrape detail pages for items we don't have yet
        for item in listing_items:
            try:
                auction = _scrape_ov_detail(page, item)
                if auction:
                    upsert_auction(conn, auction)
                    new += 1
                fetched += 1
            except Exception as e:
                logger.debug(f"Error scraping OV detail {item['form_id']}: {e}")
                fetched += 1

        conn.commit()

        # Navigate back to search results and go to next page
        page.goto(f"{OV_BASE}/Formular/FormulareZverejnene.aspx", timeout=30000, wait_until="networkidle")
        page.wait_for_timeout(2000)

        # Re-apply search filters
        page.select_option('select[name*="ddlKapitola"]', value=chapter_code)
        page.select_option('select[name*="ddlRocnik"]', value=str(datetime.now().year))
        page.click('input[value*="Vyhľadať"]')
        page.wait_for_timeout(5000)

        # Set 100 per page
        try:
            count_sel = page.query_selector('select[name*="CountOnPage"]')
            if count_sel:
                count_sel.select_option(value='100')
                page.wait_for_timeout(5000)
        except Exception:
            pass

        # Navigate to next page
        if not _ov_next_page(page, current_page):
            break

    return {'fetched': fetched, 'new': new}


def _ov_next_page(page, current_page):
    """Click next page in OV search results. Returns True if successful."""
    try:
        # The pager uses __doPostBack for navigation
        next_page_num = current_page + 1
        pager = page.query_selector(f'a[href*="Page${next_page_num}"]')
        if not pager:
            # Try finding the next page link by text
            pager_links = page.query_selector_all('[id*="Pager"] a')
            for link in pager_links:
                text = link.inner_text().strip()
                if text == str(next_page_num):
                    pager = link
                    break

        if pager:
            pager.click()
            page.wait_for_timeout(5000)
            return True
    except Exception as e:
        logger.debug(f"Next page navigation failed: {e}")

    return False


def _scrape_ov_detail(page, item):
    """Scrape an individual OV detail page and parse auction data."""
    form_id = item['form_id']
    detail_url = f"{OV_BASE}/Formular/FormularDetailHtml.aspx?IdFormular={form_id}"

    # Open detail in same page (we'll go back to listing after)
    page.goto(detail_url, timeout=20000, wait_until="domcontentloaded")
    page.wait_for_timeout(1500)

    body_text = page.inner_text('body')
    if not body_text or len(body_text) < 50:
        return None

    # Normalize: replace non-breaking spaces (\xa0) and clean up whitespace
    body_text = body_text.replace('\xa0', ' ').replace('\u00a0', ' ')

    # ── Parse structured fields from the OV detail ──

    # Title
    title_parts = []
    if item.get('typ_podania'):
        title_parts.append(item['typ_podania'])
    if item.get('subjekt'):
        title_parts.append(f"| {item['subjekt']}")
    title = " ".join(title_parts) if title_parts else f"OV #{form_id}"
    title = title[:200]

    # Extract auction date (Dátum konania dražby)
    auction_date = ""
    date_patterns = [
        r'[Dd]átum\s+konania\s+dražby[:\s]*(\d{1,2})\.\s*(\d{1,2})\.\s*(20\d{2})',
        r'[Dd]átum\s+dražby[:\s]*(\d{1,2})\.\s*(\d{1,2})\.\s*(20\d{2})',
        r'[Dd]eň\s+konania\s+dražby[:\s]*(\d{1,2})\.\s*(\d{1,2})\.\s*(20\d{2})',
        r'(\d{1,2})\.\s*(\d{1,2})\.\s*(20\d{2})',
    ]
    for pattern in date_patterns:
        match = re.search(pattern, body_text)
        if match:
            try:
                d, m, y = match.groups()
                auction_date = f"{y}-{int(m):02d}-{int(d):02d}"
                break
            except (ValueError, IndexError):
                pass

    # Extract price (Najnižšie podanie / Vyvolávacia cena)
    # Priority: specific auction price labels first, then generic EUR amounts
    price = None
    price_patterns = [
        # Primary: "podanie" followed by price (handles Najnižšie with encoding issues)
        r'podanie[\s\t:]+([\d][\d\s]*[.,]\d{2})\s*EUR',
        r'podanie[\s\t:]+([\d][\d\s]*[.,]\d{2})',
        # Secondary: "cena" followed by price
        r'cena[\s\t:]+([\d][\d\s]*[.,]\d{2})\s*EUR',
        r'cena[\s\t:]+([\d][\d\s]*[.,]\d{2})',
        # Tertiary: "hodnota" followed by price
        r'hodnot[ay][\s\t:]+([\d][\d\s]*[.,]\d{2})\s*EUR',
    ]
    for pattern in price_patterns:
        match = re.search(pattern, body_text)
        if match:
            try:
                price_str = match.group(1).replace(" ", "").replace("\t", "").replace(",", ".")
                price = float(price_str)
                if price < 100:  # Auction prices should be at least 100 EUR
                    price = None
                    continue
            except ValueError:
                continue
            break

    # ── Extract PROPERTY location (not auctioneer address) ──
    city, district, region, street = _extract_property_location(body_text)

    # Subject type from text (with vehicle detection)
    subject_type = _detect_subject_type(body_text)

    # Skip non-real-estate auctions (vehicles, movables only)
    if subject_type in ("Vozidlo", "Hnuteľný majetok"):
        logger.debug(f"Skipping non-real-estate auction {form_id}: {subject_type}")
        return None

    # Extract predmet drazby section for description
    description = _extract_description(body_text, item)

    # Address
    address = ", ".join(filter(None, [street, city]))

    # Determine status from typ_podania
    status = "planned"
    typ_lower = (item.get('typ_podania') or '').lower()
    if 'výsledk' in typ_lower:
        status = "completed"
    elif 'upusten' in typ_lower or 'zrušen' in typ_lower:
        status = "cancelled"
    elif 'opakovan' in typ_lower:
        status = "planned"  # repeated auction = still planned

    return {
        "id": f"ov_{form_id}",
        "source": "obchodny_vestnik",
        "title": title,
        "subject_type": subject_type,
        "subject_subtype": item.get('typ_podania', ''),
        "region": region,
        "district": district,
        "city": city,
        "address": address,
        "auction_date": auction_date,
        "price": price,
        "currency": "EUR",
        "status": status,
        "description": description,
        "url": f"{OV_BASE}/Formular/FormularDetailHtml.aspx?IdFormular={form_id}",
        "lat": None,
        "lon": None,
        "raw_data": json.dumps({
            "source": "playwright_ov",
            "chapter": item.get('kapitola', ''),
            "typ_podania": item.get('typ_podania', ''),
            "subjekt": item.get('subjekt', ''),
            "datum_zverejnenia": item.get('datum', ''),
        }, ensure_ascii=False)
    }


def _extract_property_location(body_text):
    """
    Extract property location from OV detail page.
    The OV pages have TWO address blocks:
    1. Auctioneer/executor address (early in the page, under Sídlo)
    2. Property address (later, under Predmet dražby, with katastrálny odbor / LV references)
    We want #2.
    """
    city = ""
    district = ""
    region = ""
    street = ""

    # Strategy 1: Find location from the property description section
    # Look for "katastrálny odbor" or "LV č." context which describes property location
    # Pattern: "okres DISTRICT, obec CITY" or "obec CITY, okres DISTRICT"
    property_section = ""
    for marker in ['Predmet dražby', 'predmet dražby', 'PREDMET', 'predmetom']:
        idx = body_text.lower().find(marker.lower())
        if idx >= 0:
            property_section = body_text[idx:idx + 5000]
            break

    if property_section:
        # Primary pattern: "okres: DISTRICT, obec: CITY, katastrálne územie: KU"
        m = re.search(r'okres[:\s]+([^,\n]+?),\s*obec[:\s]+([^,\n]+?)(?:,\s*katastr|\s*$|\n)', property_section, re.IGNORECASE)
        if m:
            district = m.group(1).strip()[:80]
            city = m.group(2).strip()[:80]

        # Alt pattern: "obec CITY, okres DISTRICT"
        if not city:
            m = re.search(r'obec[:\s]+([A-ZÁ-Ža-zá-ž\s-]+?)(?:\s*,\s*okres[:\s]+([A-ZÁ-Ža-zá-ž\s-]+?))?(?:\s*,|\s*katastr|\s*\n|\s*$)', property_section, re.IGNORECASE)
            if m:
                candidate = m.group(1).strip()
                # Reject table headers
                if candidate.lower() not in ('', 'katastrálne územie', 'katastrálne', 'územie'):
                    city = candidate[:80]
                if m.group(2):
                    district = m.group(2).strip()[:80]

        # Alt pattern: "okres DISTRICT" alone
        if not district:
            m = re.search(r'okres[:\s]+([A-ZÁ-Ža-zá-ž\s-]{3,40})(?:\s*,|\s*obec|\s*katastr|\s*\n|\s*zapís)', property_section, re.IGNORECASE)
            if m:
                district = m.group(1).strip()[:80]

        # Try: "k.ú. KATASTER, obec CITY, okres DISTRICT"
        if not city:
            m = re.search(r'k\.ú\.\s*([^,]+),\s*obec[:\s]+([^,]+),\s*okres[:\s]+([^\n,]+)', property_section)
            if m:
                city = m.group(2).strip()[:80]
                if not district:
                    district = m.group(3).strip()[:80]

        # Table format: "Okres || Obec || Katastrálne územie" followed by values
        if not city:
            m = re.search(r'Okres[^\n]*Obec[^\n]*Katastr[^\n]*\n+([^\n]+)', property_section)
            if m:
                values_line = m.group(1)
                parts = re.split(r'\t+|\|\|', values_line)
                parts = [p.strip() for p in parts if p.strip() and p.strip() not in ('', '>', '|') and not p.strip().isdigit()]
                # Usually format: [OKRES_URAD, OKRES, OBEC, KAT_UZEMIE] or [OKRES, OBEC, KAT_UZEMIE]
                if len(parts) >= 3:
                    # Take the 2nd-last and 3rd-last as likely obec and okres
                    city = parts[-2][:80] if len(parts[-2]) > 2 else ""
                    if not district:
                        district = parts[-3][:80] if len(parts) > 3 and len(parts[-3]) > 2 else ""
                elif len(parts) >= 2:
                    city = parts[-1][:80]

    # Strategy 2: Fallback — look for "Názov obce" specifically in property section
    if not city:
        # Skip the first "Názov obce" (auctioneer) and look for the property one
        all_city_matches = list(re.finditer(r'zov obce[:\s\t]+([^\n\t]+)', body_text, re.IGNORECASE))
        if len(all_city_matches) >= 2:
            # Use the LAST occurrence (more likely to be property)
            city = all_city_matches[-1].group(1).strip()[:80]
        elif len(all_city_matches) == 1 and property_section:
            # Only use the single match if it's in the property section
            match_pos = all_city_matches[0].start()
            prop_start = body_text.lower().find('predmet')
            if prop_start >= 0 and match_pos > prop_start:
                city = all_city_matches[0].group(1).strip()[:80]

    # Strategy 3: For executor auctions, look for property address after "Nehnuteľnosti"
    if not city:
        m = re.search(r'[Nn]ehnuteľnost[ií][^\n]*obec\s+([A-ZÁ-Ža-zá-ž\s-]+?)(?:\s*,|\s*okres)', body_text)
        if m:
            city = m.group(1).strip()[:80]

    # Clean up district
    if district:
        district = re.sub(r'\s+(a to|a|zapís|evidovan|katastr|na LV).*', '', district, flags=re.IGNORECASE).strip()
        if len(district) > 60:
            district = district[:60]

    # Clean up city - reject bad values
    bad_values = ['katastrálne územie', 'katastrálne', 'územie', 'okresný úrad',
                  'parcelné číslo', 'register', 'evidované', 'mape', 'parcely',
                  'obec', 'okres', 'druh pozemku', 'výmera', 'spoluvlastnícky']
    if city.lower().strip() in bad_values or len(city) < 2:
        city = ""
    if district.lower().strip() in bad_values or len(district) < 2:
        district = ""

    # Region detection
    text_lower = body_text.lower()
    for rcode, rname in REGION_MAP.items():
        if rname.lower() in text_lower:
            region = rname
            break
    # Also try "XY kraj" pattern
    if not region:
        m = re.search(r'(\w+)\s+kraj', body_text)
        if m:
            candidate = m.group(1).strip()
            for rname in REGION_MAP.values():
                if candidate.lower() in rname.lower():
                    region = rname
                    break

    return city, district, region, street


def _detect_subject_type(text):
    """Detect property type from OV detail text. Also detects non-real-estate items."""
    text_lower = text.lower()

    # First check for non-real-estate items to filter out
    vehicle_keywords = ['vozidlo', 'automobil', 'motocyk', 'prívesn', 'nákladn', 'autobus',
                        'vin:', 'ečv:', 'evidenčné číslo', 'toyota', 'volkswagen', 'škoda',
                        'bmw', 'audi', 'mercedes', 'ford', 'hyundai', 'kia', 'peugeot',
                        'renault', 'citroën', 'opel', 'fiat', 'honda', 'nissan', 'mazda']

    # Check in "predmet dražby" section specifically
    predmet_idx = text_lower.find('predmet')
    predmet_section = text_lower[predmet_idx:predmet_idx+2000] if predmet_idx >= 0 else text_lower[:3000]

    if any(kw in predmet_section for kw in vehicle_keywords):
        # Check if there's ALSO real estate
        has_realestate = any(kw in predmet_section for kw in ['nehnuteľnost', 'pozemok', 'parcela', 'dom', 'byt', 'stavba'])
        if not has_realestate:
            return "Vozidlo"

    # Check for pure movable property
    movable_only_patterns = [
        r'predmetom\s+dražby\s+(?:je|sú)\s+(?:hnuteľn|strojov|zariadeni|technológi)',
    ]
    for pattern in movable_only_patterns:
        if re.search(pattern, predmet_section):
            has_realestate = any(kw in predmet_section for kw in ['nehnuteľnost', 'pozemok', 'parcela', 'dom', 'byt', 'stavba'])
            if not has_realestate:
                return "Hnuteľný majetok"

    # Real estate types
    if "rodinný dom" in text_lower:
        return "Rodinný dom"
    elif re.search(r'\bbyt\b', text_lower) or "bytov" in text_lower:
        return "Byt"
    elif "pozemok" in text_lower or "pozemk" in text_lower or "orná pôda" in text_lower:
        return "Pozemok"
    elif re.search(r'parcela\b', text_lower) and 'nehnuteľnost' in text_lower:
        return "Pozemok"
    elif "nebytov" in text_lower:
        return "Nebytový priestor"
    elif re.search(r'\bdom\b', text_lower):
        return "Dom"
    elif "garáž" in text_lower:
        return "Garáž"
    elif any(kw in text_lower for kw in ["priemysel", "výrobn", "sklad", "hala", "administrat"]):
        return "Komerčný objekt"
    elif "hotel" in text_lower or "penzión" in text_lower:
        return "Komerčný objekt"
    elif "nehnuteľnost" in text_lower or "stavba" in text_lower:
        return "Nehnuteľnosť"
    return "Neurčené"


def _extract_description(body_text, item):
    """Extract meaningful description from OV detail page."""
    parts = []

    # Add chapter and type info
    if item.get('kapitola'):
        parts.append(f"Kapitola: {item['kapitola']}")
    if item.get('typ_podania'):
        parts.append(f"Typ: {item['typ_podania']}")
    if item.get('subjekt'):
        parts.append(f"Dražobník: {item['subjekt']}")

    # Extract key sections
    sections_to_extract = [
        (r'[Pp]redmet\s+dražby(.*?)(?=[A-Z]\.|Označenie|Miesto konania|Najnižšie podanie|Vyvolávacia cena|Znaleck[áý]|$)', "Predmet dražby"),
        (r'[Mm]iesto\s+konania\s+dražby[:\s]+(.*?)(?=\n[A-Z]|\n\d+\.|\t[A-Z])', "Miesto konania"),
        (r'[Nn]ajnižš[eí]\s+podanie[:\s]+(.*?)(?=\n|$)', "Najnižšie podanie"),
        (r'[Vv]yvolávacia\s+cena[:\s]+(.*?)(?=\n|$)', "Vyvolávacia cena"),
        (r'[Zz]naleck[áý]\s+hodnot[ay][:\s]+(.*?)(?=\n|$)', "Znalecká hodnota"),
    ]

    for pattern, label in sections_to_extract:
        match = re.search(pattern, body_text, re.DOTALL)
        if match:
            text = match.group(1).strip()
            # Clean excessive whitespace
            text = re.sub(r'\s+', ' ', text)
            if text and len(text) > 5:
                parts.append(f"\n{label}: {text[:500]}")

    # If we didn't get much, use a chunk of the body text
    if len(parts) < 4:
        # Get the main content, skip navigation
        content_start = body_text.find('Oznámenie o')
        if content_start == -1:
            content_start = body_text.find('Dražby')
        if content_start > 0:
            content = body_text[content_start:content_start + 2000]
            content = re.sub(r'\s+', ' ', content)
            parts.append(f"\n{content[:1500]}")

    return "\n".join(parts)[:3000]


# ─────────────────────────────────────────────
# Drazby.sk Playwright scraper
# ─────────────────────────────────────────────

def sync_drazby_playwright():
    """
    Scrape drazby.sk SPA using Playwright.
    Gets the 18 free-tier auctions and enriches them with detail page data.
    """
    from playwright.sync_api import sync_playwright

    conn = get_db()
    log_id = conn.execute(
        "INSERT INTO sync_log (source, sync_type) VALUES ('drazby.sk', 'playwright')"
    ).lastrowid
    conn.commit()

    total_fetched = 0
    total_new = 0

    try:
        with sync_playwright() as p:
            browser = _get_browser(p)
            context = browser.new_context(
                viewport={'width': 1280, 'height': 900},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = context.new_page()

            # Capture API responses
            api_rows = []

            def handle_response(response):
                if "auction_www_list" in response.url:
                    try:
                        data = response.json()
                        rows = data.get("resp", {}).get("datatable", {}).get("rows", [])
                        api_rows.extend(rows)
                    except Exception:
                        pass

            page.on("response", handle_response)

            # Load homepage to trigger API call
            page.goto(DRAZBY_BASE, timeout=30000, wait_until="networkidle")
            page.wait_for_timeout(4000)

            # Also grab auction links from rendered DOM
            dom_links = page.query_selector_all('a[href*="/drazba/"]')
            dom_auctions = {}
            for link in dom_links:
                href = link.get_attribute('href') or ''
                match = re.search(r'/drazba/([a-zA-Z0-9_-]+)', href)
                if match:
                    aid = match.group(1)
                    text = link.inner_text().strip()
                    if aid not in dom_auctions and text:
                        dom_auctions[aid] = text

            logger.info(f"Captured {len(api_rows)} API rows, {len(dom_auctions)} DOM auctions")

            # Process API rows
            for row in api_rows:
                auction = _parse_drazby_api_row(row)
                if auction:
                    upsert_auction(conn, auction)
                    total_new += 1
                    total_fetched += 1

            # Process DOM auctions not in API
            api_ids = {r.get('id') for r in api_rows if r.get('id')}
            for aid, text in dom_auctions.items():
                if aid not in api_ids:
                    auction = _parse_drazby_dom_auction(aid, text)
                    if auction:
                        upsert_auction(conn, auction)
                        total_new += 1
                        total_fetched += 1

            conn.commit()

            # Enrich auctions from detail pages
            enriched = 0
            for aid in list(dom_auctions.keys())[:30]:
                try:
                    detail = _scrape_drazby_detail(page, aid)
                    if detail:
                        conn.execute("""
                            UPDATE auctions SET
                                description=COALESCE(NULLIF(?, ''), description),
                                city=COALESCE(NULLIF(?, ''), city),
                                district=COALESCE(NULLIF(?, ''), district),
                                region=COALESCE(NULLIF(?, ''), region),
                                address=COALESCE(NULLIF(?, ''), address),
                                updated_at=datetime('now')
                            WHERE id=?
                        """, (
                            detail.get('description', ''),
                            detail.get('city', ''),
                            detail.get('district', ''),
                            detail.get('region', ''),
                            detail.get('address', ''),
                            f"drazby_{aid}"
                        ))
                        enriched += 1
                except Exception as e:
                    logger.debug(f"Detail enrichment failed for {aid}: {e}")

            conn.commit()
            browser.close()

        conn.execute("""
            UPDATE sync_log SET finished_at=datetime('now'), records_fetched=?,
            records_new=?, status='success' WHERE id=?
        """, (total_fetched, total_new, log_id))
        conn.commit()
        logger.info(f"Drazby.sk Playwright sync: fetched={total_fetched}, new={total_new}, enriched={enriched}")

    except Exception as e:
        conn.execute("""
            UPDATE sync_log SET finished_at=datetime('now'), status='error',
            error=? WHERE id=?
        """, (str(e)[:500], log_id))
        conn.commit()
        logger.error(f"Drazby.sk Playwright sync error: {e}")
        raise
    finally:
        conn.close()

    return total_fetched, total_new


def _parse_drazby_api_row(row):
    """Parse auction from intercepted drazby.sk API response."""
    if not row:
        return None

    auction_id = row.get("id")
    if not auction_id:
        return None

    val = row.get("value", {})
    detail = val.get("detail", {})
    subject = val.get("subject", {})
    basic = val.get("basic", {})
    status = val.get("status", "")

    items = subject.get("items", {})
    first_item = next(iter(items.values()), {}) if items else {}
    address_info = first_item.get("address", {})

    subject_type = first_item.get("id_auction_subject_type", "")
    subject_subtype = first_item.get("id_auction_subject_subtype", "")

    region_code = address_info.get("region_code", "")
    region = REGION_MAP.get(region_code, address_info.get("region", ""))
    district = address_info.get("district", "")
    city = address_info.get("city", "")
    street = address_info.get("street")
    address = ", ".join(filter(None, [street, city]))

    price_info = detail.get("price", {})
    price = price_info.get("min_amount") or price_info.get("amount")

    auction_date = detail.get("auction_date", "")
    if auction_date and "T" in str(auction_date):
        auction_date = str(auction_date).split("T")[0]

    pin = address_info.get("pin", {})
    lat = _safe_float(pin.get("latitude"))
    lon = _safe_float(pin.get("longitude"))

    round_info = basic.get("round", "")
    title_parts = [subject_type, subject_subtype, "-", city or district]
    if round_info:
        title_parts.append(f"({round_info})")
    title = " ".join(filter(None, title_parts))

    desc_parts = []
    parts = first_item.get("parts", {}).get("children", [])
    for group in parts:
        for child in group.get("children", []):
            name = child.get("name", "")
            size = child.get("size", "")
            lot_num = child.get("landlot_number", "")
            if name and name != "-":
                line = name
                if size and size != "-":
                    line += f" ({size} m²)"
                if lot_num and lot_num != "-":
                    line += f" - parcela {lot_num}"
                desc_parts.append(line)

    return {
        "id": f"drazby_{auction_id}",
        "source": "drazby.sk",
        "title": title,
        "subject_type": subject_type,
        "subject_subtype": subject_subtype,
        "region": region,
        "district": district,
        "city": city,
        "address": address,
        "auction_date": auction_date,
        "price": price,
        "currency": "EUR",
        "status": status if isinstance(status, str) else "",
        "description": "\n".join(desc_parts),
        "url": f"{DRAZBY_BASE}/drazba/{auction_id}",
        "lat": lat,
        "lon": lon,
        "raw_data": json.dumps(val, ensure_ascii=False, default=str)
    }


def _parse_drazby_dom_auction(auction_id, text):
    """Parse auction from drazby.sk rendered DOM text."""
    lines = text.split('\n')
    title = lines[0][:200] if lines else f"Dražba {auction_id}"

    # Extract price
    price = None
    price_match = re.search(r'([\d\s]+[.,]\d{2})\s*€', text)
    if price_match:
        try:
            price = float(price_match.group(1).replace(" ", "").replace(",", "."))
        except (ValueError, TypeError):
            pass

    # Extract date
    auction_date = ""
    date_match = re.search(r'(\d{1,2})\.\s*(\d{1,2})\.\s*(20\d{2})', text)
    if date_match:
        try:
            d, m, y = date_match.groups()
            auction_date = f"{y}-{int(m):02d}-{int(d):02d}"
        except (ValueError, IndexError):
            pass

    subject_type = _detect_subject_type(text)

    return {
        "id": f"drazby_{auction_id}",
        "source": "drazby.sk",
        "title": title,
        "subject_type": subject_type,
        "subject_subtype": "",
        "region": "",
        "district": "",
        "city": "",
        "address": "",
        "auction_date": auction_date,
        "price": price,
        "currency": "EUR",
        "status": "current",
        "description": text[:2000],
        "url": f"{DRAZBY_BASE}/drazba/{auction_id}",
        "lat": None,
        "lon": None,
        "raw_data": json.dumps({"dom_text": text[:3000]}, ensure_ascii=False)
    }


def _scrape_drazby_detail(page, auction_id):
    """Scrape a single drazby.sk detail page for additional data."""
    url = f"{DRAZBY_BASE}/drazba/{auction_id}"
    page.goto(url, timeout=20000, wait_until="networkidle")
    page.wait_for_timeout(2000)

    body_text = page.inner_text('body')
    if not body_text or len(body_text) < 100:
        return None

    result = {}

    # Extract city from address section
    addr_match = re.search(r'ADRESA\s+NEHNUTEĽNOSTI\s*(.*?)(?=POPIS|ROZLOHA|OTVORIŤ)', body_text, re.DOTALL)
    if addr_match:
        addr_block = addr_match.group(1).strip()
        lines = [l.strip() for l in addr_block.split('\n') if l.strip()]
        if lines:
            # Usually format: "PSC City" then "okres X, Y kraj"
            for line in lines:
                district_match = re.search(r'okres\s+([^,]+)', line)
                if district_match:
                    result['district'] = district_match.group(1).strip()
                region_match = re.search(r'(\w+)\s+kraj', line)
                if region_match:
                    result['region'] = region_match.group(1).strip() + " kraj" if "ský" not in region_match.group(1) else region_match.group(1).strip()
                # City is usually the line with PSC
                psc_match = re.search(r'\d{3}\s*\d{2}\s+(.+)', line)
                if psc_match:
                    result['city'] = psc_match.group(1).strip()

        result['address'] = addr_block.replace('\n', ', ')[:200]

    # Description from POPIS section
    desc_match = re.search(r'POPIS\s+NEHNUTEĽNOSTI\s*(.*?)(?=ROZLOHA|INFORMÁCIE|DÁTUM)', body_text, re.DOTALL)
    if desc_match:
        result['description'] = desc_match.group(1).strip()[:2000]

    return result if result else None


def _safe_float(val):
    """Safely convert to float, returning None for invalid values."""
    if val is None or val == "-" or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
