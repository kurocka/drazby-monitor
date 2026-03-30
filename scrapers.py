import re
import requests
import json
import logging
import xml.etree.ElementTree as ET
from html import unescape
from datetime import datetime, timedelta
from models import get_db, upsert_auction

logger = logging.getLogger(__name__)

DRAZBY_API = "https://www.drazby.sk/api/www"
DATAHUB_API = "https://datahub.ekosystem.slovensko.digital/api/data"

# Map region codes to names
REGION_MAP = {
    "BA": "Bratislavský", "TT": "Trnavský", "TN": "Trenčiansky",
    "NR": "Nitriansky", "ZA": "Žilinský", "BB": "Banskobystrický",
    "PO": "Prešovský", "KE": "Košický"
}

SUBJECT_TYPE_MAP = {
    1: "Byt", 2: "Dom", 3: "Pozemok", 4: "Nebytový priestor",
    5: "Iný", 6: "Podnik", 7: "Súbor vecí"
}


def sync_drazby_sk():
    """Fetch auctions from drazby.sk API."""
    conn = get_db()
    log_id = conn.execute(
        "INSERT INTO sync_log (source, sync_type) VALUES ('drazby.sk', 'full')"
    ).lastrowid
    conn.commit()

    total_fetched = 0
    total_new = 0

    try:
        for status_group in ["planned", "current"]:
            last_key = None
            last_docid = None
            page = 0

            while page < 50:  # safety limit
                params = {"status_group": status_group, "limit": 50}
                if last_key and last_docid:
                    params["last_key"] = last_key
                    params["last_docid"] = last_docid

                resp = requests.get(
                    f"{DRAZBY_API}/auction_www_list",
                    params=params,
                    timeout=30
                )
                resp.raise_for_status()
                data = resp.json()

                datatable = data.get("resp", {}).get("datatable", {})
                rows = datatable.get("rows", [])
                if not rows:
                    break

                for row in rows:
                    auction = _parse_drazby_auction(row, status_group)
                    if auction:
                        upsert_auction(conn, auction)
                        total_new += 1

                conn.commit()
                total_fetched += len(rows)
                paginator = datatable.get("paginator", {})
                last_key = paginator.get("last_key")
                last_docid = paginator.get("last_docid")
                if not last_key or not paginator.get("active"):
                    break
                page += 1

        conn.execute("""
            UPDATE sync_log SET finished_at=datetime('now'), records_fetched=?,
            records_new=?, status='success' WHERE id=?
        """, (total_fetched, total_new, log_id))
        conn.commit()
        logger.info(f"drazby.sk sync: fetched={total_fetched}, new={total_new}")

    except Exception as e:
        conn.execute("""
            UPDATE sync_log SET finished_at=datetime('now'), status='error',
            error=? WHERE id=?
        """, (str(e), log_id))
        conn.commit()
        logger.error(f"drazby.sk sync error: {e}")
        raise
    finally:
        conn.close()

    return total_fetched, total_new


def _parse_drazby_auction(row, status_group):
    """Parse a drazby.sk auction row into our format."""
    if not row:
        return None

    auction_id = row.get("id")
    if not auction_id:
        return None

    val = row.get("value", {})
    detail = val.get("detail", {})
    subject = val.get("subject", {})
    basic = val.get("basic", {})
    status = val.get("status", status_group)

    # Get the first subject item
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

    # Price
    price_info = detail.get("price", {})
    price = price_info.get("min_amount") or price_info.get("amount")

    # Date
    auction_date = detail.get("auction_date", "")
    if auction_date and "T" in str(auction_date):
        auction_date = str(auction_date).split("T")[0]

    # GPS
    pin = address_info.get("pin", {})
    lat = pin.get("latitude")
    lon = pin.get("longitude")
    if lat == "-":
        lat = None
    if lon == "-":
        lon = None
    if lat:
        try:
            lat = float(lat)
        except (ValueError, TypeError):
            lat = None
    if lon:
        try:
            lon = float(lon)
        except (ValueError, TypeError):
            lon = None

    # Build title
    round_info = basic.get("round", "")
    title_parts = [subject_type, subject_subtype, "-", city or district]
    if round_info:
        title_parts.append(f"({round_info})")
    title = " ".join(filter(None, title_parts))

    # Description from parts
    desc_parts = []
    parts = first_item.get("parts", {}).get("children", [])
    for group in parts:
        group_name = group.get("name", "")
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
    description = "\n".join(desc_parts)

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
        "status": status if isinstance(status, str) else status_group,
        "description": description,
        "url": f"https://www.drazby.sk/drazba/{auction_id}",
        "lat": lat,
        "lon": lon,
        "raw_data": json.dumps(val, ensure_ascii=False, default=str)
    }


def sync_datahub_ov():
    """Fetch recent auction-related OV announcements from Slovensko.Digital DataHub."""
    conn = get_db()
    log_id = conn.execute(
        "INSERT INTO sync_log (source, sync_type) VALUES ('datahub_ov', 'incremental')"
    ).lastrowid
    conn.commit()

    total_fetched = 0
    total_new = 0

    try:
        # Get last sync time or default to 30 days ago
        last_sync = conn.execute("""
            SELECT MAX(finished_at) FROM sync_log
            WHERE source='datahub_ov' AND status='success'
        """).fetchone()[0]

        if last_sync:
            since = last_sync
        else:
            since = (datetime.now() - timedelta(days=30)).isoformat()

        # Fetch raw OV issues and filter for auction-related content
        url = f"{DATAHUB_API}/ov/raw_issues/sync"
        params = {"since": since}

        while url:
            resp = requests.get(url, params=params, timeout=60)
            resp.raise_for_status()
            items = resp.json()

            if not items:
                break

            for item in items:
                auction = _parse_ov_raw_issue(item)
                if auction:
                    upsert_auction(conn, auction)
                    total_new += 1
                total_fetched += 1

            # Follow pagination via Link header
            link_header = resp.headers.get("Link", "")
            url = None
            params = None
            if 'rel="next"' in link_header:
                for part in link_header.split(","):
                    if 'rel="next"' in part:
                        url = part.split(";")[0].strip().strip("<>")
                        break

        conn.execute("""
            UPDATE sync_log SET finished_at=datetime('now'), records_fetched=?,
            records_new=?, status='success' WHERE id=?
        """, (total_fetched, total_new, log_id))
        conn.commit()
        logger.info(f"DataHub OV sync: fetched={total_fetched}, new={total_new}")

    except Exception as e:
        conn.execute("""
            UPDATE sync_log SET finished_at=datetime('now'), status='error',
            error=? WHERE id=?
        """, (str(e), log_id))
        conn.commit()
        logger.error(f"DataHub OV sync error: {e}")
        raise
    finally:
        conn.close()

    return total_fetched, total_new


def _strip_html(text):
    """Remove HTML tags and decode entities."""
    if not text:
        return ""
    text = unescape(text)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'&\w+;', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def _xml_text(root, tag):
    """Get text from an XML element, searching without namespace."""
    for elem in root.iter():
        local = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        if local == tag and elem.text:
            return elem.text.strip()
    return ""


def _xml_all_text(root, tag):
    """Get text from all matching XML elements."""
    results = []
    for elem in root.iter():
        local = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        if local == tag and elem.text:
            results.append(elem.text.strip())
    return results


def _is_auction_xml(content):
    """Strictly check if OV XML is an auction announcement, not a business register extract etc."""
    # STRICT: Only accept XML with explicit auction PodanieKapitola codes
    AUCTION_KAPITOLA_CODES = [
        "OV_Ex_Drazby", "OV_DRAZBA", "OV_Drazby_Dobrovolne",
        "OV_DRAZBA_SUDNY_EXEKUTOR", "OV_DRAZBA_SPRAVCA_DANE",
    ]
    for code in AUCTION_KAPITOLA_CODES:
        if code in content:
            return True

    # Also accept if the root element is a known auction type
    AUCTION_ROOT_ELEMENTS = [
        "DrazbaDobrovolna", "DrazbaExekutor", "DrazbaSpravcaDane",
        "OznamenieODrazbe", "VyhlaskaDrazby",
    ]
    for root_el in AUCTION_ROOT_ELEMENTS:
        if f"<{root_el}" in content:
            return True

    # Accept OpravaZrusenie only if it references a drazba kapitola
    if "OpravaZrusenie" in content and any(code in content for code in AUCTION_KAPITOLA_CODES):
        return True

    # REJECT everything else - VypisOv, OznamenieVyzvaLikvidatorov, etc.
    return False


def _parse_ov_raw_issue(item):
    """Parse a raw OV issue XML. Only accept actual auction announcements."""
    if not item:
        return None

    content = item.get("content", "") or ""
    item_id = item.get("id", "")

    # Strict filter - reject non-auction XML types
    if not _is_auction_xml(content):
        return None

    # Parse XML to extract structured data
    subject_name = ""
    chapter = ""
    filing_type = ""
    ov_number = ""
    publish_date = ""
    publish_text = ""
    city = ""
    street = ""
    zip_code = ""

    try:
        xml_content = content.lstrip('\ufeff')
        root = ET.fromstring(xml_content)

        subject_name = _xml_text(root, "Subjekt") or _xml_text(root, "ObchodneMenoNazov") or ""
        chapter = _xml_text(root, "PodanieKapitola") or ""
        filing_type = _xml_text(root, "PodanieTyp") or ""
        ov_number = _xml_text(root, "OV") or ""
        publish_date = _xml_text(root, "DatumVydania") or ""

        city = _xml_text(root, "Obec") or ""
        street = _xml_text(root, "Ulica") or ""
        zip_code = _xml_text(root, "Psc") or ""

        publish_text = _xml_text(root, "ZverejnujeText") or ""
        publish_text = _strip_html(publish_text)

        # Also try to get structured auction fields
        if not publish_text:
            # Try common auction XML fields
            for field in ["PredmetDrazby", "MiestoDrazby", "PopisPredmetu",
                          "OpisNehnutelnosti", "PopisNehnutelnosti"]:
                val = _xml_text(root, field)
                if val:
                    publish_text += _strip_html(val) + "\n"
    except ET.ParseError:
        return None

    # Build meaningful title
    title_parts = []
    if chapter:
        title_parts.append(chapter)
    if filing_type:
        title_parts.append(f"- {filing_type}")
    if subject_name:
        title_parts.append(f"| {subject_name}")
    if city:
        title_parts.append(f"({city})")
    title = " ".join(title_parts) if title_parts else f"Dražba OV #{item_id}"
    title = title[:200]

    # Determine property type from publish text (not from full XML which has noise)
    text_lower = publish_text.lower()
    subject_type = "Neurčené"

    # Check for monetary claims (pohľadávky) - not real estate
    has_claim = any(kw in text_lower for kw in ['pohľadávk', 'peňažn', 'odkúpenie pohľadávk'])
    has_realestate = any(kw in text_lower for kw in ['nehnuteľnost', 'pozemok', 'parcela', 'dom', 'byt', 'stavba'])
    if has_claim and not has_realestate:
        return None  # Skip monetary claims entirely

    if "pozemok" in text_lower or "pozemk" in text_lower or "orná pôda" in text_lower:
        subject_type = "Pozemok"
    elif "rodinný dom" in text_lower:
        subject_type = "Rodinný dom"
    elif re.search(r'\bbyt\b', text_lower) or "bytov" in text_lower:
        subject_type = "Byt"
    elif re.search(r'\bdom\b', text_lower):
        subject_type = "Dom"
    elif "nebytov" in text_lower:
        subject_type = "Nebytový priestor"

    # Extract price from publish text only
    price = None
    price_patterns = [
        r'najnižš[eí]\s+podanie[:\s]*(\d[\d\s]*[\d,.]+)',
        r'vyvolávacia\s+cena[:\s]*(\d[\d\s]*[\d,.]+)',
        r'(\d[\d\s]*[\d,.]+)\s*(?:EUR|eur|€)',
    ]
    for pattern in price_patterns:
        match = re.search(pattern, publish_text)
        if match:
            price_str = match.group(1).replace(" ", "").replace(",", ".")
            try:
                price = float(price_str)
                if price < 1:
                    price = None
            except ValueError:
                pass
            break

    # Extract district from publish text
    district = ""
    district_match = re.search(r'okres[:\s]+([A-ZÁ-Ža-zá-ž][A-ZÁ-Ža-zá-ž\s-]{2,50}?)(?:\s*,|\s*obec|\s*katastr|\s*\n)', publish_text, re.IGNORECASE)
    if district_match:
        district = district_match.group(1).strip()[:80]

    # Extract city from katastrálne územie if not found from XML
    if not city:
        ku_match = re.search(r'katastrálne\s+územie[:\s]+([A-ZÁ-Ža-zá-ž][A-ZÁ-Ža-zá-ž\s-]{1,50}?)(?:\s*,|\s*zapís|\s*okres|\s*\n)', publish_text, re.IGNORECASE)
        if ku_match:
            city = ku_match.group(1).strip()[:80]
    if not city:
        ku_match = re.search(r'k\.ú\.\s+([A-ZÁ-Ža-zá-ž][A-ZÁ-Ža-zá-ž\s-]{1,50}?)(?:\s*,|\s*obec|\s*okres|\s*\n)', publish_text, re.IGNORECASE)
        if ku_match:
            city = ku_match.group(1).strip()[:80]

    # Extract region from publish text
    region = ""
    for rcode, rname in REGION_MAP.items():
        if rname.lower() in text_lower:
            region = rname
            break

    # Extract auction date from publish text
    auction_date = ""
    date_match = re.search(r'(\d{1,2})\.\s*(\d{1,2})\.\s*(20\d{2})', publish_text)
    if date_match:
        try:
            d, m, y = date_match.groups()
            auction_date = f"{y}-{int(m):02d}-{int(d):02d}"
        except (ValueError, IndexError):
            pass

    # Build description
    desc_parts = []
    if chapter:
        desc_parts.append(f"Kapitola: {chapter}")
    if filing_type:
        desc_parts.append(f"Typ: {filing_type}")
    if ov_number:
        desc_parts.append(f"OV: {ov_number}")
    if publish_date:
        desc_parts.append(f"Dátum vydania: {publish_date}")
    if publish_text:
        desc_parts.append("")
        desc_parts.append(publish_text[:1500])
    description = "\n".join(desc_parts)

    address = ", ".join(filter(None, [street, city, zip_code]))

    return {
        "id": f"ov_{item_id}",
        "source": "obchodny_vestnik",
        "title": title,
        "subject_type": subject_type,
        "subject_subtype": filing_type,
        "region": region,
        "district": district,
        "city": city,
        "address": address,
        "auction_date": auction_date,
        "price": price,
        "currency": "EUR",
        "status": "planned",
        "description": description,
        "url": f"https://obchodnyvestnik.justice.gov.sk/ObchodnyVestnik/Formular/FormularDetailHtml.aspx?IdFormular={item_id}",
        "lat": None,
        "lon": None,
        "raw_data": json.dumps(item, ensure_ascii=False, default=str)[:5000]
    }
