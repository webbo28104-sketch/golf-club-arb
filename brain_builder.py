"""
Pricing Brain Builder -- daily generator of market price recommendations.

Uses eBay Finding API findCompletedItems (90-day lookback, sold only).
Pulls golfbidder data as ceiling + all private sellers for recommended price.
Writes 13 rows/day to Postgres + Notion review DB.
"""
import os
import json
import time
import re
import statistics
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

import ebay

load_dotenv()

FINDING_URL = "https://svcs.ebay.com/services/search/FindingService/v1"
FINDING_SITE_ID = "3"
NOTION_API = "https://api.notion.com/v1"


ROWS_PER_DAY = 13
MIN_PRIVATE_COMPS = 5
LOOKBACK_DAYS = 90

# ---------------------------------------------------------------------------
# CLUB MODEL CATALOGUE
# Each entry: (club_type, make, model)
# Conditions are iterated as Poor / Good / Excellent per entry.
# ---------------------------------------------------------------------------

_CATALOGUE = [
    # Iron Sets
    ("Iron Set", "Titleist", "T100"),
    ("Iron Set", "Titleist", "T150"),
    ("Iron Set", "Titleist", "T200"),
    ("Iron Set", "Titleist", "T300"),
    ("Iron Set", "Titleist", "T350"),
    ("Iron Set", "Titleist", "AP1"),
    ("Iron Set", "Titleist", "AP2"),
    ("Iron Set", "Titleist", "AP3"),
    ("Iron Set", "Titleist", "CB"),
    ("Iron Set", "Titleist", "MB"),
    ("Iron Set", "Titleist", "716 AP2"),
    ("Iron Set", "Titleist", "718 AP2"),
    ("Iron Set", "Titleist", "620 MB"),
    ("Iron Set", "Titleist", "690 MB"),
    ("Iron Set", "TaylorMade", "P790"),
    ("Iron Set", "TaylorMade", "P770"),
    ("Iron Set", "TaylorMade", "P760"),
    ("Iron Set", "TaylorMade", "P730"),
    ("Iron Set", "TaylorMade", "SIM2"),
    ("Iron Set", "TaylorMade", "SIM"),
    ("Iron Set", "TaylorMade", "Stealth"),
    ("Iron Set", "TaylorMade", "Qi10"),
    ("Iron Set", "TaylorMade", "Burner"),
    ("Iron Set", "Callaway", "Apex"),
    ("Iron Set", "Callaway", "Apex Pro"),
    ("Iron Set", "Callaway", "Rogue ST"),
    ("Iron Set", "Callaway", "Mavrik"),
    ("Iron Set", "Callaway", "Epic Forged"),
    ("Iron Set", "Callaway", "X Forged"),
    ("Iron Set", "Callaway", "Big Bertha"),
    ("Iron Set", "Ping", "G430"),
    ("Iron Set", "Ping", "G425"),
    ("Iron Set", "Ping", "G410"),
    ("Iron Set", "Ping", "G400"),
    ("Iron Set", "Ping", "i230"),
    ("Iron Set", "Ping", "i210"),
    ("Iron Set", "Ping", "i500"),
    ("Iron Set", "Ping", "i525"),
    ("Iron Set", "Ping", "Blueprint"),
    ("Iron Set", "Ping", "S159"),
    ("Iron Set", "Mizuno", "JPX 923"),
    ("Iron Set", "Mizuno", "JPX 921"),
    ("Iron Set", "Mizuno", "JPX 919"),
    ("Iron Set", "Mizuno", "MP 20"),
    ("Iron Set", "Mizuno", "MP 18"),
    ("Iron Set", "Mizuno", "Pro 223"),
    ("Iron Set", "Mizuno", "Pro 225"),
    ("Iron Set", "Mizuno", "Pro 241"),
    ("Iron Set", "Cobra", "King Tour"),
    ("Iron Set", "Cobra", "Aerojet"),
    ("Iron Set", "Cobra", "LTDx"),
    ("Iron Set", "Srixon", "ZX5"),
    ("Iron Set", "Srixon", "ZX7"),
    ("Iron Set", "Srixon", "ZX Mk II"),
    ("Iron Set", "Srixon", "Z785"),
    ("Iron Set", "Wilson", "Staff Model"),
    ("Iron Set", "Wilson", "D9"),
    ("Iron Set", "Cleveland", "Launcher XL"),
    ("Iron Set", "Cleveland", "ZipCore"),
    ("Iron Set", "PXG", "0311"),
    ("Iron Set", "PXG", "0311P"),
    ("Iron Set", "Nike", "VR Pro"),
    ("Iron Set", "Nike", "Vapor"),
    # Drivers
    ("Driver", "TaylorMade", "Qi10"),
    ("Driver", "TaylorMade", "Stealth 2"),
    ("Driver", "TaylorMade", "Stealth"),
    ("Driver", "TaylorMade", "SIM2"),
    ("Driver", "TaylorMade", "SIM"),
    ("Driver", "TaylorMade", "M6"),
    ("Driver", "TaylorMade", "M5"),
    ("Driver", "Callaway", "Paradym"),
    ("Driver", "Callaway", "Rogue ST"),
    ("Driver", "Callaway", "Epic Max"),
    ("Driver", "Callaway", "Epic Speed"),
    ("Driver", "Callaway", "Big Bertha"),
    ("Driver", "Titleist", "TSR3"),
    ("Driver", "Titleist", "TSR2"),
    ("Driver", "Titleist", "TS3"),
    ("Driver", "Titleist", "TS2"),
    ("Driver", "Ping", "G430"),
    ("Driver", "Ping", "G425"),
    ("Driver", "Ping", "G410"),
    ("Driver", "Ping", "G400"),
    ("Driver", "Cobra", "Aerojet"),
    ("Driver", "Cobra", "LTDx"),
    ("Driver", "Cobra", "Speedzone"),
    ("Driver", "Cobra", "F9"),
    ("Driver", "Mizuno", "ST-Z 220"),
    ("Driver", "Srixon", "ZX5 Mk II"),
    ("Driver", "Cleveland", "Launcher XL"),
    # Fairway Woods
    ("Fairway Wood", "TaylorMade", "Qi10"),
    ("Fairway Wood", "TaylorMade", "Stealth 2"),
    ("Fairway Wood", "TaylorMade", "SIM2"),
    ("Fairway Wood", "Callaway", "Paradym"),
    ("Fairway Wood", "Callaway", "Rogue ST"),
    ("Fairway Wood", "Callaway", "Epic Max"),
    ("Fairway Wood", "Titleist", "TSR2"),
    ("Fairway Wood", "Titleist", "TS3"),
    ("Fairway Wood", "Ping", "G430"),
    ("Fairway Wood", "Ping", "G425"),
    ("Fairway Wood", "Cobra", "Aerojet"),
    ("Fairway Wood", "Cobra", "LTDx"),
    # Hybrids
    ("Hybrid", "TaylorMade", "Stealth 2"),
    ("Hybrid", "TaylorMade", "SIM2"),
    ("Hybrid", "Callaway", "Apex"),
    ("Hybrid", "Callaway", "Rogue ST"),
    ("Hybrid", "Titleist", "TSR2"),
    ("Hybrid", "Ping", "G430"),
    ("Hybrid", "Ping", "G425"),
    ("Hybrid", "Mizuno", "CLK"),
    ("Hybrid", "Cobra", "Aerojet"),
    # Utility Irons
    ("Utility Iron", "Titleist", "U510"),
    ("Utility Iron", "Titleist", "U500"),
    ("Utility Iron", "TaylorMade", "P790 UDI"),
    ("Utility Iron", "Callaway", "Apex Utility"),
    ("Utility Iron", "Ping", "Blueprint T"),
    ("Utility Iron", "Mizuno", "Pro Utility"),
    # Wedge Sets
    ("Wedge Set", "Titleist", "Vokey SM9"),
    ("Wedge Set", "Titleist", "Vokey SM8"),
    ("Wedge Set", "Cleveland", "RTX 6"),
    ("Wedge Set", "Cleveland", "RTX ZipCore"),
    ("Wedge Set", "Callaway", "Jaws Full Toe"),
    ("Wedge Set", "TaylorMade", "Milled Grind 3"),
    ("Wedge Set", "Ping", "Glide 4.0"),
    ("Wedge Set", "Mizuno", "T22"),
    # Wedges (individual)
    ("Wedge", "Titleist", "Vokey SM9"),
    ("Wedge", "Titleist", "Vokey SM8"),
    ("Wedge", "Titleist", "Vokey SM7"),
    ("Wedge", "Cleveland", "RTX 6"),
    ("Wedge", "Cleveland", "RTX ZipCore"),
    ("Wedge", "Callaway", "Jaws Full Toe"),
    ("Wedge", "Callaway", "Mack Daddy 5"),
    ("Wedge", "TaylorMade", "Milled Grind 3"),
    ("Wedge", "TaylorMade", "Hi-Toe"),
    ("Wedge", "Ping", "Glide 4.0"),
    ("Wedge", "Mizuno", "T22"),
    ("Wedge", "Mizuno", "T20"),
    ("Wedge", "Cobra", "King Tour"),
    # Putters
    ("Putter", "Scotty Cameron", "Newport 2"),
    ("Putter", "Scotty Cameron", "Phantom X"),
    ("Putter", "Scotty Cameron", "Special Select"),
    ("Putter", "Scotty Cameron", "Fastback"),
    ("Putter", "Scotty Cameron", "Flowback"),
    ("Putter", "Odyssey", "White Hot"),
    ("Putter", "Odyssey", "Tri-Hot 5K"),
    ("Putter", "Odyssey", "Eleven"),
    ("Putter", "Odyssey", "Ten"),
    ("Putter", "TaylorMade", "Spider X"),
    ("Putter", "TaylorMade", "Spider EX"),
    ("Putter", "Ping", "Anser"),
    ("Putter", "Ping", "Sigma 2"),
    ("Putter", "Ping", "PLD"),
    ("Putter", "Cleveland", "Huntington Beach"),
    # Bags
    ("Bag", "TaylorMade", "Tour Staff"),
    ("Bag", "Callaway", "Chev"),
    ("Bag", "Titleist", "Players"),
    ("Bag", "Ping", "Hoofer"),
]

CONDITIONS = ["Poor", "Good", "Excellent"]

# Expanded to full entry list: catalogue x 3 conditions in category order
_QUEUE_ENTRIES = [
    (ct, make, model, cond)
    for ct, make, model in _CATALOGUE
    for cond in CONDITIONS
]


# ---------------------------------------------------------------------------
# CONDITION DETECTION
# ---------------------------------------------------------------------------

_CONDITION_EXCELLENT_KW = [
    "mint", "immaculate", "as new", "pristine", "unused",
    "unplayed", "9/10", "10/10", "9 out of 10", "10 out of 10",
]
_CONDITION_GOOD_KW = [
    "good condition", "good used", "some wear", "bag wear",
    "light marks", "light scratches", "7/10", "8/10",
    "7 out of 10", "8 out of 10",
]
_CONDITION_POOR_KW = [
    "marks", "scratches", "worn", "damaged", "spares", "repair",
    "5/10", "6/10", "5 out of 10", "6 out of 10", "cracked",
    "broken", "for parts",
]


def detect_condition(title: str, description: str = "") -> str:
    text = (title + " " + description).lower()
    for kw in _CONDITION_EXCELLENT_KW:
        if kw in text:
            return "Excellent"
    for kw in _CONDITION_GOOD_KW:
        if kw in text:
            return "Good"
    for kw in _CONDITION_POOR_KW:
        if kw in text:
            return "Poor"
    return "Unknown"


def _detect_year(title: str) -> Optional[int]:
    m = re.search(r"(20[0-2][0-9])", title)
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------------------
# POSTGRES HELPERS
# ---------------------------------------------------------------------------

def _pg_conn():
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        raise EnvironmentError("DATABASE_URL is not set")
    return psycopg2.connect(url, cursor_factory=psycopg2.extras.DictCursor)


def init_tables():
    ddl = (
        "CREATE TABLE IF NOT EXISTS brain_queue ("
        "    id              SERIAL PRIMARY KEY,"
        "    club_type       TEXT NOT NULL,"
        "    make            TEXT NOT NULL,"
        "    model           TEXT NOT NULL,"
        "    condition       TEXT NOT NULL,"
        "    status          TEXT NOT NULL DEFAULT 'pending',"
        "    scheduled_date  DATE,"
        "    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
        "    UNIQUE (club_type, make, model, condition)"
        ");"

        "CREATE TABLE IF NOT EXISTS brain ("
        "    id                  SERIAL PRIMARY KEY,"
        "    club_type           TEXT NOT NULL,"
        "    make                TEXT NOT NULL,"
        "    model               TEXT NOT NULL,"
        "    condition           TEXT NOT NULL,"
        "    recommended_price   NUMERIC(10,2),"
        "    golfbidder_ceiling  NUMERIC(10,2),"
        "    confidence          TEXT,"
        "    comp_count          INTEGER,"
        "    approved_at         TIMESTAMPTZ,"
        "    last_refreshed      TIMESTAMPTZ,"
        "    active              BOOLEAN NOT NULL DEFAULT TRUE,"
        "    UNIQUE (club_type, make, model, condition)"
        ");"

        "CREATE TABLE IF NOT EXISTS pending_prices ("
        "    id                      SERIAL PRIMARY KEY,"
        "    club_type               TEXT NOT NULL,"
        "    make                    TEXT NOT NULL,"
        "    model                   TEXT NOT NULL,"
        "    condition               TEXT NOT NULL,"
        "    recommended_price       NUMERIC(10,2),"
        "    golfbidder_ceiling_price NUMERIC(10,2),"
        "    private_comp_count      INTEGER NOT NULL DEFAULT 0,"
        "    golfbidder_comp_count   INTEGER NOT NULL DEFAULT 0,"
        "    comp_data               JSONB,"
        "    excluded_comps          JSONB,"
        "    insufficient_data       BOOLEAN NOT NULL DEFAULT FALSE,"
        "    notion_page_id          TEXT,"
        "    status                  TEXT NOT NULL DEFAULT 'pending',"
        "    notes                   TEXT,"
        "    date_generated          DATE NOT NULL DEFAULT CURRENT_DATE,"
        "    date_reviewed           DATE,"
        "    brain_entry_id          INTEGER REFERENCES brain(id) ON DELETE SET NULL"
        ");"
    )
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    print("[brain] Postgres tables initialised")


def seed_queue():
    sql = (
        "INSERT INTO brain_queue (club_type, make, model, condition)"
        " VALUES (%s, %s, %s, %s)"
        " ON CONFLICT (club_type, make, model, condition) DO NOTHING"
    )
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, _QUEUE_ENTRIES)
        conn.commit()
    print(f"[brain] Queue seeded ({len(_QUEUE_ENTRIES)} entries in catalogue)")


def get_todays_batch() -> list[dict]:
    sql = "SELECT id, club_type, make, model, condition FROM brain_queue WHERE status = 'pending' ORDER BY id LIMIT %s"
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (ROWS_PER_DAY,))
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def mark_in_progress(queue_id: int):
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE brain_queue SET status='in_progress', scheduled_date=CURRENT_DATE WHERE id=%s", (queue_id,))
        conn.commit()


def mark_done(queue_id: int):
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE brain_queue SET status='done' WHERE id=%s", (queue_id,))
        conn.commit()


def hard_reset_queue():
    """Reset ALL queue entries to pending, regardless of status. Use to restart from the top."""
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE brain_queue SET status='pending', scheduled_date=NULL")
        conn.commit()
    print("[brain] Queue hard-reset -- all entries set to pending")


def reset_queue_cycle():
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM brain_queue WHERE status != 'done'")
            remaining = cur.fetchone()[0]
        if remaining == 0:
            with conn.cursor() as cur:
                cur.execute("UPDATE brain_queue SET status='pending', scheduled_date=NULL")
            conn.commit()
            print("[brain] Full cycle complete -- queue reset for next 90-day refresh")


def save_pending_price(entry: dict, result: dict) -> int:
    sql = (
        "INSERT INTO pending_prices"
        "    (club_type, make, model, condition, recommended_price, golfbidder_ceiling_price,"
        "     private_comp_count, golfbidder_comp_count, comp_data, excluded_comps,"
        "     insufficient_data, date_generated)"
        " VALUES (%(club_type)s, %(make)s, %(model)s, %(condition)s,"
        "     %(recommended_price)s, %(golfbidder_ceiling_price)s,"
        "     %(private_comp_count)s, %(golfbidder_comp_count)s,"
        "     %(comp_data)s, %(excluded_comps)s, %(insufficient_data)s, CURRENT_DATE)"
        " RETURNING id"
    )
    params = {
        "club_type": entry["club_type"],
        "make": entry["make"],
        "model": entry["model"],
        "condition": entry["condition"],
        "recommended_price": result.get("recommended_price"),
        "golfbidder_ceiling_price": result.get("golfbidder_ceiling"),
        "private_comp_count": result.get("private_comp_count", 0),
        "golfbidder_comp_count": result.get("golfbidder_comp_count", 0),
        "comp_data": json.dumps(result.get("comp_data", [])),
        "excluded_comps": json.dumps(result.get("excluded_comps", [])),
        "insufficient_data": result.get("insufficient_data", False),
    }
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row_id = cur.fetchone()[0]
        conn.commit()
    return row_id


def update_notion_page_id(pending_id: int, notion_page_id: str):
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE pending_prices SET notion_page_id=%s WHERE id=%s", (notion_page_id, pending_id))
        conn.commit()


# ---------------------------------------------------------------------------
# EBAY FINDING API -- COMPLETED ITEMS
# ---------------------------------------------------------------------------

_FINDING_NS = "http://www.ebay.com/marketplace/search/v1/services"


_finding_call_count = 0
FINDING_API_DAILY_LIMIT = 100

def _finding_request(keywords: str, seller_filter: Optional[str], page: int = 1) -> requests.Response:
    from datetime import timezone as _tz
    app_id = os.environ.get("EBAY_CLIENT_ID", "")
    now = datetime.now(_tz.utc)
    cutoff = (now - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    seller_xml = ""
    if seller_filter == "golfbidder":
        seller_xml = "<itemFilter><name>Seller</name><value>golfbidder</value></itemFilter>"
    elif seller_filter == "exclude_golfbidder":
        seller_xml = "<itemFilter><name>ExcludeSeller</name><value>golfbidder</value></itemFilter>"

    body = (
        f'<?xml version="1.0" encoding="utf-8"?>'
        f'<findCompletedItemsRequest xmlns="http://www.ebay.com/marketplace/search/v1/services">'
        f'<keywords>{keywords}</keywords>'
        f'<itemFilter><name>SoldItemsOnly</name><value>true</value></itemFilter>'
        f'<itemFilter><name>MinPrice</name><value>30</value>'
        f'<paramName>Currency</paramName><paramValue>GBP</paramValue></itemFilter>'
        f'<itemFilter><name>EndTimeFrom</name><value>{cutoff}</value></itemFilter>'
        f'{seller_xml}'
        f'<sortOrder>EndTimeSoonest</sortOrder>'
        f'<pagination><entriesPerPage>100</entriesPerPage><pageNumber>{page}</pageNumber></pagination>'
        f'</findCompletedItemsRequest>'
    )
    headers = {
        "X-EBAY-SOA-OPERATION-NAME": "findCompletedItems",
        "X-EBAY-SOA-SECURITY-APPNAME": app_id,
        "X-EBAY-SOA-RESPONSE-DATA-FORMAT": "XML",
        "X-EBAY-SOA-REQUEST-DATA-FORMAT": "XML",
        "Content-Type": "text/xml",
        "X-EBAY-SOA-GLOBAL-ID": "EBAY-GB",
    }
    app_id_preview = app_id[:12] + "..." if len(app_id) > 12 else repr(app_id)
    print(f"[brain] Finding API call -- URL: {FINDING_URL}")
    print(f"[brain] Finding API call -- headers: { {k: (v if k != 'X-EBAY-SOA-SECURITY-APPNAME' else app_id_preview) for k, v in headers.items()} }")
    print(f"[brain] Finding API call -- body[:120]: {body[:120]}")
    resp = requests.post(FINDING_URL, headers=headers, data=body.encode("utf-8"), timeout=20)
    print(f"[brain] Finding API response: {resp.status_code} -- {resp.text[:300]}")
    return resp


def _parse_finding_items(xml_text: str) -> list[dict]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    ns = {"e": _FINDING_NS}
    items = []
    for item in root.findall(".//e:item", ns):
        def _txt(tag):
            el = item.find(f"e:{tag}", ns)
            return el.text.strip() if el is not None and el.text else ""
        try:
            price_el = item.find(".//e:currentPrice", ns)
            price = float(price_el.text) if price_el is not None else 0.0
            shipping_el = item.find(".//e:shippingServiceCost", ns)
            shipping = float(shipping_el.text) if shipping_el is not None else 0.0
            bid_el = item.find(".//e:bidCount", ns)
            bid_count = int(bid_el.text) if bid_el is not None else 0
            listing_type_el = item.find(".//e:listingType", ns)
            listing_type = listing_type_el.text if listing_type_el is not None else ""
            seller_el = item.find(".//e:sellerInfo/e:sellerUserName", ns)
            seller_name = seller_el.text.lower() if seller_el is not None and seller_el.text else ""
            feedback_el = item.find(".//e:sellerInfo/e:feedbackScore", ns)
            feedback_score = int(feedback_el.text) if feedback_el is not None and feedback_el.text else 0
            items.append({
                "title": _txt("title"),
                "price": price,
                "shipping": shipping,
                "total_price": price + shipping,
                "bid_count": bid_count,
                "listing_type": listing_type,
                "seller": seller_name,
                "feedback_score": feedback_score,
                "item_id": _txt("itemId"),
                "url": _txt("viewItemURL"),
                "end_time": _txt("endTime"),
                "condition_text": _txt("conditionDisplayName"),
            })
        except (ValueError, TypeError):
            continue
    return items


_SCRAPE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}
_EBAY_SCRAPE_BASE = "https://www.ebay.co.uk/sch/i.html"

# Known dealer eBay seller names — exclude from private sold comps
_DEALER_EBAY_SELLERS = {"golfbidder", "cashforeclubs", "golfavenue"}

_DEALER_SITES = [
    {
        "name": "Golf Bidder",
        # Use category listing page — more accessible than search endpoint
        "search_url": "https://www.golfbidder.co.uk/used-golf-clubs?q={query}",
        "price_selectors": [".product-price", ".price", "[class*='price']"],
        "title_selectors": [".product-title", ".product-name", "h2", "h3"],
        "condition_selectors": [".condition", "[class*='condition']", ".product-condition"],
    },
    {
        "name": "Cash Fore Clubs",
        "search_url": "https://www.cashforeclubs.co.uk/search?q={query}",
        "price_selectors": [".price", ".product-price", "[class*='price']"],
        "title_selectors": [".product-title", ".product-name", "h2", "h3"],
        "condition_selectors": [".condition", "[class*='condition']"],
    },
    {
        "name": "Golf Avenue",
        "search_url": "https://www.golfavenue.co.uk/search?q={query}",
        "price_selectors": [".price", ".product-price", "[class*='price']"],
        "title_selectors": [".product-title", ".product-name", "h2", "h3"],
        "condition_selectors": [".condition", "[class*='condition']"],
    },
]


def _extract_gbp(text: str) -> Optional[float]:
    """Extract first GBP price from a string."""
    m = re.search(r"£\s*([\d,]+(?:\.\d{1,2})?)", text)
    if m:
        return float(m.group(1).replace(",", ""))
    return None


def _scrape_one_dealer(site: dict, keywords: str) -> list[dict]:
    """Scrape a single dealer site and return list of {source, title, condition, price}."""
    import cloudscraper
    from bs4 import BeautifulSoup
    import urllib.parse

    url = site["search_url"].format(query=urllib.parse.quote_plus(keywords))
    print(f"[brain] Dealer scrape: {site['name']} -- {url}")
    try:
        scraper = cloudscraper.create_scraper()
        resp = scraper.get(url, headers=_SCRAPE_HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as exc:
        print(f"[brain] Dealer scrape failed ({site['name']}): {exc}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    results = []

    # Try to find product cards — common patterns across Shopify/WooCommerce sites
    cards = (
        soup.select(".product-item") or
        soup.select(".product-card") or
        soup.select(".product") or
        soup.select("[class*='product-item']") or
        soup.select("li.grid__item") or      # Shopify
        soup.select(".collection-item") or
        soup.select("article") or
        soup.select(".woocommerce-LoopProduct") or  # WooCommerce
        []
    )

    if not cards:
        # Fallback: look for any price on the page with nearby title
        prices_found = []
        for el in soup.select(", ".join(site["price_selectors"])):
            price = _extract_gbp(el.get_text())
            if price and price > 10:
                # Try to find a nearby title
                parent = el.find_parent(["li", "div", "article", "section"])
                title_text = ""
                if parent:
                    for sel in site["title_selectors"]:
                        t = parent.select_one(sel)
                        if t:
                            title_text = t.get_text(strip=True)
                            break
                if not title_text:
                    title_text = keywords
                prices_found.append({
                    "source": site["name"],
                    "title": title_text[:100],
                    "condition": "",
                    "price": price,
                })
        if prices_found:
            print(f"[brain] Dealer scrape ({site['name']}): {len(prices_found)} prices via fallback")
        return prices_found[:10]

    for card in cards[:20]:
        try:
            title_text = ""
            for sel in site["title_selectors"]:
                t = card.select_one(sel)
                if t:
                    title_text = t.get_text(strip=True)
                    break
            if not title_text:
                continue

            price = None
            for sel in site["price_selectors"]:
                p = card.select_one(sel)
                if p:
                    price = _extract_gbp(p.get_text())
                    if price:
                        break
            if not price or price < 10:
                continue

            condition_text = ""
            for sel in site["condition_selectors"]:
                c = card.select_one(sel)
                if c:
                    condition_text = c.get_text(strip=True)
                    break

            results.append({
                "source": site["name"],
                "title": title_text[:100],
                "condition": condition_text[:50],
                "price": price,
            })
        except Exception:
            continue

    print(f"[brain] Dealer scrape ({site['name']}): {len(results)} results")
    return results


def scrape_dealer_ceiling_prices(keywords: str) -> list[dict]:
    """
    Scrape all dealer sites for current retail prices.
    Returns list of {source, title, condition, price}.
    """
    all_results = []
    for site in _DEALER_SITES:
        results = _scrape_one_dealer(site, keywords)
        all_results.extend(results)
        time.sleep(2)
    return all_results


def _scrape_sold_comps(keywords: str, seller_filter: Optional[str] = None) -> list[dict]:
    """Scrape eBay UK completed/sold listings as fallback when Finding API is rate-limited."""
    import cloudscraper
    from bs4 import BeautifulSoup
    import urllib.parse

    # Establish session with cookies by hitting the homepage first
    scraper = cloudscraper.create_scraper()
    try:
        scraper.get("https://www.ebay.co.uk", headers=_SCRAPE_HEADERS, timeout=20)
        time.sleep(1)
    except Exception as exc:
        print(f"[brain] eBay session warm-up failed: {exc}")

    all_items: list[dict] = []
    for page in range(1, 4):  # max 3 pages
        params: dict = {
            "_nkw": keywords,
            "_sacat": "0",
            "LH_Sold": "1",
            "LH_Complete": "1",
            "LH_ItemCondition": "3000",
            "_sop": "13",
            "_pgn": str(page),
        }
        if seller_filter == "golfbidder":
            params["_ssn"] = "golfbidder"
        # exclude_dealers: no URL param available — filtered post-scrape

        url = _EBAY_SCRAPE_BASE + "?" + urllib.parse.urlencode(params)
        print(f"[brain] Scrape fallback page {page}: {url[:120]}")
        try:
            resp = scraper.get(url, headers=_SCRAPE_HEADERS, timeout=20)
            resp.raise_for_status()
        except Exception as exc:
            print(f"[brain] Scrape error page {page}: {exc}")
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        items_on_page = 0
        for li in soup.select("li.s-item"):
            try:
                title_el = li.select_one(".s-item__title")
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)
                if title.lower() in ("shop on ebay", "results matching fewer words"):
                    continue

                price_el = li.select_one(".s-item__price")
                if not price_el:
                    continue
                price_text = price_el.get_text(strip=True).replace("£", "").replace(",", "").split()[0]
                price = float(price_text)

                url_el = li.select_one("a.s-item__link")
                item_url = url_el["href"].split("?")[0] if url_el else ""

                date_el = li.select_one(".s-item__ended-date, .s-item__listingDate, .POSITIVE")
                end_time = date_el.get_text(strip=True) if date_el else ""

                seller_el = li.select_one(".s-item__seller-info-text")
                seller_name = seller_el.get_text(strip=True).lower() if seller_el else ""

                shipping_el = li.select_one(".s-item__shipping")
                shipping_text = shipping_el.get_text(strip=True) if shipping_el else ""
                if "free" in shipping_text.lower():
                    shipping = 0.0
                else:
                    ship_match = re.search(r"£([\d.]+)", shipping_text)
                    shipping = float(ship_match.group(1)) if ship_match else 0.0

                if seller_filter == "exclude_dealers":
                    if any(d in seller_name for d in _DEALER_EBAY_SELLERS):
                        continue

                all_items.append({
                    "title": title,
                    "price": price,
                    "shipping": shipping,
                    "total_price": price + shipping,
                    "bid_count": 0,
                    "listing_type": "FixedPrice",
                    "seller": seller_name,
                    "feedback_score": 0,
                    "item_id": item_url.split("/")[-1] if item_url else "",
                    "url": item_url,
                    "end_time": end_time,
                    "condition_text": "",
                })
                items_on_page += 1
            except Exception:
                continue

        print(f"[brain] Scrape page {page}: {items_on_page} items")
        if items_on_page == 0:
            break
        time.sleep(2)

    print(f"[brain] Scrape fallback total: {len(all_items)} items for {keywords!r}")
    return all_items


def _is_rate_limit_error(xml_text: str) -> bool:
    """Return True if the Finding API response contains errorId 10001."""
    try:
        root = ET.fromstring(xml_text)
        ns = {"e": _FINDING_NS}
        for code_el in root.findall(".//e:errorId", ns):
            if code_el.text and code_el.text.strip() == "10001":
                return True
    except ET.ParseError:
        pass
    return False


def fetch_sold_comps(keywords: str, seller_filter: Optional[str] = None) -> list[dict]:
    global _finding_call_count
    if _finding_call_count >= FINDING_API_DAILY_LIMIT:
        print(f"[brain] Finding API daily limit reached -- using scrape fallback for {keywords!r}")
        return _scrape_sold_comps(keywords, seller_filter)

    all_items = []
    rate_limited = False
    for page in range(1, 4):  # max 3 pages = 300 items
        if _finding_call_count >= FINDING_API_DAILY_LIMIT:
            print(f"[brain] Finding API daily limit reached mid-fetch -- stopping")
            rate_limited = True
            break
        try:
            resp = _finding_request(keywords, seller_filter, page)
            _finding_call_count += 1
        except Exception as exc:
            print(f"[brain] Finding API error page {page}: {exc}")
            break

        if resp.status_code == 500 and _is_rate_limit_error(resp.text):
            print(f"[brain] Finding API rate limited (error 10001) -- switching to scrape fallback")
            rate_limited = True
            break

        try:
            resp.raise_for_status()
        except Exception as exc:
            print(f"[brain] Finding API HTTP error page {page}: {exc}")
            break

        items = _parse_finding_items(resp.text)
        if not items:
            break
        all_items.extend(items)
        time.sleep(3)  # throttle: avoid per-second rate limit (error 10001)

    if rate_limited and not all_items:
        return _scrape_sold_comps(keywords, seller_filter)
    return all_items


# ---------------------------------------------------------------------------
# COMP FILTERING AND PRICING LOGIC
# ---------------------------------------------------------------------------

def _is_auction_valid(item: dict) -> bool:
    return item["listing_type"] in ("Auction", "AuctionWithBIN") and item["bid_count"] >= 2


def _is_likely_dealer(item: dict) -> bool:
    return item["feedback_score"] >= 100


def filter_and_classify_comps(
    raw_items: list[dict],
    target_condition: str,
    target_club_count: Optional[int] = None,
    target_shaft: Optional[str] = None,
    target_year: Optional[int] = None,
) -> tuple[list[dict], list[dict]]:
    accepted = []
    excluded = []

    for item in raw_items:
        title = item["title"]
        price = item["price"]
        reject_reason = None

        # Price floor
        if price < 30:
            reject_reason = "price below minimum GBP30"

        # Auction bid count
        elif item["listing_type"] in ("Auction", "AuctionWithBIN") and item["bid_count"] < 2:
            reject_reason = f"auction with only {item['bid_count']} bid(s) -- possible shill"

        # Club count filter (iron sets)
        elif target_club_count is not None:
            comp_count = ebay.count_clubs(title)
            if comp_count is not None and abs(comp_count - target_club_count) > 1:
                reject_reason = f"club count mismatch ({comp_count} vs expected {target_club_count}+-1)"

        # Shaft type filter
        if reject_reason is None and target_shaft:
            comp_shaft = ebay._detect_shaft(title)
            if comp_shaft and comp_shaft != target_shaft:
                reject_reason = f"shaft mismatch ({comp_shaft} vs {target_shaft})"

        # Condition classification
        if reject_reason is None:
            cond = detect_condition(title, item.get("condition_text", ""))
            if cond != target_condition and cond != "Unknown":
                reject_reason = f"condition mismatch (detected {cond}, want {target_condition})"

        if reject_reason:
            excluded.append({**item, "reject_reason": reject_reason})
        else:
            year_flag = None
            if target_year:
                comp_year = _detect_year(title)
                if comp_year and abs(comp_year - target_year) > 2:
                    year_flag = f"year {comp_year} outside +-2 of {target_year}"
            accepted.append({**item, "year_flag": year_flag})

    return accepted, excluded


def compute_pricing(accepted_comps: list[dict]) -> Optional[dict]:
    if len(accepted_comps) < MIN_PRIVATE_COMPS:
        return None

    prices = [c["price"] for c in accepted_comps]
    med = statistics.median(prices)

    # Outlier removal: 25%-300% of median
    clean = [c for c in accepted_comps if med * 0.25 <= c["price"] <= med * 3.0]
    if len(clean) < MIN_PRIVATE_COMPS:
        clean = accepted_comps  # revert if too few remain

    clean_prices = [c["price"] for c in clean]
    recommended = round(statistics.median(clean_prices) / 5) * 5  # round to nearest 5

    comp_count = len(clean)
    if comp_count >= 10:
        confidence = "high"
    elif comp_count >= 5:
        confidence = "medium"
    else:
        confidence = "low"

    has_year_flags = any(c.get("year_flag") for c in clean)

    return {
        "recommended_price": recommended,
        "private_comp_count": comp_count,
        "confidence": confidence,
        "comp_prices": clean_prices,
        "comp_data": clean,
        "has_year_flags": has_year_flags,
    }


# ---------------------------------------------------------------------------
# NOTION WRITER
# ---------------------------------------------------------------------------

def _notion_headers() -> dict:
    token = os.environ.get("NOTION_TOKEN", "")
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }


def _build_evidence_text(result: dict, entry: dict) -> str:
    lines_out = []
    lines_out.append("PRIVATE SOLD COMPS (used):")
    for c in result.get("comp_data", []):
        dt = c.get("end_time", "")[:10]
        flag = f" [YEAR FLAG: {c['year_flag']}]" if c.get("year_flag") else ""
        lines_out.append(f"  * {c['title'][:80]} -- GBP{c['price']:.0f} -- {dt} -- {c.get('url','')}" + flag)

    dealer_prices = result.get("dealer_prices", [])
    lines_out.append("")
    lines_out.append("DEALER CEILING PRICES:")
    lines_out.append("  Source | Title | Condition | Price")
    if dealer_prices:
        for d in dealer_prices:
            cond_str = f" | {d['condition']}" if d.get("condition") else ""
            lines_out.append(f"  * {d['source']} | {d['title'][:70]}{cond_str} | GBP{d['price']:.0f}")
    else:
        lines_out.append("  (no dealer prices found)")

    excluded = result.get("excluded_comps", [])
    if excluded:
        lines_out.append("")
        lines_out.append("EXCLUDED COMPS:")
        for c in excluded[:10]:
            lines_out.append(f"  * {c['title'][:70]} -- GBP{c['price']:.0f} -- REASON: {c.get('reject_reason','')}")

    lines_out.append("")
    lines_out.append(f"CONFIDENCE: {result.get('confidence','?').upper()}")
    lines_out.append(f"COMP COUNT: {result.get('private_comp_count', 0)} private comps across 90 days")

    notes = []
    if result.get("has_year_flags"):
        notes.append("cross-year comps used")
    if result.get("insufficient_data"):
        notes.append(f"insufficient data ({result.get('private_comp_count',0)} of {MIN_PRIVATE_COMPS} required)")
    if notes:
        lines_out.append(f"DATA NOTES: {', '.join(notes)}")

    return chr(10).join(lines_out)[:2000]  # Notion rich_text limit


def create_notion_review_page(entry: dict, result: dict) -> str:
    db_id = os.environ.get("NOTION_BRAIN_REVIEW_DB_ID", "")
    print(f"[brain] Notion DB ID in use: {repr(db_id)}")
    if not db_id:
        print("[brain] NOTION_BRAIN_REVIEW_DB_ID not set -- skipping Notion write")
        return ""

    title = f"{entry['make']} {entry['model']} -- {entry['condition']}"
    evidence = _build_evidence_text(result, entry)

    recommended = result.get("recommended_price")
    ceiling = result.get("overall_ceiling") or result.get("golfbidder_ceiling")
    gb_ceiling = result.get("golfbidder_ceiling")
    is_iron = entry["club_type"] in ("Iron Set", "Wedge Set")

    props = {
        "Name": {"title": [{"text": {"content": title}}]},
        "Club Type": {"select": {"name": entry["club_type"]}},
        "Make": {"select": {"name": entry["make"]}},
        "Model": {"rich_text": [{"text": {"content": entry["model"]}}]},
        "Condition": {"select": {"name": entry["condition"]}},
        "Status": {"select": {"name": "Pending"}},
        "Evidence": {"rich_text": [{"text": {"content": evidence}}]},
        "Date Generated": {"date": {"start": datetime.now().date().isoformat()}},
    }
    if recommended is not None:
        props["Recommended Price"] = {"number": float(recommended)}
    # Golf Bidder Ceiling = Golf Bidder specific price; fall back to overall dealer ceiling
    effective_ceiling = gb_ceiling if gb_ceiling is not None else ceiling
    if effective_ceiling is not None:
        props["Golf Bidder Ceiling"] = {"number": float(effective_ceiling)}

    try:
        resp = requests.post(
            f"{NOTION_API}/pages",
            headers=_notion_headers(),
            json={"parent": {"database_id": db_id}, "properties": props},
            timeout=15,
        )
        resp.raise_for_status()
        page_id = resp.json()["id"]
        print(f"[notion] Created review page: {title}")
        return page_id
    except Exception as exc:
        body = ""
        try:
            body = exc.response.text if hasattr(exc, "response") and exc.response is not None else ""
        except Exception:
            pass
        print(f"[notion] Failed to create page for {title}: {exc}")
        if body:
            print(f"[notion] Error response body: {body}")
        print(f"[notion] Payload sent: {json.dumps({'properties': list(props.keys())})}")
        return ""


# ---------------------------------------------------------------------------
# CORE PROCESSING LOGIC
# ---------------------------------------------------------------------------

def process_entry(entry: dict) -> dict:
    make = entry["make"]
    model = entry["model"]
    club_type = entry["club_type"]
    condition = entry["condition"]

    keywords = f"{make} {model}"
    is_iron_type = club_type in ("Iron Set", "Wedge Set", "Utility Iron")

    print(f"[brain] Processing: {make} {model} ({club_type}) -- {condition}")

    # Scrape dealer retail prices for ceiling reference
    dealer_prices = scrape_dealer_ceiling_prices(keywords)
    gb_prices = [d["price"] for d in dealer_prices if d["source"] == "Golf Bidder"]
    golfbidder_ceiling = round(min(gb_prices) / 5) * 5 if gb_prices else None
    all_ceiling_prices = [d["price"] for d in dealer_prices]
    overall_ceiling = round(max(all_ceiling_prices) / 5) * 5 if all_ceiling_prices else None

    # Fetch private seller comps (excluding known dealers)
    private_raw = fetch_sold_comps(keywords, seller_filter="exclude_dealers")

    # Determine shaft and club count for filtering
    target_shaft = None  # default: no shaft filter (results will mix, note it)
    target_club_count = None
    if is_iron_type:
        # Try to infer typical club count from first few results
        counts = [ebay.count_clubs(i["title"]) for i in private_raw[:20]]
        counts = [c for c in counts if c is not None]
        if counts:
            from collections import Counter
            target_club_count = Counter(counts).most_common(1)[0][0]

    private_accepted, private_excluded = filter_and_classify_comps(
        private_raw, condition,
        target_club_count=target_club_count,
        target_shaft=target_shaft,
    )

    pricing = compute_pricing(private_accepted)

    _base = {
        "golfbidder_ceiling": golfbidder_ceiling,
        "overall_ceiling": overall_ceiling,
        "dealer_prices": dealer_prices,
        "excluded_comps": private_excluded,
    }

    if pricing is None:
        result = {
            **_base,
            "insufficient_data": True,
            "private_comp_count": len(private_accepted),
            "comp_data": private_accepted,
            "recommended_price": None,
            "confidence": "low",
            "has_year_flags": False,
        }
        print(f"  [brain] Insufficient data ({len(private_accepted)}/{MIN_PRIVATE_COMPS} comps)")
    else:
        result = {
            **pricing,
            **_base,
            "insufficient_data": False,
        }
        print(f"  [brain] Recommended GBP{result['recommended_price']} ({result['confidence']}, {result['private_comp_count']} comps)")

    return result


# ---------------------------------------------------------------------------
# DAILY RUN
# ---------------------------------------------------------------------------

def run_day():
    print()
    print(f"[brain] Brain builder starting -- {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    try:
        init_tables()
        seed_queue()
        reset_queue_cycle()
    except Exception as exc:
        print(f"[brain] DB setup failed: {exc}")
        return

    batch = get_todays_batch()
    if not batch:
        print("[brain] No pending entries in queue")
        return

    print(f"[brain] Processing {len(batch)} entries today")

    for entry in batch:
        queue_id = entry["id"]
        mark_in_progress(queue_id)

        try:
            result = process_entry(entry)
            pending_id = save_pending_price(entry, result)
            if not result.get("insufficient_data"):
                notion_page_id = create_notion_review_page(entry, result)
                if notion_page_id:
                    update_notion_page_id(pending_id, notion_page_id)
            else:
                print(f"  [brain] Skipping Notion page -- insufficient data")
            mark_done(queue_id)
            time.sleep(5)  # throttle gap between entries
        except Exception as exc:
            print(f"  [brain] Error on {entry['make']} {entry['model']}: {exc}")
            mark_done(queue_id)  # mark done anyway to avoid infinite retry
            continue

    print(f"[brain] Day complete -- {len(batch)} entries processed")


if __name__ == "__main__":
    run_day()
