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
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

import ebay

load_dotenv()

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
# EBAY BROWSE API -- SOLD COMPS
# ---------------------------------------------------------------------------

BROWSE_API_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
_browse_token_cache: dict = {}


def _get_browse_token() -> str:
    """Return a cached Browse API OAuth token, refreshing if expired."""
    import time as _time
    now = _time.time()
    if _browse_token_cache.get("token") and now < _browse_token_cache.get("expires_at", 0) - 60:
        return _browse_token_cache["token"]
    token = ebay.get_access_token()
    _browse_token_cache["token"] = token
    _browse_token_cache["expires_at"] = now + 7000  # eBay tokens last ~2h
    return token


def fetch_sold_comps(keywords: str, seller_filter: Optional[str] = None) -> list[dict]:
    """Fetch recently sold eBay UK listings via Browse API."""
    token = _get_browse_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_GB",
        "X-EBAY-C-ENDUSERCTX": "contextualLocation=country=GB",
    }

    filters = "buyingOptions:{AUCTION|FIXED_PRICE},itemLocationCountry:GB,conditions:{USED}"

    all_items: list[dict] = []
    for offset in range(0, 150, 50):  # up to 3 pages of 50 = 150 items
        params = {
            "q": keywords,
            "filter": filters,
            "sort": "-endDate",
            "limit": "50",
            "offset": str(offset),
        }
        print(f"[brain] Browse API: {keywords!r} offset={offset}")
        try:
            resp = requests.get(BROWSE_API_URL, headers=headers, params=params, timeout=20)
            print(f"[brain] Browse API response: {resp.status_code}")
            if resp.status_code == 401:
                # Token expired mid-run — refresh once and retry
                _browse_token_cache.clear()
                token = _get_browse_token()
                headers["Authorization"] = f"Bearer {token}"
                resp = requests.get(BROWSE_API_URL, headers=headers, params=params, timeout=20)
            resp.raise_for_status()
        except Exception as exc:
            print(f"[brain] Browse API error at offset {offset}: {exc}")
            break

        data = resp.json()
        item_summaries = data.get("itemSummaries", [])
        if not item_summaries:
            break

        for item in item_summaries:
            try:
                price_obj = item.get("price", {})
                price = float(price_obj.get("value", 0))
                shipping_options = item.get("shippingOptions", [])
                shipping = 0.0
                if shipping_options:
                    s = shipping_options[0].get("shippingCost", {})
                    shipping = float(s.get("value", 0))

                seller = item.get("seller", {}).get("username", "").lower()
                if seller_filter == "exclude_dealers":
                    if any(d in seller for d in {"golfbidder", "cashforeclubs", "golfavenue"}):
                        continue

                buying_options = item.get("buyingOptions", [])
                listing_type = "Auction" if "AUCTION" in buying_options else "FixedPrice"

                all_items.append({
                    "title": item.get("title", ""),
                    "price": price,
                    "shipping": shipping,
                    "total_price": price + shipping,
                    "bid_count": 0,
                    "listing_type": listing_type,
                    "seller": seller,
                    "feedback_score": 0,
                    "item_id": item.get("itemId", ""),
                    "url": item.get("itemWebUrl", ""),
                    "end_time": item.get("itemEndDate", ""),
                    "condition_text": item.get("condition", ""),
                })
            except (ValueError, TypeError, KeyError):
                continue

        print(f"[brain] Browse API offset {offset}: {len(item_summaries)} items")
        if len(item_summaries) < 50:
            break
        time.sleep(1)

    print(f"[brain] Browse API total: {len(all_items)} items for {keywords!r}")
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

    # Fetch private seller comps via Browse API (excluding known dealers)
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
        "golfbidder_ceiling": None,
        "overall_ceiling": None,
        "dealer_prices": [],
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
