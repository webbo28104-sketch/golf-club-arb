import os
import requests
from datetime import date, timedelta
from zoneinfo import ZoneInfo
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"
BROWSE_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
FINDING_URL = "https://svcs.ebay.com/services/search/FindingService/v1"

MARKETPLACE_ID = "EBAY_GB"
FINDING_SITE_ID = "3"  # UK

UK_TZ = ZoneInfo("Europe/London")


def _require_env(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise EnvironmentError(f"Required environment variable {key!r} is not set.")
    return val


def get_access_token() -> str:
    resp = requests.post(
        TOKEN_URL,
        auth=(_require_env("EBAY_CLIENT_ID"), _require_env("EBAY_CLIENT_SECRET")),
        data={"grant_type": "client_credentials", "scope": "https://api.ebay.com/oauth/api_scope"},
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _parse_price(price_dict: dict) -> float:
    return float(price_dict.get("value", 0))


def _get_shipping_cost(item: dict) -> float | None:
    """Returns shipping cost in GBP, or None if collection-only / no shipping info."""
    options = item.get("shippingOptions", [])
    if not options:
        return None
    cost = options[0].get("shippingCost", {})
    if not cost:
        return None
    return float(cost.get("value", 0))


def _build_listing(item: dict, listing_type: str, shipping: float) -> dict:
    price = _parse_price(item.get("price", {}))
    return {
        "item_id": item.get("itemId", ""),
        "title": item.get("title", ""),
        "price": price,
        "shipping_cost": shipping,
        "total_cost": price + shipping,
        "url": item.get("itemWebUrl", ""),
        "listing_type": listing_type,
        "end_time": item.get("itemEndDate"),
        "condition": item.get("condition", ""),
    }


def _browse_all(token: str, extra_filter: str, sort: str = "newlyListed") -> list[dict]:
    import time as _time
    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": MARKETPLACE_ID,
    }
    results = []
    offset = 0
    limit = 200

    while True:
        params = {
            "q": "golf clubs",
            "limit": limit,
            "offset": offset,
            "filter": extra_filter,
            "sort": sort,
            "fieldgroups": "EXTENDED",
        }
        for attempt in range(4):
            resp = requests.get(BROWSE_URL, headers=headers, params=params, timeout=15)
            if resp.status_code == 429:
                wait = 60 * (2 ** attempt)  # 60s, 120s, 240s, 480s
                print(f"[ebay] 429 rate limited — waiting {wait}s before retry {attempt + 1}/4")
                _time.sleep(wait)
                continue
            resp.raise_for_status()
            break
        else:
            print("[ebay] Giving up after 4 rate-limit retries — returning partial results")
            return results

        data = resp.json()
        items = data.get("itemSummaries", [])
        if not items:
            break
        results.extend(items)
        total = int(data.get("total", 0))
        offset += limit
        if offset >= total or offset >= 10000:
            break

    return results


def _uk_day_to_utc_range(day: date) -> tuple[str, str]:
    """Return UTC ISO start/end strings covering a full UK calendar day (BST/GMT aware)."""
    utc = ZoneInfo("UTC")
    start = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=UK_TZ).astimezone(utc)
    end = (start + timedelta(days=1))
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    return start.strftime(fmt), end.strftime(fmt)


def search_bin_listings(token: str, yesterday: date) -> list[dict]:
    """BIN listings posted during the previous UK calendar day."""
    start_str, end_str = _uk_day_to_utc_range(yesterday)

    raw = _browse_all(
        token,
        extra_filter=f"buyingOptions:{{FIXED_PRICE}},itemStartDate:[{start_str}..{end_str}]",
        sort="newlyListed",
    )
    listings = []
    for item in raw:
        shipping = _get_shipping_cost(item)
        if shipping is None:
            continue
        listings.append(_build_listing(item, "BIN", shipping))
    return listings


def search_auction_listings(token: str, tomorrow: date) -> list[dict]:
    """Auctions ending on the next UK calendar day only."""
    start_str, end_str = _uk_day_to_utc_range(tomorrow)

    raw = _browse_all(
        token,
        extra_filter=f"buyingOptions:{{AUCTION}},itemEndDate:[{start_str}..{end_str}]",
    )
    listings = []
    for item in raw:
        shipping = _get_shipping_cost(item)
        if shipping is None:
            continue
        listings.append(_build_listing(item, "Auction", shipping))
    return listings


_IRON_VALUE = {
    "3": 3, "4": 4, "5": 5, "6": 6, "7": 7, "8": 8, "9": 9,
    "pw": 10, "p": 10, "aw": 11, "gw": 11, "sw": 11, "lw": 12, "w": 11,
}

def count_clubs(title: str) -> int | None:
    """Extract number of clubs in a set from a listing title. Returns None if unknown."""
    import re as _re
    tl = title.lower()
    # Match patterns like "4-pw", "5-9", "4-aw", "6-sw" etc.
    m = _re.search(r'\b(\d|pw|aw|gw|sw|lw)\s*[-–]\s*(\d{1,2}|pw|aw|gw|sw|lw)\b', tl)
    if not m:
        return None
    start_str, end_str = m.group(1).strip(), m.group(2).strip()
    start = _IRON_VALUE.get(start_str)
    end = _IRON_VALUE.get(end_str)
    if start is None or end is None or end < start:
        return None
    return end - start + 1


def _browse_ended(token: str, keywords: str, buying_option: str, cutoff: str, now_str: str) -> list[dict]:
    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": MARKETPLACE_ID,
    }
    try:
        resp = requests.get(
            BROWSE_URL,
            headers=headers,
            params={
                "q": keywords,
                "filter": f"buyingOptions:{{{buying_option}}},itemEndDate:[{cutoff}..{now_str}]",
                "limit": 10,
                "sort": "endingSoonest",
            },
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json().get("itemSummaries", [])
    except requests.exceptions.RequestException:
        return []


def search_sold_comps(keywords: str, token: str, listing_title: str = "") -> dict:
    """Ended UK auction + BIN listings from last 30 days as sold-price comps.

    Returns dict with keys: prices, auction_count, bin_count, club_count_unknown.

    Auctions: bidCount >= 1 only (real hammer prices).
    BINs: ended FIXED_PRICE, filtered to <= 2x median auction price.
    Club count: comps are filtered to ±1 of listing club count where detectable.
    """
    from datetime import timezone as _tz
    import statistics

    now = datetime.now(_tz.utc)
    cutoff = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    now_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    listing_club_count = count_clubs(listing_title) if listing_title else None
    club_count_unknown = listing_club_count is None

    def _club_count_ok(title: str) -> bool:
        if listing_club_count is None:
            return True  # unknown — include all
        comp_count = count_clubs(title)
        if comp_count is None:
            return True  # can't determine comp count — give benefit of doubt
        return abs(comp_count - listing_club_count) <= 1

    # --- Auction comps ---
    auction_prices = []
    for item in _browse_ended(token, keywords, "AUCTION", cutoff, now_str):
        try:
            if int(item.get("bidCount", 0)) < 1:
                continue
            if not _club_count_ok(item.get("title", "")):
                continue
            auction_prices.append(float(item["currentBidPrice"]["value"]))
        except (KeyError, TypeError, ValueError):
            continue

    # --- BIN comps ---
    bin_raw = []
    for item in _browse_ended(token, keywords, "FIXED_PRICE", cutoff, now_str):
        try:
            if not _club_count_ok(item.get("title", "")):
                continue
            bin_raw.append(float(item["price"]["value"]))
        except (KeyError, TypeError, ValueError):
            continue

    bin_prices = []
    if bin_raw:
        if auction_prices:
            ceiling = statistics.median(auction_prices) * 2.0
            bin_prices = [p for p in bin_raw if p <= ceiling]
        elif len(bin_raw) >= 5:
            bin_prices = bin_raw

    return {
        "prices": auction_prices + bin_prices,
        "auction_count": len(auction_prices),
        "bin_count": len(bin_prices),
        "club_count_unknown": club_count_unknown,
    }
