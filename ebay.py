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


def search_sold_comps(keywords: str, token: str) -> dict:
    """Ended UK auction + BIN listings from last 30 days as sold-price comps.

    Returns dict with keys: prices, auction_count, bin_count.

    Auctions: only include if bidCount >= 1 (real hammer prices).
    BINs: ended FIXED_PRICE listings filtered to exclude outliers:
      - if auction comps exist: drop any BIN price > 2x median auction price
      - if no auction comps: only use BINs if 5+ results (reduces noise risk)
    Browse API has no sold-only filter; BIN ended != BIN sold. The 2x ceiling
    and minimum-count guard are heuristics to limit unsold-listing contamination.
    """
    from datetime import timezone as _tz
    import statistics

    now = datetime.now(_tz.utc)
    cutoff = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    now_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    # --- Auction comps (reliable: bidCount >= 1 = real transaction) ---
    auction_prices = []
    for item in _browse_ended(token, keywords, "AUCTION", cutoff, now_str):
        try:
            if int(item.get("bidCount", 0)) < 1:
                continue
            auction_prices.append(float(item["currentBidPrice"]["value"]))
        except (KeyError, TypeError, ValueError):
            continue

    # --- BIN comps (heuristic: ended listings, filtered by outlier ceiling) ---
    bin_raw = []
    for item in _browse_ended(token, keywords, "FIXED_PRICE", cutoff, now_str):
        try:
            bin_raw.append(float(item["price"]["value"]))
        except (KeyError, TypeError, ValueError):
            continue

    bin_prices = []
    if bin_raw:
        if auction_prices:
            median_auction = statistics.median(auction_prices)
            ceiling = median_auction * 2.0
            bin_prices = [p for p in bin_raw if p <= ceiling]
        elif len(bin_raw) >= 5:
            # No auction reference — only trust BINs when we have enough to self-filter
            bin_prices = bin_raw

    return {
        "prices": auction_prices + bin_prices,
        "auction_count": len(auction_prices),
        "bin_count": len(bin_prices),
    }
