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
        resp = requests.get(BROWSE_URL, headers=headers, params=params)
        resp.raise_for_status()
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


def search_sold_comps(keywords: str) -> list[float]:
    """eBay UK sold listings from last 30 days — returns list of sold prices in GBP."""
    import time
    utc = ZoneInfo("UTC")
    cutoff = (datetime.now(utc) - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    params = {
        "OPERATION-NAME": "findCompletedItems",
        "SERVICE-VERSION": "1.0.0",
        "SECURITY-APPNAME": _require_env("EBAY_CLIENT_ID"),
        "RESPONSE-DATA-FORMAT": "JSON",
        "REST-PAYLOAD": "",
        "keywords": keywords,
        "itemFilter(0).name": "SoldItemsOnly",
        "itemFilter(0).value": "true",
        "itemFilter(1).name": "EndTimeFrom",
        "itemFilter(1).value": cutoff,
        "itemFilter(2).name": "Currency",
        "itemFilter(2).value": "GBP",
        "sortOrder": "EndTimeSoonest",
        "paginationInput.entriesPerPage": "10",
        "siteid": FINDING_SITE_ID,
    }

    for attempt in range(2):
        try:
            resp = requests.get(FINDING_URL, params=params, timeout=10)
            if resp.status_code in (500, 502, 503, 504):
                if attempt == 0:
                    time.sleep(2)
                    continue
                return []  # Finding API unavailable — treat as no comps
            resp.raise_for_status()
            break
        except requests.exceptions.RequestException:
            if attempt == 0:
                time.sleep(2)
                continue
            return []
    else:
        return []

    try:
        data = resp.json()
        items = data["findCompletedItemsResponse"][0]["searchResult"][0].get("item", [])
    except (KeyError, IndexError, ValueError):
        return []

    prices = []
    for item in items:
        try:
            price = float(item["sellingStatus"][0]["currentPrice"][0]["__value__"])
            prices.append(price)
        except (KeyError, IndexError, ValueError):
            continue

    return prices
