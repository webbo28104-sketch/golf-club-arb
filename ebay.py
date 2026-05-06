import os
import time
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
FINDING_SITE_ID = "3"

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


def _browse_get(headers: dict, params: dict) -> requests.Response | None:
    time.sleep(2)
    for attempt in range(3):
        resp = requests.get(BROWSE_URL, headers=headers, params=params, timeout=15)
        if resp.status_code == 429:
            waits = [60, 120, 300]
            wait = waits[attempt]
            print(f"[ebay] 429 rate limited — waiting {wait}s before retry {attempt + 1}/3")
            time.sleep(wait)
            continue
        return resp
    print("[ebay] Giving up after 3 rate-limit retries")
    return None


_SEARCH_QUERIES = [
    "Callaway irons", "Cleveland irons", "Cobra irons", "Mizuno irons",
    "Nike irons", "Ping irons", "PXG irons", "TaylorMade irons",
    "Titleist irons", "Wilson irons", "Wilson Staff irons", "Miura irons",
    "Callaway driver", "Cobra driver", "Mizuno driver", "Nike driver",
    "Ping driver", "PXG driver", "TaylorMade driver", "Titleist driver",
    "Wilson driver",
    "Callaway fairway wood", "Ping fairway wood", "TaylorMade fairway wood",
    "Titleist fairway wood", "Cobra fairway wood",
    "Callaway hybrid", "Ping hybrid", "TaylorMade hybrid", "Titleist hybrid",
    "Mizuno hybrid", "Cobra hybrid",
    "Scotty Cameron putter", "Odyssey putter", "Ping putter",
    "TaylorMade putter", "Callaway putter", "Cleveland putter",
    "Callaway wedge", "Cleveland wedge", "Titleist Vokey wedge",
    "TaylorMade wedge", "Ping wedge", "Mizuno wedge",
    "Callaway golf set", "Ping golf set", "TaylorMade golf set",
    "Titleist golf set", "Cobra golf set",
]


def _browse_search(token: str, query: str, extra_filter: str, sort: str) -> list[dict]:
    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": MARKETPLACE_ID,
    }
    results = []
    offset = 0
    limit = 50

    while True:
        params = {
            "q": query,
            "limit": limit,
            "offset": offset,
            "filter": extra_filter,
            "sort": sort,
            "fieldgroups": "EXTENDED",
        }
        resp = _browse_get(headers, params)
        if resp is None:
            return results
        try:
            resp.raise_for_status()
        except Exception:
            return results

        data = resp.json()
        items = data.get("itemSummaries", [])
        if not items:
            break
        results.extend(items)
        total = int(data.get("total", 0))
        offset += limit
        if offset >= total:
            break
        time.sleep(5)

    return results


def _utc_range(start_dt: datetime, end_dt: datetime) -> tuple[str, str]:
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    return start_dt.strftime(fmt), end_dt.strftime(fmt)


def search_all_listings(token: str) -> list[dict]:
    from datetime import timezone as _tz
    now = datetime.now(_tz.utc)
    bin_start, bin_end = _utc_range(now - timedelta(hours=24), now)
    auc_start, auc_end = _utc_range(now, now + timedelta(hours=48))

    bin_filter = (
        f"buyingOptions:{{FIXED_PRICE}},"
        f"conditions:{{USED}},"
        f"itemStartDate:[{bin_start}..{bin_end}]"
    )
    auc_filter = (
        f"buyingOptions:{{AUCTION}},"
        f"conditions:{{USED}},"
        f"itemEndDate:[{auc_start}..{auc_end}]"
    )

    seen: dict[str, dict] = {}
    total_queries = len(_SEARCH_QUERIES) * 2
    done = 0

    for query in _SEARCH_QUERIES:
        for listing_type, extra_filter, sort in [
            ("BIN",     bin_filter, "newlyListed"),
            ("Auction", auc_filter, "endingSoonest"),
        ]:
            raw = _browse_search(token, query, extra_filter, sort)
            done += 1
            for item in raw:
                item_id = item.get("itemId", "")
                if not item_id or item_id in seen:
                    continue
                shipping = _get_shipping_cost(item)
                if shipping is None:
                    continue
                seen[item_id] = _build_listing(item, listing_type, shipping)
            print(f"  [{done}/{total_queries}] {listing_type}: {query!r} -> {len(raw)} results")

    return list(seen.values())


_IRON_VALUE = {
    "3": 3, "4": 4, "5": 5, "6": 6, "7": 7, "8": 8, "9": 9,
    "pw": 10, "p": 10, "aw": 11, "gw": 11, "sw": 11, "lw": 12, "w": 11,
}


def count_clubs(title: str) -> int | None:
    import re as _re
    tl = title.lower()
    m = _re.search(r'\b(\d|pw|aw|gw|sw|lw)\s*[-]\s*(\d{1,2}|pw|aw|gw|sw|lw)\b', tl)
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
    resp = _browse_get(headers, {
        "q": keywords,
        "filter": f"buyingOptions:{{{buying_option}}},itemEndDate:[{cutoff}..{now_str}]",
        "limit": 10,
        "sort": "endingSoonest",
        "fieldgroups": "EXTENDED",
    })
    if resp is None:
        return []
    try:
        resp.raise_for_status()
        return resp.json().get("itemSummaries", [])
    except requests.exceptions.RequestException:
        return []


_LH_TERMS = {"left handed", "left-handed", " lh ", "lh ", " lh"}
_RH_TERMS = {"right handed", "right-handed", " rh ", "rh ", " rh"}
_STEEL_TERMS = {"steel", "dynamic gold", "kbs", "modus", "px", "project x", "true temper"}
_GRAPHITE_TERMS = {"graphite", "aldila", "fujikura", "mitsubishi", "tensei", "fubuki"}


def _detect_handedness(title: str) -> str:
    tl = " " + title.lower() + " "
    if any(t in tl for t in _LH_TERMS):
        return "LH"
    if any(t in tl for t in _RH_TERMS):
        return "RH"
    return ""


def _detect_shaft(title: str) -> str:
    tl = title.lower()
    if any(t in tl for t in _STEEL_TERMS):
        return "steel"
    if any(t in tl for t in _GRAPHITE_TERMS):
        return "graphite"
    return ""


def search_sold_comps(keywords: str, token: str, listing_title: str = "") -> dict:
    from datetime import timezone as _tz
    import statistics

    now = datetime.now(_tz.utc)
    cutoff = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    now_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    listing_hand = _detect_handedness(listing_title)
    listing_shaft = _detect_shaft(listing_title)
    listing_club_count = count_clubs(listing_title) if listing_title else None
    club_count_unknown = listing_club_count is None

    def _passes(title: str, check_hand: bool, check_shaft: bool, check_count: bool) -> bool:
        if check_hand and listing_hand:
            comp_hand = _detect_handedness(title)
            if comp_hand and comp_hand != listing_hand:
                return False
        if check_shaft and listing_shaft:
            comp_shaft = _detect_shaft(title)
            if comp_shaft and comp_shaft != listing_shaft:
                return False
        if check_count and listing_club_count is not None:
            comp_count = count_clubs(title)
            if comp_count is not None and abs(comp_count - listing_club_count) > 1:
                return False
        return True

    raw_auction = _browse_ended(token, keywords, "AUCTION", cutoff, now_str)
    raw_bin = _browse_ended(token, keywords, "FIXED_PRICE", cutoff, now_str)

    def _extract(check_hand: bool, check_shaft: bool, check_count: bool):
        auctions = []
        for item in raw_auction:
            try:
                if int(item.get("bidCount", 0)) < 1:
                    continue
                if not _passes(item.get("title", ""), check_hand, check_shaft, check_count):
                    continue
                auctions.append(float(item["currentBidPrice"]["value"]))
            except (KeyError, TypeError, ValueError):
                continue

        bins_raw = []
        for item in raw_bin:
            try:
                if not _passes(item.get("title", ""), check_hand, check_shaft, check_count):
                    continue
                bins_raw.append(float(item["price"]["value"]))
            except (KeyError, TypeError, ValueError):
                continue

        bins = []
        if bins_raw:
            if auctions:
                ceiling = statistics.median(auctions) * 2.0
                bins = [p for p in bins_raw if p <= ceiling]
            elif len(bins_raw) >= 5:
                bins = bins_raw

        return auctions, bins

    filters_relaxed = []
    for check_hand, check_shaft, check_count, note in [
        (True,  True,  True,  None),
        (True,  False, True,  "shaft filter relaxed"),
        (True,  False, False, "shaft+club count relaxed"),
        (False, False, False, "all filters relaxed"),
    ]:
        auction_prices, bin_prices = _extract(check_hand, check_shaft, check_count)
        if len(auction_prices) >= 1:
            if note:
                filters_relaxed.append(note)
            break
        if note:
            filters_relaxed.append(note)

    return {
        "prices": auction_prices + bin_prices,
        "auction_count": len(auction_prices),
        "bin_count": len(bin_prices),
        "club_count_unknown": club_count_unknown,
        "filters_relaxed": filters_relaxed,
    }


# ---------------------------------------------------------------------------
# PRE-BUILT COMP PRICE TABLE
# ---------------------------------------------------------------------------

_PRICE_TABLE_COMBOS = [
    ("Titleist T100 irons",    "Titleist T100 irons"),
    ("Titleist T150 irons",    "Titleist T150 irons"),
    ("Titleist T200 irons",    "Titleist T200 irons"),
    ("Titleist T300 irons",    "Titleist T300 irons"),
    ("Titleist T350 irons",    "Titleist T350 irons"),
    ("Titleist AP1 irons",     "Titleist AP1 irons"),
    ("Titleist AP2 irons",     "Titleist AP2 irons"),
    ("Titleist AP3 irons",     "Titleist AP3 irons"),
    ("Titleist CB irons",      "Titleist CB irons"),
    ("Titleist MB irons",      "Titleist MB irons"),
    ("Titleist 716 irons",     "Titleist 716 irons"),
    ("Titleist 718 irons",     "Titleist 718 irons"),
    ("Titleist 620 irons",     "Titleist 620 irons"),
    ("Titleist 690 irons",     "Titleist 690 irons"),
    ("TaylorMade P790 irons",  "TaylorMade P790 irons"),
    ("TaylorMade P770 irons",  "TaylorMade P770 irons"),
    ("TaylorMade P760 irons",  "TaylorMade P760 irons"),
    ("TaylorMade P730 irons",  "TaylorMade P730 irons"),
    ("TaylorMade SIM irons",   "TaylorMade SIM irons"),
    ("TaylorMade SIM2 irons",  "TaylorMade SIM2 irons"),
    ("TaylorMade Stealth irons","TaylorMade Stealth irons"),
    ("TaylorMade Qi10 irons",  "TaylorMade Qi10 irons"),
    ("TaylorMade Burner irons","TaylorMade Burner irons"),
    ("TaylorMade BRNR irons",  "TaylorMade BRNR irons"),
    ("Callaway Apex irons",    "Callaway Apex irons"),
    ("Callaway Rogue irons",   "Callaway Rogue irons"),
    ("Callaway Mavrik irons",  "Callaway Mavrik irons"),
    ("Callaway Epic irons",    "Callaway Epic irons"),
    ("Callaway X Forged irons","Callaway X Forged irons"),
    ("Callaway Big Bertha irons","Callaway Big Bertha irons"),
    ("Callaway Steelhead irons","Callaway Steelhead irons"),
    ("Callaway Razr irons",    "Callaway Razr irons"),
    ("Ping G425 irons",        "Ping G425 irons"),
    ("Ping G410 irons",        "Ping G410 irons"),
    ("Ping G400 irons",        "Ping G400 irons"),
    ("Ping G700 irons",        "Ping G700 irons"),
    ("Ping i230 irons",        "Ping i230 irons"),
    ("Ping i210 irons",        "Ping i210 irons"),
    ("Ping i500 irons",        "Ping i500 irons"),
    ("Ping i525 irons",        "Ping i525 irons"),
    ("Ping Blueprint irons",   "Ping Blueprint irons"),
    ("Ping S159 irons",        "Ping S159 irons"),
    ("Mizuno JPX 923 irons",   "Mizuno JPX 923 irons"),
    ("Mizuno JPX 921 irons",   "Mizuno JPX 921 irons"),
    ("Mizuno JPX 919 irons",   "Mizuno JPX 919 irons"),
    ("Mizuno MP 20 irons",     "Mizuno MP 20 irons"),
    ("Mizuno MP 18 irons",     "Mizuno MP 18 irons"),
    ("Mizuno MP 25 irons",     "Mizuno MP 25 irons"),
    ("Mizuno Pro 223 irons",   "Mizuno Pro 223 irons"),
    ("Mizuno Pro 225 irons",   "Mizuno Pro 225 irons"),
    ("Mizuno Pro 241 irons",   "Mizuno Pro 241 irons"),
    ("Mizuno Pro 243 irons",   "Mizuno Pro 243 irons"),
    ("Cobra King irons",       "Cobra King irons"),
    ("Cobra Aerojet irons",    "Cobra Aerojet irons"),
    ("Cobra LTDx irons",       "Cobra LTDx irons"),
    ("Cobra Speedzone irons",  "Cobra Speedzone irons"),
    ("Cobra F9 irons",         "Cobra F9 irons"),
    ("Srixon ZX5 irons",       "Srixon ZX5 irons"),
    ("Srixon ZX7 irons",       "Srixon ZX7 irons"),
    ("Srixon ZX Mk II irons",  "Srixon ZX Mk II irons"),
    ("Srixon Z785 irons",      "Srixon Z785 irons"),
    ("Srixon Z585 irons",      "Srixon Z585 irons"),
    ("Cleveland Launcher irons","Cleveland Launcher irons"),
    ("Cleveland ZipCore irons","Cleveland ZipCore irons"),
    ("Cleveland RTX wedge",    "Cleveland RTX wedge"),
    ("Scotty Cameron Newport putter",       "Scotty Cameron Newport putter"),
    ("Scotty Cameron Phantom putter",       "Scotty Cameron Phantom putter"),
    ("Scotty Cameron Special Select putter","Scotty Cameron Special Select putter"),
    ("Scotty Cameron Fastback putter",      "Scotty Cameron Fastback putter"),
    ("Odyssey White Hot putter","Odyssey White Hot putter"),
    ("Odyssey Tri-Hot putter", "Odyssey Tri-Hot putter"),
    ("Odyssey Eleven putter",  "Odyssey Eleven putter"),
    ("Odyssey Ten putter",     "Odyssey Ten putter"),
    ("Wilson Staff Model irons","Wilson Staff Model irons"),
    ("Wilson D9 irons",        "Wilson D9 irons"),
    ("Wilson D7 irons",        "Wilson D7 irons"),
    ("Nike VR Pro irons",      "Nike VR Pro irons"),
    ("Nike Vapor irons",       "Nike Vapor irons"),
    ("Nike VR_S irons",        "Nike VR_S irons"),
    ("PXG 0311 irons",         "PXG 0311 irons"),
    ("PXG 0311P irons",        "PXG 0311P irons"),
    ("PXG 0311T irons",        "PXG 0311T irons"),
    ("PXG 0311XF irons",       "PXG 0311XF irons"),
    ("Titleist irons",         "Titleist irons"),
    ("Titleist driver",        "Titleist driver"),
    ("Titleist putter",        "Titleist putter"),
    ("TaylorMade irons",       "TaylorMade irons"),
    ("TaylorMade driver",      "TaylorMade driver"),
    ("TaylorMade putter",      "TaylorMade putter"),
    ("Callaway irons",         "Callaway irons"),
    ("Callaway driver",        "Callaway driver"),
    ("Callaway putter",        "Callaway putter"),
    ("Ping irons",             "Ping irons"),
    ("Ping driver",            "Ping driver"),
    ("Ping putter",            "Ping putter"),
    ("Mizuno irons",           "Mizuno irons"),
    ("Mizuno driver",          "Mizuno driver"),
    ("Cobra irons",            "Cobra irons"),
    ("Cobra driver",           "Cobra driver"),
    ("Srixon irons",           "Srixon irons"),
    ("Cleveland irons",        "Cleveland irons"),
    ("Cleveland wedge",        "Cleveland wedge"),
    ("Scotty Cameron putter",  "Scotty Cameron putter"),
    ("Odyssey putter",         "Odyssey putter"),
    ("Wilson irons",           "Wilson irons"),
    ("PXG irons",              "PXG irons"),
]


def _remove_outliers(prices: list, urls: list, counts: list) -> tuple[list, list, list]:
    import statistics
    if len(prices) < 2:
        return prices, urls, counts
    med = statistics.median(prices)
    lo, hi = med * 0.25, med * 3.0
    filtered = [(p, u, c) for p, u, c in zip(prices, urls, counts) if lo <= p <= hi]
    if not filtered:
        return prices, urls, counts
    fp, fu, fc = zip(*filtered)
    return list(fp), list(fu), list(fc)


def build_price_table(token: str) -> tuple[dict, dict, dict]:
    from datetime import timezone as _tz

    now = datetime.now(_tz.utc)
    cutoff = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    now_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    price_table: dict[str, list] = {}
    url_table: dict[str, list] = {}
    count_table: dict[str, list] = {}

    total = len(_PRICE_TABLE_COMBOS)
    total_comps = 0

    print(f"Building price table: {total} brand/model combinations...")

    for i, (key, query) in enumerate(_PRICE_TABLE_COMBOS):
        raw = _browse_ended(token, query, "AUCTION", cutoff, now_str)

        prices: list[float] = []
        urls: list[str] = []
        counts: list[int | None] = []

        for item in raw:
            try:
                if int(item.get("bidCount", 0)) < 1:
                    continue
                price = float(item["currentBidPrice"]["value"])
                url = item.get("itemWebUrl", "")
                cnt = count_clubs(item.get("title", ""))
                prices.append(price)
                urls.append(url)
                counts.append(cnt)
            except (KeyError, TypeError, ValueError):
                continue

        if len(prices) >= 2:
            prices, urls, counts = _remove_outliers(prices, urls, counts)

        price_table[key] = prices
        url_table[key] = urls
        count_table[key] = counts
        total_comps += len(prices)

        if (i + 1) % 20 == 0 or (i + 1) == total:
            print(f"  [{i + 1}/{total}] {total_comps} comps so far...")

        time.sleep(1)

    print(f"Price table built: {total} combinations, {total_comps} total comps")
    return price_table, url_table, count_table


def lookup_comps_from_table(
    title: str,
    comp_query: str,
    price_table: dict,
    url_table: dict,
    count_table: dict,
) -> dict:
    listing_count = count_clubs(title)
    is_iron_set = listing_count is not None and listing_count >= 4

    key = comp_query
    if key not in price_table or not price_table[key]:
        parts = comp_query.split()
        brand = parts[0] if parts else ""
        if len(parts) > 1 and f"{parts[0]} {parts[1]}".lower() in ("scotty cameron",):
            brand = f"{parts[0]} {parts[1]}"
        tl = title.lower()
        matched = False
        for ct in ["irons", "putter", "driver", "wedge", "fairway wood", "hybrid"]:
            if ct in tl:
                generic_key = f"{brand} {ct}"
                if generic_key in price_table and price_table[generic_key]:
                    key = generic_key
                    matched = True
                    break
        if not matched:
            generic_key = f"{brand} irons"
            if generic_key in price_table and price_table[generic_key]:
                key = generic_key

    if key not in price_table or not price_table[key]:
        return {
            "prices": [], "urls": [], "auction_count": 0, "bin_count": 0,
            "club_count_unknown": listing_count is None,
            "filters_relaxed": [], "no_match": True,
        }

    prices = list(price_table[key])
    urls = list(url_table[key])
    counts = list(count_table[key])
    filters_relaxed = []

    if listing_count is not None and prices:
        filtered = [
            (p, u, c) for p, u, c in zip(prices, urls, counts)
            if c is None or abs(c - listing_count) <= 1
        ]
        if filtered:
            prices, urls, counts = map(list, zip(*filtered))
        else:
            filters_relaxed.append("club count filter relaxed")

    if is_iron_set and prices:
        filtered2 = [(p, u, c) for p, u, c in zip(prices, urls, counts) if p >= 30]
        if filtered2:
            prices, urls, counts = map(list, zip(*filtered2))

    if len(prices) >= 2:
        prices, urls, counts = _remove_outliers(prices, urls, counts)

    return {
        "prices": prices,
        "urls": urls[:5],
        "auction_count": len(prices),
        "bin_count": 0,
        "club_count_unknown": listing_count is None,
        "filters_relaxed": filters_relaxed,
        "no_match": False,
    }
