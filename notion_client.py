import os
import requests
from datetime import date
from dotenv import load_dotenv

load_dotenv()

NOTION_API = "https://api.notion.com/v1"
HEADERS = {
    "Authorization": f"Bearer {os.environ['NOTION_TOKEN']}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

WRITE_FLAGS = {"🔥 Strong buy", "👀 Worth a look"}

_BRANDS = [
    ("titleist", "Titleist"),
    ("taylormade", "TaylorMade"),
    ("callaway", "Callaway"),
    ("ping", "Ping"),
    ("mizuno", "Mizuno"),
    ("cobra", "Cobra"),
    ("cleveland", "Cleveland"),
    ("srixon", "Srixon"),
]

_CLUB_TYPES = [
    (["iron set", "irons", "iron"], "Irons Set"),
    (["driver"], "Driver"),
    (["fairway wood", "fairway"], "Fairway Wood"),
    (["hybrid"], "Hybrid"),
    (["wedge"], "Wedge"),
    (["putter"], "Putter"),
]

_CONDITION_MAP = {
    "new": "Excellent",
    "like new": "Excellent",
    "very good": "Very Good",
    "good": "Good",
    "acceptable": "Poor",
}


def get_db_id() -> str:
    db_id = os.environ.get("NOTION_OPPORTUNITY_DB_ID")
    if not db_id:
        raise EnvironmentError(
            "NOTION_OPPORTUNITY_DB_ID is not set. "
            "Add it to your .env file before running."
        )
    return db_id


def _detect_brand(title: str) -> str:
    tl = title.lower()
    for keyword, label in _BRANDS:
        if keyword in tl:
            return label
    return "Other"


def _detect_club_type(title: str) -> str:
    tl = title.lower()
    for keywords, label in _CLUB_TYPES:
        if any(kw in tl for kw in keywords):
            return label
    return "Other"


def _detect_condition(condition_str: str) -> str:
    cl = condition_str.lower()
    for key, label in _CONDITION_MAP.items():
        if key in cl:
            return label
    return "Unknown"


def _build_notes(opp: dict) -> str:
    lines = [f"eBay item ID: {opp['item_id']}"]
    if opp.get("avg_sold") is not None:
        comp_count = opp.get("comp_count", "?")
        auction_count = opp.get("auction_count", "?")
        bin_count = opp.get("bin_count", "?")
        comp_prices = opp.get("comp_prices", [])
        lines.append(f"{comp_count} sold comps ({auction_count} auction, {bin_count} BIN)")
        if comp_prices:
            lines.append(f"range £{min(comp_prices):.0f}–£{max(comp_prices):.0f}")
        lines.append(f"avg £{opp['avg_sold']:.0f}")
    else:
        lines.append(f"Insufficient sold data ({opp.get('comp_count', 0)} comps found)")
    if opp.get("flag") == "⚠️ Check manually" and opp.get("avg_sold") is None:
        lines.append("Caveat: max bid not calculated — verify manually")
    return "\n".join(lines)


def add_opportunity(opp: dict) -> None:
    """Write a listing row to the eBay Opportunity Log. Only call for 🔥, 👀 flags."""
    if opp.get("flag") not in WRITE_FLAGS:
        return

    db_id = get_db_id()
    today = date.today().isoformat()

    props = {
        "Listing Title": {"title": [{"text": {"content": str(opp["title"])}}]},
        "eBay Link": {"url": str(opp["url"])},
        "Buy It Now Price": {"number": float(opp["price"])},
        "Shipping Cost": {"number": float(opp["shipping_cost"])},
        "Total Cost": {"number": float(opp["total_cost"])},
        "Gross Profit": {"number": float(opp["projected_profit"]) if opp.get("projected_profit") is not None else 0.0},
        "ROI %": {"number": float(opp["roi"]) if opp.get("roi") is not None else 0.0},
        "My Max Bid": {"number": float(opp["max_bid"]) if opp.get("max_bid") is not None else 0.0},
        "Flag": {"select": {"name": opp["flag"]}},
        "Club Type": {"select": {"name": _detect_club_type(opp["title"])}},
        "Brand": {"select": {"name": _detect_brand(opp["title"])}},
        "Condition Assessed": {"select": {"name": _detect_condition(opp.get("condition", ""))}},
        "Date Spotted": {"date": {"start": today}},
        "Snipe?": {"checkbox": False},
        "Notes": {"rich_text": [{"text": {"content": _build_notes(opp)}}]},
    }

    if opp.get("listing_type") == "Auction" and opp.get("end_time"):
        props["Auction Ends"] = {"date": {"start": opp["end_time"]}}

    payload = {"parent": {"database_id": db_id}, "properties": props}

    try:
        resp = requests.post(
            f"{NOTION_API}/pages",
            headers=HEADERS,
            json=payload,
        )
        resp.raise_for_status()
        print(f"[notion] ✅ Logged: {opp['title'][:60]}")
    except Exception as exc:
        print(f"[notion] ❌ Failed '{opp['title'][:60]}': {exc} — {getattr(exc, 'response', None) and exc.response.text[:300]}")


def check_already_logged(item_id: str) -> bool:
    """Returns True if this item ID was already logged to Notion today."""
    db_id = get_db_id()
    today = date.today().isoformat()

    payload = {
        "filter": {
            "and": [
                {
                    "property": "Date Spotted",
                    "date": {"equals": today},
                },
                {
                    "property": "Notes",
                    "rich_text": {"contains": item_id},
                },
            ]
        }
    }

    try:
        resp = requests.post(
            f"{NOTION_API}/databases/{db_id}/query",
            headers=HEADERS,
            json=payload,
        )
        resp.raise_for_status()
        return len(resp.json().get("results", [])) > 0
    except Exception as exc:
        print(f"[notion] check_already_logged failed for {item_id}: {exc}")
        return False
