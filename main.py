import os
import re
import sys
import threading
import schedule
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from flask import Flask, jsonify, request
import ebay
import notion_client as nc

load_dotenv()

UK_TZ = ZoneInfo("Europe/London")
MODE = os.getenv("MODE", "learning")
BRAIN_ENABLED = os.getenv("BRAIN_ENABLED", "false").lower() == "true"

# --- Environment validation ---

REQUIRED_VARS = ["EBAY_CLIENT_ID", "EBAY_CLIENT_SECRET", "NOTION_TOKEN", "NOTION_OPPORTUNITY_DB_ID"]


def _check_env():
    missing = [k for k in REQUIRED_VARS if not os.environ.get(k)]
    if missing:
        sys.exit(f"[error] Missing required environment variables: {', '.join(missing)}")


# --- Filters ---

SKIP_OLD = ["persimmon", "vintage", "hickory", "metal wood", "wooden driver", "wound ball"]
SKIP_MIXED = ["mixed", "job lot", "various", "assorted", "bundle"]
SKIP_JUNK = [
    "advertisement", "print ad", "magazine", "poster", "image of",
    "shirt", "polo", "jacket", "clothing", "apparel", "hat", "cap", "glove", "vest", "sweater", "jumper",
    "rangefinder", "range finder", "gps", "trolley", "push cart", "towel", "tee", "ball marker",
    "head cover", "headcover", "club cover", "head only", "shaft only", "grip only",
]
SINGLE_IRON_RE = re.compile(r'\b([3-9]|three|four|five|six|seven|eight|nine)\s*[-\s]?iron\b', re.IGNORECASE)
KEEP_SINGLE_TYPES = ["putter", "wedge", "driver", "hybrid", "fairway wood", "fairway", "wood"]
MAX_TOTAL_COST = 500.0

BRANDS = [
    "titleist", "callaway", "taylormade", "ping", "cobra", "mizuno",
    "cleveland", "srixon", "wilson", "vokey", "scotty cameron", "odyssey",
    "honma", "ben hogan", "tour edge", "adams",
]


def _is_single_iron(title: str) -> bool:
    if not SINGLE_IRON_RE.search(title):
        return False
    tl = title.lower()
    if any(kw in tl for kw in ["set", "irons", "iron set", "half set", "full set"]):
        return False
    if any(kw in tl for kw in KEEP_SINGLE_TYPES):
        return False
    return True


def should_skip(listing: dict) -> tuple[bool, str]:
    tl = listing["title"].lower()
    if _is_single_iron(listing["title"]):
        return True, "single iron"
    if any(kw in tl for kw in SKIP_OLD):
        return True, "vintage/old club"
    if any(kw in tl for kw in SKIP_MIXED):
        return True, "mixed/job lot"
    if any(kw in tl for kw in SKIP_JUNK):
        return True, "junk listing"
    if listing["total_cost"] > MAX_TOTAL_COST:
        return True, f"total £{listing['total_cost']:.2f} exceeds limit"
    return False, ""


# --- Strict comp query extraction ---

# Known models per brand (lowercase for matching)
_KNOWN_MODELS: dict[str, list[str]] = {
    "titleist":       ["t100", "t150", "t200", "t300", "t350", "ap1", "ap2", "ap3", "cb", "mb",
                       "716", "718", "620", "690"],
    "taylormade":     ["p790", "p770", "p760", "p730", "sim2", "sim", "stealth", "qi10", "burner", "brnr"],
    "callaway":       ["x forged", "big bertha", "steelhead", "apex", "rogue", "mavrik", "epic", "razr"],
    "ping":           ["g425", "g410", "g400", "g700", "i230", "i210", "i500", "i525", "blueprint", "s159"],
    "mizuno":         ["jpx 923", "jpx 921", "jpx 919", "jpx923", "jpx921", "jpx919",
                       "mp 20", "mp 18", "mp 25", "pro 223", "pro 225", "pro 241", "pro 243"],
    "cobra":          ["aerojet", "ltdx", "speedzone", "king", "f9"],
    "srixon":         ["zx mk ii", "zx5", "zx7", "z785", "z585"],
    "cleveland":      ["zipcore", "launcher", "rtx"],
    "scotty cameron": ["special select", "white hot", "newport", "phantom", "fastback"],
    "odyssey":        ["tri-hot", "white hot", "eleven", "ten"],
    "wilson":         ["staff model", "d9", "d7"],
    "nike":           ["vr pro", "vr_s", "vapor"],
    "pxg":            ["0311p", "0311t", "0311xf", "0311"],
}

_BRAND_DISPLAY: dict[str, str] = {
    "titleist": "Titleist", "taylormade": "TaylorMade", "callaway": "Callaway",
    "ping": "Ping", "mizuno": "Mizuno", "cobra": "Cobra", "srixon": "Srixon",
    "cleveland": "Cleveland", "scotty cameron": "Scotty Cameron", "odyssey": "Odyssey",
    "wilson": "Wilson", "nike": "Nike", "pxg": "PXG",
}

_MODEL_DISPLAY: dict[str, str] = {
    "t100": "T100", "t150": "T150", "t200": "T200", "t300": "T300", "t350": "T350",
    "ap1": "AP1", "ap2": "AP2", "ap3": "AP3", "cb": "CB", "mb": "MB",
    "716": "716", "718": "718", "620": "620", "690": "690",
    "p790": "P790", "p770": "P770", "p760": "P760", "p730": "P730",
    "sim": "SIM", "sim2": "SIM2", "stealth": "Stealth", "qi10": "Qi10",
    "burner": "Burner", "brnr": "BRNR",
    "apex": "Apex", "rogue": "Rogue", "mavrik": "Mavrik", "epic": "Epic",
    "x forged": "X Forged", "big bertha": "Big Bertha", "steelhead": "Steelhead", "razr": "Razr",
    "g425": "G425", "g410": "G410", "g400": "G400", "g700": "G700",
    "i230": "i230", "i210": "i210", "i500": "i500", "i525": "i525",
    "blueprint": "Blueprint", "s159": "S159",
    "jpx 923": "JPX 923", "jpx 921": "JPX 921", "jpx 919": "JPX 919",
    "jpx923": "JPX 923", "jpx921": "JPX 921", "jpx919": "JPX 919",
    "mp 20": "MP 20", "mp 18": "MP 18", "mp 25": "MP 25",
    "pro 223": "Pro 223", "pro 225": "Pro 225", "pro 241": "Pro 241", "pro 243": "Pro 243",
    "king": "King", "aerojet": "Aerojet", "ltdx": "LTDx", "speedzone": "Speedzone", "f9": "F9",
    "zx5": "ZX5", "zx7": "ZX7", "zx mk ii": "ZX Mk II", "z785": "Z785", "z585": "Z585",
    "launcher": "Launcher", "zipcore": "ZipCore", "rtx": "RTX",
    "newport": "Newport", "phantom": "Phantom", "special select": "Special Select", "fastback": "Fastback",
    "white hot": "White Hot", "tri-hot": "Tri-Hot", "eleven": "Eleven", "ten": "Ten",
    "staff model": "Staff Model", "d9": "D9", "d7": "D7",
    "vr pro": "VR Pro", "vapor": "Vapor", "vr_s": "VR_S",
    "0311": "0311", "0311p": "0311P", "0311t": "0311T", "0311xf": "0311XF",
}

_CLUB_TYPE_PATTERNS = [
    ("irons",        ["irons", "iron set"]),
    ("driver",       ["driver"]),
    ("putter",       ["putter"]),
    ("wedge",        ["wedge"]),
    ("fairway wood", ["fairway wood", "fairway"]),
    ("hybrid",       ["hybrid"]),
]

# Brands whose models are putters by default
_PUTTER_BRANDS = {"scotty cameron", "odyssey"}
# Models that are wedges
_WEDGE_MODELS = {"rtx"}


def extract_comp_query(title: str) -> str:
    """Return '{Brand} {Model} {club_type}' or '{Brand} {club_type}'. Max 4 words.
    Never includes shaft, flex, grip, condition, or year.
    """
    tl = title.lower()

    # Find brand (try multi-word first by sorting longest first)
    found_brand_key: str | None = None
    for brand_key in sorted(_KNOWN_MODELS.keys(), key=len, reverse=True):
        if brand_key in tl:
            found_brand_key = brand_key
            break

    if not found_brand_key:
        return ""

    brand = _BRAND_DISPLAY[found_brand_key]

    # Find model (try multi-word first)
    found_model_key: str | None = None
    for model_key in sorted(_KNOWN_MODELS[found_brand_key], key=len, reverse=True):
        if model_key in tl:
            found_model_key = model_key
            break

    # Detect club type from title
    club_type = ""
    for ct, keywords in _CLUB_TYPE_PATTERNS:
        if any(kw in tl for kw in keywords):
            club_type = ct
            break

    if not club_type:
        if found_brand_key in _PUTTER_BRANDS:
            club_type = "putter"
        elif found_model_key in _WEDGE_MODELS:
            club_type = "wedge"
        else:
            club_type = "irons"

    if found_model_key:
        model = _MODEL_DISPLAY.get(found_model_key, found_model_key.title())
        result = f"{brand} {model} {club_type}"
    else:
        result = f"{brand} {club_type}"

    return " ".join(result.split()[:4])


# --- Pricing ---

def _get_divisor(avg_sold: float) -> float:
    if avg_sold < 50:    return 1.25
    if avg_sold < 100:   return 1.20
    if avg_sold < 200:   return 1.15
    if avg_sold < 350:   return 1.12
    if avg_sold < 600:   return 1.10
    return 1.07


def calc_max_bid(avg_sold: float, shipping_cost: float) -> float:
    return round((avg_sold / _get_divisor(avg_sold)) - shipping_cost, 2)


def calc_roi(avg_sold: float, total_cost: float) -> float:
    if total_cost <= 0:
        return 0.0
    return round((avg_sold - total_cost) / total_cost * 100, 1)


def assess_flag(total_cost: float, max_bid: float, avg_sold: float) -> str:
    profit = avg_sold - total_cost
    if total_cost < max_bid * 0.90:
        return "🔥 Strong buy"
    if total_cost <= max_bid:
        return "👀 Worth a look"
    if profit > 80:
        return "⚠️ Check manually"
    return "❌ Not viable"


# --- Console output ---

def _print_opportunity(listing: dict, avg_sold: float, comp_count: int,
                        max_bid: float, projected_profit: float, roi: float, flag: str):
    print(f"\n  {flag}  {listing['title']}")
    print(f"  URL:      {listing['url']}")
    print(f"  Type:     {listing['listing_type']}", end="")
    if listing["end_time"]:
        print(f"  |  Ends: {listing['end_time']}", end="")
    print()
    print(f"  Price:    £{listing['price']:.2f}  |  Shipping: £{listing['shipping_cost']:.2f}  |  Total: £{listing['total_cost']:.2f}")
    print(f"  Avg sold: £{avg_sold:.2f} from {comp_count} comps  |  Max bid: £{max_bid:.2f}")
    print(f"  Profit:   £{projected_profit:.2f}  |  ROI: {roi}%")


# --- Main scan ---

def run_scan():
    if not BRAIN_ENABLED:
        print("[brain] Listing scanner disabled -- brain not yet enabled. "
              "Set BRAIN_ENABLED=true when pricing coverage is sufficient.")
        return
    _check_env()

    now_uk = datetime.now(UK_TZ)
    print(
        f"\n⛳ Golf Club Arb — {now_uk.strftime('%Y-%m-%d %H:%M %Z')} — "
        f"BIN last 24h + auctions ending next 48h — used UK clubs only"
    )

    try:
        token = ebay.get_access_token()
    except Exception as exc:
        sys.exit(f"[error] Failed to get eBay access token: {exc}")

    # Build price table once at scan start
    price_table, url_table, count_table = ebay.build_price_table(token)

    print(f"Running {len(ebay._SEARCH_QUERIES) * 2} targeted searches (BIN + auction per brand/type)...")
    all_listings = ebay.search_all_listings(token)

    total_found = len(all_listings)
    skipped_filter = 0
    skipped_logged = 0
    not_viable = 0
    written_to_notion = 0
    insufficient_data = 0
    processed = 0

    for listing in all_listings:
        try:
            skip, reason = should_skip(listing)
            if skip:
                print(f"  [skip] {listing['title'][:80]} — {reason}")
                skipped_filter += 1
                continue

            if nc.check_already_logged(listing["item_id"]):
                skipped_logged += 1
                continue

            processed += 1
            if processed % 25 == 0:
                print(f"Processed {processed}/{total_found} listings...")

            comp_query = extract_comp_query(listing["title"])
            if not comp_query:
                print(f"  ⚠️ Check manually — can't extract model from title")
                print(f"  {listing['title']}")
                insufficient_data += 1
                continue

            comps = ebay.lookup_comps_from_table(
                listing["title"], comp_query, price_table, url_table, count_table
            )

            if comps["no_match"]:
                print(f"  ⚠️ Check manually — no comp data: {listing['title'][:70]}")
                insufficient_data += 1
                continue

            sold_prices = comps["prices"]
            auction_count = comps["auction_count"]
            bin_count = comps["bin_count"]
            comp_urls = comps["urls"]
            club_count_unknown = comps["club_count_unknown"]
            filters_relaxed = comps["filters_relaxed"]

            if auction_count < 1:
                insufficient_data += 1
                print(f"\n  ⚠️ Check manually — no auction comps found")
                print(f"  {listing['title']}")
                print(f"  {listing['url']}")
                continue

            avg_sold = round(sum(sold_prices) / len(sold_prices), 2)

            # Price sanity checks
            if avg_sold > listing["price"] * 4:
                print(f"\n  ⚠️ Check manually — comp data suspect (avg £{avg_sold:.0f} is 4x+ listing £{listing['price']:.0f})")
                print(f"  {listing['title']}")
                insufficient_data += 1
                continue
            if avg_sold < listing["price"] * 0.8:
                print(f"  ❌ {listing['title'][:70]} — overpriced vs comps (listing £{listing['price']:.0f}, avg sold £{avg_sold:.0f})")
                not_viable += 1
                continue

            max_bid = calc_max_bid(avg_sold, listing["shipping_cost"])
            projected_profit = round(avg_sold - listing["total_cost"], 2)
            roi = calc_roi(avg_sold, listing["total_cost"])
            flag = assess_flag(listing["total_cost"], max_bid, avg_sold)

            if flag == "❌ Not viable":
                print(f"  ❌ {listing['title'][:70]} — not viable "
                      f"(total £{listing['total_cost']:.2f} > max bid £{max_bid:.2f})")
                not_viable += 1
                continue

            _print_opportunity(listing, avg_sold, len(sold_prices), max_bid, projected_profit, roi, flag)

            if MODE != "learning" and flag in ("🔥 Strong buy", "👀 Worth a look"):
                nc.add_opportunity({
                    **listing,
                    "flag": flag,
                    "avg_sold": avg_sold,
                    "max_bid": max_bid,
                    "projected_profit": projected_profit,
                    "roi": roi,
                    "comp_count": len(sold_prices),
                    "comp_prices": sold_prices,
                    "comp_urls": comp_urls,
                    "auction_count": auction_count,
                    "bin_count": bin_count,
                    "club_count_unknown": club_count_unknown,
                    "filters_relaxed": filters_relaxed,
                    "comp_query": comp_query,
                })
                written_to_notion += 1

        except Exception as exc:
            print(f"  [error] Failed processing '{listing.get('title', '?')[:60]}': {exc}")
            continue

    print(
        f"\nScan complete — {total_found} unique listings found, "
        f"{skipped_filter} skipped (filter), {skipped_logged} skipped (already logged), "
        f"{not_viable} not viable, {written_to_notion} written to Notion "
        f"({insufficient_data} insufficient sold data)"
    )


def _midnight_job():
    import brain_builder
    print("[brain] Running midnight brain build...")
    try:
        brain_builder.run_day()
    except Exception as exc:
        print(f"[error] Brain builder failed: {exc}")
    run_scan()


def _schedule_midnight_run():
    """Schedule brain build + scan to fire at midnight UK time every day."""
    import pytz
    london = pytz.timezone("Europe/London")
    schedule.every().day.at("00:00", london).do(_midnight_job)
    print(f"⛳ Next scheduled run: midnight UK time")
    while True:
        schedule.run_pending()
        time.sleep(30)


# --- Flask web server ---

_flask_app = Flask(__name__)
_BRAIN_RUN_TOKEN = os.getenv("BRAIN_RUN_TOKEN", "Mollybuster456!")


@_flask_app.route("/run-brain")
def _run_brain_endpoint():
    token = request.args.get("token", "")
    if token != _BRAIN_RUN_TOKEN:
        return jsonify({"status": "forbidden", "message": "Invalid token"}), 403

    def _bg():
        import brain_builder
        print("[brain] Manual /run-brain triggered")
        try:
            brain_builder.run_day()
        except Exception as exc:
            print(f"[error] Manual brain run failed: {exc}")

    threading.Thread(target=_bg, daemon=True).start()
    return jsonify({"status": "started", "message": "Brain run triggered - check Railway logs"})


@_flask_app.route("/health")
def _health():
    return jsonify({"status": "ok"})


def _start_flask():
    port = int(os.getenv("PORT", 8080))
    _flask_app.run(host="0.0.0.0", port=port, use_reloader=False)


if __name__ == "__main__":
    import brain_builder

    threading.Thread(target=_start_flask, daemon=True).start()
    print(f"[web] Flask listening on port {os.getenv('PORT', 8080)}")

    print("[brain] Running startup brain build...")
    try:
        brain_builder.run_day()
    except Exception as exc:
        print(f"[error] Brain builder failed: {exc}")

    print("⛳ Startup scan running...")
    try:
        run_scan()
    except Exception as exc:
        print(f"[error] Startup scan failed: {exc}")

    _schedule_midnight_run()
