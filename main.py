import os
import re
import sys
import schedule
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import ebay
import notion_client as nc

load_dotenv()

UK_TZ = ZoneInfo("Europe/London")
MODE = os.getenv("MODE", "learning")

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


# --- Sold comp keyword extraction ---

GENERIC_TERMS = {"lh", "rh", "left", "right", "handed", "iron", "set", "steel", "graphite",
                 "flex", "shaft", "regular", "stiff", "senior", "store", "new", "used"}

def extract_search_terms(title: str) -> str:
    tl = title.lower()
    found_brand = next((b for b in BRANDS if b in tl), None)
    if found_brand:
        idx = tl.find(found_brand)
        after_brand = [w for w in title[idx + len(found_brand):].strip().split()
                       if w.lower() not in GENERIC_TERMS]
        model = " ".join(after_brand[:2]) if after_brand else ""
        query = f"{found_brand} {model}".strip()
        # Only return if we have brand + at least one meaningful model word
        if len(query.split()) >= 2 and model:
            return query
        return found_brand  # bare brand — caller will skip if too short
    stop = {"used", "golf", "club", "clubs", "good", "excellent", "condition",
            "inc", "with", "great", "lovely"} | GENERIC_TERMS
    meaningful = [w for w in title.split()[:10] if w.lower() not in stop]
    return " ".join(meaningful[:4])


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

            keywords = extract_search_terms(listing["title"])
            if len(keywords.split()) < 2:
                print(f"  ⚠️ Check manually — can't extract model from title")
                print(f"  {listing['title']}")
                insufficient_data += 1
                continue
            comps = ebay.search_sold_comps(keywords, token, listing_title=listing["title"])
            sold_prices = comps["prices"]
            auction_count = comps["auction_count"]
            bin_count = comps["bin_count"]
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
                nc.add_opportunity({**listing, "flag": flag, "avg_sold": avg_sold,
                                    "max_bid": max_bid, "projected_profit": projected_profit,
                                    "roi": roi, "comp_count": len(sold_prices),
                                    "comp_prices": sold_prices,
                                    "auction_count": auction_count,
                                    "bin_count": bin_count,
                                    "club_count_unknown": club_count_unknown,
                                    "filters_relaxed": filters_relaxed,
                                    "comp_query": keywords})
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


def _schedule_midnight_run():
    """Schedule run_scan to fire at midnight UK time every day."""
    import pytz
    london = pytz.timezone("Europe/London")
    schedule.every().day.at("00:00", london).do(run_scan)
    print(f"[scheduler] Next run scheduled for midnight Europe/London.")
    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    if os.getenv("TEST_RUN", "").lower() == "true":
        print("⛳ TEST_RUN mode — running single scan now...")
        try:
            run_scan()
        except Exception as exc:
            print(f"[error] Test scan failed: {exc}")
        print("Test run complete — exiting")
        sys.exit(0)

    print("⛳ Golf Club Arb started — waiting for midnight UK to run scan.")
    _schedule_midnight_run()
