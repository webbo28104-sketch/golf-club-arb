"""One-off script: reprice existing Notion listings with improved comp logic.

Run with: railway run python reprice.py
"""
import os
import sys
import requests
from dotenv import load_dotenv

load_dotenv()

# --- Env ---
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
NOTION_DB_ID = os.environ.get("NOTION_OPPORTUNITY_DB_ID")
if not NOTION_TOKEN or not NOTION_DB_ID:
    sys.exit("[error] NOTION_TOKEN and NOTION_OPPORTUNITY_DB_ID must be set")

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}
NOTION_API = "https://api.notion.com/v1"

# --- Import project modules ---
import ebay
import notion_client as nc
from main import (
    extract_search_terms, calc_max_bid, calc_roi, assess_flag, _get_divisor,
)


def _fetch_all_rows() -> list[dict]:
    rows = []
    payload = {"page_size": 100}
    while True:
        resp = requests.post(
            f"{NOTION_API}/databases/{NOTION_DB_ID}/query",
            headers=NOTION_HEADERS,
            json=payload,
        )
        resp.raise_for_status()
        data = resp.json()
        rows.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        payload["start_cursor"] = data["next_cursor"]
    return rows


def _get_prop(page: dict, name: str, prop_type: str):
    prop = page.get("properties", {}).get(name, {})
    if prop_type == "title":
        parts = prop.get("title", [])
        return "".join(p.get("plain_text", "") for p in parts)
    if prop_type == "url":
        return prop.get("url", "")
    if prop_type == "number":
        return prop.get("number")
    if prop_type == "rich_text":
        parts = prop.get("rich_text", [])
        return "".join(p.get("plain_text", "") for p in parts)
    return None


def _update_row(page_id: str, props: dict):
    resp = requests.patch(
        f"{NOTION_API}/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"properties": props},
    )
    resp.raise_for_status()


def _delete_row(page_id: str):
    resp = requests.patch(
        f"{NOTION_API}/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"archived": True},
    )
    resp.raise_for_status()


def _build_notes(keywords: str, avg_sold: float, comps: dict) -> str:
    from notion_client import _confidence
    comp_prices = comps["prices"]
    comp_count = len(comp_prices)
    auction_count = comps["auction_count"]
    bin_count = comps["bin_count"]
    confidence = _confidence(comp_count, comp_prices)
    line = f"Comp query: {keywords}"
    if comps.get("club_count_unknown"):
        line += " / club count unknown"
    line += f"\n{comp_count} comps ({auction_count} auction, {bin_count} BIN)"
    if comp_prices:
        line += f" / range £{min(comp_prices):.0f}–£{max(comp_prices):.0f}"
    line += f" / avg £{avg_sold:.0f} / confidence: {confidence}"
    return line


def main():
    print("Fetching eBay access token...")
    try:
        token = ebay.get_access_token()
    except Exception as exc:
        sys.exit(f"[error] Could not get eBay token: {exc}")

    print("Fetching all Notion rows...")
    try:
        rows = _fetch_all_rows()
    except Exception as exc:
        sys.exit(f"[error] Could not fetch Notion rows: {exc}")

    print(f"Found {len(rows)} rows to reprice.\n")

    repriced = 0
    deleted = 0
    errors = 0

    for row in rows:
        page_id = row["id"]
        title = _get_prop(row, "Listing Title", "title")
        price = _get_prop(row, "Buy It Now Price", "number") or 0.0
        shipping = _get_prop(row, "Shipping Cost", "number") or 0.0
        total_cost = price + shipping

        if not title:
            print(f"  [skip] Row {page_id[:8]} — no title")
            errors += 1
            continue

        try:
            keywords = extract_search_terms(title)
            if len(keywords.split()) < 2:
                print(f"  [skip] '{title[:60]}' — can't extract model")
                errors += 1
                continue

            comps = ebay.search_sold_comps(keywords, token, listing_title=title)
            sold_prices = comps["prices"]
            auction_count = comps["auction_count"]

            if auction_count < 5:
                print(f"  [skip] '{title[:60]}' — only {auction_count} auction comps")
                errors += 1
                continue

            avg_sold = round(sum(sold_prices) / len(sold_prices), 2)

            # Sanity checks
            if avg_sold > price * 4:
                print(f"  [skip] '{title[:60]}' — comp data suspect (avg £{avg_sold:.0f} vs list £{price:.0f})")
                errors += 1
                continue
            if avg_sold < price * 0.8:
                print(f"  🗑  '{title[:60]}' — overpriced vs comps, deleting")
                _delete_row(page_id)
                deleted += 1
                continue

            max_bid = calc_max_bid(avg_sold, shipping)
            projected_profit = round(avg_sold - total_cost, 2)
            roi = calc_roi(avg_sold, total_cost)
            flag = assess_flag(total_cost, max_bid, avg_sold)

            if flag == "❌ Not viable":
                print(f"  🗑  '{title[:60]}' — not viable, deleting")
                _delete_row(page_id)
                deleted += 1
                continue

            notes = _build_notes(keywords, avg_sold, comps)

            _update_row(page_id, {
                "Expected Revenue": {"number": float(avg_sold)},
                "Gross Profit": {"number": float(projected_profit)},
                "ROI %": {"number": float(roi)},
                "My Max Bid": {"number": float(max_bid)},
                "Flag": {"select": {"name": flag}},
                "Notes": {"rich_text": [{"text": {"content": notes}}]},
            })

            print(f"  ✅ '{title[:60]}' → {flag} | avg £{avg_sold:.0f} | max bid £{max_bid:.0f} | {len(sold_prices)} comps")
            repriced += 1

        except Exception as exc:
            print(f"  [error] '{title[:60]}': {exc}")
            errors += 1
            continue

    print(f"\n{'='*60}")
    print(f"Reprice complete: {repriced} updated, {deleted} deleted (not viable), {errors} skipped/errors")


if __name__ == "__main__":
    main()
