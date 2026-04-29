# golf-club-arb

Scans eBay for used golf club arbitrage opportunities and logs them to Notion.

## Setup

1. Copy `.env.example` to `.env` and fill in your credentials.
2. Install dependencies: `pip install -r requirements.txt`
3. Run: `python main.py`

## Modes

- `learning` — scans and prints opportunities without writing to Notion
- `live` — writes opportunities to the Notion Opportunity database

## Environment Variables

| Variable | Description |
|---|---|
| `EBAY_CLIENT_ID` | eBay developer app client ID |
| `EBAY_CLIENT_SECRET` | eBay developer app client secret |
| `NOTION_TOKEN` | Notion integration token |
| `NOTION_OPPORTUNITY_DB_ID` | Notion database ID for opportunities |
| `NOTION_CACHE_DB_ID` | Notion database ID for seen item cache |
| `MODE` | `learning` or `live` |
