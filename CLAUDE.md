# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A multi-source used car deal finder. It scrapes listings from Facebook Marketplace, Craigslist, Cars.com, and Autotrader, enriches them with VIN decoding, NHTSA safety/recall data, and EPA fuel economy data, scores deals, and presents results in a Flask web UI.

## Running

```bash
# Activate venv (Python 3.14)
source venv/bin/activate

# Full pipeline: scrape → analyze → launch web UI
python main.py

# Skip scraping, just re-analyze existing data and launch UI
python main.py --ui-only
```

The web UI launches at `http://127.0.0.1:5000` and auto-opens in a browser.

## Configuration

`Config.json` — defines desired cars, price range, location, and which sources to enable. Required keys: `DesiredCar`, `MinPrice`, `MaxPrice`, `PriceThreshold`. The `Sources` block controls which scrapers run and their source-specific settings (CityID, region, zip, etc.).

## Architecture

**Pipeline flow:** `main.py` → scrapers → database → analysis → web UI

### Scraping Layer
- `scrapers/base.py` — `BaseScraper` ABC with shared Selenium helpers (scrolling, anti-detection delays, stealth JS injection)
- `scrapers/facebook.py`, `craigslist.py`, `carscom.py`, `autotrader.py` — each implements `scrape()`, iterates over `DesiredCar` list, calls `insert_fn` for each listing found
- `scrapers/__init__.py` — `ALL_SCRAPERS` registry dict mapping source name → scraper class
- `driver.py` — Firefox WebDriver factory with anti-detection settings; uses a dedicated Firefox profile (`6kmbn0d4.fbscraper`) for FB cookies
- `scraper_worker.py` — background threading wrapper that exposes status tracking for the web UI to poll

### Data Layer
- `database.py` — `Database` class wrapping SQLite (`marketplace_listings.db`). Handles schema creation, migrations, and all CRUD. Key tables: `listings`, `average_prices`, `price_history`, `vin_cache`, `vehicle_ratings`, `vehicle_recalls`
- `parsing.py` — shared helpers: `parse_price`, `parse_mileage`, `extract_year`
- Listings are keyed by `(href, source)` with upsert-on-conflict logic

### Enrichment Layer (all use free public APIs, no keys needed)
- `vin.py` — VIN regex extraction from listing text + NHTSA vPIC batch decoding
- `vin_validate.py` — cross-validates VIN decode data against listing claims (year, make, drivetrain), scores mismatches as major/minor
- `nhtsa.py` — safety ratings, complaint counts, recall data; cached in `vehicle_ratings`/`vehicle_recalls` tables for 30 days
- `epa.py` — fuel economy (MPG) from fueleconomy.gov; cached alongside NHTSA data
- `trim_tiers.py` — maps trim strings to tiers 1-4 (Base → Premium) for scoring
- `drivetrain.py` — detects AWD/4WD/FWD/RWD from listing text with model-specific defaults

### Analysis & Scoring
- `analysis.py` — `clean_listings()` removes mismatches, `calculate_averages()` computes per-year per-title-group averages split by mileage bucket, `find_deals()` scores each listing. Deal scores incorporate price-vs-average, mileage, safety ratings, recalls, MPG, trim tier, drivetrain, and VIN validation penalties

### Web UI
- `web_ui.py` — Flask app with routes for deals list, favorites, analytics, and settings
- `templates/` — Jinja2 templates (`base.html`, `deals.html`, `favorites.html`, `analytics.html`, `settings.html`, `_deal_card.html` partial)
- State files: `favorite_listings.txt`, `deleted_listings.txt` (line-delimited href sets)

## Key Patterns

- **Title groups:** Listings are grouped by title status (clean, rebuilt, salvage, lemon) for fair average comparison — a rebuilt car is scored against other rebuilt cars
- **Per-car keyword searches:** Each scraper iterates `DesiredCar` list and runs a separate search per car model, not a single broad search
- **Anti-detection:** Randomized delays, scroll patterns, and stealth JS injection throughout scrapers to avoid bot detection
- **Upsert with price tracking:** Re-scraped listings update in place; price changes are logged to `price_history`
- **Stale marking:** Listings not seen in 7 days get soft-deleted (`deleted_at` set)
