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
- `scrapers/base.py` — `BaseScraper` ABC with shared Selenium helpers (scrolling, anti-detection delays, stealth JS injection, screenshot-on-error, yield counting via `counted_insert()`)
- `scrapers/facebook.py`, `craigslist.py`, `carscom.py`, `autotrader.py` — each implements `scrape()`, iterates over `DesiredCar` list, calls `counted_insert()` for each listing found
- `scrapers/__init__.py` — `ALL_SCRAPERS` registry dict mapping source name → scraper class
- `driver.py` — Firefox WebDriver factory with anti-detection settings; uses a dedicated Firefox profile (`6kmbn0d4.fbscraper`) for FB cookies
- `scraper_worker.py` — background threading wrapper that exposes status tracking for the web UI to poll; records per-source run metrics to `scrape_runs` table and logs yield health warnings

### Data Layer
- `database.py` — `Database` class wrapping SQLite (`marketplace_listings.db`). Handles schema creation, migrations, and all CRUD. Key tables: `listings`, `average_prices`, `price_history`, `vin_cache`, `vehicle_ratings`, `vehicle_recalls`, `scrape_runs`
- `parsing.py` — shared helpers: `parse_price`, `parse_mileage`, `extract_year`, `parse_owner_count`, `parse_service_history`
- Listings are keyed by `(href, source)` with upsert-on-conflict logic

### Enrichment Layer (all use free public APIs, no keys needed)
- `vin.py` — VIN regex extraction from listing text + NHTSA vPIC batch decoding
- `vin_validate.py` — cross-validates VIN decode data against listing claims (year, make, drivetrain), scores mismatches as major/minor
- `nhtsa.py` — safety ratings, complaint counts, recall data; cached in `vehicle_ratings`/`vehicle_recalls` tables for 30 days
- `epa.py` — fuel economy (MPG) from fueleconomy.gov; cached alongside NHTSA data
- `trim_tiers.py` — maps trim strings to tiers 1-4 (Base → Premium) for scoring
- `drivetrain.py` — detects AWD/4WD/FWD/RWD from listing text with model-specific defaults

### Analysis & Scoring
- `analysis.py` — `clean_listings()` removes mismatches, `calculate_averages()` computes per-year per-title-group averages split by mileage bucket, `find_deals()` scores each listing
- Deal scores (0-100) are composed of 7 factors totaling 100 points:
  - **Price vs Average (30pts):** Square-root curve; moderate discounts score well
  - **Title & Condition (25pts):** Title base (clean=15, unknown=3, bad=0) + accident history (+5 to -3) + owner count (1-owner=+2, unknown=+0.5) + service history (records=+1.5) + deal rating (+0.5 to +1.5)
  - **Mileage (15pts):** Age-relative (12k mi/yr baseline) + make-specific lifespan bonus/penalty
  - **Reliability (10pts):** NHTSA stars + complaint thresholds
  - **Drivetrain (10pts):** AWD/4WD bonus (explicit=10, inferred=6)
  - **Trim Value (5pts):** Higher trim at a discount
  - **Freshness (5pts):** Days since first scraped
- Hard score caps: rebuilt≤45, salvage≤30, lemon≤25
- VIN cross-validation penalty (up to -15) applied before caps
- Owner count and service history parsed from listing descriptions via `parsing.py`

### Scraper Monitoring
- `scrape_runs` table tracks every scrape run per source: start/end time, status, listings_found, errors, screenshot_path, duration
- After each source completes, yield is compared to historical average — warnings logged at <50%, critical at <20%
- On scraper crash, a browser screenshot is saved to `screenshots/` for debugging
- `/api/health` endpoint returns per-source health (good/warning/critical) and recent run history
- Analytics page renders health badges and a recent runs table with yield-vs-average percentages

### Web UI
- `web_ui.py` — Flask app with routes for deals list, favorites, analytics, settings, and scraper health
- `templates/` — Jinja2 templates (`base.html`, `deals.html`, `favorites.html`, `analytics.html`, `settings.html`, `_deal_card.html` partial)
- State files: `favorite_listings.txt`, `deleted_listings.txt` (line-delimited href sets)

## Key Patterns

- **Title groups:** Listings are grouped by title status (clean, rebuilt, salvage, lemon) for fair average comparison — a rebuilt car is scored against other rebuilt cars
- **Per-car keyword searches:** Each scraper iterates `DesiredCar` list and runs a separate search per car model, not a single broad search
- **Anti-detection:** Randomized delays, scroll patterns, and stealth JS injection throughout scrapers to avoid bot detection
- **Upsert with price tracking:** Re-scraped listings update in place; price changes are logged to `price_history`
- **Stale marking:** Listings not seen in 7 days get soft-deleted (`deleted_at` set)
- **Accident penalty:** Accident-reported cars receive a -3 penalty (not zero) in the condition factor — reflects 20-40% real-world value impact
- **Description parsing:** Owner count and service history signals are extracted from listing descriptions and car names via regex patterns in `parsing.py`, then fed into the scoring pipeline
- **Yield tracking:** Every scraper run records metrics to `scrape_runs`; health checks compare yield to historical norms and flag breakage
- **Screenshot on error:** `BaseScraper.capture_screenshot()` saves browser state to `screenshots/` when a scraper crashes, aiding diagnosis of selector changes or bot detection
