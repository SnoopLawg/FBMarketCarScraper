---
name: analyze-deals
description: Perform an in-depth AI analysis of the top car deals — visit listings, inspect photos, check for red flags, compare market values, fix DB discrepancies, and soft-delete bad deals
---

# Analyze Top Deals

Perform an in-depth AI analysis of the top car deals. Visit each listing, inspect images, read descriptions, check for red flags, and compare against market values. All analysis runs against the **production database** on mothership2.

## Steps

### 1. Pull top deals from the production database

This syncs the production DB from mothership2 (via Tailscale SSH) then pulls deals:
```bash
source venv/bin/activate && python analyze_deals.py pull --count 20
```
The `pull` command automatically runs `scp mothership2:/home/snoop/docker/carscraper/data/marketplace_listings.db` first, along with deleted_listings.txt and favorite_listings.txt.

### 2. Visit and inspect each listing

For each deal (starting from highest score), use the Playwright browser tools to:

- **Navigate** to the listing URL (href)
- **Close** any login/cookie modals that appear
- **Take a screenshot** to inspect the vehicle photos
- **Read the full description** — click "See more" if truncated
- **Check for red flags:**
  - Year/model mismatch between title, description, and structured data
  - Title type discrepancy (listing says clean but description mentions rebuilt/salvage)
  - Price has changed from what we have in DB
  - Suspiciously low price for the year/miles (possible scam or hidden issues)
  - Stock photos or low-quality images
  - Dealer posing as private seller
  - Description mentions mechanical issues, accidents, flood, frame damage
  - Description is in all caps or has excessive emojis (often curbstoners)
  - Car is "paid off" but title type is unknown (could be hiding lien or rebuilt)

### 3. Research market values

For the top 5-10 deals, use WebSearch to look up:
- KBB/Edmunds private party value for that exact year/make/model/miles
- Known reliability issues for that model year
- Whether the price is actually below market or our average is skewed

### 4. Apply corrections to the database

When you find discrepancies, fix them immediately. For PASS verdicts, soft-delete the listing:
```bash
# Fix data discrepancies (corrects listing, syncs to prod)
source venv/bin/activate && python analyze_deals.py fix "<href>" --title_type rebuilt --notes "description says rebuilt title"
source venv/bin/activate && python analyze_deals.py fix "<href>" --price 13000 --notes "price dropped from 14500"
source venv/bin/activate && python analyze_deals.py fix "<href>" --year 2019 --notes "listed as 2021 but description says 2019"

# Soft-delete bad deals (removes from UI + Discord alerts, syncs to prod)
source venv/bin/activate && python analyze_deals.py pass "<href>" --notes "rebuilt title, overpriced for condition"
```
Each `fix` and `pass` command automatically pushes the updated DB and deleted_listings.txt back to production via scp.

### 5. Produce a summary report

Format the final output as a markdown table and detailed breakdown:

**Summary table** with columns: Rank, Car, Price, Score, Verdict (BUY/MAYBE/PASS), Key Issue

**Detailed breakdown** for each listing covering:
- What we found vs what was listed
- Corrections applied to DB
- Market value comparison
- Specific recommendation with offer price (if applicable)

**Overall market insight:**
- Which car model has the best deals right now
- Whether the market is trending buyer-friendly or seller-friendly
- Any patterns in the listings (e.g., lots of rebuilt titles, dealer-dominated, etc.)
