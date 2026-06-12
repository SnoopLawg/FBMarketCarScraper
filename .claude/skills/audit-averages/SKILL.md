---
name: audit-averages
description: Data-quality watchdog for deal pricing. Finds thin/skewed comp pools and cross-checks our expected prices against external valuations (KBB/CarGurus) to catch false deals before they reach the board. Run periodically or when a deal looks too good.
---

# Audit Averages

Catches the false-deal class: an inflated or thin comp pool makes an
overpriced car look like a steal (a 2014 RAV4 once priced at $20k vs a real
~$13k). Surfaces where our pricing diverges from reality so it can be fixed.

All analysis runs against the production DB (sync first, same as analyze-deals).

## Steps

### 1. Sync the production DB
```bash
source venv/bin/activate && python analyze_deals.py pull --count 1 >/dev/null
```

### 2. Pool-quality scan
For each (model, year, clean+gas) pool: report sample size, and the new
generation-aware expected price vs the raw mean/median. Flag pools where:
- **n < 5** (thin — one outlier dominates), or
- **mean diverges >15% from median** (skewed by an outlier), or
- **price method == "bucket"** in find_deals output (no robust fit available).

Build a small script over `pricing.PriceModels` + `database` to list the
worst offenders (model, year, n, expected, median, min-max range).

### 3. External reality-check (the independent anchor)
For the top deals and any flagged pool, pull an external valuation via
`valuations.py` (KBB / CarGurus IMV — both work; CarGurus returns a clean
"avg market price"):
```python
from valuations import _fetch_cargurus, _fetch_kbb   # needs FLARESOLVERR_URL for KBB
```
If our expected price diverges **>25%** from KBB/CarGurus, the pool is
untrustworthy — note it. (CarGurus IMV for the exact year/model is the
fastest sanity anchor.)

### 4. Surface false deals
A "deal" is suspect when ANY of:
- its pool is thin (n<5) or flagged skewed,
- our expected price is >25% above the external anchor,
- it's an unknown-title listing >25% below the robust expected price
  (presumed branded — see analyze-deals).

### 5. Report
- Table of flagged pools (model, year, n, ours vs KBB, %divergence).
- List of false deals currently on the board, with the corrected expected
  price and recommended action (PASS / re-check title).
- Apply fixes via `analyze_deals.py pass/fix` for confirmed false deals.

## Notes
- The robust price model already mitigates most skew (pricing.py); this audit
  catches what slips through (genuinely thin generations, or systemic
  asking-price inflation an external anchor reveals).
- KBB private-party runs ~20-30% below our asking-price-based numbers — that
  gap is expected; flag only divergence beyond it.
