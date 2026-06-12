"""Generation-aware, mileage-adjusted expected-price model.

Replaces the per-(model, year, mileage-bucket) average — which starved every
cell (54% of pools had <5 listings, so one outlier produced false deals like
the 2014 RAV4 priced at $20k vs a real ~$13k) — with a robust price curve
fit across a *comparable* pool:

    same model + same GENERATION (never across a redesign) + age + mileage

Year stays the dominant signal: it selects the generation and is a regression
feature, so a 2014 RAV4 still gets a 2014-appropriate price — just computed
from the whole 4th-gen pool (2013-2018) instead of three noisy 2014 listings.

Scalable: the only model-specific input is a redesign-year table; everything
else (the fit, mileage/age sensitivity, outlier rejection) is data-driven and
self-maintaining. Unknown models degrade gracefully to a single generation.
Pure-Python (no numpy) so it runs anywhere the app does.
"""

import logging
from datetime import datetime

# ── Redesign (generation-start) years, US market ─────────────────
# A year belongs to the generation opened by the latest redesign year <= it.
# Approximate is fine: an off-by-one only shifts one boundary; the graceful
# fallback (single generation) still beats per-year n=3.
GENERATIONS = {
    # SUVs / crossovers
    "rav4": [2013, 2019], "crv": [2012, 2017, 2023], "outback": [2015, 2020],
    "cx5": [2013, 2017], "highlander": [2014, 2020], "4runner": [2010, 2025],
    "tucson": [2016, 2022], "sportage": [2017, 2023], "escape": [2013, 2020],
    "equinox": [2018, 2025], "grandcherokee": [2011, 2022], "wrangler": [2018],
    "forester": [2014, 2019], "pilot": [2016, 2023], "cx9": [2016],
    "santafe": [2013, 2019, 2024], "broncosport": [2021], "telluride": [2020],
    # trucks
    "tacoma": [2016, 2024], "f150": [2015, 2021], "ranger": [2019, 2024],
    "frontier": [2022], "colorado": [2015, 2023], "sierra1500": [2014, 2019],
    "tundra": [2022], "ram1500": [2019], "ridgeline": [2017],
    "silverado1500": [2014, 2019],
    # sedans / cars
    "camry": [2012, 2018, 2025], "accord": [2013, 2018, 2023],
    "civic": [2012, 2016, 2022], "corolla": [2014, 2020], "mazda3": [2014, 2019],
    "altima": [2013, 2019], "sonata": [2015, 2020], "elantra": [2017, 2021],
    "forte": [2014, 2019], "legacy": [2015, 2020], "jetta": [2019],
    "mazda6": [2014], "wrx": [2015, 2022], "370z": [2009], "sienna": [2021],
    "odyssey": [2018], "insight": [2019],
}


def _model_key(car_query):
    norm = (car_query or "").lower().replace("-", "").replace(" ", "")
    # Longest key first so 'silverado1500' wins over a hypothetical 'silverado'.
    for key in sorted(GENERATIONS, key=len, reverse=True):
        if key in norm:
            return key
    return None


def generation_of(car_query, year):
    """Generation index for (model, year). 0 for unknown models (one gen)."""
    key = _model_key(car_query)
    if not key or not year:
        return 0
    return sum(1 for r in GENERATIONS[key] if year >= r)


def _median(xs):
    s = sorted(xs)
    n = len(s)
    if not n:
        return 0
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def _solve3(m, b):
    """Solve 3x3 linear system by Gaussian elimination. None if singular."""
    a = [row[:] + [b[i]] for i, row in enumerate(m)]
    for c in range(3):
        piv = max(range(c, 3), key=lambda r: abs(a[r][c]))
        if abs(a[piv][c]) < 1e-9:
            return None
        a[c], a[piv] = a[piv], a[c]
        for r in range(3):
            if r != c:
                f = a[r][c] / a[c][c]
                for k in range(c, 4):
                    a[r][k] -= f * a[c][k]
    return [a[i][3] / a[i][i] for i in range(3)]


def _fit(points):
    """Robust OLS price ~ 1 + age + mileage. points: [(age, miles, price)].

    Trimmed: fit, drop the worst ~15% by residual, refit. Returns
    (b0, b1, b2) or None.
    """
    pts = points
    for _ in range(2):
        n = len(pts)
        if n < 6:
            return None
        sa = sm = sp = saa = sam = smm = sap = smp = 0.0
        for age, mi, pr in pts:
            sa += age; sm += mi; sp += pr
            saa += age * age; sam += age * mi; smm += mi * mi
            sap += age * pr; smp += mi * pr
        M = [[n, sa, sm], [sa, saa, sam], [sm, sam, smm]]
        beta = _solve3(M, [sp, sap, smp])
        if beta is None:
            return None
        resid = [(abs(pr - (beta[0] + beta[1] * age + beta[2] * mi)), p)
                 for p in pts for (age, mi, pr) in [p]]
        resid.sort(key=lambda x: x[0])
        keep = max(6, int(len(resid) * 0.85))
        trimmed = [p for _, p in resid[:keep]]
        if len(trimmed) == len(pts):
            return beta
        pts = trimmed
    return beta


class PriceModels:
    """Per-(comp_group, generation) robust price models for one car_query.

    Built once from the candidate list, then queried per listing.
    """

    def __init__(self, candidates, car_query):
        self.car_query = car_query
        self._pools = {}   # (comp_group, gen) -> [(age, miles, price)]
        self._fits = {}    # cached fit results
        now_year = datetime.utcnow().year
        from analysis import comp_group  # local import to avoid cycle
        for row in candidates:
            price = row["price"]; year = row["year"]; mileage = row["mileage"]
            if not price or not year or price < 800:
                continue
            grp = comp_group(row["title_type"], (row.get("powertrain") or ""))
            gen = generation_of(car_query, year)
            self._pools.setdefault((grp, gen), []).append(
                (now_year - year, mileage or 0, price))

    def expected(self, year, mileage, comp_grp):
        """Expected price for a listing. Returns (price|None, n_comps, method).

        Tries the robust age+mileage fit; falls back to a mileage/age-
        normalized median for thin pools; None when there's nothing
        comparable (caller then uses the legacy bucket average).
        """
        if not year:
            return None, 0, "none"
        gen = generation_of(self.car_query, year)
        pool = self._pools.get((comp_grp, gen), [])
        if len(pool) < 3:
            return None, len(pool), "none"

        # Gross-outlier rejection vs pool median (mis-scrapes / salvage).
        med = _median([p for _, _, p in pool])
        clean = [pt for pt in pool if 0.25 * med <= pt[2] <= 4 * med]
        if len(clean) < 3:
            clean = pool
        now_year = datetime.utcnow().year
        age = now_year - year
        mi = mileage or 0

        beta = self._fits.get((comp_grp, gen, "_beta"), "miss")
        if beta == "miss":
            beta = _fit(clean) if len(clean) >= 6 else None
            self._fits[(comp_grp, gen, "_beta")] = beta

        if beta is not None:
            pred = beta[0] + beta[1] * age + beta[2] * mi
            lo, hi = 0.4 * med, 2.2 * med   # sanity clamp
            if lo <= pred <= hi:
                return round(pred), len(clean), "fit"

        # Thin-pool fallback: normalize each comp to (age, mileage) of target
        # with conservative default rates, then take the median.
        DEP_PER_YEAR = 0.09      # ~9%/yr early-life depreciation
        MILE_PER_10K = 0.05      # ~5% per 10k miles
        adj = []
        for c_age, c_mi, c_pr in clean:
            f = (1 - DEP_PER_YEAR * (age - c_age)) * \
                (1 - MILE_PER_10K * ((mi - c_mi) / 10000.0))
            f = max(0.4, min(2.2, f))
            adj.append(c_pr * f)
        return round(_median(adj)), len(clean), "normalized"
