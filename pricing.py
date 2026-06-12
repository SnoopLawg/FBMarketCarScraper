"""Generation-aware, mileage-adjusted expected-price model.

Replaces the per-(model, year, mileage-bucket) average — which starved every
cell (54% of pools had <5 listings, so one outlier produced false deals like
a 2014 RAV4 priced at $20k vs a real ~$13k) — with a robust price curve fit
over a *comparable* pool:

    same model + same GENERATION (never across a redesign)
    features: age, mileage, trim_tier

Year stays the dominant signal: it selects the generation and is a regression
feature, so a 2014 RAV4 still gets a 2014-appropriate price — built from the
whole 4th-gen pool, not three noisy 2014 listings.

Design, grounded in an audit of our own data (June 2026):
  • Per-generation LINEAR fits as well as log-linear (identical MAE) — the
    generation split already linearizes the short ~5yr span, so we keep the
    simple, transparent linear model (no log/ensemble: zero measured gain).
  • TRIM is a real covariate — premium trims were under-priced ~$1.2-1.7k by
    the age+mileage-only model; trim_tier is now a regression feature.
  • SHRINKAGE — thin pools blend their fit toward a stable normalized-median
    prior (weight n/(n+K)) instead of a hard fallback cliff.
  • MEASURED — quality() returns 5-fold-CV MAE per pool so accuracy is
    tracked, not guessed.

Pure-Python (no numpy) so it runs anywhere the app does.
"""

import logging
from datetime import datetime

# ── Redesign (generation-start) years, US market ─────────────────
GENERATIONS = {
    "rav4": [2013, 2019], "crv": [2012, 2017, 2023], "outback": [2015, 2020],
    "cx5": [2013, 2017], "highlander": [2014, 2020], "4runner": [2010, 2025],
    "tucson": [2016, 2022], "sportage": [2017, 2023], "escape": [2013, 2020],
    "equinox": [2018, 2025], "grandcherokee": [2011, 2022], "wrangler": [2018],
    "forester": [2014, 2019], "pilot": [2016, 2023], "cx9": [2016],
    "santafe": [2013, 2019, 2024], "broncosport": [2021], "telluride": [2020],
    "tacoma": [2016, 2024], "f150": [2015, 2021], "ranger": [2019, 2024],
    "frontier": [2022], "colorado": [2015, 2023], "sierra1500": [2014, 2019],
    "tundra": [2022], "ram1500": [2019], "ridgeline": [2017],
    "silverado1500": [2014, 2019],
    "camry": [2012, 2018, 2025], "accord": [2013, 2018, 2023],
    "civic": [2012, 2016, 2022], "corolla": [2014, 2020], "mazda3": [2014, 2019],
    "altima": [2013, 2019], "sonata": [2015, 2020], "elantra": [2017, 2021],
    "forte": [2014, 2019], "legacy": [2015, 2020], "jetta": [2019],
    "mazda6": [2014], "wrx": [2015, 2022], "370z": [2009], "sienna": [2021],
    "odyssey": [2018], "insight": [2019],
}

_SHRINK_K = 8          # shrinkage strength: w = n / (n + K)
_TRIM_PER_TIER = 1200  # prior's trim adjustment ($/tier), only for fallback


def _model_key(car_query):
    norm = (car_query or "").lower().replace("-", "").replace(" ", "")
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


def _solve(M, b):
    """Gaussian elimination for an NxN system. None if singular."""
    n = len(b)
    a = [row[:] + [b[i]] for i, row in enumerate(M)]
    for c in range(n):
        piv = max(range(c, n), key=lambda r: abs(a[r][c]))
        if abs(a[piv][c]) < 1e-9:
            return None
        a[c], a[piv] = a[piv], a[c]
        for r in range(n):
            if r != c:
                f = a[r][c] / a[c][c]
                for k in range(c, n + 1):
                    a[r][k] -= f * a[c][k]
    return [a[i][n] / a[i][i] for i in range(n)]


def _ols(rows):
    """Weighted OLS over [(feat_tuple, y, weight), ...]. feat includes the 1.

    Weights let sold comps (real transactions) pull the curve toward what
    cars actually went for, not just asking prices. Returns coef or None.
    """
    if not rows:
        return None
    k = len(rows[0][0])
    XtX = [[0.0] * k for _ in range(k)]
    Xty = [0.0] * k
    for feat, y, w in rows:
        for i in range(k):
            Xty[i] += w * feat[i] * y
            for j in range(k):
                XtX[i][j] += w * feat[i] * feat[j]
    return _solve(XtX, Xty)


def _robust_fit(rows):
    """Weighted trimmed OLS: fit, drop worst ~15% by residual, refit.

    The weighted fit already sits near the heavily-weighted sold comps, so
    they show small residuals and survive trimming; only genuine outliers go.
    None if too small.
    """
    cur = rows
    beta = None
    for _ in range(2):
        if len(cur) < len(cur[0][0]) + 2:
            return beta
        beta = _ols(cur)
        if beta is None:
            return None
        resid = sorted(
            ((abs(y - sum(b * f for b, f in zip(beta, feat))), (feat, y, w))
             for feat, y, w in cur), key=lambda x: x[0])
        keep = max(len(rows[0][0]) + 2, int(len(resid) * 0.85))
        trimmed = [r for _, r in resid[:keep]]
        if len(trimmed) == len(cur):
            return beta
        cur = trimmed
    return beta


def _features(kind, age, mi, tier, dl):
    """Feature row for a fit `kind` (which optional columns it includes)."""
    if kind == "dealer":
        return (1, age, mi, tier, dl)
    if kind == "trim":
        return (1, age, mi, tier)
    if kind == "dealeronly":
        return (1, age, mi, dl)
    return (1, age, mi)   # notrim


class PriceModels:
    """Per-(comp_group, generation) robust price models for one car_query."""

    def __init__(self, candidates, car_query):
        self.car_query = car_query
        # (grp, gen) -> [(age, mileage, trim_tier, is_dealer, price, weight)]
        self._pools = {}
        self._fits = {}
        from analysis import comp_group, SOLD_WEIGHT, PRESUMED_SOLD_WEIGHT
        from trim_tiers import get_trim_tier
        now = datetime.utcnow().year
        for row in candidates:
            price = row["price"]; year = row["year"]
            if not price or not year or price < 800:
                continue
            grp = comp_group(row["title_type"], (row.get("powertrain") or ""))
            gen = generation_of(car_query, year)
            tier, _ = get_trim_tier(row["car_name"], car_query, row.get("trim") or "")
            is_dealer = 1 if (row.get("seller_type") or "") == "dealer" else 0
            if row.get("sold"):
                w = PRESUMED_SOLD_WEIGHT if row.get("sold_presumed") else SOLD_WEIGHT
            else:
                w = 1
            self._pools.setdefault((grp, gen), []).append(
                (now - year, row["mileage"] or 0, tier, is_dealer, price, w))

    def _clean_pool(self, key):
        pool = self._pools.get(key, [])
        if len(pool) < 3:
            return pool, 0
        med = _median([p[4] for p in pool])
        clean = [pt for pt in pool if 0.25 * med <= pt[4] <= 4 * med]
        return (clean if len(clean) >= 3 else pool), med

    def _fit_pool(self, clean):
        """Weighted robust fit with only the optional features that VARY.

        Including a constant column (all same trim, or all dealer) makes the
        design singular, so trim/is_dealer are added only when they actually
        vary (and the pool is big enough to spend the degree of freedom).
        Returns (kind, beta); kind names which features are in beta.
        """
        n = len(clean)
        if n < 6:
            return "none", None
        has_trim = n >= 10 and len({t for _, _, t, _, _, _ in clean}) > 1
        has_dealer = n >= 14 and len({dl for _, _, _, dl, _, _ in clean}) > 1
        if has_trim and has_dealer:
            order = ["dealer", "trim", "notrim"]
        elif has_dealer:
            order = ["dealeronly", "notrim"]
        elif has_trim:
            order = ["trim", "notrim"]
        else:
            order = ["notrim"]
        for kind in order:
            rows = [(_features(kind, a, m, t, dl), p, w)
                    for a, m, t, dl, p, w in clean]
            b = _robust_fit(rows)
            if b is not None:
                return kind, b
        return "none", None

    def _prior(self, clean, age, mi, tier):
        """Weighted normalized-median baseline (age+mileage+trim adjusted).

        Sold comps count by their weight so the prior also leans toward
        real transaction prices.
        """
        DEP, MILE = 0.09, 0.05
        adj = []
        for c_age, c_mi, c_tier, c_dl, c_pr, c_w in clean:
            f = (1 - DEP * (age - c_age)) * (1 - MILE * ((mi - c_mi) / 10000.0))
            f = max(0.4, min(2.2, f))
            adj.extend([c_pr * f + _TRIM_PER_TIER * (tier - c_tier)] * int(c_w))
        return _median(adj)

    def expected(self, year, mileage, comp_grp, trim_tier=1, is_dealer=0):
        """Expected price for THIS listing's channel. Returns
        (price|None, n_comps, method). is_dealer lets a private listing be
        priced against the private-adjusted curve (and vice versa) so the
        estimate isn't biased by a dealer-heavy comp pool."""
        if not year:
            return None, 0, "none"
        gen = generation_of(self.car_query, year)
        clean, med = self._clean_pool((comp_grp, gen))
        n = len(clean)
        if n < 3:
            return None, n, "none"

        age = datetime.utcnow().year - year
        mi = mileage or 0
        prior = self._prior(clean, age, mi, trim_tier)

        # Richest fit the pool supports (age/mileage/trim/is_dealer), cached.
        if (comp_grp, gen) not in self._fits:
            self._fits[(comp_grp, gen)] = self._fit_pool(clean)
        kind, b = self._fits[(comp_grp, gen)]
        if b is not None:
            feat = _features(kind, age, mi, trim_tier, is_dealer)
            pred = sum(c * f for c, f in zip(b, feat))
            if 0.4 * med <= pred <= 2.2 * med:
                # Shrink toward the stable prior for thin pools.
                w = n / (n + _SHRINK_K)
                blended = round(w * pred + (1 - w) * prior)
                return blended, n, ("fit" if w >= 0.75 else "shrunk")

        return round(prior), n, "normalized"

    def quality(self):
        """5-fold-CV MAE per pool (out-of-sample accuracy tracking)."""
        out = {}
        for key, pool in self._pools.items():
            if len(pool) < 10:
                continue
            errs = []
            for fold in range(5):
                test = [pool[i] for i in range(len(pool)) if i % 5 == fold]
                train = [pool[i] for i in range(len(pool)) if i % 5 != fold]
                kind, b = self._fit_pool(train)
                if not b:
                    continue
                for a, m, t, dl, p, _w in test:
                    feat = _features(kind, a, m, t, dl)
                    errs.append(abs(p - sum(c * f for c, f in zip(b, feat))))
            if errs:
                out[key] = (round(sum(errs) / len(errs)), len(pool))
        return out
