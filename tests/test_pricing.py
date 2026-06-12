"""Generation-aware price model — the fix for thin-pool false deals."""
from pricing import generation_of, PriceModels, _fit


def test_generation_boundaries_respect_redesigns():
    # RAV4: 4th gen 2013-2018, 5th gen 2019+
    assert generation_of("Toyota RAV4", 2014) == generation_of("Toyota RAV4", 2018)
    assert generation_of("Toyota RAV4", 2019) != generation_of("Toyota RAV4", 2018)
    # CR-V: 2012, 2017, 2023 redesigns → four generations
    assert generation_of("Honda CR-V", 2011) == 0
    assert generation_of("Honda CR-V", 2023) == 3


def test_unknown_model_is_single_generation():
    assert generation_of("Fictional Carmobile", 2015) == 0
    assert generation_of("Fictional Carmobile", 2022) == 0


def _row(year, mileage, price, tt="clean", pt=""):
    return {"year": year, "mileage": mileage, "price": price,
            "title_type": tt, "powertrain": pt}


def test_fit_predicts_mileage_and_age_sensitive_price():
    # Synthetic linear truth: $30k base, -$1500/yr, -$400/10k mi
    import datetime
    now = datetime.datetime.utcnow().year
    cands = []
    for age in range(0, 8):
        for mi in (20000, 50000, 90000):
            price = 30000 - 1500 * age - 0.04 * mi
            cands.append(_row(now - age, mi, round(price)))
    pm = PriceModels(cands, "Toyota RAV4")
    exp, n, method = pm.expected(now - 4, 50000, "clean")
    assert method == "fit"
    assert abs(exp - (30000 - 1500 * 4 - 0.04 * 50000)) < 800  # within ~$800


def test_single_outlier_does_not_skew_the_fit():
    import datetime
    now = datetime.datetime.utcnow().year
    cands = [_row(now - 5, 90000, 14000) for _ in range(12)]
    cands.append(_row(now - 5, 90000, 45000))  # one wild mis-scrape
    pm = PriceModels(cands, "Toyota RAV4")
    exp, n, _ = pm.expected(now - 5, 90000, "clean")
    assert 12000 <= exp <= 16000  # the $45k outlier is rejected


def test_thin_pool_falls_back_then_gives_none():
    pm = PriceModels([_row(2014, 80000, 13000), _row(2014, 90000, 12500)],
                     "Toyota RAV4")
    exp, n, method = pm.expected(2014, 85000, "clean")
    assert method == "none" and exp is None  # <3 comps → caller uses bucket
