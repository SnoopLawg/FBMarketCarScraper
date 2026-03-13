"""External car valuation lookups: KBB, Edmunds, CarGurus.

All three use Selenium (headless Firefox) since their sites block plain
HTTP requests. Edmunds loads its appraisal page via Selenium, then calls
its internal TMV API from within the browser context for mileage-adjusted
values. Results are cached in the database for 7 days.
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

_CACHE_DAYS = 7

# Map our condition to KBB's URL param and pricing key
_KBB_CONDITION_URL = {
    "excellent": "excellent",
    "very good": "very-good",
    "good": "good",
    "fair": "fair",
    "poor": "fair",
}
_KBB_CONDITION_KEY = {
    "excellent": "excellent",
    "very good": "verygood",
    "good": "good",
    "fair": "fair",
    "poor": "fair",
}


# ── Helpers ───────────────────────────────────────────────────────

def _make_cache_key(sell_car):
    """Build a normalized cache key from sell car attributes."""
    parts = [
        (sell_car.get("name") or "").lower().strip(),
        str(sell_car.get("year") or ""),
        str(sell_car.get("mileage") or ""),
        (sell_car.get("title_type") or "clean").lower(),
        (sell_car.get("condition") or "good").lower(),
        (sell_car.get("drivetrain") or "").lower(),
        (sell_car.get("trim") or "").lower(),
    ]
    return "|".join(parts)


def _parse_make_model(name):
    """Extract make and model from a car name like 'Ford Escape'."""
    parts = name.strip().split()
    if len(parts) >= 2:
        return parts[0], " ".join(parts[1:])
    return name, ""


def _get_zip_code(config):
    """Extract zip code from config, checking multiple sources."""
    z = config.get("Zip")
    if z:
        return str(z)
    sources = config.get("Sources", {})
    for src in ["carscom", "autotrader"]:
        z = sources.get(src, {}).get("zip")
        if z:
            return str(z)
    return "84101"


def _create_headless_driver():
    """Create a headless Firefox driver without the FB profile."""
    from selenium import webdriver
    from selenium.webdriver.firefox.options import Options
    from selenium.webdriver.firefox.service import Service

    options = Options()
    options.add_argument("--headless")
    options.set_preference("dom.webdriver.enabled", False)
    options.set_preference("useAutomationExtension", False)
    options.set_preference(
        "general.useragent.override",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:128.0) "
        "Gecko/20100101 Firefox/128.0",
    )
    options.set_preference("privacy.resistFingerprinting", False)
    options.set_preference("toolkit.telemetry.enabled", False)

    service = Service(log_output="/dev/null")
    driver = webdriver.Firefox(options=options, service=service)
    driver.set_page_load_timeout(20)

    # Inject stealth JS
    driver.execute_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
    """)

    return driver


def _trim_to_slug(name):
    """Convert KBB trim name to URL slug. E.g. 'SE Sport Utility 4D' -> 'se-sport-utility-4d'."""
    return re.sub(r'[^a-z0-9]+', '-', name.lower().strip()).strip('-')


# ── KBB Fetcher (Selenium) ────────────────────────────────────────

def _fetch_kbb(sell_car, zip_code):
    """Fetch KBB private-party and trade-in values via headless Selenium.

    Two-step process:
    1. Load the overview page to find available trims and base pricing
    2. Load the trim-specific page to get condition/mileage-adjusted values

    Returns a valuation dict or None.
    """
    name = sell_car["name"]
    year = sell_car.get("year")
    mileage = sell_car.get("mileage") or 0
    condition = (sell_car.get("condition") or "good").lower()
    user_trim = sell_car.get("trim", "")

    if not year:
        return None

    make, model = _parse_make_model(name)
    make_slug = make.lower().strip()
    model_slug = model.lower().strip().replace(" ", "-").replace("_", "-")
    kbb_condition = _KBB_CONDITION_URL.get(condition, "good")
    condition_key = _KBB_CONDITION_KEY.get(condition, "good")

    driver = None
    try:
        driver = _create_headless_driver()

        # Step 1: Load overview page to get trims and base pricing
        overview_url = f"https://www.kbb.com/{make_slug}/{model_slug}/{year}/"
        driver.get(overview_url)
        time.sleep(3)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        nd = soup.find("script", id="__NEXT_DATA__")
        if not nd:
            logging.warning("KBB: no __NEXT_DATA__ on overview page")
            return None

        data = json.loads(nd.string)
        apollo = data.get("props", {}).get("apolloState", {})
        iq = apollo.get("_INITIAL_QUERY", {})

        # Find trims from ymmPageQuery
        trims_data = []
        base_price = None
        for key in iq:
            if "ymmPageQuery" in key or "ymmPage" in key:
                result = iq[key].get("result", {})
                dl = result.get("loader", {}).get("dataLayer", {})
                info = dl.get("info", {})
                reviews = info.get("reviews", {}).get("generatedReview", {})
                trims_data = reviews.get("trimsData", [])
                pricing_data = reviews.get("pricingData", {})
                base_price = (
                    info.get("vehicle", {}).get("nationalbasedefaultprice")
                    or pricing_data.get("fppPrice")
                )
                break

        if not trims_data:
            # Try stylesPageQuery as fallback
            for key in iq:
                if "stylesPageQuery" in key:
                    result = iq[key].get("result", {})
                    ymm = result.get("ymm", {})
                    for bs in ymm.get("bodyStyles", []):
                        for t in bs.get("trims", []):
                            trims_data.append({
                                "trimName": t.get("name", ""),
                                "trimId": t.get("id"),
                            })
                    break

        if not trims_data:
            logging.warning("KBB: no trims found")
            return None

        # Pick best matching trim
        trim_slug = _pick_best_trim(trims_data, user_trim)
        logging.info(f"KBB: using trim '{trim_slug}' for {year} {name}")

        # Step 2: Load trim page with mileage/condition params
        trim_url = (
            f"https://www.kbb.com/{make_slug}/{model_slug}/{year}/{trim_slug}/"
            f"?intent=trade-in-sell&mileage={mileage}"
            f"&pricetype=private-party&condition={kbb_condition}"
            f"&zipcode={zip_code}"
        )
        driver.get(trim_url)
        time.sleep(4)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        nd = soup.find("script", id="__NEXT_DATA__")
        if not nd:
            logging.warning("KBB: no __NEXT_DATA__ on trim page")
            # Fall back to base pricing from overview
            if base_price and base_price > 500:
                return _build_kbb_result_from_base(
                    base_price, trims_data, user_trim, condition,
                    overview_url,
                )
            return None

        data = json.loads(nd.string)
        apollo = data.get("props", {}).get("apolloState", {})
        iq = apollo.get("_INITIAL_QUERY", {})

        # Extract pricing from ymmtPageQuery
        for key in iq:
            if "ymmtPageQuery" in key:
                result = iq[key].get("result", {})
                ymmt = result.get("ymmt", {})
                pricing = ymmt.get("pricing", {})

                if not pricing:
                    break

                pp = pricing.get("privateparty", {})
                ti = pricing.get("tradein", {})

                pp_mid = pp.get(condition_key) or pp.get("good")
                ti_mid = ti.get(condition_key) or ti.get("good")

                if not pp_mid:
                    logging.warning(f"KBB: no private party pricing. Keys: {list(pp.keys())}")
                    break

                return {
                    "source": "kbb",
                    "source_label": "Kelley Blue Book",
                    "private_party_low": pp.get("fair"),
                    "private_party_high": pp.get("excellent"),
                    "private_party_mid": pp_mid,
                    "trade_in_value": ti_mid,
                    "dealer_retail": None,
                    "url": trim_url,
                    "condition_used": condition,
                    "fetched_at": datetime.utcnow().isoformat(),
                }

        # Fallback: use base pricing
        if base_price and base_price > 500:
            return _build_kbb_result_from_base(
                base_price, trims_data, user_trim, condition, overview_url,
            )

        return None

    except Exception as e:
        logging.warning(f"KBB fetch error: {e}")
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def _build_kbb_result_from_base(base_price, trims_data, user_trim, condition, url):
    """Build a KBB result from overview page base pricing (no condition adjustment)."""
    # Find the user's trim fppPrice if available
    if user_trim:
        user_lower = user_trim.lower()
        for td in trims_data:
            tname = (td.get("trimName") or td.get("displayName") or "").lower()
            if user_lower.split()[0] in tname:
                fpp = td.get("fppPrice")
                if fpp and fpp > 500:
                    base_price = fpp
                break

    return {
        "source": "kbb",
        "source_label": "Kelley Blue Book",
        "private_party_low": None,
        "private_party_high": None,
        "private_party_mid": base_price,
        "trade_in_value": None,
        "dealer_retail": None,
        "url": url,
        "condition_used": condition,
        "fetched_at": datetime.utcnow().isoformat(),
    }


def _pick_best_trim(trims_data, user_trim):
    """Pick the KBB trim slug that best matches the user's trim."""
    if not trims_data:
        return ""

    if user_trim:
        user_lower = user_trim.lower()
        user_first = user_lower.split()[0]
        for td in trims_data:
            tname = td.get("trimName") or td.get("displayName") or ""
            if user_first in tname.lower():
                return _trim_to_slug(tname)

    # Default to first trim
    first = trims_data[0]
    tname = first.get("trimName") or first.get("displayName") or ""
    return _trim_to_slug(tname)


# ── Edmunds Fetcher (Selenium + browser fetch) ───────────────────

# Map our condition to Edmunds condition keys
_EDMUNDS_CONDITION = {
    "excellent": "OUTSTANDING",
    "very good": "CLEAN",
    "good": "AVERAGE",
    "fair": "ROUGH",
    "poor": "ROUGH",
}


def _fetch_edmunds(sell_car, zip_code):
    """Fetch Edmunds TMV (True Market Value) via Selenium + internal API.

    Two-step process:
    1. Load appraisal-value page with Selenium to get style IDs + cookies
    2. Call the TMV API with cookies for mileage-adjusted values

    Returns a valuation dict or None.
    """
    name = sell_car["name"]
    year = sell_car.get("year")
    mileage = sell_car.get("mileage") or 0
    condition = (sell_car.get("condition") or "good").lower()
    user_trim = (sell_car.get("trim") or "").lower()
    user_drivetrain = (sell_car.get("drivetrain") or "").lower()

    if not year:
        return None

    make, model = _parse_make_model(name)
    make_slug = make.lower().strip()
    model_slug = model.lower().strip().replace(" ", "-")
    edmunds_condition = _EDMUNDS_CONDITION.get(condition, "AVERAGE")

    appraisal_url = (
        f"https://www.edmunds.com/{make_slug}/{model_slug}/{year}/appraisal-value/"
    )

    driver = None
    try:
        driver = _create_headless_driver()

        # Step 1: Load appraisal-value page to get style IDs + cookies
        driver.get(appraisal_url)
        time.sleep(5)

        # Extract __PRELOADED_STATE__ from HTML source (JS access is unreliable
        # because the variable may be consumed and deleted by the app)
        page_source = driver.page_source
        logging.info("Edmunds: page title = %s, url = %s", driver.title, driver.current_url)
        marker = "window.__PRELOADED_STATE__ = "
        idx = page_source.find(marker)
        if idx < 0:
            logging.warning("Edmunds: no __PRELOADED_STATE__ in page source")
            # Save screenshot for debugging
            try:
                os.makedirs("screenshots", exist_ok=True)
                driver.save_screenshot("screenshots/edmunds_debug.png")
                logging.info("Edmunds: debug screenshot saved")
            except Exception:
                pass
            return None

        json_start = idx + len(marker)
        # Use Python's json decoder which handles nested braces correctly
        decoder = json.JSONDecoder()
        try:
            preloaded, _ = decoder.raw_decode(page_source, json_start)
        except json.JSONDecodeError as e:
            logging.warning(f"Edmunds: failed to parse __PRELOADED_STATE__: {e}")
            return None

        appraisal = preloaded.get("appraisal", {})
        makes = appraisal.get("makes", {})

        # Try case variations for make key
        make_data = None
        for mk in [make_slug, make.title(), make.upper(), make.lower()]:
            if mk in makes:
                make_data = makes[mk]
                break
        if not make_data:
            if makes:
                make_data = next(iter(makes.values()))
            else:
                logging.warning("Edmunds: no makes in preloaded state")
                return None

        models = make_data.get("models", {})
        model_data = None
        model_lower = model.lower()
        for mk, mv in models.items():
            if mk.lower() == model_lower or mk.lower().replace("-", " ") == model_lower:
                model_data = mv
                break
        if not model_data and models:
            model_data = next(iter(models.values()))

        if not model_data:
            logging.warning("Edmunds: no model data found")
            return None

        years = model_data.get("years", {})
        year_data = years.get(str(year), {})
        if not year_data:
            logging.warning(f"Edmunds: no data for year {year}")
            return None

        styles = year_data.get("styles", {})
        all_tmvs = styles.get("allconditionstmvs", [])
        if not all_tmvs:
            logging.warning("Edmunds: no style/TMV data found")
            return None

        # Step 2: Pick best matching style (trim + drivetrain)
        best_style = _pick_edmunds_style(all_tmvs, user_trim, user_drivetrain)
        if not best_style:
            logging.warning("Edmunds: could not match a style")
            return None

        style_id = best_style.get("styleId")
        style_name = best_style.get("styleName", "")
        logging.info(f"Edmunds: using style '{style_name}' (ID: {style_id})")

        # Step 3: Call TMV API from within the browser (external requests blocked)
        tmv_path = (
            f"/gateway/api/v2/usedtmv/getalltmvbands"
            f"?mileage={mileage}&styleid={style_id}&zipcode={zip_code}"
            f"&typical=false&view=full&priceband=false"
        )

        try:
            tmv_json = driver.execute_script(
                f"return fetch('{tmv_path}')"
                ".then(r => r.json())"
                ".then(d => JSON.stringify(d))"
                ".catch(e => null);"
            )
            if tmv_json:
                tmv_data = json.loads(tmv_json)
                conditions_data = tmv_data.get("tmvconditions", {})
                if conditions_data:
                    return _extract_edmunds_tmv(
                        conditions_data, edmunds_condition, condition,
                        appraisal_url,
                    )
        except Exception as e:
            logging.info(f"Edmunds: TMV API call failed: {e}")

        # Fallback: use default TMV from preloaded state (not mileage-adjusted)
        logging.info("Edmunds: TMV API unavailable, using page defaults")
        return _extract_edmunds_preloaded(
            best_style, edmunds_condition, condition, appraisal_url,
        )

    except Exception as e:
        logging.warning(f"Edmunds fetch error: {e}")
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def _pick_edmunds_style(all_tmvs, user_trim, user_drivetrain):
    """Pick the Edmunds style that best matches user's trim and drivetrain."""
    if not all_tmvs:
        return None

    scored = []
    for style in all_tmvs:
        sname = (style.get("styleName") or "").lower()
        score = 0

        # Trim match
        if user_trim:
            trim_words = user_trim.lower().split()
            for word in trim_words:
                if word in sname:
                    score += 10

        # Drivetrain match
        if user_drivetrain:
            if user_drivetrain in sname:
                score += 5
            elif user_drivetrain in ("awd", "4wd") and "awd" in sname:
                score += 5

        scored.append((score, style))

    scored.sort(key=lambda x: -x[0])
    return scored[0][1]


def _extract_edmunds_tmv(tmv_data, edmunds_condition, condition, url):
    """Extract pricing from Edmunds TMV API response.

    TMV API nests data as: {CONDITION: {Current: {totalWithOptions: {...}}}}
    """
    def _get_totals(cond_key):
        return (tmv_data.get(cond_key, {})
                .get("Current", {})
                .get("totalWithOptions", {}))

    totals = _get_totals(edmunds_condition)
    if not totals.get("usedPrivateParty"):
        # Try any available condition
        for c in ["AVERAGE", "CLEAN", "OUTSTANDING", "ROUGH"]:
            totals = _get_totals(c)
            if totals.get("usedPrivateParty"):
                break

    pp = totals.get("usedPrivateParty")
    if not pp:
        return None

    ti = totals.get("usedTradeIn")
    dr = totals.get("usedTmvRetail")

    # Get range from other conditions
    rough_totals = _get_totals("ROUGH")
    outstanding_totals = _get_totals("OUTSTANDING")
    pp_low = rough_totals.get("usedPrivateParty")
    pp_high = outstanding_totals.get("usedPrivateParty")

    return {
        "source": "edmunds",
        "source_label": "Edmunds",
        "private_party_low": pp_low,
        "private_party_high": pp_high,
        "private_party_mid": pp,
        "trade_in_value": ti,
        "dealer_retail": dr,
        "url": url,
        "condition_used": condition,
        "fetched_at": datetime.utcnow().isoformat(),
    }


def _extract_edmunds_preloaded(style, edmunds_condition, condition, url):
    """Extract pricing from Edmunds __PRELOADED_STATE__ (not mileage-adjusted)."""
    tmv = style.get("tmv", {})
    conditions = tmv.get("conditions", {})
    cond_data = conditions.get(edmunds_condition, {})
    if not cond_data:
        for c in ["AVERAGE", "CLEAN", "OUTSTANDING", "ROUGH"]:
            cond_data = conditions.get(c, {})
            if cond_data:
                break

    if not cond_data:
        return None

    pp = cond_data.get("usedPrivateParty")
    ti = cond_data.get("usedTradeIn")
    dr = cond_data.get("usedTmvRetail")

    if not pp:
        return None

    pp_low = None
    pp_high = None
    rough = conditions.get("ROUGH", {})
    outstanding = conditions.get("OUTSTANDING", {})
    if rough:
        pp_low = rough.get("usedPrivateParty")
    if outstanding:
        pp_high = outstanding.get("usedPrivateParty")

    return {
        "source": "edmunds",
        "source_label": "Edmunds",
        "private_party_low": pp_low,
        "private_party_high": pp_high,
        "private_party_mid": pp,
        "trade_in_value": ti,
        "dealer_retail": dr,
        "url": url,
        "condition_used": condition,
        "fetched_at": datetime.utcnow().isoformat(),
    }


# ── CarGurus Fetcher (Selenium) ──────────────────────────────────

def _fetch_cargurus(sell_car, zip_code):
    """Fetch CarGurus average market price via their price trends page.

    Uses the Remix-based research/price-trends page with entity ID slugs.
    CarGurus entity IDs (e.g., Ford-Escape-d330) are required in the URL
    and can't be easily discovered programmatically, so we try a
    Make-Model slug with common suffixes.

    Returns a valuation dict or None.
    """
    name = sell_car["name"]
    year = sell_car.get("year")

    if not year:
        return None

    make, model = _parse_make_model(name)
    condition = (sell_car.get("condition") or "good").lower()

    make_title = make.strip().title().replace(" ", "-")
    model_title = model.strip().title().replace(" ", "-")
    slug = f"{make_title}-{model_title}"

    # CarGurus requires entity ID suffix (e.g., "Ford-Escape-d330").
    # Try to discover it by loading the price trends page and checking
    # if the slug gets resolved.
    driver = None
    try:
        driver = _create_headless_driver()
        driver.set_page_load_timeout(30)

        # Try entity ID from config first (most reliable)
        entity_id = sell_car.get("cargurus_entity_id")
        logging.info("CarGurus: entity_id from config = %s, slug = %s", entity_id, slug)
        if entity_id:
            trends_url = (
                f"https://www.cargurus.com/research/price-trends/"
                f"{slug}-{entity_id}"
            )
            driver.get(trends_url)
            time.sleep(4)
            logging.info("CarGurus: page title = %s, url = %s", driver.title, driver.current_url)
            if "not found" not in driver.title.lower():
                return _extract_cargurus_trends(driver, year, name, condition)
            else:
                # Save screenshot for debugging
                try:
                    os.makedirs("screenshots", exist_ok=True)
                    driver.save_screenshot("screenshots/cargurus_debug.png")
                    logging.info("CarGurus: debug screenshot saved")
                except Exception:
                    pass

        # Fallback: try slug-only URL (may redirect to correct page)
        trends_url = f"https://www.cargurus.com/research/price-trends/{slug}"
        try:
            driver.get(trends_url)
            time.sleep(4)
            title = driver.title
            if slug.lower().replace("-", " ") in title.lower().replace("-", " "):
                return _extract_cargurus_trends(driver, year, name, condition)
        except Exception:
            pass  # Timeout on slug-only URL is expected

        logging.info("CarGurus: could not resolve entity ID for %s", name)
        return None

    except Exception as e:
        logging.warning(f"CarGurus fetch error: {e}")
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def _extract_cargurus_trends(driver, year, name, condition):
    """Extract per-year average price from CarGurus price trends page."""
    try:
        trends_json = driver.execute_script(
            "try {"
            "  var ctx = window.__remixContext;"
            "  if (!ctx) return null;"
            "  var ld = ctx.state.loaderData;"
            "  var key = Object.keys(ld).find(function(k) {"
            "    return k.indexOf('price-trends.$') >= 0;"
            "  });"
            "  return key ? JSON.stringify(ld[key]) : null;"
            "} catch(e) { return null; }"
        )

        if not trends_json:
            return None

        trends_data = json.loads(trends_json)
        all_trends = trends_data.get("priceTrends", [])

        target_price = None
        all_year_prices = {}

        for section in all_trends:
            if section.get("type") == "CARS":
                for entity in section.get("entities", []):
                    label = entity.get("label", "")
                    pts = entity.get("priceTrends", [])
                    if pts:
                        avg = pts[0].get("averagePrice", 0)
                        parts = label.split()
                        if parts and parts[0].isdigit():
                            ey = int(parts[0])
                            all_year_prices[ey] = avg
                            if ey == year:
                                target_price = avg

        if not target_price:
            return None

        prices_near = [
            all_year_prices[y]
            for y in [year - 1, year, year + 1]
            if y in all_year_prices
        ]

        logging.info(f"CarGurus: {year} {name} avg market price ${target_price:,}")

        return {
            "source": "cargurus",
            "source_label": "CarGurus",
            "private_party_low": min(prices_near) if len(prices_near) > 1 else None,
            "private_party_high": max(prices_near) if len(prices_near) > 1 else None,
            "private_party_mid": target_price,
            "trade_in_value": None,
            "dealer_retail": target_price,
            "url": driver.current_url,
            "condition_used": condition,
            "fetched_at": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logging.warning(f"CarGurus trends extraction error: {e}")
        return None


# ── Main orchestrator ─────────────────────────────────────────────

def fetch_external_valuations(sell_car, config):
    """Fetch valuations from external sources (KBB, Edmunds, CarGurus).

    All use Selenium (headless Firefox). Returns list of valuation dicts.
    """
    zip_code = _get_zip_code(config)
    results = []

    # KBB
    logging.info(f"Fetching KBB valuation for {sell_car.get('name')}...")
    try:
        kbb = _fetch_kbb(sell_car, zip_code)
        if kbb:
            results.append(kbb)
            ti = kbb.get("trade_in_value")
            ti_str = f", trade-in: ${ti:,.0f}" if ti else ""
            low = kbb.get("private_party_low", 0) or 0
            high = kbb.get("private_party_high", 0) or 0
            logging.info(
                f"  KBB: ${kbb['private_party_mid']:,.0f} "
                f"(range: ${low:,.0f}–${high:,.0f}){ti_str}"
            )
        else:
            logging.info("  KBB: no data returned")
    except Exception as e:
        logging.warning(f"KBB valuation failed: {e}")

    # Edmunds
    logging.info(f"Fetching Edmunds valuation for {sell_car.get('name')}...")
    try:
        edmunds = _fetch_edmunds(sell_car, zip_code)
        if edmunds:
            results.append(edmunds)
            pp = edmunds["private_party_mid"]
            ti = edmunds.get("trade_in_value")
            dr = edmunds.get("dealer_retail")
            parts = [f"private party: ${pp:,.0f}"]
            if ti:
                parts.append(f"trade-in: ${ti:,.0f}")
            if dr:
                parts.append(f"dealer: ${dr:,.0f}")
            logging.info(f"  Edmunds: {', '.join(parts)}")
        else:
            logging.info("  Edmunds: no data returned")
    except Exception as e:
        logging.warning(f"Edmunds valuation failed: {e}")

    # CarGurus
    logging.info(f"Fetching CarGurus valuation for {sell_car.get('name')}...")
    try:
        cargurus = _fetch_cargurus(sell_car, zip_code)
        if cargurus:
            results.append(cargurus)
            imv = cargurus["private_party_mid"]
            low = cargurus.get("private_party_low", 0) or 0
            high = cargurus.get("private_party_high", 0) or 0
            logging.info(
                f"  CarGurus: IMV ${imv:,.0f} "
                f"(range: ${low:,.0f}–${high:,.0f})"
            )
        else:
            logging.info("  CarGurus: no data returned")
    except Exception as e:
        logging.warning(f"CarGurus valuation failed: {e}")

    return results


_ALL_SOURCES = {"kbb", "edmunds", "cargurus"}


def get_external_valuations(db, sell_car, config):
    """Get external valuations with database caching (7-day TTL).

    If the cache is missing sources (e.g., only KBB cached but Edmunds/
    CarGurus now available), fetches the missing ones and merges.
    """
    cache_key = _make_cache_key(sell_car)

    # Check cache
    cached = db.get_cached_valuations(cache_key)
    fresh = []
    if cached:
        cutoff = datetime.utcnow() - timedelta(days=_CACHE_DAYS)
        for row in cached:
            fetched = datetime.fromisoformat(row["fetched_at"])
            if fetched > cutoff:
                fresh.append({
                    "source": row["source"],
                    "source_label": row["source_label"],
                    "private_party_low": row["private_party_low"],
                    "private_party_high": row["private_party_high"],
                    "private_party_mid": row["private_party_mid"],
                    "trade_in_value": row["trade_in_value"],
                    "dealer_retail": row["dealer_retail"],
                    "url": row["source_url"],
                    "condition_used": row["condition_used"],
                    "fetched_at": row["fetched_at"],
                })

    # Check if any sources are missing from cache
    cached_sources = {v["source"] for v in fresh}
    missing = _ALL_SOURCES - cached_sources

    if fresh and not missing:
        return fresh

    # Fetch missing sources (or all if cache is empty)
    logging.info(f"Fetching external valuations for {sell_car.get('name')}...")
    new_results = fetch_external_valuations(sell_car, config)

    # Save all new results to cache
    for val in new_results:
        db.upsert_valuation(
            car_key=cache_key,
            source=val["source"],
            source_label=val["source_label"],
            private_party_low=val.get("private_party_low"),
            private_party_high=val.get("private_party_high"),
            private_party_mid=val.get("private_party_mid"),
            trade_in_value=val.get("trade_in_value"),
            dealer_retail=val.get("dealer_retail"),
            source_url=val.get("url"),
            condition_used=val.get("condition_used"),
            zip_code=_get_zip_code(config),
        )

    # Merge: keep cached entries for sources that didn't return new data,
    # prefer new data for sources that did
    new_sources = {v["source"] for v in new_results}
    merged = list(new_results)
    for v in fresh:
        if v["source"] not in new_sources:
            merged.append(v)

    return merged
