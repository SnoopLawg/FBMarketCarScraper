"""Flask web UI for browsing car deals."""

import logging
import webbrowser
from pathlib import Path
from threading import Timer

from flask import Flask, render_template, request, jsonify

from analysis import title_group, compute_market_range
from config import load_config, save_config
from database import Database

SCRIPT_DIR = Path(__file__).parent
app = Flask(__name__, template_folder=str(SCRIPT_DIR / "templates"))

# Shared state
_db = None
_deals = []
_favorites = set()
_deleted = set()
_favorites_file = SCRIPT_DIR / "favorite_listings.txt"
_deleted_file = SCRIPT_DIR / "deleted_listings.txt"


def _load_set_from_file(filepath):
    try:
        with open(filepath, "r") as f:
            return set(line.strip() for line in f if line.strip())
    except FileNotFoundError:
        return set()


def _append_to_file(filepath, value):
    with open(filepath, "a") as f:
        f.write(value + "\n")


# ── Deals page ────────────────────────────────────────────────────

@app.route("/")
def index():
    source_filter = request.args.get("source", "")
    car_filter = request.args.get("car", "")
    title_filter = request.args.get("title", "")
    sort_by = request.args.get("sort", "score")
    search_query = request.args.get("q", "").strip().lower()
    year_min = request.args.get("year_min", "")
    year_max = request.args.get("year_max", "")
    mileage_min = request.args.get("mileage_min", "")
    mileage_max = request.args.get("mileage_max", "")

    # Parse numeric filters
    try:
        year_min_val = int(year_min) if year_min else None
    except ValueError:
        year_min_val = None
    try:
        year_max_val = int(year_max) if year_max else None
    except ValueError:
        year_max_val = None
    try:
        mileage_min_val = int(mileage_min) if mileage_min else None
    except ValueError:
        mileage_min_val = None
    try:
        mileage_max_val = int(mileage_max) if mileage_max else None
    except ValueError:
        mileage_max_val = None

    filtered = []
    for d in _deals:
        if d["href"] in _deleted:
            continue
        if source_filter and d["source"] != source_filter:
            continue
        if car_filter and d["car_query"] != car_filter:
            continue
        if title_filter:
            dt = (d.get("title_type") or "").lower()
            if title_filter == "clean" and dt != "clean":
                continue
            elif title_filter == "rebuilt" and dt != "rebuilt":
                continue
            elif title_filter == "salvage" and dt != "salvage":
                continue
            elif title_filter == "lemon" and dt != "lemon":
                continue
            elif title_filter == "unknown" and dt not in ("", "unknown"):
                continue
        if search_query:
            searchable = " ".join([
                d.get("car_name") or "", d.get("car_query") or "",
                d.get("seller") or "", d.get("location") or "",
                d.get("trim") or "",
            ]).lower()
            if search_query not in searchable:
                continue
        if year_min_val and (not d.get("year") or d["year"] < year_min_val):
            continue
        if year_max_val and (not d.get("year") or d["year"] > year_max_val):
            continue
        if mileage_min_val and (not d.get("mileage") or d["mileage"] < mileage_min_val):
            continue
        if mileage_max_val and (not d.get("mileage") or d["mileage"] > mileage_max_val):
            continue
        filtered.append(d)

    # Sorting
    if sort_by == "price":
        filtered.sort(key=lambda d: d["price"])
    elif sort_by == "mileage":
        filtered.sort(key=lambda d: d.get("mileage") or 999999)
    elif sort_by == "score":
        filtered.sort(key=lambda d: d.get("deal_score", 0), reverse=True)
    else:  # discount
        filtered.sort(key=lambda d: d["price"] - d.get("avg_price", d["price"]))

    # Enrich with price history
    deal_hrefs = [d["href"] for d in filtered]
    price_histories = _db.get_price_history_batch(deal_hrefs) if _db else {}
    for d in filtered:
        d["price_history"] = price_histories.get(d["href"])

    # Enrich with VIN decode data
    deal_vins = [d["vin"] for d in filtered if d.get("vin")]
    vin_data = _db.get_vin_data_batch(deal_vins) if _db and deal_vins else {}
    for d in filtered:
        d["vin_data"] = vin_data.get((d.get("vin") or "").upper())

    # Enrich with market value ranges (cached per car/year/title group)
    _market_cache = {}
    for d in filtered:
        if not d.get("car_query") or not d.get("year"):
            d["market_range"] = None
            continue
        grp = title_group(d.get("title_type"))
        cache_key = (d["car_query"], d["year"], grp)
        if cache_key not in _market_cache:
            prices = _db.get_market_prices(d["car_query"], d["year"], grp) if _db else []
            _market_cache[cache_key] = compute_market_range(prices)
        d["market_range"] = _market_cache[cache_key]

    sources = sorted(set(d["source"] for d in _deals))
    cars = sorted(set(d["car_query"] for d in _deals))

    return render_template(
        "deals.html",
        deals=filtered,
        favorites=_favorites,
        sources=sources,
        cars=cars,
        current_source=source_filter,
        current_car=car_filter,
        current_title=title_filter,
        current_sort=sort_by,
        total=len(filtered),
        current_search=search_query,
        current_year_min=year_min,
        current_year_max=year_max,
        current_mileage_min=mileage_min,
        current_mileage_max=mileage_max,
    )


# ── Favorites page ────────────────────────────────────────────────

@app.route("/favorites")
def favorites_page():
    fav_listings = _db.get_listings_by_hrefs(list(_favorites)) if _db else []

    config = load_config()
    mileage_threshold = config.get("MileageMax") or 150000

    enriched = []
    for row in fav_listings:
        d = dict(row)
        # Compute avg_price for display (title-group aware)
        avg_price = 0
        if d.get("car_query") and d.get("year"):
            avgs = _db.get_averages(d["car_query"])
            grp = title_group(d.get("title_type"))
            avg_key = (d["year"], grp)
            if avg_key not in avgs:
                avg_key = (d["year"], "all")
            if avg_key in avgs:
                avg_lower, avg_higher = avgs[avg_key]
                mileage = d.get("mileage") or 0
                avg_price = avg_lower if mileage <= mileage_threshold else avg_higher
        d["avg_price"] = avg_price
        d["deal_score"] = None
        d["deal_grade"] = None
        d["score_breakdown"] = None
        d["price_history"] = None
        d["nhtsa_rating"] = None
        d["vin_data"] = None
        d["market_range"] = None
        enriched.append(d)

    # Enrich with price history
    deal_hrefs = [d["href"] for d in enriched]
    price_histories = _db.get_price_history_batch(deal_hrefs) if _db else {}
    for d in enriched:
        d["price_history"] = price_histories.get(d["href"])

    # Enrich with VIN decode data
    fav_vins = [d.get("vin") for d in enriched if d.get("vin")]
    vin_data = _db.get_vin_data_batch(fav_vins) if _db and fav_vins else {}
    for d in enriched:
        d["vin_data"] = vin_data.get((d.get("vin") or "").upper())

    return render_template(
        "favorites.html",
        deals=enriched,
        favorites=_favorites,
        total=len(enriched),
    )


# ── API endpoints ─────────────────────────────────────────────────

@app.route("/api/favorite", methods=["POST"])
def favorite():
    href = request.json.get("href")
    if href:
        if href in _favorites:
            # Toggle off
            _favorites.discard(href)
            _rewrite_favorites()
        else:
            # Toggle on
            _favorites.add(href)
            _append_to_file(_favorites_file, href)
    return jsonify({"ok": True, "saved": href in _favorites})


@app.route("/api/unfavorite", methods=["POST"])
def unfavorite():
    href = request.json.get("href")
    if href and href in _favorites:
        _favorites.discard(href)
        _rewrite_favorites()
    return jsonify({"ok": True})


def _rewrite_favorites():
    """Rewrite the entire favorites file from the current set."""
    with open(_favorites_file, "w") as f:
        for h in _favorites:
            f.write(h + "\n")


@app.route("/api/delete", methods=["POST"])
def delete():
    href = request.json.get("href")
    if href:
        _deleted.add(href)
        _append_to_file(_deleted_file, href)
        _db.delete_listing(href)
    return jsonify({"ok": True})


@app.route("/api/market-range/<car_query>/<int:year>")
def market_range_api(car_query, year):
    """Get market value range for a car/year combo."""
    grp = request.args.get("title_group", "clean")
    if not _db:
        return jsonify({"error": "Database not available"}), 500
    prices = _db.get_market_prices(car_query, year, grp)
    mrange = compute_market_range(prices)
    if mrange:
        return jsonify(mrange)
    return jsonify({"error": "Not enough data", "count": len(prices)}), 404


@app.route("/api/vin-decode/<vin>")
def vin_decode_api(vin):
    """Decode a VIN and return the result."""
    from vin import decode_vin_cached
    if not _db or not vin or len(vin) != 17:
        return jsonify({"error": "Invalid VIN"}), 400
    result = decode_vin_cached(_db, vin)
    if result:
        return jsonify(result)
    return jsonify({"error": "VIN not found or decode failed"}), 404


@app.route("/api/price-history/<path:href>")
def price_history_api(href):
    rows = _db.get_price_history(href) if _db else []
    history = [{"old_price": r["old_price"], "new_price": r["new_price"],
                "changed_at": r["changed_at"]} for r in rows]
    return jsonify({"history": history})


# ── Settings ──────────────────────────────────────────────────────

@app.route("/settings")
def settings():
    config = load_config()
    return render_template("settings.html", config=config)


@app.route("/api/settings", methods=["POST"])
def update_settings():
    config = load_config()
    data = request.json

    if "desired_cars" in data:
        config["DesiredCar"] = [c.strip() for c in data["desired_cars"] if c.strip()]
    if "min_price" in data:
        config["MinPrice"] = int(data["min_price"])
    if "max_price" in data:
        config["MaxPrice"] = int(data["max_price"])
    if "mileage_max" in data:
        config["MileageMax"] = int(data["mileage_max"]) if data["mileage_max"] else None
    if "price_threshold" in data:
        config["PriceThreshold"] = int(data["price_threshold"])
    if "location_filter" in data:
        config["LocationFilter"] = data["location_filter"]
    if "sources" in data:
        for name, enabled in data["sources"].items():
            if name in config.get("Sources", {}):
                config["Sources"][name]["enabled"] = enabled

    save_config(config)
    return jsonify({"ok": True})


# ── Analytics ─────────────────────────────────────────────────────

@app.route("/analytics")
def analytics():
    return render_template("analytics.html")


@app.route("/api/analytics")
def analytics_data():
    rows = _db.get_analytics_data() if _db else []
    listings = []
    for r in rows:
        listings.append({
            "car_query": r["car_query"],
            "price": r["price"],
            "mileage": r["mileage"],
            "year": r["year"],
            "source": r["source"],
            "location": r["location"],
            "seller": r["seller"],
            "deal_rating": r["deal_rating"],
            "distance": r["distance"],
            "created_at": r["created_at"],
        })

    avg_rows = _db.get_analytics_averages() if _db else []
    averages = []
    for r in avg_rows:
        averages.append({
            "car_query": r["car_query"],
            "year": r["year"],
            "avg_lower": r["avg_lower_mileage_price"],
            "avg_higher": r["avg_higher_mileage_price"],
        })

    return jsonify({"listings": listings, "averages": averages})


# ── Scrape Now ────────────────────────────────────────────────────

@app.route("/api/scrape", methods=["POST"])
def trigger_scrape():
    from scraper_worker import start_scrape

    def on_complete(deals):
        global _deals
        _deals = deals

    started, msg = start_scrape(on_complete=on_complete)
    return jsonify({"started": started, "message": msg})


@app.route("/api/enrich", methods=["POST"])
def trigger_enrich():
    from scraper_worker import start_enrich

    def on_complete(deals):
        global _deals
        _deals = deals

    limit = request.json.get("limit", 100) if request.is_json else 100
    started, msg = start_enrich(on_complete=on_complete, limit=limit)
    return jsonify({"started": started, "message": msg})


@app.route("/api/scrape/status")
def scrape_status():
    from scraper_worker import get_status
    return jsonify(get_status())


# ── Startup ───────────────────────────────────────────────────────

def start_web_ui(deals, port=5001):
    """Launch the Flask web UI with the given deals list."""
    global _db, _deals, _favorites, _deleted

    _deals = deals
    _db = Database()
    _db.open()
    _favorites = _load_set_from_file(_favorites_file)
    _deleted = _load_set_from_file(_deleted_file)

    logging.info(f"Starting web UI at http://localhost:{port}")

    # Auto-open browser after a short delay
    Timer(1.5, lambda: webbrowser.open(f"http://localhost:{port}")).start()

    app.run(host="127.0.0.1", port=port, debug=False)
