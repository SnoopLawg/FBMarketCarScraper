"""Flask web UI for browsing car deals."""

import logging
import webbrowser
from pathlib import Path
from threading import Timer

from flask import Flask, render_template, request, jsonify, redirect, url_for

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


@app.route("/")
def index():
    source_filter = request.args.get("source", "")
    car_filter = request.args.get("car", "")
    sort_by = request.args.get("sort", "discount")

    filtered = []
    for d in _deals:
        if d["href"] in _deleted:
            continue
        if source_filter and d["source"] != source_filter:
            continue
        if car_filter and d["car_query"] != car_filter:
            continue
        filtered.append(d)

    if sort_by == "price":
        filtered.sort(key=lambda d: d["price"])
    elif sort_by == "mileage":
        filtered.sort(key=lambda d: d.get("mileage") or 999999)
    else:  # discount
        filtered.sort(key=lambda d: d["price"] - d.get("avg_price", d["price"]))

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
        current_sort=sort_by,
        total=len(filtered),
    )


@app.route("/api/favorite", methods=["POST"])
def favorite():
    href = request.json.get("href")
    if href and href not in _favorites:
        _favorites.add(href)
        _append_to_file(_favorites_file, href)
    return jsonify({"ok": True})


@app.route("/api/delete", methods=["POST"])
def delete():
    href = request.json.get("href")
    if href:
        _deleted.add(href)
        _append_to_file(_deleted_file, href)
        _db.delete_listing(href)
    return jsonify({"ok": True})


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


@app.route("/analytics")
def analytics():
    return render_template("analytics.html")


@app.route("/api/analytics")
def analytics_data():
    rows = _db.get_analytics_data()
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

    avg_rows = _db.get_analytics_averages()
    averages = []
    for r in avg_rows:
        averages.append({
            "car_query": r["car_query"],
            "year": r["year"],
            "avg_lower": r["avg_lower_mileage_price"],
            "avg_higher": r["avg_higher_mileage_price"],
        })

    return jsonify({"listings": listings, "averages": averages})


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
