"""Discord webhook notifications for scrape results and deal alerts."""

import json
import logging
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError

DATA_DIR = Path(__file__).parent
_NOTIFIED_FILE = None


def _get_notified_file():
    """Return path to the file tracking already-notified deal hrefs."""
    import os
    data_dir = Path(os.environ.get("DATA_DIR", Path(__file__).parent))
    return data_dir / "notified_deals.txt"


def _load_notified():
    """Load set of hrefs we've already sent notifications for."""
    path = _get_notified_file()
    if path.exists():
        return set(l.strip() for l in path.read_text().splitlines() if l.strip())
    return set()


def _save_notified(hrefs):
    """Append newly notified hrefs to the tracking file."""
    path = _get_notified_file()
    with open(path, "a") as f:
        for href in hrefs:
            f.write(href + "\n")


def _send_webhook(webhook_url, payload):
    """Send a Discord webhook payload. Returns True on success."""
    try:
        data = json.dumps(payload).encode("utf-8")
        req = Request(webhook_url, data=data, headers={"Content-Type": "application/json"})
        urlopen(req, timeout=10)
        return True
    except URLError as e:
        logging.error(f"Discord webhook failed: {e}")
        return False
    except Exception as e:
        logging.error(f"Discord webhook error: {e}")
        return False


def send_scrape_summary(webhook_url, deals, scrape_stats=None):
    """Send a scrape completion summary to Discord.

    Args:
        webhook_url: Discord webhook URL
        deals: List of deal dicts from find_deals()
        scrape_stats: Optional dict with per-source listing counts
    """
    total = len(deals)
    grade_a = sum(1 for d in deals if d.get("deal_grade") == "A")
    grade_b = sum(1 for d in deals if d.get("deal_grade") == "B")

    # Source breakdown
    by_source = {}
    for d in deals:
        src = d.get("source", "unknown")
        by_source[src] = by_source.get(src, 0) + 1
    source_lines = " | ".join(f"{src}: {cnt}" for src, cnt in sorted(by_source.items()))

    description = f"**{total}** deals scored"
    if grade_a or grade_b:
        description += f" — **{grade_a}** Grade A, **{grade_b}** Grade B"
    if source_lines:
        description += f"\n{source_lines}"

    payload = {
        "embeds": [{
            "title": "Scrape Complete",
            "description": description,
            "color": 0x5B8DEE,  # accent blue
        }]
    }

    _send_webhook(webhook_url, payload)


def send_deal_alerts(webhook_url, deals, app_url=""):
    """Send individual notifications for new Grade A deals.

    Only sends for deals not previously notified (tracked in notified_deals.txt).

    Args:
        webhook_url: Discord webhook URL
        deals: List of deal dicts from find_deals()
        app_url: Base URL of the web UI (e.g., "https://cars.single10.app")
    """
    notified = _load_notified()
    grade_a = [d for d in deals if d.get("deal_grade") == "A" and d["href"] not in notified]

    if not grade_a:
        return

    newly_notified = []
    for deal in grade_a:
        score = deal.get("deal_score", 0)
        price = deal.get("price", 0)
        mileage = deal.get("mileage", 0)
        year = deal.get("year", "")
        name = deal.get("car_name", deal.get("car_query", "Unknown"))
        source = deal.get("source", "")
        location = deal.get("location", "")
        drivetrain = deal.get("drivetrain", "")
        title_type = deal.get("title_type", "")
        avg_price = deal.get("trim_avg_price") or deal.get("avg_price", 0)
        href = deal.get("href", "")

        # Build fields
        fields = [
            {"name": "Price", "value": f"${price:,.0f}", "inline": True},
            {"name": "Score", "value": f"{score:.0f}/100", "inline": True},
            {"name": "Source", "value": source, "inline": True},
        ]

        if mileage:
            fields.append({"name": "Mileage", "value": f"{mileage:,.0f} mi", "inline": True})
        if drivetrain:
            fields.append({"name": "Drivetrain", "value": drivetrain, "inline": True})
        if title_type:
            fields.append({"name": "Title", "value": title_type.title(), "inline": True})
        if location:
            fields.append({"name": "Location", "value": location, "inline": True})
        if avg_price and avg_price > price:
            savings = avg_price - price
            fields.append({"name": "Below Avg", "value": f"${savings:,.0f}", "inline": True})

        # Color based on score
        if score >= 85:
            color = 0x34D399  # green
        else:
            color = 0x5B8DEE  # blue

        embed = {
            "title": f"Grade A Deal: {name}",
            "url": href,
            "color": color,
            "fields": fields,
        }

        if deal.get("image_url"):
            embed["thumbnail"] = {"url": deal["image_url"]}

        payload = {"embeds": [embed]}
        if _send_webhook(webhook_url, payload):
            newly_notified.append(href)

    if newly_notified:
        _save_notified(newly_notified)
        logging.info(f"Discord: sent {len(newly_notified)} Grade A deal alerts")


def notify_scrape_complete(config, deals):
    """Main entry point — called after a scrape finishes.

    Sends scrape summary + individual Grade A alerts if Discord is configured.
    """
    notif_config = config.get("Notifications", {})
    webhook_url = notif_config.get("discord_webhook_url", "")

    if not webhook_url:
        return

    app_url = notif_config.get("app_url", "")

    send_scrape_summary(webhook_url, deals)
    send_deal_alerts(webhook_url, deals, app_url=app_url)
