"""Selenium WebDriver setup with anti-detection and optional proxy support."""

import logging
import os
import random
from pathlib import Path
from urllib.parse import urlparse

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service

_DEFAULT_PROFILE = str(
    Path.home() / "Library" / "Application Support" / "Firefox" / "Profiles" / "6kmbn0d4.fbscraper"
)
FIREFOX_PROFILE = os.environ.get("FIREFOX_PROFILE", _DEFAULT_PROFILE)


def _apply_proxy(options, proxy_url):
    """Configure Firefox proxy settings from a URL like http://host:port or socks5://host:port."""
    parsed = urlparse(proxy_url)
    scheme = (parsed.scheme or "http").lower()
    host = parsed.hostname
    port = parsed.port

    if not host or not port:
        logging.warning(f"Invalid proxy URL: {proxy_url}")
        return

    if scheme in ("socks5", "socks5h"):
        options.set_preference("network.proxy.type", 1)
        options.set_preference("network.proxy.socks", host)
        options.set_preference("network.proxy.socks_port", port)
        options.set_preference("network.proxy.socks_version", 5)
        # Route DNS through proxy too (prevents DNS leaks)
        options.set_preference("network.proxy.socks_remote_dns", True)
        logging.info(f"Using SOCKS5 proxy: {host}:{port}")
    elif scheme in ("socks4",):
        options.set_preference("network.proxy.type", 1)
        options.set_preference("network.proxy.socks", host)
        options.set_preference("network.proxy.socks_port", port)
        options.set_preference("network.proxy.socks_version", 4)
        logging.info(f"Using SOCKS4 proxy: {host}:{port}")
    else:
        # HTTP/HTTPS proxy
        options.set_preference("network.proxy.type", 1)
        options.set_preference("network.proxy.http", host)
        options.set_preference("network.proxy.http_port", port)
        options.set_preference("network.proxy.ssl", host)
        options.set_preference("network.proxy.ssl_port", port)
        options.set_preference("network.proxy.no_proxies_on", "localhost, 127.0.0.1")
        logging.info(f"Using HTTP proxy: {host}:{port}")


def create_driver(proxy_config=None):
    """Create a Firefox WebDriver with stealth settings.

    Args:
        proxy_config: Optional proxy configuration dict from Config.json.
            Supports:
              {"url": "http://host:port"}             — single HTTP proxy
              {"url": "socks5://host:port"}           — single SOCKS5 proxy
              {"urls": ["http://...", "socks5://..."]} — random rotation
            The proxy is applied per-driver, so each new driver creation
            picks a random proxy from the list (if urls is provided).
    """
    options = Options()

    # Headless mode for Docker / CI
    if os.environ.get("HEADLESS") == "1":
        options.add_argument("--headless")

    # Use dedicated Firefox profile (has FB login cookies)
    profile_path = Path(FIREFOX_PROFILE)
    if profile_path.exists():
        logging.info(f"Using Firefox profile: {FIREFOX_PROFILE}")
        options.profile = FIREFOX_PROFILE

    # ── Proxy configuration ──
    if proxy_config:
        urls = proxy_config.get("urls", [])
        single = proxy_config.get("url", "")
        proxy_url = random.choice(urls) if urls else single
        if proxy_url:
            _apply_proxy(options, proxy_url)

    # ── Core anti-detection: disable the webdriver flag ──
    options.set_preference("dom.webdriver.enabled", False)
    options.set_preference("useAutomationExtension", False)

    # Disable Marionette's navigator.webdriver exposure
    options.set_preference("marionette.enabled", True)

    # ── Make the browser look normal ──
    if os.environ.get("DOCKER_MODE"):
        ua = "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0"
    else:
        ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:128.0) Gecko/20100101 Firefox/128.0"
    options.set_preference("general.useragent.override", ua)

    # Don't resist fingerprinting (it makes you look MORE suspicious)
    options.set_preference("privacy.resistFingerprinting", False)

    # Normal browser features FB checks for
    options.set_preference("media.navigator.enabled", True)
    options.set_preference("media.peerconnection.enabled", True)
    options.set_preference("geo.enabled", True)

    # Disable telemetry / automation markers
    options.set_preference("toolkit.telemetry.enabled", False)
    options.set_preference("datareporting.policy.dataSubmissionEnabled", False)
    options.set_preference("devtools.jsonview.enabled", False)

    # Don't show automation toolbar / banners
    options.set_preference("toolkit.legacyUserProfileCustomizations.stylesheets", True)

    service = Service(log_output="/dev/null")
    driver = webdriver.Firefox(options=options, service=service)

    # Inject stealth scripts to hide automation signals
    stealth_js = """
    // Hide webdriver property
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined
    });

    // Normal plugins array (empty looks suspicious)
    Object.defineProperty(navigator, 'plugins', {
        get: () => [1, 2, 3, 4, 5]
    });

    // Normal languages
    Object.defineProperty(navigator, 'languages', {
        get: () => ['en-US', 'en']
    });

    // Hide automation-related properties
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications' ?
            Promise.resolve({ state: Notification.permission }) :
            originalQuery(parameters)
    );

    // Chrome runtime check (some sites check for this even in Firefox)
    window.chrome = { runtime: {} };
    """
    driver.execute_script(stealth_js)

    return driver
