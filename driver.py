"""Selenium WebDriver setup with anti-detection measures."""

import logging
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service


FIREFOX_PROFILE = str(
    Path.home() / "Library" / "Application Support" / "Firefox" / "Profiles" / "6kmbn0d4.fbscraper"
)


def create_driver():
    """Create a Firefox WebDriver with stealth settings."""
    options = Options()

    # Use dedicated Firefox profile (has FB login cookies)
    profile_path = Path(FIREFOX_PROFILE)
    if profile_path.exists():
        logging.info(f"Using Firefox profile: {FIREFOX_PROFILE}")
        options.profile = FIREFOX_PROFILE

    # ── Core anti-detection: disable the webdriver flag ──
    options.set_preference("dom.webdriver.enabled", False)
    options.set_preference("useAutomationExtension", False)

    # Disable Marionette's navigator.webdriver exposure
    options.set_preference("marionette.enabled", True)

    # ── Make the browser look normal ──
    # Real user-agent (Firefox 128 on macOS)
    options.set_preference("general.useragent.override",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:128.0) Gecko/20100101 Firefox/128.0")

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
