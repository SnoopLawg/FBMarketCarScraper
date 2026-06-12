"""Scraper registry — import and list all available scrapers."""

from scrapers.facebook import FacebookScraper
from scrapers.carscom import CarsComScraper
from scrapers.autotrader import AutotraderScraper
from scrapers.ksl import KSLScraper

ALL_SCRAPERS = {
    "facebook": FacebookScraper,
    "carscom": CarsComScraper,
    "autotrader": AutotraderScraper,
    "ksl": KSLScraper,
}
