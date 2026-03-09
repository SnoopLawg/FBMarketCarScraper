"""Scraper registry — import and list all available scrapers."""

from scrapers.facebook import FacebookScraper
from scrapers.craigslist import CraigslistScraper
from scrapers.carscom import CarsComScraper
from scrapers.autotrader import AutotraderScraper

ALL_SCRAPERS = {
    "facebook": FacebookScraper,
    "craigslist": CraigslistScraper,
    "carscom": CarsComScraper,
    "autotrader": AutotraderScraper,
}
