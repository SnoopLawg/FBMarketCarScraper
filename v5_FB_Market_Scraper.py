import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
from selenium.webdriver.firefox.options import Options
import json
import sqlite3
import requests
import re
import logging
import time
import webbrowser
import tkinter as tk
import sys
from tkinter import messagebox

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class FacebookMarketplaceScraper:
    def __init__(self):
        logging.info("Initializing FacebookMarketplaceScraper...")
        options = Options()
        # options.add_argument("-headless")
        self.driver = webdriver.Firefox(options=options)
        # Read JSON file
        try:
            with open("FB Marketplace/Config.json", "r") as json_file:
                config = json.load(json_file)
        except:
            with open("Config.json", "r") as json_file:
                config = json.load(json_file)
        self.config = config
        self.passive_mode = config.get("Passive")
        self.desired_car = config.get("DesiredCar")
        self.min_mileage = config.get("MileageMin")
        self.max_mileage = config.get("MileageMax")
        self.city_id = config.get("CityID")
        self.min_price = config.get("MinPrice")
        self.max_price = config.get("MaxPrice")
        self.price_threshold = config.get("PriceThreshold")
        self.db_name = "marketplace_listings"
        self.purchase_location = config.get("PurchaseLocation")
        if self.passive_mode != "yes":
            self.desired_car = config.get("DesiredCar", [])
            self.db_name = f"{self.desired_car}"

    def create_database(self):
        logging.info("Creating database...")
        self.conn = sqlite3.connect(
            f"marketplace_listings.db"
        )  # Always connect to the same database
        self.cur = self.conn.cursor()

    def create_table(self, car_name):
        cleaned_name = self.clean_table_name(car_name)
        self.cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {cleaned_name}_listings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                href TEXT,
                image_url TEXT,
                price TEXT,
                car TEXT,
                location TEXT,
                mileage TEXT
            )
        """
        )
        self.conn.commit()

    def check_duplicate(self, car_name, image_url):
        cleaned_name = self.clean_table_name(car_name)
        self.cur.execute(
            f"SELECT * FROM {cleaned_name}_listings WHERE image_url=?", (image_url,)
        )
        existing_listing = self.cur.fetchone()
        return existing_listing is not None

    def insert_data(self, car_name, listing):
        cleaned_name = self.clean_table_name(car_name)
        href, image_url, price, car, location, mileage = listing
        if not self.check_duplicate(car_name, image_url):
            logging.info(f"Inserting new car data for: {car_name}")
            self.cur.execute(
                f"""
                INSERT INTO {cleaned_name}_listings (href, image_url, price, car, location, mileage)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (href, image_url, price, car, location, mileage),
            )
            self.conn.commit()

    def get_url(self, car):
        if not self.passive_mode:
            return f"https://www.facebook.com/marketplace/{self.city_id}/search?minPrice={self.min_price}&maxPrice={self.max_price}&query={car}&exact=false"
        else:
            return f"https://www.facebook.com/marketplace/{self.city_id}/vehicles/?minPrice={self.min_price}&maxPrice={self.max_price}&maxMileage={self.max_mileage}&topLevelVehicleType=car_truck&exact=false"

    def scrape_data(self):
        logging.info("Starting data scraping...")
        try:
            self.create_database()
            time.sleep(5)
            # Iterate through each desired car
            for desired_car in self.desired_car:
                # Update URL for the current desired car
                current_desired_car = desired_car
                self.driver.get(self.get_url(current_desired_car))
                # time.sleep(5)
                self.driver.implicitly_wait(15)

                # Create a table for the current desired car
                self.create_table(current_desired_car)

                for _ in range(10):
                    self.driver.find_element(By.TAG_NAME, "body").send_keys(Keys.END)
                    time.sleep(2)

                page_source = self.driver.page_source
                soup = BeautifulSoup(page_source, "html.parser")
                listings = soup.find_all(class_="x3ct3a4")

                test = soup.find_all(
                    class_="x1lliihq x6ikm8r x10wlt62 x1n2onr6 xlyipyv xuxw1ft"
                )

                num = 0
                loc_index = 925
                for item in listings:
                    price_test = item.find_all(
                        "div",
                        class_="x1gslohp xkh6y0r",
                    )
                    if len(price_test) == 3:
                        price, title, city = price_test
                        price = price.text.strip().split("$")[1:]
                        title = title.text.strip()
                        city = city.text.strip()
                        miles = "N/A"
                        if len(price) > 1:
                            price, compare_at_price = price
                            car_elems = price + compare_at_price + title + city
                        else:
                            price = price[0]
                            car_elems = price + title + city

                    elif len(price_test) == 4:
                        price, title, city, miles = price_test
                        price = price.text.strip().split("$")[1:]
                        title = title.text.strip()
                        city = city.text.strip()
                        miles = miles.text.strip()
                        if len(price) > 1:
                            price, compare_at_price = price
                            car_elems = price + compare_at_price + title + city + miles
                        elif len(price) == 0:
                            break
                        else:
                            price = price[0]
                            car_elems = price + title + city + miles
                    img_element = item.find("img", {"src": True})
                    match = item.text.strip()
                    if match and car_elems:
                        href = item.find("a")["href"]
                        if href:
                            deleted = self.has_listing_been_deleted(href)
                            if deleted:
                                continue
                            listing = (
                                href,
                                img_element["src"],
                                price,
                                title,
                                city,
                                miles,
                                num,
                            )
                            # self.print_listing(listing)
                            listing = (
                                href,
                                img_element["src"],
                                price,
                                title,
                                city,
                                miles,
                            )
                            self.insert_data(current_desired_car, listing)
                            loc_index += 2
        finally:
            self.desired_car = self.config.get("DesiredCar", [])
            logging.info("Data scraping completed.")
            self.driver.quit()

    def clean_database_listings(self):
        """Clean up the database listings that don't have the desired car's name."""
        logging.info("Cleaning up database listings...")

        for desired_car in self.desired_car:
            cleaned_name = self.clean_table_name(desired_car)
            table_name = f"{cleaned_name}_listings"

            # Fetch all listings from the table
            self.cur.execute(f"SELECT id, car FROM {table_name}")
            listings = self.cur.fetchall()

            # Check each listing and delete if the car's name doesn't match the desired car's name
            for listing_id, car_name in listings:
                if desired_car.lower() not in car_name.lower():
                    self.cur.execute(
                        f"DELETE FROM {table_name} WHERE id=?", (listing_id,)
                    )
                    logging.info(
                        f"Deleted listing with ID {listing_id} from {table_name} as it doesn't match the desired car's name."
                    )

            self.conn.commit()

    def extract_year_from_car_name(self, car_name):
        year_match = re.search(r"\b\d{4}\b", car_name)
        if year_match:
            return int(year_match.group())
        else:
            return None

    def clean_table_name(self, car_name):
        # Remove special characters from the car_name and replace spaces with underscores
        cleaned_name = re.sub(r"[^\w\s]", "", car_name)
        cleaned_name = cleaned_name.replace(" ", "_")
        return cleaned_name

    def calculate_average_prices(self, car_name):
        logging.info(f"Calculating average prices for car: {car_name}")
        cleaned_name = self.clean_table_name(car_name)
        cur = self.conn.cursor()
        cur.execute(f"SELECT * FROM {cleaned_name}_listings")
        rows = cur.fetchall()

        car_prices = {}
        for row in rows:
            # Extract year from the car string
            year = re.search(r"\b\d{4}\b", row[4])
            if year:
                year = year.group(0)
                if year not in car_prices:
                    car_prices[year] = []
                price = float(row[3].replace("$", "").replace(",", ""))
                # Extract numeric mileage from the string using regular expression
                mileage_match = re.search(r"\b\d+\.?\d*\.?[Kk]?\b", row[6])
                if mileage_match:
                    mileage_str = mileage_match.group(0)
                    # Remove 'K' or 'k' if present, replace commas, and convert to float
                    mileage = float(mileage_str.rstrip("Kk").replace(",", ""))
                else:
                    mileage = 0  # Set a default value or handle this case as needed

                car_prices[year].append((price, mileage))

        average_prices = {}
        for year, data in car_prices.items():
            # Calculate average price for cars with 10,000 less and 10,000 more miles
            lower_mileage_prices = [price for price, mileage in data if mileage <= 150]
            higher_mileage_prices = [price for price, mileage in data if mileage >= 150]
            if lower_mileage_prices:
                average_lower_mileage_price = round(
                    sum(lower_mileage_prices) / len(lower_mileage_prices)
                )
            else:
                average_lower_mileage_price = 0
            if higher_mileage_prices:
                average_higher_mileage_price = round(
                    sum(higher_mileage_prices) / len(higher_mileage_prices)
                )
            else:
                average_higher_mileage_price = 0

            average_prices[year] = {
                "average_lower_mileage_price": average_lower_mileage_price,
                "average_higher_mileage_price": average_higher_mileage_price,
            }
        # Sort the average_prices dictionary by year in ascending order
        sorted_average_prices = dict(sorted(average_prices.items()))

        return sorted_average_prices

    def create_average_price_tables(self):
        for desired_car in self.desired_car:
            average_prices = self.calculate_average_prices(desired_car)
            cleaned_name = self.clean_table_name(desired_car)
            table_name = f"{cleaned_name}_average_prices"

            # Create a new table for average prices
            self.cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    year INTEGER PRIMARY KEY,
                    average_lower_mileage_price REAL,
                    average_higher_mileage_price REAL
                )
            """
            )

            # Populate the table with average prices data, updating existing rows if duplicates are found
            for year, prices in average_prices.items():
                self.cur.execute(
                    f"""
                    INSERT OR REPLACE INTO {table_name} (year, average_lower_mileage_price, average_higher_mileage_price)
                    VALUES (?, ?, ?)
                """,
                    (
                        year,
                        prices["average_lower_mileage_price"],
                        prices["average_higher_mileage_price"],
                    ),
                )

            self.conn.commit()

    def deal_assesment(self):
        logging.info("Assessing deals...")
        deals = []
        for desired_car in self.desired_car:
            cleaned_name = self.clean_table_name(desired_car)
            avg_table_name = f"{cleaned_name}_average_prices"
            listing_table_name = f"{cleaned_name}_listings"
            cur = self.conn.cursor()
            cur.execute(f"SELECT * FROM {listing_table_name}")
            listing_rows = cur.fetchall()
            cur.execute(f"SELECT * FROM {avg_table_name}")
            avg_rows = cur.fetchall()
            for i, row in enumerate(listing_rows):
                # Extract year and mileage from all desired car listing
                year = re.search(r"\b\d{4}\b", row[4])
                mileage_match = re.search(r"\b\d+\.?\d*\.?[Kk]?\b", row[6])
                if mileage_match:
                    mileage_str = mileage_match.group(0)
                    # Remove 'K' or 'k' if present, replace commas, and convert to float
                    mileage = float(mileage_str.rstrip("Kk").replace(",", ""))
                    price = row[3]
                    if price == "Sold":
                        continue
                    price = float(row[3].replace("$", "").replace(",", ""))
                else:
                    mileage = 0
                if year is None:
                    continue
                year = year.group()
                year = int(year)
                mileage = int(mileage)
                city = row[5]
                if year:
                    if self.purchase_location in city:
                        for avgrow in avg_rows:
                            avgyear = avgrow[0]
                            if year == avgyear:
                                low_mileage = False
                                high_mileage = False
                                low_avg = avgrow[1]
                                high_avg = avgrow[2]
                                if mileage <= 150:
                                    low_mileage = mileage
                                elif mileage >= 150:
                                    high_mileage = mileage
                                new_low_price_threshold = price - self.price_threshold
                                new_high_price_threshold = price + self.price_threshold
                                if low_mileage:
                                    if new_low_price_threshold < low_avg:
                                        try:
                                            full_low_mileage = low_mileage * 1000
                                            if self.min_mileage:
                                                if full_low_mileage > self.min_mileage:
                                                    deals.append(
                                                        f"https://www.facebook.com{listing_rows[i][1]}"
                                                    )
                                                    print(
                                                        f"\n https://www.facebook.com{listing_rows[i][1]} is a low mileage deal. Lower than {high_avg}"
                                                    )
                                            elif self.min_mileage == None:
                                                deals.append(
                                                    f"https://www.facebook.com{listing_rows[i][1]}"
                                                )
                                        except:
                                            print("\nFAIL AGAIN")
                                elif high_mileage:
                                    if new_high_price_threshold < high_avg:
                                        try:
                                            full_high_mileage = high_mileage * 1000
                                            if self.max_mileage:
                                                if full_high_mileage < self.max_mileage:

                                                    deals.append(
                                                        f"https://www.facebook.com{listing_rows[i][1]}"
                                                    )
                                                    # print(
                                                    #     f"\n https://www.facebook.com{listing_rows[i][1]} is a high mileage deal. Lower than {high_avg}"
                                                    # )

                                            elif self.max_mileage == None:
                                                deals.append(
                                                    f"https://www.facebook.com{listing_rows[i][1]}"
                                                )
                                        except:
                                            print("\nFAIL AGAIN")
        self.current_deal_index = 0
        self.deals = deals

        # Call the GUI navigator
        self.gui_navigator()

    def gui_navigator(self):
        # Create the main window for GUI
        self.root = tk.Tk()
        self.root.title("Deal Navigator")

        # Ensure the GUI window always stays on top
        self.root.attributes("-topmost", True)

        # Create a frame to hold the buttons
        frame = tk.Frame(self.root)
        frame.pack(pady=20)

        # Create and place the "Quit" button on the left
        quit_button = tk.Button(frame, text="Quit", command=self.quit_program)
        quit_button.pack(side=tk.LEFT, padx=10)

        # Create and place the "Next" button on the right
        next_button = tk.Button(frame, text="Next", command=self.open_next_deal)
        next_button.pack(side=tk.RIGHT, padx=10)

        delete_button = tk.Button(
            frame, text="Delete Listing", command=self.delete_current_listing
        )
        delete_button.pack(side=tk.LEFT, padx=10)

        favorite_button = tk.Button(
            self.root, text="Favorite", command=self.favorite_listing
        )
        favorite_button.pack(pady=5)

        # Start the GUI event loop
        self.root.mainloop()

    def has_listing_been_viewed(self, href):
        """Check if the href of the listing exists in the txt file."""
        with open("viewed_listings.txt", "a+") as file:
            file.seek(0)  # Move the file pointer to the beginning of the file
            return href in file.read()

    def save_listing_to_txt(self, href):
        """Save the href of the listing to a txt file if it doesn't exist."""
        with open("viewed_listings.txt", "a+") as file:
            file.seek(0)  # Move the file pointer to the beginning of the file
            if href not in file.read():
                file.write(f"{href}\n")

    def favorite_listing(self):
        # Check if the href is available
        if self.current_listing_href:
            # Append the href to favorite_listings.txt
            with open("favorite_listings.txt", "a") as file:
                file.write(self.current_listing_href + "\n")
            messagebox.showinfo("Info", "Listing added to favorites!")
        else:
            messagebox.showwarning("Warning", "No listing selected!")

    def save_deleted_listing_to_txt(self, href):
        """Save the href of the listing to a txt file if it doesn't exist."""
        with open("deleted_listings.txt", "a+") as file:
            file.seek(0)  # Move the file pointer to the beginning of the file
            if href not in file.read():
                file.write(f"{href}\n")

    def has_listing_been_deleted(self, href):
        """Check if the href of the listing exists in the txt file."""
        with open("deleted_listings.txt", "a+") as file:
            file.seek(0)  # Move the file pointer to the beginning of the file
            if href in file.read():
                return href in file.read()

    def open_next_deal(self):
        while self.current_deal_index < len(self.deals):
            # Check if the deal has been viewed
            if not self.has_listing_been_viewed(self.deals[self.current_deal_index]):
                # Extract the current listing's ID from the URL
                url_parts = self.deals[self.current_deal_index].split("/")
                self.current_listing_id = url_parts[url_parts.index("item") + 1]
                self.current_listing_href = self.deals[self.current_deal_index]
                # If there's more than one tab open, close the last tab
                if len(self.driver.window_handles) > 1:
                    self.driver.switch_to.window(self.driver.window_handles[-1])
                    self.driver.close()
                    self.driver.switch_to.window(self.driver.window_handles[0])

                # Open the next deal in a new tab
                self.driver.execute_script(
                    f"window.open('{self.deals[self.current_deal_index]}', '_blank')"
                )

                # Save the href to the txt file
                self.save_listing_to_txt(self.deals[self.current_deal_index])

                time.sleep(2)  # Give the browser time to open
                break  # Exit the loop after opening the deal

            self.current_deal_index += 1  # Move to the next deal

        else:
            self.quit_program()

    def delete_current_listing(self):
        """Delete the currently viewed listing from the database."""
        if hasattr(self, "current_listing_id"):
            # Determine which car is currently being viewed based on the href
            current_car = None
            current_listing, current_car = self.find_listing_by_href(
                self.current_listing_href
            )
            if current_car:
                cleaned_name = self.clean_table_name(current_car)
                table_name = f"{cleaned_name}_listings"
                self.cur.execute(
                    f"DELETE FROM {table_name} WHERE id=?", (self.current_listing_id,)
                )
                self.conn.commit()
                self.save_deleted_listing_to_txt(self.current_listing_href)
                logging.info(
                    f"Deleted listing with ID {self.current_listing_id} from {table_name}."
                )
                self.open_next_deal()

    def find_listing_by_href(self, href):
        # Connect to the SQLite database
        conn = sqlite3.connect("marketplace_listings.db")
        cursor = conn.cursor()
        relative_href = href.replace("https://www.facebook.com", "")
        for i in self.desired_car:
            j = self.clean_table_name(i)
            # Execute the SQL query to search for the href
            cursor.execute(f"SELECT * FROM {j}_listings WHERE href=?", (relative_href,))
            result = cursor.fetchone()
            if result:
                break
        # Close the connection
        conn.close()

        return result, i

    def quit_program(self):
        # Close the Selenium browser window
        self.driver.quit()
        # Close the tkinter window
        self.root.quit()
        # Terminate the script
        sys.exit()

    def process_data(self):
        self.create_average_price_tables()

    def close_connection(self):
        logging.info("Closing database connection...")
        self.conn.close()

    @staticmethod
    def print_listing(listing):
        print(
            f"\n\n**({listing[5]})**\nPrice: {listing[1]}\nCar: {listing[2]}\nLocation: {listing[3]}\nMileage: {listing[4]}\nImage URL: {listing[0]}"
        )

    def open_favorites(self):
        try:
            with open("favorite_listings.txt", "r") as file:
                # Read each line in the file
                for line in file:
                    # Strip any leading/trailing whitespace
                    url = line.strip()
                    # Open the URL in the default browser
                    if url:  # Check if the line is not empty
                        webbrowser.open(url)
        except:
            self.quit_program()


def gather():
    scraper = FacebookMarketplaceScraper()
    scraper.scrape_data()
    scraper.create_database()
    scraper.clean_database_listings()
    scraper.process_data()


def check_em_out():
    scraper = FacebookMarketplaceScraper()
    scraper.create_database()
    scraper.deal_assesment()
    scraper.close_connection()


def human():
    scraper = FacebookMarketplaceScraper()
    scraper.open_favorites()


gather()
check_em_out()
human()
