# Made by Kasra Tookallo
# In the year 2026
#-------------------------------------------------------------------------------------------------
"""
scrapes the country name, capital, population, and area
for the first 20 countries from the ‘Countries’ page on the educational website
 'ScrapeThisSite' and saves them into a database of your choice.

Source URL:
https://www.scrapethissite.com/pages/simple/
"""
#-------------------------------------------------------------------------------------------------
import requests
from bs4 import BeautifulSoup
import sqlite3

# ===== Stage 1: Configuration and Setup =====
SOURCE_URL = "https://www.scrapethissite.com/pages/simple/"
DB_NAME = "countries.db"
TABLE_NAME = "countries"


# ===== Stage 2: Database Schema Initialization =====
def init_db(db_name: str):
    conn = sqlite3.connect(db_name)
    try:
        cur = conn.cursor()
        # Create table (if it exists, it will be cleared first)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_name TEXT NOT NULL,
                capital TEXT,
                population INTEGER,
                area INTEGER
            );
        """)
        # Clear the table for new data insertion
        cur.execute(f"DELETE FROM {TABLE_NAME};")
        conn.commit()
        return conn
    except Exception as e:
        print("Error initializing/clearing database:", e)
        conn.close()
        raise


# ===== Stage 3: Data Scraping =====
def fetch_and_parse(url: str):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/115.0 Safari/537.36"
        }
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
        return BeautifulSoup(resp.text, "html.parser")
    except requests.RequestException as e:
        print("Error fetching web page:", e)
        return None


def to_int_from_text(value_text: str):
    if value_text is None:
        return None
    text = value_text.strip()
    # Allow characters relevant for numbers, including scientific notation and decimals
    allowed = "0123456789.eE"
    cleaned = "".join(ch for ch in text if ch in allowed)
    if cleaned == "":
        return None
    try:
        # First, convert to float, then truncate to int (floor)
        return int(float(cleaned))
    except ValueError:
        # Fallback: Keep only digits
        digits_only = "".join(ch for ch in text if ch.isdigit())
        return int(digits_only) if digits_only else None


def scrape_countries_data(soup: BeautifulSoup, limit: int = 20):
    if soup is None:
        return []
    data = []
    # Select country blocks. Common structure on this page: div.country
    country_cards = soup.select("div.country")
    # Fallback if structure changes: based on headers
    if not country_cards:
        country_cards = soup.select("h3.country-name")

    for i, card in enumerate(country_cards[:limit]):
        # Extract Country Name
        name = None
        # Multiple attempts to find the name
        name_tag = card.select_one(".country-name") if card else None
        if name_tag and name_tag.get_text(strip=True):
            name = name_tag.get_text(strip=True)
        else:
            # If the card itself is h3.country-name
            if card.name == "h3":
                name = card.get_text(strip=True)

        # Extract Capital
        capital = None
        cap_tag = card.select_one(".country-capital")
        if cap_tag and cap_tag.get_text(strip=True):
            capital = cap_tag.get_text(strip=True)

        # Extract Population
        population = None
        pop_tag = card.select_one(".country-population")
        if pop_tag and pop_tag.get_text(strip=True):
            population = to_int_from_text(pop_tag.get_text())

        # Extract Area
        area = None
        area_tag = card.select_one(".country-area")
        if area_tag and area_tag.get_text(strip=True):
            area = to_int_from_text(area_tag.get_text())

        # Construct the record
        data.append({
            "country_name": name or "",
            "capital": capital or None,
            "population": population,
            "area": area
        })
    return data


# ===== Stage 4: Data Saving =====
def save_batch(conn: sqlite3.Connection, records):
    try:
        cur = conn.cursor()
        cur.executemany(
            f"INSERT INTO {TABLE_NAME} (country_name, capital, population, area) VALUES (?, ?, ?, ?);",
            [(r["country_name"], r["capital"], r["population"], r["area"]) for r in records]
        )
        conn.commit()
    except Exception as e:
        print("Error during batch insertion:", e)
        raise


# ===== Stage 5: Validation and Reporting =====
def report(conn: sqlite3.Connection):
    try:
        cur = conn.cursor()
        print("Scraping and saving completed successfully.")
        # Display the first 5 records
        cur.execute(f"SELECT id, country_name, capital, population, area FROM {TABLE_NAME} ORDER BY id LIMIT 5;")
        rows = cur.fetchall()
        print("--- First 5 Records ---")
        for row in rows:
            print(f"id={row[0]} | Country={row[1]} | Capital={row[2]} | Population={row[3]} | Area(km^2)={row[4]}")
        # Sum of population for all 20 records
        cur.execute(f"SELECT SUM(population) FROM {TABLE_NAME};")
        total_pop = cur.fetchone()[0]
        print("--------------------")
        print(f"Total Population for 20 records: {total_pop}")
    except Exception as e:
        print("Error during reporting:", e)
        raise


# ===== Final Execution =====
def main():
    conn = None
    try:
        conn = init_db(DB_NAME)
        soup = fetch_and_parse(SOURCE_URL)
        if soup is None:
            print("Could not access website or content retrieval failed. Program halted.")
            return
        records = scrape_countries_data(soup, limit=20)
        if not records:
            print("No data found to save. Program halted.")
            return
        save_batch(conn, records)
        report(conn)
    except Exception:
        # Catches errors from init_db, save_batch, or report
        pass
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
