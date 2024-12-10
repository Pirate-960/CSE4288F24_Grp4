import json
import logging
import os
import random
import time
from typing import Dict, List
from urllib.parse import urljoin

import aiohttp
import asyncio
from bs4 import BeautifulSoup

# Configurations
# Navigate up one level from the script directory to the parent directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'Output')
DATA_FILE = os.path.join(OUTPUT_DIR, 'aym_kararlar.json')
LOG_FILE = os.path.join(OUTPUT_DIR, 'scraping_log.txt')

ROOT_URL = "https://kararlarbilgibankasi.anayasa.gov.tr"
TOTAL_PAGES = 1410  # Adjust as needed
CONCURRENT_REQUESTS = 20

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Logging setup
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Utility functions
def load_previous_data(path: str) -> Dict:
    """Load previously scraped data."""
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as file:
                return json.load(file)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            logging.error(f"Error loading data from {path}: {e}. Initializing empty data.")
            return {"Kararlar": []}
    return {"Kararlar": []}

def save_data(path: str, data: Dict):
    """Save data to a JSON file with robust error handling."""
    try:
        # Ensure the directory exists before saving
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        with open(path, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
    except Exception as e:
        logging.error(f"Error saving data to {path}: {e}")
        print(f"Error saving data: {e}")

def is_data_exist(data: Dict, url: str) -> bool:
    """Check if a URL already exists in the dataset."""
    return any(karar.get("Kararın Bağlantı Linki") == url for karar in data.get("Kararlar", []))

# Scraping functions
async def fetch(session, url: str) -> str:
    """Fetch HTML content from a URL."""
    try:
        async with session.get(url, ssl=False) as response:
            if response.status == 200:
                return await response.text()
            logging.error(f"Non-200 response for {url}: {response.status}")
    except aiohttp.ClientError as e:
        logging.error(f"Error fetching {url}: {e}")
    return ""

async def get_links_from_page(session, url: str) -> List[str]:
    """Extract decision links from a page."""
    html_content = await fetch(session, url)
    if not html_content:
        return []

    soup = BeautifulSoup(html_content, 'lxml')
    box = soup.find("div", class_="karargoruntulemealani col-sm-12")
    if not box:
        return []

    return [urljoin(ROOT_URL, link['href']) for link in box.find_all('a', href=True) if "?" not in link['href']]

async def scrape_decision(session, link: str) -> Dict:
    """Scrape data for a single decision."""
    html_content = await fetch(session, link)
    if not html_content:
        return {}

    soup = BeautifulSoup(html_content, 'lxml')

    karar_html = soup.find("span", class_="kararHtml")
    decision_text = karar_html.text.strip() if karar_html else ""

    karar_bilgi = {}
    kimlik_bilgi_table = soup.find("div", id="KararDetaylari").find("table")
    if kimlik_bilgi_table:
        for row in kimlik_bilgi_table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) == 2:
                key = cells[0].text.strip()
                value = cells[1].text.strip()
                karar_bilgi[key] = value

    basvuru_konusu = ""
    basvuru_section = soup.find("h4", string="II. BAŞVURU KONUSU")
    if basvuru_section:
        basvuru_konusu = basvuru_section.find_next("br").next_sibling.strip()

    inceleme_sonuclari = []
    results_table = soup.find("div", class_="table-responsive")
    if results_table:
        for row in results_table.find_all("tr"):
            columns = [col.text.strip() for col in row.find_all("td")]
            if columns:
                inceleme_sonuclari.append({
                    "Hak": columns[0] if len(columns) > 0 else "",
                    "Müdahale İddiası": columns[1] if len(columns) > 1 else "",
                    "Sonuç": columns[2] if len(columns) > 2 else "",
                    "Giderim": columns[3] if len(columns) > 3 else "",
                })

    return {
        "Kararın Bağlantı Linki": link,
        "Karar Metni": decision_text,
        "Karar Bilgileri": karar_bilgi,
        "Başvuru Konusu": basvuru_konusu,
        "İnceleme Sonuçları": inceleme_sonuclari,
    }

async def scrape_page(session, data: Dict, page_number: int):
    """Scrape data from a single page."""
    page_url = f"{ROOT_URL}/?page={page_number}"
    print(f"Scraping page {page_number}...")

    links = await get_links_from_page(session, page_url)
    for link in links:
        if is_data_exist(data, link):
            print(f"Decision already exists: {link}")
            continue

        decision_data = await scrape_decision(session, link)
        if decision_data:
            data["Kararlar"].append(decision_data)
            save_data(DATA_FILE, data)  # Save progress immediately
            print(f"Scraped decision from {link}")
        await asyncio.sleep(random.uniform(1, 2))  # Simulate human behavior

async def main():
    # Load existing data or initialize
    data = load_previous_data(DATA_FILE)
    
    # Calculate the start page based on existing data
    decisions_per_page = 10
    start_page = (len(data.get("Kararlar", [])) // decisions_per_page) + 1

    print(f"Starting scraping from page {start_page}")

    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

        async def limited_scrape(page):
            async with semaphore:
                await scrape_page(session, data, page)

        tasks = [limited_scrape(page) for page in range(start_page, TOTAL_PAGES + 1)]
        await asyncio.gather(*tasks)

    save_data(DATA_FILE, data)
    print("Scraping complete. Data saved.")

# Entry point
if __name__ == "__main__":
    start_time = time.time()
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Unexpected error during scraping: {e}")
        print(f"An error occurred: {e}")
    finally:
        print(f"Scraping finished in {time.time() - start_time:.2f} seconds.")