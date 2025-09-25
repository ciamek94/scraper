#!/usr/bin/env python3
# main.py
"""
Scraper OLX -> Excel + OneDrive + Telegram notifications (accepted/rejected).

Requirements:
  pip install requests beautifulsoup4 pandas openpyxl python-dotenv

Environment variables (set as GitHub Secrets):
  ONEDRIVE_CLIENT_ID
  ONEDRIVE_REFRESH_TOKEN
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID

Optional:
  ONEDRIVE_UPLOAD_FOLDER (e.g. "olx") - folder name in OneDrive root where files will be stored
"""

import os
import time
import random
import json
import requests
from bs4 import BeautifulSoup
import pandas as pd
import openpyxl
from collections import defaultdict
import re
from dotenv import load_dotenv

load_dotenv()

# --- Config (edytowalne) ---
# Lista wyszukiwań: każde wyszukiwanie to dict z 'name', 'url' i filtrami
SEARCHES = [
    {
        "name": "falownik",
        "urls": [
            "https://www.olx.pl/oferty/q-falownik/?search%5Bfilter_float_price:to%5D=200",
            "https://www.olx.pl/oferty/q-falownik/?search%5Bfilter_float_price:from%5D=201&search%5Bfilter_float_price:to%5D=400",
            "https://www.olx.pl/oferty/q-falownik/?search%5Bfilter_float_price:from%5D=401&search%5Bfilter_float_price:to%5D=700",
            "https://www.olx.pl/oferty/q-falownik/?search%5Bfilter_float_price:from%5D=701&search%5Bfilter_float_price:to%5D=1000",
            "https://www.olx.pl/oferty/q-falownik/?search%5Bfilter_float_price:from%5D=1001&search%5Bfilter_float_price:to%5D=1400",
            "https://www.olx.pl/oferty/q-falownik/?search%5Bfilter_float_price:from%5D=1401&search%5Bfilter_float_price:to%5D=2000",
            "https://www.olx.pl/oferty/q-falownik/?search%5Bfilter_float_price:from%5D=2001&search%5Bfilter_float_price:to%5D=3000",
            "https://www.olx.pl/oferty/q-falownik/?search%5Bfilter_float_price:from%5D=3001&search%5Bfilter_float_price:to%5D=4000"

        ],
        "forbidden_words": [
            "fotowoltaiczny", "fotowoltaika", "fotowoltaiki", "fotowoltaicznej", "pv", "fotowoltaiczne", "fotowoltaiczna",
            "solar", "solarny", "magazyn energii", "mikroinwerter", "wifi",
            "off-grid", "on-grid", "off grid", "on grid", "offgrid", "ongrid",
            "hybrydowy", "hybrydowa", "solaredge", "deye", "growatt", "huawei",
            "sofar", "sma", "fox", "foxess", "fronius", "mppt", "easun",
            "sinuspro", "anern", "jebao", "godwe", "goodwe", "afore", "solis",
            "solax", "akwarium", "samochód", "toyota", "kia", "tunze", "opel",
            "audi", "volkswagen", "nissan", "victron", "solplanet", "sunny", "boy", "sunny boy", 
            "growat", "solax", "hypontech", "kempingowy", "suszarka", "pralka", "pompa ciepła",
            "anenji", "mercedes", "prius", "betoniarka", "słoneczne", "słoneczny", "volt polska",
            "cyrkulator", "ups", "akwariowa", "frezarka", "tokarka", "daye", "hoymiles", "dokio",
            "mieszkanie", "victron", "peleciarka", "samochodowa", "peugeot", "renault", "wavemaker",
            "bmw", "suzuki", "kodak", "kostal", "fox", "suszarke", "pralke", "ibo", "rolmasaż",
            "hypnotech", "greencell", "green cell", "masażer", "rubik", "lexus", "motech", "ford",
            "blaupunkt", "rollmasaż", "volvo", "still", "kamper", "bank energii", "zoe", "eclipse cross",
            "turystyczny", "hyundai", "suszarki", "pralki", "bosch", "chevrolet", "outlander", "dewalt", "makita",
            "milwuakee", "lodówka", "lodówki", "agregat", "spawarka", "spawarki", "refusol", "wilo", "glebogryzarka",
            "rower", "hulajnoga", "hulajnogi", "zoe", "kangoo", "grundfoss", "grundfos", "mazda", "tesla", "pulsor", "dacia",
            "walcarka", "do włosów", "płyta indukcyjna", "optymalizator", "optymalizatora", "turbiny wiatrowej", "turbina wiatrowa",
            "powerstocc", "milwaukee", "akwarystyczny", "akumulator", "belkin", "samochodowy", "chrysler", "aqua", "jvp", "SSR",
            "skoda", "mostek trapezowy", "remington", "aquael", "yaris", "willo", "wkład kominowy", "einhell", "simet", "honda",
            "osram", "lokówka", "prostownica", "lunchbox", "hydroforowy", "falownica", "dell", "kotła", "dzban", "sukienka",
            "odstraszacz", "klimatyzatora", "hybryda", "gyre", "konwerter", "sun lite", "aurora", "mokka", "sunwind", "rav 4",
            "vw id3", "vw id.3", "stecagrid", "micovert", "omnigena", "podgrzewacz", "Jaguar", "mikrofala", "tosima", "maszynka do mięsa",
            "citroen", "jeep", "linde", "steca", "mastervolt", "wózek widłowy", "wózka widłowego", "fluke", "maszynka do mielenia", "porsche",
            "selfa", "pompy ciepla", "maszynka alfa","kuchenka", "mikrofala", "mikrofalówka",

            
        ],
        "required_words": [],  # jeżeli pusta -> brak wymagań, inaczej co najmniej 1 musi występować
        "max_price": None,     # liczba lub None
        "min_price": None
    },
    {
        "name": "sprężarka",
        "urls": ["https://www.olx.pl/oferty/q-spre%C5%BCarka-%C5%9Brubowa/?search%5Bfilter_float_price:to%5D=6000",],
        "forbidden_words": ["wynajem,"],
        "required_words": [],
        "max_price": None,
        "min_price": None
    }
]

WORKDIR = "output"
os.makedirs(WORKDIR, exist_ok=True)

CLIENT_ID = os.environ.get("ONEDRIVE_CLIENT_ID")
REFRESH_TOKEN = os.environ.get("ONEDRIVE_REFRESH_TOKEN")
TOKEN_URL = 'https://login.microsoftonline.com/consumers/oauth2/v2.0/token'
ONEDRIVE_UPLOAD_FOLDER = os.environ.get("ONEDRIVE_UPLOAD_FOLDER", "olx")

MAX_PAGES = 30
MAX_EMPTY_PAGES = 2

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# OneDrive paths
EXCEL_ACCEPTED_ONEDRIVE = f"{ONEDRIVE_UPLOAD_FOLDER}/accepted.xlsx"
EXCEL_REJECTED_ONEDRIVE = f"{ONEDRIVE_UPLOAD_FOLDER}/rejected.xlsx"
JSON_ACCEPTED_ONEDRIVE = f"{ONEDRIVE_UPLOAD_FOLDER}/accepted.json"
JSON_REJECTED_ONEDRIVE = f"{ONEDRIVE_UPLOAD_FOLDER}/rejected.json"
STATE_ONEDRIVE_PATH = f"{ONEDRIVE_UPLOAD_FOLDER}/state.json"

# Local paths
EXCEL_ACCEPTED_LOCAL = os.path.join(WORKDIR, "accepted.xlsx")
EXCEL_REJECTED_LOCAL = os.path.join(WORKDIR, "rejected.xlsx")
JSON_ACCEPTED_LOCAL = os.path.join(WORKDIR, "accepted.json")
JSON_REJECTED_LOCAL = os.path.join(WORKDIR, "rejected.json")
STATE_LOCAL = os.path.join(WORKDIR, "state.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; olx-scraper/1.0)",
    "Accept-Language": "pl-PL,pl;q=0.9"
}

def get_with_retry(url, headers=HEADERS, retries=4, backoff=2.0):
    for i in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code == 200:
                return r
        except Exception:
            pass
        time.sleep(backoff * (1 + random.random()))
    return None

def authenticate_onedrive():
    if not CLIENT_ID or not REFRESH_TOKEN:
        print("⚠️ OneDrive credentials not set.")
        return None
    data = {
        'client_id': CLIENT_ID,
        'refresh_token': REFRESH_TOKEN,
        'grant_type': 'refresh_token',
        'scope': 'offline_access Files.ReadWrite.All'
    }
    try:
        r = requests.post(TOKEN_URL, data=data, timeout=20)
        r.raise_for_status()
        print("✅ OneDrive auth successful. Access token obtained.")
        return r.json()
    except requests.exceptions.RequestException as e:
        print("❌ OneDrive auth failed:", e, r.text if 'r' in locals() else "")
        return None

def upload_to_onedrive_localpath(local_path, onedrive_path, token):
    if token is None:
        print("⚠️ No OneDrive token, skipping upload:", onedrive_path)
        return False
    access_token = token['access_token']
    upload_url = f'https://graph.microsoft.com/v1.0/me/drive/root:/{onedrive_path}:/content'
    headers = {'Authorization': f"Bearer {access_token}"}
    with open(local_path, "rb") as f:
        data = f.read()
    r = requests.put(upload_url, headers=headers, data=data, timeout=60)
    if r.status_code in (200, 201):
        print("✅ Uploaded to OneDrive:", onedrive_path)
        return True
    else:
        print("❌ Upload failed:", r.status_code, r.text)
        return False

def download_from_onedrive(onedrive_path, local_path, token):
    if token is None:
        print("⚠️ No OneDrive token, cannot download", onedrive_path)
        return False
    access_token = token['access_token']
    url = f'https://graph.microsoft.com/v1.0/me/drive/root:/{onedrive_path}:/content'
    headers = {'Authorization': f'Bearer {access_token}'}
    r = requests.get(url, headers=headers, timeout=60)
    if r.status_code == 200:
        with open(local_path, "wb") as f:
            f.write(r.content)
        print("✅ Downloaded from OneDrive:", onedrive_path)
        return True
    else:
        print("ℹ️ File not found on OneDrive (or download failed):", onedrive_path, r.status_code)
        return False

def send_telegram_notification(title, price, link, photo_url=None):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram not configured — skipping notification.")
        return False
    base = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
    caption = f"<b>{title}</b>\n{price}\n{link}"
    if photo_url:
        try:
            r = requests.post(f"{base}/sendPhoto", data={
                "chat_id": TELEGRAM_CHAT_ID,
                "photo": photo_url,
                "caption": caption,
                "parse_mode": "HTML",
                "disable_web_page_preview": False
            }, timeout=15)
            if r.status_code == 200:
                return True
        except Exception:
            pass
    try:
        r = requests.post(f"{base}/sendMessage", data={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": caption,
            "parse_mode": "HTML",
            "disable_web_page_preview": False
        }, timeout=10)
        return r.status_code == 200
    except Exception:
        return False

def clean_price(price_str):
    if not price_str:
        return ""
    return price_str.replace("\n", " ").strip()

def parse_search_page(html):
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.find_all("div", {"data-cy": "l-card"})
    results = []
    for card in cards:
        title_elem = card.select_one('div[data-cy="ad-card-title"] h4')
        title = title_elem.get_text(strip=True) if title_elem else ""
        link_elem = card.find("a", href=True)
        link = link_elem["href"] if link_elem else ""
        if link and not link.startswith("http"):
            link = "https://www.olx.pl" + link
        price_elem = card.find("p", {"data-testid": "ad-price"})
        price = clean_price(price_elem.get_text(strip=True)) if price_elem else ""
        loc_date_elem = card.find("p", {"data-testid": "location-date"})
        loc_date = loc_date_elem.get_text(" ", strip=True) if loc_date_elem else ""
        results.append({"title": title, "link": link, "price": price, "loc_date": loc_date})
    return results

def parse_listing_page(html):
    soup = BeautifulSoup(html, "html.parser")
    desc_elem = soup.find("div", {"data-cy": "ad_description"})
    if not desc_elem:
        desc_elem = soup.find("div", {"class": lambda x: x and "description" in x})
    description = desc_elem.get_text(" ", strip=True) if desc_elem else ""
    image_url = None
    meta_img = soup.find("meta", property="og:image")
    if meta_img and meta_img.has_attr("content"):
        image_url = meta_img["content"]
    else:
        img = soup.find("img", {"class": lambda x: x and ("swiper" in x or "image" in x or "gallery" in x)})
        if img and img.has_attr("src"):
            image_url = img["src"]
        else:
            gallery_img = soup.select_one("div.photos img")
            if gallery_img and gallery_img.has_attr("src"):
                image_url = gallery_img["src"]
    return description, image_url

def normalize_text(text):
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def passes_filters(item, search_conf):
    text = normalize_text(item.get("title","") + " " + item.get("description",""))
    for bad in search_conf.get("forbidden_words", []):
        if normalize_text(bad) in text:
            return False
    reqs = search_conf.get("required_words", [])
    if reqs and not any(normalize_text(r) in text for r in reqs):
        return False
    price = item.get("price","")
    if price:
        digits = "".join(ch for ch in price if ch.isdigit())
        if digits:
            try:
                pnum = int(digits)
                maxp = search_conf.get("max_price")
                minp = search_conf.get("min_price")
                if maxp is not None and pnum > maxp:
                    return False
                if minp is not None and pnum < minp:
                    return False
            except Exception:
                pass
    return True

# Load/Save helpers for Excel/JSON
def load_json(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
    return []

def save_json(data, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_excel(path):
    if os.path.exists(path):
        try:
            return pd.read_excel(path)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def save_excel(df, path):
    df.to_excel(path, index=False)
    try:
        wb = openpyxl.load_workbook(path)
        ws = wb.active
        for col in ws.columns:
            max_len = max((len(str(cell.value)) for cell in col if cell.value), default=0)
            ws.column_dimensions[col[0].column_letter].width = max_len + 2
        wb.save(path)
    except Exception as e:
        print("⚠️ autosize failed:", e)

# ---- Main run ----
def main():
    print("🚀 OLX scraper starting")
    token = authenticate_onedrive() if (CLIENT_ID and REFRESH_TOKEN) else None

    # Download previous state + Excel/JSONs
    if token:
        download_from_onedrive(STATE_ONEDRIVE_PATH, STATE_LOCAL, token)
        download_from_onedrive(EXCEL_ACCEPTED_ONEDRIVE, EXCEL_ACCEPTED_LOCAL, token)
        download_from_onedrive(EXCEL_REJECTED_ONEDRIVE, EXCEL_REJECTED_LOCAL, token)
        download_from_onedrive(JSON_ACCEPTED_ONEDRIVE, JSON_ACCEPTED_LOCAL, token)
        download_from_onedrive(JSON_REJECTED_ONEDRIVE, JSON_REJECTED_LOCAL, token)

    state = load_json(STATE_LOCAL) or {"seen": [], "last_prices": {}, "last_run": int(time.time())}

    accepted_json = load_json(JSON_ACCEPTED_LOCAL)
    rejected_json = load_json(JSON_REJECTED_LOCAL)
    accepted_df = load_excel(EXCEL_ACCEPTED_LOCAL)
    rejected_df = load_excel(EXCEL_REJECTED_LOCAL)

    accepted_map = {row['Link']: row for row in accepted_json}
    rejected_map = {row['Link']: row for row in rejected_json}

    last_prices = state.get("last_prices", {})

    current_links_found = set()
    new_accepted = []
    new_rejected = []
    price_changed = []

    for search_conf in SEARCHES:
        name = search_conf["name"]
        urls = search_conf.get("urls", [search_conf.get("url")])
        for base_url in urls:
            if not base_url:
                continue
            print(f"🔎 Searching '{name}' at {base_url}")

            page = 1
            empty_pages = 0
            while page <= MAX_PAGES and empty_pages < MAX_EMPTY_PAGES:
                paged = base_url + (f"&page={page}" if "?" in base_url else f"?page={page}")
                print(" - fetching", paged)
                r = get_with_retry(paged)
                if r is None:
                    empty_pages += 1
                    page += 1
                    time.sleep(random.uniform(1.5, 3.5))
                    continue

                results = parse_search_page(r.text)
                if not results:
                    empty_pages += 1
                    page += 1
                    time.sleep(random.uniform(1.0, 2.5))
                    continue

                empty_pages = 0
                for res in results:
                    link = res.get("link")
                    if not link:
                        continue

                    current_links_found.add(link)
                    # Sprawdzenie w accepted/rejected bez pobierania strony
                    price = res.get("price")
                    in_accepted = link in accepted_map and accepted_map[link]["Price"] == price
                    in_rejected = link in rejected_map and rejected_map[link]["Price"] == price
                    price_diff = link in accepted_map and accepted_map[link]["Price"] != price

                    if in_accepted or in_rejected:
                        # pomiń pobieranie strony
                        continue

                    # fetch listing page
                    lr = get_with_retry(link)
                    if lr is None:
                        continue
                    description, image_url = parse_listing_page(lr.text)
                    res["description"] = description
                    res["image"] = image_url
                    res["search_name"] = name

                    if passes_filters(res, search_conf):
                        # accepted
                        row = {
                            "Title": res.get("title",""),
                            "Price": price,
                            "Location/Date": res.get("loc_date",""),
                            "Description": res.get("description",""),
                            "Link": link,
                            "Image": res.get("image"),
                            "SearchName": name,
                            "Notified": False,
                            "Timestamp": int(time.time())
                        }
                        accepted_json.append(row)
                        accepted_map[link] = row
                        new_accepted.append(row)
                        if price_diff:
                            row["Title"] += " ⚠️ Price changed"
                            price_changed.append(row)
                    else:
                        # rejected
                        row = {
                            "Title": res.get("title",""),
                            "Price": price,
                            "Location/Date": res.get("loc_date",""),
                            "Description": description,
                            "Link": link,
                            "Image": image_url,
                            "SearchName": name,
                            "Timestamp": int(time.time())
                        }
                        rejected_json.append(row)
                        rejected_map[link] = row
                        new_rejected.append(row)

                    last_prices[link] = price
                    time.sleep(random.uniform(0.8, 1.8))
                page += 1
                time.sleep(random.uniform(1.5, 3.0))

    # Usuń ogłoszenia, które już nie istnieją
    def filter_existing(json_list):
        return [row for row in json_list if row["Link"] in current_links_found]

    if state.get("last_run"):  # tylko jeśli nie pierwsze uruchomienie
        accepted_json = filter_existing(accepted_json)
        rejected_json = filter_existing(rejected_json)

    # Zapis lokalny
    save_json(accepted_json, JSON_ACCEPTED_LOCAL)
    save_json(rejected_json, JSON_REJECTED_LOCAL)
    save_excel(pd.DataFrame(accepted_json), EXCEL_ACCEPTED_LOCAL)
    save_excel(pd.DataFrame(rejected_json), EXCEL_REJECTED_LOCAL)

    # 🔄 Notifications
    to_notify = new_accepted + price_changed
    if to_notify:
        print(f"🔔 New accepted listings or price changes: {len(to_notify)} - sending notifications")
        for item in to_notify:
            ok = send_telegram_notification(
                title=item["Title"],
                price=item["Price"],
                link=item["Link"],
                photo_url=item.get("Image")
            )
            item["Notified"] = ok
            time.sleep(1.2 + random.random())
    else:
        print("ℹ️ No new accepted listings or price changes")

    # Aktualizacja stanu i upload do OneDrive
    state = {"seen": list(current_links_found), "last_prices": last_prices, "last_run": int(time.time())}
    save_json(state, STATE_LOCAL)

    if token:
        upload_to_onedrive_localpath(EXCEL_ACCEPTED_LOCAL, EXCEL_ACCEPTED_ONEDRIVE, token)
        upload_to_onedrive_localpath(EXCEL_REJECTED_LOCAL, EXCEL_REJECTED_ONEDRIVE, token)
        upload_to_onedrive_localpath(JSON_ACCEPTED_LOCAL, JSON_ACCEPTED_ONEDRIVE, token)
        upload_to_onedrive_localpath(JSON_REJECTED_LOCAL, JSON_REJECTED_ONEDRIVE, token)
        upload_to_onedrive_localpath(STATE_LOCAL, STATE_ONEDRIVE_PATH, token)
    else:
        print("⚠️ Skipping OneDrive upload (no token)")

    print("✅ Done.")

if __name__ == "__main__":
    main()
