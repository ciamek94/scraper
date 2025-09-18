#!/usr/bin/env python3
# main.py
"""
Scraper OLX -> Excel + OneDrive + Telegram notifications.

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
            "https://www.olx.pl/oferty/q-falownik/?search%5Bfilter_float_price:from%5D=200&search%5Bfilter_float_price:to%5D=400",
            "https://www.olx.pl/oferty/q-falownik/?search%5Bfilter_float_price:from%5D=400&search%5Bfilter_float_price:to%5D=700",
            "https://www.olx.pl/oferty/q-falownik/?search%5Bfilter_float_price:from%5D=700&search%5Bfilter_float_price:to%5D=1000",
            "https://www.olx.pl/oferty/q-falownik/?search%5Bfilter_float_price:from%5D=1000&search%5Bfilter_float_price:to%5D=1400",
            "https://www.olx.pl/oferty/q-falownik/?search%5Bfilter_float_price:from%5D=1400&search%5Bfilter_float_price:to%5D=2000",
            "https://www.olx.pl/oferty/q-falownik/?search%5Bfilter_float_price:from%5D=2000&search%5Bfilter_float_price:to%5D=3000",
            "https://www.olx.pl/oferty/q-falownik/?search%5Bfilter_float_price:from%5D=3000&search%5Bfilter_float_price:to%5D=4000"

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
            "turystyczny", "hyundai", "suszarki", "pralki", "bosh", "chevrolet", "outlander", "dewalt", "makita",
            "milwuakee", "lodówka", "lodówki", "agregat", "spawarka", "refusol", "wilo", "glebogryzarka",
            "rower", "hulajnoga", "hulajnogi", "zoe", "kangoo", "grundfoss", "grundfos", "mazda", "tesla", "pulsor", "dacia",
            "walcarka", "do włosów", "płyta indukcyjna", "optymalizator", "optymalizatora", "turbiny wiatrowej", "turbina wiatrowa",
            "powerstocc", "milwaukee", "akwarystyczny", "akumulator", "belkin", "samochodowy", "chrysler", "aqua", "jvp", "SSR",
            "skoda", "mostek trapezowy", "remington", "aquael", "yaris", "willo", "wkład kominowy", "einhell", "simet", "honda",
            "osram", "lokówka", "prostownica", "lunchbox", "hydroforowy", "falownica", "dell", "kotła", "dzban", "sukienka",
            "odstraszacz", "klimatyzatora", "hybryda", "gyre"

            
        ],
        "required_words": [],  # jeżeli pusta -> brak wymagań, inaczej co najmniej 1 musi występować
        "max_price": None,     # liczba lub None
        "min_price": None
    },
    # {
    #     "name": "sprężarka",
    #     "url": "https://www.olx.pl/oferty/q-spre%C5%BCarka-%C5%9Brubowa/",
    #     "forbidden_words": ["wynajem"],
    #     "required_words": [],
    #     "max_price": None,
    #     "min_price": None
    # }
]

# Gdzie tymczasowo zapiszemy pliki lokalnie w runnerze
WORKDIR = "output"
os.makedirs(WORKDIR, exist_ok=True)

# OneDrive settings
CLIENT_ID = os.environ.get("ONEDRIVE_CLIENT_ID")
REFRESH_TOKEN = os.environ.get("ONEDRIVE_REFRESH_TOKEN")
TOKEN_URL = 'https://login.microsoftonline.com/consumers/oauth2/v2.0/token'
ONEDRIVE_UPLOAD_FOLDER = os.environ.get("ONEDRIVE_UPLOAD_FOLDER", "olx")

# App definition
MAX_PAGES = 30
MAX_EMPTY_PAGES = 2

# Telegram
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# Filenames on OneDrive (in root or in ONEDRIVE_UPLOAD_FOLDER)
EXCEL_ONEDRIVE_PATH = f"{ONEDRIVE_UPLOAD_FOLDER}/olx_listings.xlsx"
STATE_ONEDRIVE_PATH = f"{ONEDRIVE_UPLOAD_FOLDER}/state.json"  # to track already-seen links
JSON_NEW_PATH = f"{ONEDRIVE_UPLOAD_FOLDER}/new_listings.json"

# Local paths
EXCEL_LOCAL = os.path.join(WORKDIR, "olx_listings.xlsx")
STATE_LOCAL = os.path.join(WORKDIR, "state.json")
NEW_JSON_LOCAL = os.path.join(WORKDIR, "new_listings.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; olx-scraper/1.0)",
    "Accept-Language": "pl-PL,pl;q=0.9"
}

# Helper: retry GET
def get_with_retry(url, headers=HEADERS, retries=4, backoff=2.0):
    for i in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code == 200:
                return r
            # handle 429 or others by waiting
        except Exception as e:
            pass
        time.sleep(backoff * (1 + random.random()))
    return None

# OneDrive: authenticate using refresh token (public client, no client_secret needed)
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
        return r.json()  # używaj token['access_token'] do uploadu
    except requests.exceptions.RequestException as e:
        print("❌ OneDrive auth failed:", e, r.text if 'r' in locals() else "")
        return None


# OneDrive: upload file
def upload_to_onedrive_localpath(local_path, onedrive_path, token):
    """Upload file to OneDrive path (creates or replaces). onedrive_path example: 'folder/file.ext'"""
    if token is None:
        print("⚠️ No OneDrive token, skipping upload:", onedrive_path)
        return False
    access_token = token['access_token']
    upload_url = f'https://graph.microsoft.com/v1.0/me/drive/root:/{onedrive_path}:/content'
    headers = {
        'Authorization': f"Bearer {access_token}"
    }
    with open(local_path, "rb") as f:
        data = f.read()
    r = requests.put(upload_url, headers=headers, data=data, timeout=60)
    if r.status_code in (200, 201):
        print("✅ Uploaded to OneDrive:", onedrive_path)
        return True
    else:
        print("❌ Upload failed:", r.status_code, r.text)
        return False

# OneDrive: download to local (returns True if success)
def download_from_onedrive(onedrive_path, local_path, token):
    if token is None:
        print("⚠️ No OneDrive token, cannot download", onedrive_path)
        return False
    access_token = token['access_token']
    url = f'https://graph.microsoft.com/v1.0/me/drive/root:/{onedrive_path}:/content'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    r = requests.get(url, headers=headers, timeout=60)
    if r.status_code == 200:
        with open(local_path, "wb") as f:
            f.write(r.content)
        print("✅ Downloaded from OneDrive:", onedrive_path)
        return True
    else:
        print("ℹ️ File not found on OneDrive (or download failed):", onedrive_path, r.status_code)
        return False

# Telegram: send message with photo (if photo_url present, try sendPhoto; else fallback to sendMessage)
def send_telegram_notification(title, price, link, photo_url=None):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram not configured — skipping notification.")
        return False
    base = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
    caption = f"<b>{title}</b>\n{price}\n{link}"
    # Try sending photo
    if photo_url:
        send_photo_url = f"{base}/sendPhoto"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "photo": photo_url,
            "caption": caption,
            "parse_mode": "HTML",
            "disable_web_page_preview": False
        }
        try:
            r = requests.post(send_photo_url, data=payload, timeout=15)
            if r.status_code == 200:
                return True
            else:
                print("ℹ️ sendPhoto failed, status:", r.status_code, r.text)
        except Exception as e:
            print("ℹ️ sendPhoto error:", e)
    # Fallback to sendMessage
    try:
        r = requests.post(f"{base}/sendMessage", data={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": caption,
            "parse_mode": "HTML",
            "disable_web_page_preview": False
        }, timeout=10)
        if r.status_code == 200:
            return True
        else:
            print("❌ sendMessage failed:", r.status_code, r.text)
            return False
    except Exception as e:
        print("❌ sendMessage exception:", e)
        return False

# Utility: clean price string to readable (simple)
def clean_price(price_str):
    if not price_str:
        return ""
    return price_str.replace("\n", " ").strip()

# Parse search result page: find cards and extract minimal data (title, link, price)
def parse_search_page(html):
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.find_all("div", {"data-cy": "l-card"})
    results = []
    for card in cards:
        # title
        title_elem = card.select_one('div[data-cy="ad-card-title"] h4')
        title = title_elem.get_text(strip=True) if title_elem else ""

        # link
        link_elem = card.find("a", href=True)
        link = link_elem["href"] if link_elem else ""
        if link and not link.startswith("http"):
            link = "https://www.olx.pl" + link

        # price
        price_elem = card.find("p", {"data-testid": "ad-price"})
        price = clean_price(price_elem.get_text(strip=True)) if price_elem else ""

        # location & date
        loc_date_elem = card.find("p", {"data-testid": "location-date"})
        loc_date = loc_date_elem.get_text(" ", strip=True) if loc_date_elem else ""

        results.append({
            "title": title,
            "link": link,
            "price": price,
            "loc_date": loc_date
        })
    return results

# Parse single listing page: get description and main image (if any)
def parse_listing_page(html):
    soup = BeautifulSoup(html, "html.parser")
    # description
    desc_elem = soup.find("div", {"data-cy": "ad_description"})
    if not desc_elem:
        # sometimes it's different - fallback to any description tag
        desc_elem = soup.find("div", {"class": lambda x: x and "description" in x})
    description = desc_elem.get_text(" ", strip=True) if desc_elem else ""

    # main image - try to find meta og:image first
    image_url = None
    meta_img = soup.find("meta", property="og:image")
    if meta_img and meta_img.has_attr("content"):
        image_url = meta_img["content"]
    else:
        # try to find images carousel
        img = soup.find("img", {"class": lambda x: x and ("swiper" in x or "image" in x or "gallery" in x)})
        if img and img.has_attr("src"):
            image_url = img["src"]
        else:
            # generic find first <img> within gallery
            gallery_img = soup.select_one("div.photos img")
            if gallery_img and gallery_img.has_attr("src"):
                image_url = gallery_img["src"]
    return description, image_url

# Filter check
def normalize_text(text):
    # małe litery, usuń znaki specjalne, zamień kilka spacji na jedną
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def passes_filters(item, search_conf):
    text = normalize_text(item.get("title","") + " " + item.get("description",""))

    # forbidden words
    for bad in search_conf.get("forbidden_words", []):
        bad_norm = normalize_text(bad)
        if bad_norm in text:
            return False

    # required words
    reqs = search_conf.get("required_words", [])
    if reqs:
        if not any(normalize_text(r) in text for r in reqs):
            return False

    # price filter (best effort)
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


# Load local state (seen links)
def load_state_local():
    if os.path.exists(STATE_LOCAL):
        try:
            with open(STATE_LOCAL, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"seen": []}
    return {"seen": []}

def save_state_local(state):
    with open(STATE_LOCAL, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

# Merge with existing Excel (if exists)
def load_existing_excel():
    if os.path.exists(EXCEL_LOCAL):
        try:
            return pd.read_excel(EXCEL_LOCAL)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def save_excel(df):
    df.to_excel(EXCEL_LOCAL, index=False)
    # autosize columns quickly (simple)
    try:
        wb = openpyxl.load_workbook(EXCEL_LOCAL)
        ws = wb.active
        for col in ws.columns:
            max_len = 0
            col_letter = col[0].column_letter
            for cell in col:
                if cell.value:
                    l = len(str(cell.value))
                    if l > max_len:
                        max_len = l
            ws.column_dimensions[col_letter].width = max_len + 2
        wb.save(EXCEL_LOCAL)
    except Exception as e:
        print("⚠️ autosize failed:", e)

# ---- Main run
def main():
    # Start
    print("🚀 OLX scraper starting")
    token = authenticate_onedrive() if (CLIENT_ID and REFRESH_TOKEN) else None

    # Attempt to download current state + Excel from OneDrive
    if token:
        download_from_onedrive(STATE_ONEDRIVE_PATH, STATE_LOCAL, token)
        # try to get the latest excel from OneDrive (this is the source of truth for "already seen")
        download_from_onedrive(EXCEL_ONEDRIVE_PATH, EXCEL_LOCAL, token)

    # Load state and previously-seen links (state.json is auxiliary)
    state = load_state_local()
    seen = set(state.get("seen", []))

    # Load existing Excel (the authoritative "history" of items)
    existing_df = load_existing_excel()
    if existing_df is None:
        existing_df = pd.DataFrame()

    # Build mapping from Link -> row dict for quick updates
    existing_map = {}
    existing_links = set()
    if not existing_df.empty and "Link" in existing_df.columns:
        for r in existing_df.to_dict(orient="records"):
            link = r.get("Link")
            if link:
                existing_map[link] = r
                existing_links.add(link)

    # We'll reconstruct all_rows from existing_map (updated) + newly found rows
    all_rows = list(existing_map.values())  # start from existing rows
    new_found = []            # items that are new (not present in existing_df)
    current_links_found = set()  # links found in this run (used to detect expired)

    # For each configured search
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
                    print("  ⚠️ failed to fetch page", page)
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

                    # Always register that this link exists on OLX now
                    current_links_found.add(link)

                    # If already processed in this run or already known in state, still we need to update current_links_found
                    # But we only notify if the link was not in existing_links (Excel) before this run.
                    # If link in seen (state) we may skip some network calls, but we still must check whether it still exists
                    # (we use OLX page presence to know current_links_found; parse_search_page already found it)

                    # If we've already seen this link earlier in this run, skip duplicate processing
                    # (parse_search_page should not return duplicates typically)
                    # Fetch listing page for full description and image (to update Excel fields)
                    lr = get_with_retry(link)
                    if lr is None:
                        # couldn't fetch listing page - do not remove from current_links_found (we got link from listing page),
                        # but mark as seen to avoid retry storms
                        seen.add(link)
                        continue

                    description, image_url = parse_listing_page(lr.text)
                    res["description"] = description
                    res["image"] = image_url
                    res["search_name"] = name

                    # Filter checks (title + description)
                    if not passes_filters(res, search_conf):
                        # even if filtered out we consider link "found" (so it won't be treated as expired)
                        seen.add(link)
                        continue

                    # If link exists in existing_map -> update that entry (price/desc/image/timestamp)
                    if link in existing_map:
                        # update fields in existing_map row
                        row = existing_map[link]
                        row["Title"] = res.get("title", row.get("Title", ""))
                        row["Price"] = res.get("price", row.get("Price", ""))
                        row["Location/Date"] = res.get("loc_date", row.get("Location/Date", ""))
                        row["Description"] = res.get("description", row.get("Description", ""))
                        row["Image"] = res.get("image", row.get("Image"))
                        row["SearchName"] = name
                        row["Timestamp"] = int(time.time())
                        # ensure all_rows has the updated row (it starts from existing_map values so it's already there)
                    else:
                        # New link (not present in Excel) -> create a new row, add to all_rows and new_found (for Telegram)
                        row = {
                            "Title": res.get("title", ""),
                            "Price": res.get("price", ""),
                            "Location/Date": res.get("loc_date", ""),
                            "Description": res.get("description", ""),
                            "Link": link,
                            "Image": res.get("image"),
                            "SearchName": name,
                            "Notified": False,
                            "Timestamp": int(time.time())
                        }
                        all_rows.append(row)
                        new_found.append(row)
                        # add to existing_map so further updates within same run update it
                        existing_map[link] = row
                        existing_links.add(link)

                    # mark seen (state) so we don't reprocess excessively in future runs
                    seen.add(link)

                    # small delay between listing pages
                    time.sleep(random.uniform(0.8, 1.8))

                page += 1
                # polite delay between search pages
                time.sleep(random.uniform(1.5, 3.0))

    # At this point:
    # - existing_map contains up-to-date rows for any link that was previously in Excel OR added this run
    # - current_links_found contains all links that OLX reported in this run
    # We must remove from all_rows any rows that are in the previous Excel but no longer present on OLX

    # Build final DataFrame: start from all_rows, but drop any rows that used to be in Excel and no longer exist on OLX
    df_all = pd.DataFrame(all_rows)
    if not df_all.empty:
        prev_links = set(existing_df["Link"]) if (not existing_df.empty and "Link" in existing_df.columns) else set()

        def keep_row(r):
            link = r.get("Link")
            # 🔄 If the link was in the previous Excel but is no longer found on OLX → treat as expired and remove
            if link in prev_links and link not in current_links_found:
                return False
            return True

        # 🔄 Remove expired rows and duplicates
        df_all = df_all[df_all.apply(keep_row, axis=1)].drop_duplicates(subset=["Link"], keep="first").reset_index(drop=True)

        # Save Excel
        save_excel(df_all)
        print("💾 Saved Excel locally:", EXCEL_LOCAL)
    else:
        print("⚠️ No rows to save")
        df_all = pd.DataFrame()

    # 🔄 Notifications: only for listings that were NOT in the previous Excel
    if new_found:
        prev_links = set(existing_df["Link"]) if (not existing_df.empty and "Link" in existing_df.columns) else set()
        to_notify = [it for it in new_found if it.get("Link") not in prev_links]

        if to_notify:
            print(f"🔔 New listings found: {len(to_notify)} - sending notifications")
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
            print("ℹ️ New listings found but none are genuinely new compared to server Excel.")
    else:
        print("ℹ️ No new listings that passed filters")

    # Save updated state (seen links) and upload files to OneDrive if authenticated
    state = {"seen": list(seen), "last_run": int(time.time())}
    save_state_local(state)
    with open(NEW_JSON_LOCAL, "w", encoding="utf-8") as f:
        json.dump(new_found, f, ensure_ascii=False, indent=2)

    if token:
        upload_to_onedrive_localpath(EXCEL_LOCAL, EXCEL_ONEDRIVE_PATH, token)
        upload_to_onedrive_localpath(STATE_LOCAL, STATE_ONEDRIVE_PATH, token)
        upload_to_onedrive_localpath(NEW_JSON_LOCAL, JSON_NEW_PATH, token)
    else:
        print("⚠️ Skipping OneDrive upload (no token)")

    print("✅ Done.")


if __name__ == "__main__":
    main()
