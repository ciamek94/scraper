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
        "url": "https://www.olx.pl/oferty/q-falownik/",
        "forbidden_words": [
            "fotowoltaiczny", "fotowoltaika", "fotowoltaiki", "fotowoltaicznej",
            "solar", "solarny", "magazyn energii", "mikroinwerter", "inverter",
            "off-grid", "on-grid", "off grid", "on grid",
            "hybrydowy", "hybrydowa", "solaredge", "deye", "growatt", "huawei",
            "sofar", "sma", "fox", "foxess", "fronius", "mppt", "easun",
            "sinuspro", "anern", "jebao", "godwe", "goodwe"
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
    print("🚀 OLX scraper starting")
    token = authenticate_onedrive() if (CLIENT_ID and REFRESH_TOKEN) else None

    # Try to download previous state and excel from OneDrive (if available)
    if token:
        download_from_onedrive(STATE_ONEDRIVE_PATH, STATE_LOCAL, token)
        download_from_onedrive(EXCEL_ONEDRIVE_PATH, EXCEL_LOCAL, token)

    state = load_state_local()
    seen = set(state.get("seen", []))

    existing_df = load_existing_excel()
    if existing_df is None:
        existing_df = pd.DataFrame()

    all_rows = []  # accumulate rows (existing + new)
    if not existing_df.empty:
        all_rows = existing_df.to_dict(orient="records")
    else:
        all_rows = []

    new_found = []
    # For each search
    for search_conf in SEARCHES:
        name = search_conf["name"]
        base_url = search_conf["url"]
        print(f"🔎 Searching '{name}' at {base_url}")

        # Crawl first N pages (safeguard)
        page = 1
        max_pages = 30
        empty_pages = 0
        while page <= max_pages and empty_pages < 2:
            paged = base_url
            if "?" in base_url:
                paged = base_url + f"&page={page}"
            else:
                paged = base_url + f"?page={page}"
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
                if link in seen:
                    # already processed earlier
                    continue
                # fetch listing page for full description and image
                lr = get_with_retry(link)
                if lr is None:
                    # couldn't fetch listing page - still mark as seen to avoid retry storm
                    seen.add(link)
                    continue
                description, image_url = parse_listing_page(lr.text)
                res["description"] = description
                res["image"] = image_url
                res["search_name"] = name

                # filter checks (title + description)
                if not passes_filters(res, search_conf):
                    # mark seen (we won't notify)
                    seen.add(link)
                    continue

                # Good new listing -> add to results
                row = {
                    "Title": res.get("title",""),
                    "Price": res.get("price",""),
                    "Location/Date": res.get("loc_date",""),
                    "Description": res.get("description",""),
                    "Link": link,
                    "Image": res.get("image"),
                    "SearchName": name,
                    "Notified": False,
                    "Timestamp": int(time.time())
                }
                all_rows.append(row)
                new_found.append(row)
                # mark seen
                seen.add(link)
                # small delay between listing pages
                time.sleep(random.uniform(0.8, 1.8))
            page += 1
            # polite delay between search pages
            time.sleep(random.uniform(1.5, 3.0))

    # If new_found, send telegram notifications (title, price, link, image)
    if new_found:
        print(f"🔔 New listings found: {len(new_found)} - sending notifications")
        for item in new_found:
            title = item["Title"]
            price = item["Price"]
            link = item["Link"]
            img = item.get("Image")
            # attempt to send (ignore failures)
            ok = send_telegram_notification(title=title, price=price, link=link, photo_url=img)
            item["Notified"] = ok
            # small pause to avoid spamming
            time.sleep(1.2 + random.random())
    else:
        print("ℹ️ No new listings that passed filters")

    # Save Excel (merge / dedupe by Link)
    df_all = pd.DataFrame(all_rows)
    if not df_all.empty:
        df_all = df_all.drop_duplicates(subset=["Link"], keep="first").reset_index(drop=True)
        save_excel(df_all)
        print("💾 Saved Excel locally:", EXCEL_LOCAL)
    else:
        print("⚠️ No rows to save")

    # Save state locally and upload state + excel + new listings JSON to OneDrive
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
