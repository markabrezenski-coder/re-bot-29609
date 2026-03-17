"""
RE Bot — Strip Mall Tracker for ZIP 29609
Scrapes CityFeet SC/NC/GA shopping center listings, detects new listings
and price changes, updates the HTML tracker file, and emails a report.
"""

import json
import os
import re
import smtplib
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────
RECIPIENT_EMAIL = "markbrezenski@yahoo.com"
SENDER_EMAIL    = os.environ.get("GMAIL_USER", "")
SENDER_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")

CRITERIA = {
    "min_price": 1_000_000,
    "max_price": 5_000_000,
    "max_miles": 200,
    "zip_center": "29609",
}

SEARCH_URLS = [
    ("SC", "https://www.cityfeet.com/cont/south-carolina/shopping-centers-for-sale"),
    ("NC", "https://www.cityfeet.com/cont/north-carolina/shopping-centers-for-sale"),
    ("GA", "https://www.cityfeet.com/cont/georgia/shopping-centers-for-sale"),
    ("TN", "https://www.cityfeet.com/cont/tennessee/shopping-centers-for-sale"),
]

KNOWN_LISTINGS_FILE = Path("known_listings.json")
HTML_TRACKER_FILE   = Path("re_bot_29609.html")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}

# Approximate driving miles from Greenville SC 29609 to major cities/regions
DISTANCE_MAP = {
    "Easley": 18, "Lyman": 22, "Spartanburg": 26, "Duncan": 24,
    "Anderson": 32, "Greer": 18, "Simpsonville": 12, "Greenville": 5,
    "Abbeville": 38, "Greenwood": 48, "Laurens": 30, "Union": 42,
    "Gaffney": 45, "Rock Hill": 75, "Lexington": 95, "Columbia": 100,
    "Lugoff": 110, "Camden": 115, "Sumter": 130, "Florence": 150,
    "Conway": 175, "Beaufort": 170, "Hilton Head": 185,
    "Asheville": 65, "Hendersonville": 55, "Brevard": 60,
    "Waynesville": 75, "Boone": 95, "Hickory": 95, "Statesville": 100,
    "Concord": 100, "Charlotte": 95, "Monroe": 100, "Gastonia": 85,
    "High Point": 115, "Greensboro": 130, "Burlington": 145,
    "Euharlee": 88, "Cartersville": 90, "Rome": 95, "Gainesville": 110,
    "Marietta": 145, "Atlanta": 150, "Kennesaw": 140, "Smyrna": 148,
    "Alpharetta": 155, "Duluth": 160, "Lawrenceville": 158,
    "Johns Creek": 165, "Augusta": 145, "Athens": 130,
    "Knoxville": 110, "Maryville": 115, "Crossville": 130,
}


def get_distance(city: str) -> int:
    """Return estimated driving miles from 29609, or 999 if unknown."""
    for key, miles in DISTANCE_MAP.items():
        if key.lower() in city.lower():
            return miles
    return 999


def parse_price(text: str):
    """Extract integer price from a string like '$2,300,000 USD'."""
    text = text.replace(",", "").replace("$", "").replace("USD", "").strip()
    nums = re.findall(r"\d+", text)
    if nums:
        return int(nums[0])
    return None


def parse_cap(text: str):
    """Extract CAP rate float from a string like '6.93% Cap Rate'."""
    m = re.search(r"(\d+\.?\d*)\s*%", text)
    if m:
        return float(m.group(1))
    return None


def scrape_state(state: str, url: str) -> list:
    """Scrape a CityFeet state shopping-centers page and return raw listings."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        print(f"  [WARN] Could not fetch {state}: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    listings = []

    # CityFeet listing cards — each is an <a> tag wrapping address + price info
    cards = soup.select("a[href*='/cont/listing/']")
    seen_hrefs = set()

    for card in cards:
        href = card.get("href", "")
        if href in seen_hrefs or not href.startswith("/cont/listing/"):
            continue
        seen_hrefs.add(href)

        full_url = "https://www.cityfeet.com" + href
        text = card.get_text(" ", strip=True)

        # Extract address (first line-like chunk before city/state)
        lines = [l.strip() for l in card.get_text("\n", strip=True).split("\n") if l.strip()]

        address = ""
        city    = ""
        price   = None
        cap     = None
        sqft    = None

        for line in lines:
            if re.match(r"^\d+[\w\s\-\.]+(?:St|Ave|Blvd|Hwy|Dr|Rd|Way|Pkwy|Ln|Ct|Sq|Gtwy)", line, re.I):
                address = line
            if re.search(r"\$[\d,]+", line):
                price = parse_price(line)
            if "Cap Rate" in line or "cap rate" in line:
                cap = parse_cap(line)
            if re.search(r"[\d,]+\s*SF", line, re.I):
                m = re.search(r"([\d,]+)\s*SF", line, re.I)
                if m:
                    sqft = int(m.group(1).replace(",", ""))

        # Try to get city from the URL slug
        slug = href.replace("/cont/listing/", "").split("/")[0]
        parts = slug.rsplit("-", 2)
        if len(parts) >= 2:
            city_guess = parts[-2].replace("-", " ").title()
            if not city:
                city = city_guess

        if not address and lines:
            address = lines[0]

        if not price:
            continue  # skip if no price found

        listings.append({
            "address": address,
            "city":    city,
            "state":   state,
            "price":   price,
            "cap":     cap,
            "sqft":    sqft,
            "url":     full_url,
            "raw":     text[:200],
        })

    print(f"  {state}: found {len(listings)} raw listings")
    return listings


def filter_listings(raw: list) -> list:
    """Apply price and distance criteria."""
    filtered = []
    for item in raw:
        p = item.get("price")
        if not p:
            continue
        if not (CRITERIA["min_price"] <= p <= CRITERIA["max_price"]):
            continue
        dist = get_distance(item.get("city", ""))
        if dist > CRITERIA["max_miles"]:
            continue
        item["dist"] = dist
        filtered.append(item)
    return filtered


def make_listing_id(item: dict) -> str:
    addr = item.get("address", "").lower()
    city = item.get("city", "").lower()
    return re.sub(r"[^a-z0-9]+", "-", f"{addr}-{city}").strip("-")


def load_known() -> dict:
    if KNOWN_LISTINGS_FILE.exists():
        return json.loads(KNOWN_LISTINGS_FILE.read_text())
    return {}


def save_known(data: dict):
    KNOWN_LISTINGS_FILE.write_text(json.dumps(data, indent=2))


def compare(current: list, known: dict):
    """Return (new_listings, price_drops, still_active_ids)."""
    new_listings  = []
    price_drops   = []
    today = datetime.date.today().isoformat()

    for item in current:
        lid = make_listing_id(item)
        item["id"] = lid

        if lid not in known:
            item["flag"]      = "new"
            item["first_seen"] = today
            new_listings.append(item)
        else:
            prev_price = known[lid].get("price")
            if prev_price and item["price"] and item["price"] < prev_price:
                item["flag"]       = "drop"
                item["prev_price"] = prev_price
                item["first_seen"] = known[lid].get("first_seen", today)
                price_drops.append(item)
            else:
                item["flag"]       = ""
                item["first_seen"] = known[lid].get("first_seen", today)

    active_ids = {make_listing_id(i) for i in current}
    return new_listings, price_drops, active_ids


def update_known(known: dict, current: list, active_ids: set) -> dict:
    today = datetime.date.today().isoformat()
    # Update/add current listings
    for item in current:
        lid = item["id"]
        known[lid] = {
            "price":      item.get("price"),
            "cap":        item.get("cap"),
            "first_seen": item.get("first_seen", today),
            "last_seen":  today,
            "address":    item.get("address"),
            "city":       item.get("city"),
            "state":      item.get("state"),
            "url":        item.get("url"),
        }
    # Mark off-market listings
    for lid, data in known.items():
        if lid not in active_ids:
            known[lid]["off_market"] = True
    return known


def fmt_price(p) -> str:
    if not p:
        return "—"
    return f"${p:,.0f}"


def fmt_cap(c) -> str:
    if c is None:
        return "—"
    return f"{c:.2f}%"


def build_email_html(current: list, new_listings: list, price_drops: list, known: dict) -> str:
    today = datetime.date.today().strftime("%B %d, %Y")
    new_ids  = {i["id"] for i in new_listings}
    drop_ids = {i["id"] for i in price_drops}

    rows_html = ""
    for item in sorted(current, key=lambda x: x.get("dist", 999)):
        lid   = item["id"]
        flag  = item.get("flag", "")
        style = ""
        badge = ""
        if flag == "new":
            style = "background:#fffbf2;"
            badge = ' <span style="font-size:10px;background:#FAEEDA;color:#633806;padding:1px 6px;border-radius:4px;font-weight:500">NEW</span>'
        elif flag == "drop":
            style = "background:#eef6fd;"
            prev  = item.get("prev_price")
            badge = f' <span style="font-size:10px;background:#E6F1FB;color:#0C447C;padding:1px 6px;border-radius:4px;font-weight:500">PRICE DROP ▼ was {fmt_price(prev)}</span>'

        maps_url = f"https://www.google.com/maps/search/?api=1&query={requests.utils.quote(item.get('address','') + ', ' + item.get('city','') + ', ' + item.get('state',''))}"

        rows_html += f"""
        <tr style="{style}">
          <td style="padding:8px 10px;border-bottom:1px solid #f0efe8;vertical-align:top;font-size:12px">
            <strong style="font-weight:500">{item.get('address','—')}</strong>{badge}<br>
            <span style="color:#888780">{item.get('city','')}, {item.get('state','')}</span>
          </td>
          <td style="padding:8px 10px;border-bottom:1px solid #f0efe8;font-size:12px">{item.get('dist','—')} mi</td>
          <td style="padding:8px 10px;border-bottom:1px solid #f0efe8;font-size:12px">{fmt_price(item.get('price'))}</td>
          <td style="padding:8px 10px;border-bottom:1px solid #f0efe8;font-size:12px">{fmt_cap(item.get('cap'))}</td>
          <td style="padding:8px 10px;border-bottom:1px solid #f0efe8;font-size:12px">{item.get('sqft', '—'):,} SF" if item.get('sqft') else "—"}</td>
          <td style="padding:8px 10px;border-bottom:1px solid #f0efe8;font-size:12px">
            <a href="{item.get('url','#')}" style="color:#185FA5">Listing</a> &nbsp;
            <a href="{maps_url}" style="color:#185FA5">Map</a>
          </td>
        </tr>"""

    summary_color = "#0F6E56" if new_listings else "#888780"
    summary_text  = f"{len(new_listings)} new listing(s) found" if new_listings else "No new listings today"
    if price_drops:
        summary_text += f" · {len(price_drops)} price drop(s)"

    return f"""
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f5f3;margin:0;padding:20px">
<div style="max-width:820px;margin:0 auto">

  <div style="background:#fff;border-radius:10px;padding:20px 24px;border:1px solid #e0dfd8;margin-bottom:16px">
    <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap">
      <h1 style="font-size:16px;font-weight:500;margin:0">RE Bot — Strip Mall Tracker · 29609</h1>
      <span style="font-size:11px;background:#EAF3DE;color:#27500A;padding:2px 9px;border-radius:4px">{len(current)} active listings</span>
      <span style="font-size:11px;color:#888780">{today}</span>
    </div>
    <p style="font-size:13px;color:{summary_color};margin:10px 0 0;font-weight:500">{summary_text}</p>
  </div>

  <div style="background:#fff;border-radius:10px;border:1px solid #e0dfd8;overflow:hidden">
    <table style="width:100%;border-collapse:collapse;font-size:13px">
      <thead>
        <tr style="background:#f8f7f2">
          <th style="padding:9px 10px;text-align:left;font-size:11px;font-weight:500;color:#888780;border-bottom:1px solid #e8e7df;white-space:nowrap">Property / address</th>
          <th style="padding:9px 10px;text-align:left;font-size:11px;font-weight:500;color:#888780;border-bottom:1px solid #e8e7df;white-space:nowrap">Distance</th>
          <th style="padding:9px 10px;text-align:left;font-size:11px;font-weight:500;color:#888780;border-bottom:1px solid #e8e7df;white-space:nowrap">Price</th>
          <th style="padding:9px 10px;text-align:left;font-size:11px;font-weight:500;color:#888780;border-bottom:1px solid #e8e7df;white-space:nowrap">CAP %</th>
          <th style="padding:9px 10px;text-align:left;font-size:11px;font-weight:500;color:#888780;border-bottom:1px solid #e8e7df;white-space:nowrap">Size</th>
          <th style="padding:9px 10px;text-align:left;font-size:11px;font-weight:500;color:#888780;border-bottom:1px solid #e8e7df;white-space:nowrap">Links</th>
        </tr>
      </thead>
      <tbody>{rows_html}</tbody>
    </table>
  </div>

  <p style="font-size:11px;color:#888780;margin-top:14px;text-align:center">
    RE Bot · Automated daily scan · CityFeet SC / NC / GA / TN · 200mi of ZIP 29609 · $1M–$5M strip malls
  </p>
</div>
</body>
</html>"""


def send_email(subject: str, html_body: str):
    if not SENDER_EMAIL or not SENDER_PASSWORD:
        print("[WARN] Email credentials not set — skipping email send.")
        return
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = SENDER_EMAIL
        msg["To"]      = RECIPIENT_EMAIL
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, msg.as_string())
        print(f"  Email sent to {RECIPIENT_EMAIL}")
    except Exception as e:
        print(f"  [ERROR] Email failed: {e}")


def update_html_tracker(current: list, new_ids: set, drop_ids: set):
    """Inject the current listings into the HTML tracker file as updated SEED data."""
    if not HTML_TRACKER_FILE.exists():
        print("  [WARN] HTML tracker file not found — skipping HTML update.")
        return

    today = datetime.date.today().isoformat()
    js_entries = []
    for item in sorted(current, key=lambda x: x.get("dist", 999)):
        lid   = item.get("id", make_listing_id(item))
        flag  = item.get("flag", "")
        first = item.get("first_seen", today)
        cap_val  = f"{item['cap']}" if item.get("cap") is not None else "null"
        sqft_val = str(item["sqft"]) if item.get("sqft") else "null"
        name = item.get("address", "Unknown") + " — " + item.get("city", "") + ", " + item.get("state", "")
        note = f"Auto-scraped from CityFeet — confirmed FOR SALE {fmt_price(item.get('price'))}"
        if flag == "drop":
            note += f" (price dropped from {fmt_price(item.get('prev_price'))})"

        entry = (
            f'  {{f:"{flag}",id:"{lid}",'
            f'n:"{name}",'
            f'a:"{item.get("address","")}",'
            f'c:"{item.get("city","")}",'
            f's:"{item.get("state","")}",'
            f'd:{item.get("dist", 999)},'
            f'p:{item.get("price", 0)},'
            f'sq:{sqft_val},'
            f'cap:{cap_val},cE:0,'
            f'nnn:null,nE:1,o:null,'
            f'aadt:null,ar:"",'
            f'u:"{item.get("url","")}",'
            f'om:null,'
            f'b:"",'
            f'nt:"{note}",'
            f'first:"{first}"}}'
        )
        js_entries.append(entry)

    new_seed = "const SEED = [\n" + ",\n".join(js_entries) + "\n];"

    html = HTML_TRACKER_FILE.read_text(encoding="utf-8")
    html = re.sub(r"const SEED = \[[\s\S]*?\];", new_seed, html)

    # Update last-run date badge
    html = re.sub(
        r'id="b-run">[^<]*<',
        f'id="b-run">Last run: {datetime.date.today().strftime("%b %d, %Y")}<',
        html
    )
    html = re.sub(
        r'Last updated: <span id="last-updated">[^<]*<',
        f'Last updated: <span id="last-updated">{datetime.date.today().strftime("%b %d, %Y")}<',
        html
    )

    HTML_TRACKER_FILE.write_text(html, encoding="utf-8")
    print(f"  HTML tracker updated with {len(current)} listings.")


def main():
    print(f"\n{'='*60}")
    print(f"RE Bot starting — {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    # 1. Scrape all states
    all_raw = []
    for state, url in SEARCH_URLS:
        print(f"\nScraping {state}...")
        all_raw.extend(scrape_state(state, url))

    print(f"\nTotal raw listings scraped: {len(all_raw)}")

    # 2. Filter by criteria
    current = filter_listings(all_raw)
    for item in current:
        item["id"] = make_listing_id(item)

    print(f"After filtering ($1M–$5M, 200mi): {len(current)} listings")

    # 3. Load known and compare
    known = load_known()
    new_listings, price_drops, active_ids = compare(current, known)

    print(f"\nNew listings:  {len(new_listings)}")
    print(f"Price drops:   {len(price_drops)}")
    if new_listings:
        for item in new_listings:
            print(f"  NEW: {item.get('address')} {item.get('city')}, {item.get('state')} — {fmt_price(item.get('price'))}")
    if price_drops:
        for item in price_drops:
            print(f"  DROP: {item.get('address')} — {fmt_price(item.get('prev_price'))} → {fmt_price(item.get('price'))}")

    # 4. Update known listings file
    updated_known = update_known(known, current, active_ids)
    save_known(updated_known)
    print(f"\nKnown listings file updated ({len(updated_known)} total tracked).")

    # 5. Update HTML tracker
    print("\nUpdating HTML tracker...")
    new_ids  = {i["id"] for i in new_listings}
    drop_ids = {i["id"] for i in price_drops}
    update_html_tracker(current, new_ids, drop_ids)

    # 6. Build and send email
    today_str = datetime.date.today().strftime("%B %d, %Y")
    if new_listings:
        subject = f"RE Bot · {len(new_listings)} NEW listing(s) found · {today_str}"
    elif price_drops:
        subject = f"RE Bot · {len(price_drops)} price drop(s) · {today_str}"
    else:
        subject = f"RE Bot · Daily update · {today_str} · {len(current)} active listings"

    print(f"\nBuilding email: '{subject}'")
    html_body = build_email_html(current, new_listings, price_drops, updated_known)
    send_email(subject, html_body)

    print(f"\n{'='*60}")
    print("RE Bot complete.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
