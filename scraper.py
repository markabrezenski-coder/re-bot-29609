"""
RE Bot — Strip Mall Tracker for ZIP 29609
Reliable multi-source scraper. CityFeet is the primary source (proven).
Additional sources use dedicated parsers. Bad data is rejected aggressively.
"""

import json, os, re, smtplib, datetime, time, math
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
import requests
from bs4 import BeautifulSoup

RECIPIENT_EMAIL = "markbrezenski@yahoo.com"
SENDER_EMAIL    = os.environ.get("GMAIL_USER", "")
SENDER_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")

CRITERIA = {"min_price": 1_000_000, "max_price": 5_000_000, "max_miles": 200}

KNOWN_LISTINGS_FILE = Path("known_listings.json")
HTML_TRACKER_FILE   = Path("re_bot_29609.html")

# Only these states are in our search radius
VALID_STATES = {"SC", "NC", "GA", "TN", "VA"}

# Keywords that indicate NOT a strip mall / retail center
EXCLUDE_KEYWORDS = [
    "dollar general", "dollar tree", "family dollar", "walgreens", "cvs",
    "autozone", "o'reilly", "advance auto", "nnn single", "single tenant",
    "cannabis", "dispensary", "trulieve", "curaleaf", "church", "warehouse",
    "industrial", "office building", "self storage", "car wash", "hotel",
    "apartment", "multifamily", "mobile home", "land", "vacant lot",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Greenville SC 29609 coordinates
HOME_LAT, HOME_LON = 34.8526, -82.3940

DISTANCE_MAP = {
    "Greenville":5,"Simpsonville":12,"Mauldin":10,"Taylors":8,"Easley":18,
    "Greer":18,"Lyman":22,"Duncan":24,"Spartanburg":26,"Moore":24,
    "Boiling Springs":28,"Anderson":32,"Belton":36,"Abbeville":38,
    "Laurens":30,"Clinton":42,"Union":42,"Gaffney":45,"Greenwood":48,
    "Seneca":48,"Clemson":38,"Walhalla":52,"Pickens":28,"Liberty":24,
    "Powdersville":14,"Fountain Inn":18,"Woodruff":28,"Inman":30,
    "Landrum":35,"Chesnee":32,"Aiken":75,"Newberry":65,"Lexington":95,
    "Irmo":90,"Columbia":100,"Lugoff":110,"Camden":115,"Sumter":130,
    "Florence":150,"Beaufort":170,"Rock Hill":75,"Fort Mill":72,
    "York":70,"Chester":75,"Lancaster":85,
    "Asheville":65,"Hendersonville":55,"Brevard":60,"Waynesville":75,
    "Canton":70,"Black Mountain":70,"Morganton":85,"Hickory":95,
    "Statesville":100,"Concord":100,"Charlotte":95,"Monroe":100,
    "Gastonia":85,"Shelby":80,"Rutherfordton":70,"Forest City":72,
    "Kings Mountain":78,"High Point":115,"Greensboro":130,"Winston-Salem":125,
    "Euharlee":88,"Cartersville":90,"Rome":95,"Gainesville":110,
    "Cornelia":95,"Toccoa":90,"Marietta":145,"Kennesaw":140,
    "Smyrna":148,"Atlanta":150,"Alpharetta":155,"Duluth":160,
    "Lawrenceville":158,"Johns Creek":165,"Roswell":152,
    "Woodstock":140,"Augusta":145,"Athens":130,"Covington":148,
    "Knoxville":110,"Maryville":115,"Cleveland":95,"Chattanooga":110,
    "Johnson City":140,"Kingsport":145,"Bristol":150,
}


def haversine_miles(lat2, lon2):
    """Straight-line miles from HOME, with 15% driving factor."""
    dlat = math.radians(lat2 - HOME_LAT)
    dlon = math.radians(lon2 - HOME_LON)
    a = (math.sin(dlat/2)**2 +
         math.cos(math.radians(HOME_LAT)) *
         math.cos(math.radians(lat2)) *
         math.sin(dlon/2)**2)
    return int(3958.8 * 2 * math.asin(math.sqrt(a)) * 1.15)


def get_distance(city, state=""):
    """Three-tier distance lookup."""
    city = city.strip()
    state = state.strip().upper()
    if not city:
        return {"SC":80,"NC":120,"GA":140,"TN":120,"VA":180}.get(state, 999)
    # 1. Exact
    for k, v in DISTANCE_MAP.items():
        if k.lower() == city.lower():
            return v
    # 2. Partial
    for k, v in DISTANCE_MAP.items():
        if k.lower() in city.lower():
            return v
    # 3. Census geocoder
    try:
        q = requests.utils.quote(f"{city}, {state}")
        r = requests.get(
            f"https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
            f"?address={q}&benchmark=2020&format=json",
            timeout=8, headers={"User-Agent": "REBot/1.0"})
        if r.status_code == 200:
            matches = r.json().get("result", {}).get("addressMatches", [])
            if matches:
                c = matches[0]["coordinates"]
                return haversine_miles(float(c["y"]), float(c["x"]))
    except Exception:
        pass
    # 4. State fallback
    return {"SC":80,"NC":120,"GA":140,"TN":120,"VA":180}.get(state, 999)


def parse_price(text):
    if not text:
        return None
    text = str(text).replace(",", "").strip()
    # "$2.3M" or "2.3M"
    m = re.search(r'([\d.]+)\s*[Mm](?:illion)?', text)
    if m:
        v = int(float(m.group(1)) * 1_000_000)
        if 500_000 <= v <= 50_000_000:
            return v
    # Plain number from "$1,234,567"
    digits = re.sub(r'[^\d]', '', text)
    if 7 <= len(digits) <= 8:
        v = int(digits)
        if 500_000 <= v <= 50_000_000:
            return v
    return None


def parse_cap(text):
    if not text:
        return None
    m = re.search(r'(\d+\.?\d*)\s*%', str(text))
    if m:
        v = float(m.group(1))
        if 2.0 <= v <= 20.0:
            return v
    return None


def safe_get(url, session, timeout=15):
    try:
        time.sleep(1.5)
        r = session.get(url, timeout=timeout, allow_redirects=True)
        if r.status_code == 200 and len(r.text) > 500:
            return r
        print(f"  HTTP {r.status_code} — {url[:70]}")
        return None
    except Exception as e:
        print(f"  ERR {url[:70]}: {str(e)[:60]}")
        return None


def make_id(address, city):
    return re.sub(r'[^a-z0-9]+', '-', f"{address}-{city}".lower()).strip('-')[:80]


def is_valid_listing(item):
    """Reject listings that don't fit strip mall criteria."""
    state = item.get("state","").upper()
    if state and state not in VALID_STATES:
        return False
    price = item.get("price")
    if not price or not (CRITERIA["min_price"] <= price <= CRITERIA["max_price"]):
        return False
    # Check for excluded keywords
    text_check = " ".join([
        item.get("address",""),
        item.get("name",""),
        item.get("notes",""),
    ]).lower()
    for kw in EXCLUDE_KEYWORDS:
        if kw in text_check:
            return False
    return True


# ── SCRAPER: CityFeet (primary — proven reliable) ───────────────────────────

def scrape_cityfeet(session):
    pages = [
        ("SC", "https://www.cityfeet.com/cont/south-carolina/shopping-centers-for-sale"),
        ("NC", "https://www.cityfeet.com/cont/north-carolina/shopping-centers-for-sale"),
        ("GA", "https://www.cityfeet.com/cont/georgia/shopping-centers-for-sale"),
        ("TN", "https://www.cityfeet.com/cont/tennessee/shopping-centers-for-sale"),
        ("VA", "https://www.cityfeet.com/cont/virginia/shopping-centers-for-sale"),
    ]
    out = []
    for state, url in pages:
        r = safe_get(url, session)
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        seen = set()
        count = 0
        for card in soup.select("a[href*='/cont/listing/']"):
            href = card.get("href", "")
            if href in seen or not href.startswith("/cont/listing/"):
                continue
            seen.add(href)

            lines = [l.strip() for l in card.get_text("\n").split("\n") if l.strip()]
            address, city, price, cap, sqft = "", "", None, None, None

            for line in lines:
                # Address: starts with number + street type
                if not address and re.match(r'^\d+\s+[\w]', line) and re.search(
                        r'\b(St|Ave|Blvd|Hwy|Hwy\.|Dr|Rd|Way|Pkwy|Ln|Ct|Sq|Gtwy|Brg|Pike)\b', line, re.I):
                    address = line[:100]
                # Price
                if not price and re.search(r'\$[\d,]+', line):
                    p = parse_price(re.search(r'\$[\d,]+(?:,\d+)*', line).group())
                    if p:
                        price = p
                # CAP rate
                if cap is None and re.search(r'\d+\.?\d*\s*%\s*Cap', line, re.I):
                    cap = parse_cap(line)
                # Square footage
                if sqft is None and re.search(r'[\d,]+\s*SF', line, re.I):
                    m = re.search(r'([\d,]+)\s*SF', line, re.I)
                    if m:
                        sqft = int(m.group(1).replace(",", ""))

            # City from URL slug: e.g. /cont/listing/200-spartanburg-hwy-lyman-sc-29365/cs38500747
            slug = href.replace("/cont/listing/", "").split("/")[0]
            # Pattern: ends with "-ST-ZIPCODE" or "-ST"
            m = re.search(r'-([a-z][a-z\-]+)-(' + '|'.join(VALID_STATES).lower() + r')(?:-\d+)?$', slug)
            if m:
                city = m.group(1).replace("-", " ").title()

            if not address and lines:
                address = lines[0][:100]
            if not city and lines:
                # Try to find "City, ST" pattern in card text
                full_text = card.get_text(" ")
                cm = re.search(r'([A-Z][a-zA-Z\s]+),\s*(' + '|'.join(VALID_STATES) + r')\b', full_text)
                if cm:
                    city = cm.group(1).strip()

            if price and address:
                item = {"address": address, "city": city, "state": state,
                        "price": price, "cap": cap, "sqft": sqft,
                        "url": "https://www.cityfeet.com" + href,
                        "source": "CityFeet"}
                if is_valid_listing(item):
                    out.append(item)
                    count += 1
        print(f"  CityFeet {state}: {count}")
    return out


# ── SCRAPER: CommercialSearch ────────────────────────────────────────────────

def scrape_commercialsearch(session):
    urls = [
        ("SC", "https://www.commercialsearch.com/listings/for-sale/?q=strip+center&location=Greenville%2C+SC&radius=150&propertyType=retail"),
        ("NC", "https://www.commercialsearch.com/listings/for-sale/?q=strip+mall&location=Charlotte%2C+NC&radius=100&propertyType=retail"),
        ("GA", "https://www.commercialsearch.com/listings/for-sale/?q=strip+center&location=Atlanta%2C+GA&radius=100&propertyType=retail"),
    ]
    out = []
    for state, url in urls:
        r = safe_get(url, session)
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        for card in soup.select("[class*='listing-card'], [class*='property-card'], [class*='result-item']"):
            text = card.get_text(" ", strip=True)
            # Must contain "for sale" signal
            if not re.search(r'for sale|\$[\d,]+', text, re.I):
                continue
            pm = re.search(r'\$[\d,]+(?:,\d+)*', text)
            cm = re.search(r'(\d+\.?\d*)\s*%\s*Cap', text, re.I)
            sm = re.search(r'([\d,]+)\s*SF', text, re.I)
            # City, State pattern
            loc = re.search(r'([A-Z][a-zA-Z\s]+),\s*(' + '|'.join(VALID_STATES) + r')\b', text)
            am = re.search(r'\d+\s+[\w\s]+(?:St|Ave|Blvd|Hwy|Dr|Rd|Way|Pkwy|Ln)\b', text, re.I)
            link = card.find("a", href=True)
            price = parse_price(pm.group()) if pm else None
            if not price:
                continue
            city  = loc.group(1).strip() if loc else ""
            lstate = loc.group(2).strip() if loc else state
            href  = link["href"] if link else url
            full_url = "https://www.commercialsearch.com" + href if href.startswith("/") else href
            item = {"address": am.group()[:80] if am else text[:60],
                    "city": city, "state": lstate, "price": price,
                    "cap": parse_cap(cm.group()) if cm else None,
                    "sqft": int(sm.group(1).replace(",","")) if sm else None,
                    "url": full_url, "source": "CommercialSearch"}
            if is_valid_listing(item):
                out.append(item)
    print(f"  CommercialSearch: {len(out)}")
    return out


# ── SCRAPER: LoopNet (extracts what's publicly visible) ─────────────────────

def scrape_loopnet(session):
    urls = [
        ("SC", "https://www.loopnet.com/search/strip-malls/sc/for-sale/"),
        ("NC", "https://www.loopnet.com/search/strip-malls/nc/for-sale/"),
        ("GA", "https://www.loopnet.com/search/strip-malls/ga/for-sale/"),
        ("TN", "https://www.loopnet.com/search/strip-malls/tn/for-sale/"),
    ]
    out = []
    for state, url in urls:
        r = safe_get(url, session)
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        # LoopNet embeds listing data in structured JSON-LD or data attributes
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                items = data if isinstance(data, list) else [data]
                for d in items:
                    if d.get("@type") in ("Offer", "Product", "RealEstateListing"):
                        price = parse_price(str(d.get("price", "")))
                        addr_obj = d.get("address", {})
                        city  = addr_obj.get("addressLocality", "")
                        lstate = addr_obj.get("addressRegion", state)
                        addr  = addr_obj.get("streetAddress", d.get("name",""))[:80]
                        if price and city:
                            item = {"address": addr, "city": city, "state": lstate,
                                    "price": price, "cap": None, "sqft": None,
                                    "url": d.get("url", url), "source": "LoopNet"}
                            if is_valid_listing(item):
                                out.append(item)
            except Exception:
                pass
        # Also look for price+address in page meta
        for meta in soup.find_all("meta", {"name": "description"}):
            content = meta.get("content","")
            if re.search(r'\$[\d,]+.*for sale', content, re.I):
                pm = re.search(r'\$[\d,]+(?:,\d+)*', content)
                lm = re.search(r'([A-Z][a-zA-Z\s]+),\s*(' + '|'.join(VALID_STATES) + r')\b', content)
                price = parse_price(pm.group()) if pm else None
                if price and lm:
                    item = {"address": content[:80], "city": lm.group(1).strip(),
                            "state": lm.group(2).strip(), "price": price,
                            "cap": None, "sqft": None, "url": url, "source": "LoopNet"}
                    if is_valid_listing(item):
                        out.append(item)
    print(f"  LoopNet: {len(out)}")
    return out


# ── SCRAPER: NAI Global ──────────────────────────────────────────────────────

def scrape_nai(session):
    urls = [
        ("SC", "https://www.naiglobal.com/properties?propertyType=Retail&transactionType=Sale&stateCode=SC"),
        ("NC", "https://www.naiglobal.com/properties?propertyType=Retail&transactionType=Sale&stateCode=NC"),
        ("GA", "https://www.naiglobal.com/properties?propertyType=Retail&transactionType=Sale&stateCode=GA"),
    ]
    out = []
    for state, url in urls:
        r = safe_get(url, session)
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        for card in soup.select("[class*='property'], [class*='listing'], article"):
            text = card.get_text(" ", strip=True)
            pm   = re.search(r'\$[\d,]+(?:,\d+)*', text)
            cm   = re.search(r'(\d+\.?\d*)\s*%\s*Cap', text, re.I)
            lm   = re.search(r'([A-Z][a-zA-Z\s]+),\s*(' + '|'.join(VALID_STATES) + r')\b', text)
            am   = re.search(r'\d+\s+[\w\s]+(?:St|Ave|Blvd|Hwy|Dr|Rd|Way|Pkwy|Ln)\b', text, re.I)
            link = card.find("a", href=True)
            price = parse_price(pm.group()) if pm else None
            if not price or not lm:
                continue
            href = link["href"] if link else url
            full_url = "https://www.naiglobal.com" + href if href.startswith("/") else href
            item = {"address": am.group()[:80] if am else text[:60],
                    "city": lm.group(1).strip(), "state": lm.group(2).strip(),
                    "price": price, "cap": parse_cap(cm.group()) if cm else None,
                    "sqft": None, "url": full_url, "source": "NAI Global"}
            if is_valid_listing(item):
                out.append(item)
    print(f"  NAI Global: {len(out)}")
    return out


# ── SCRAPER: SVN ─────────────────────────────────────────────────────────────

def scrape_svn(session):
    urls = [
        ("SC", "https://www.svn.com/properties/?property_type=retail&transaction_type=sale&state=SC"),
        ("NC", "https://www.svn.com/properties/?property_type=retail&transaction_type=sale&state=NC"),
        ("GA", "https://www.svn.com/properties/?property_type=retail&transaction_type=sale&state=GA"),
    ]
    out = []
    for state, url in urls:
        r = safe_get(url, session)
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        for card in soup.select("[class*='property-card'], [class*='listing-card'], [class*='result']"):
            text = card.get_text(" ", strip=True)
            pm   = re.search(r'\$[\d,]+(?:,\d+)*', text)
            cm   = re.search(r'(\d+\.?\d*)\s*%\s*Cap', text, re.I)
            lm   = re.search(r'([A-Z][a-zA-Z\s]+),\s*(' + '|'.join(VALID_STATES) + r')\b', text)
            am   = re.search(r'\d+\s+[\w\s]+(?:St|Ave|Blvd|Hwy|Dr|Rd|Way|Pkwy|Ln)\b', text, re.I)
            link = card.find("a", href=True)
            price = parse_price(pm.group()) if pm else None
            if not price:
                continue
            city  = lm.group(1).strip() if lm else ""
            lstate = lm.group(2).strip() if lm else state
            href  = link["href"] if link else url
            full_url = "https://www.svn.com" + href if href.startswith("/") else href
            item = {"address": am.group()[:80] if am else text[:60],
                    "city": city, "state": lstate, "price": price,
                    "cap": parse_cap(cm.group()) if cm else None,
                    "sqft": None, "url": full_url, "source": "SVN"}
            if is_valid_listing(item):
                out.append(item)
    print(f"  SVN: {len(out)}")
    return out


# ── MAIN SCRAPE RUNNER ───────────────────────────────────────────────────────

def scrape_all(session):
    out = []
    out += scrape_cityfeet(session)
    out += scrape_commercialsearch(session)
    out += scrape_loopnet(session)
    out += scrape_nai(session)
    out += scrape_svn(session)
    return out


# ── FILTER / DEDUP / COMPARE ─────────────────────────────────────────────────

def filter_listings(raw):
    out = []
    for item in raw:
        if not is_valid_listing(item):
            continue
        dist = get_distance(item.get("city",""), item.get("state",""))
        if dist > CRITERIA["max_miles"]:
            continue
        item["dist"] = dist
        out.append(item)
    return out


def dedup(listings):
    seen = {}
    for item in listings:
        lid = make_id(item.get("address",""), item.get("city",""))
        if lid not in seen:
            seen[lid] = item
        else:
            ex = seen[lid]
            # Merge: keep richer data
            merged = dict(ex)
            for k, v in item.items():
                if v and not merged.get(k):
                    merged[k] = v
            seen[lid] = merged
    return list(seen.values())


def load_known():
    if KNOWN_LISTINGS_FILE.exists():
        return json.loads(KNOWN_LISTINGS_FILE.read_text())
    return {}


def save_known(data):
    KNOWN_LISTINGS_FILE.write_text(json.dumps(data, indent=2))


def compare(current, known):
    new_l, drops = [], []
    today = datetime.date.today().isoformat()
    for item in current:
        lid = make_id(item.get("address",""), item.get("city",""))
        item["id"] = lid
        if lid not in known:
            item["flag"] = "new"
            item["first_seen"] = today
            new_l.append(item)
        else:
            prev = known[lid].get("price")
            if prev and item.get("price") and item["price"] < prev:
                item["flag"] = "drop"
                item["prev_price"] = prev
                item["first_seen"] = known[lid].get("first_seen", today)
                drops.append(item)
            else:
                item["flag"] = ""
                item["first_seen"] = known[lid].get("first_seen", today)
    active = {make_id(i.get("address",""), i.get("city","")) for i in current}
    return new_l, drops, active


def update_known(known, current, active_ids):
    today = datetime.date.today().isoformat()
    for item in current:
        lid = item["id"]
        known[lid] = {
            "price": item.get("price"), "cap": item.get("cap"),
            "first_seen": item.get("first_seen", today), "last_seen": today,
            "address": item.get("address"), "city": item.get("city"),
            "state": item.get("state"), "url": item.get("url"),
            "source": item.get("source"),
        }
    for lid in known:
        if lid not in active_ids:
            known[lid]["off_market"] = True
    return known


# ── EMAIL ─────────────────────────────────────────────────────────────────────

def fp(p): return f"${p:,.0f}" if p else "—"
def fc(c): return f"{c:.2f}%" if c else "—"


def build_email(current, new_l, drops):
    today   = datetime.date.today().strftime("%B %d, %Y")
    sources = sorted(set(i.get("source","") for i in current if i.get("source")))
    rows = ""
    for item in sorted(current, key=lambda x: x.get("dist", 999)):
        flag = item.get("flag","")
        style, badge = "", ""
        if flag == "new":
            style = "background:#fffbf2;"
            badge = (' <span style="font-size:10px;background:#FAEEDA;color:#633806;'
                     'padding:1px 6px;border-radius:4px;font-weight:bold">NEW</span>')
        elif flag == "drop":
            style = "background:#eef6fd;"
            badge = (f' <span style="font-size:10px;background:#E6F1FB;color:#0C447C;'
                     f'padding:1px 6px;border-radius:4px;font-weight:bold">'
                     f'PRICE DROP ▼ was {fp(item.get("prev_price"))}</span>')
        addr  = item.get("address","—")
        city  = item.get("city","")
        state = item.get("state","")
        dist  = item.get("dist","—")
        maps  = ("https://www.google.com/maps/search/?api=1&query=" +
                 requests.utils.quote(f"{addr}, {city}, {state}"))
        src   = (f'<span style="font-size:10px;background:#f1f0e8;color:#5f5e5a;'
                 f'padding:1px 5px;border-radius:3px">{item.get("source","")}</span>')
        sqft_str = f"{item['sqft']:,} SF" if item.get("sqft") else "—"
        rows += (
            f'<tr style="{style}">'
            f'<td style="padding:8px 10px;border-bottom:1px solid #f0efe8;font-size:12px;vertical-align:top">'
            f'<strong>{addr}</strong>{badge}<br>'
            f'<span style="color:#888780">{city}, {state}</span><br>{src}</td>'
            f'<td style="padding:8px 10px;border-bottom:1px solid #f0efe8;font-size:12px;white-space:nowrap">{dist} mi</td>'
            f'<td style="padding:8px 10px;border-bottom:1px solid #f0efe8;font-size:12px;white-space:nowrap">{fp(item.get("price"))}</td>'
            f'<td style="padding:8px 10px;border-bottom:1px solid #f0efe8;font-size:12px">{fc(item.get("cap"))}</td>'
            f'<td style="padding:8px 10px;border-bottom:1px solid #f0efe8;font-size:12px">{sqft_str}</td>'
            f'<td style="padding:8px 10px;border-bottom:1px solid #f0efe8;font-size:12px">'
            f'<a href="{item.get("url","#")}" style="color:#185FA5">Listing</a>&nbsp;'
            f'<a href="{maps}" style="color:#185FA5">Map</a></td>'
            f'</tr>'
        )
    sc = "#0F6E56" if new_l else "#888780"
    st = f"{len(new_l)} new listing(s) found" if new_l else "No new listings today"
    if drops:
        st += f" · {len(drops)} price drop(s)"
    src_list = ", ".join(sources)
    return (
        f'<!DOCTYPE html><html><head><meta charset="UTF-8"></head>'
        f'<body style="font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',sans-serif;'
        f'background:#f5f5f3;margin:0;padding:20px">'
        f'<div style="max-width:860px;margin:0 auto">'
        f'<div style="background:#fff;border-radius:10px;padding:20px 24px;'
        f'border:1px solid #e0dfd8;margin-bottom:16px">'
        f'<h1 style="font-size:16px;font-weight:500;margin:0 0 6px">'
        f'RE Bot — Strip Mall Tracker · 29609</h1>'
        f'<div style="font-size:11px;color:#888780;margin-bottom:10px">'
        f'{today} · {src_list} · $1M–$5M · 200mi · SC/NC/GA/TN</div>'
        f'<p style="font-size:13px;color:{sc};margin:0;font-weight:500">{st}</p></div>'
        f'<div style="background:#fff;border-radius:10px;border:1px solid #e0dfd8;overflow:hidden">'
        f'<table style="width:100%;border-collapse:collapse"><thead>'
        f'<tr style="background:#f8f7f2">'
        f'<th style="padding:9px 10px;text-align:left;font-size:11px;font-weight:500;color:#888780;border-bottom:1px solid #e8e7df">Property</th>'
        f'<th style="padding:9px 10px;text-align:left;font-size:11px;font-weight:500;color:#888780;border-bottom:1px solid #e8e7df">Dist</th>'
        f'<th style="padding:9px 10px;text-align:left;font-size:11px;font-weight:500;color:#888780;border-bottom:1px solid #e8e7df">Price</th>'
        f'<th style="padding:9px 10px;text-align:left;font-size:11px;font-weight:500;color:#888780;border-bottom:1px solid #e8e7df">CAP</th>'
        f'<th style="padding:9px 10px;text-align:left;font-size:11px;font-weight:500;color:#888780;border-bottom:1px solid #e8e7df">Size</th>'
        f'<th style="padding:9px 10px;text-align:left;font-size:11px;font-weight:500;color:#888780;border-bottom:1px solid #e8e7df">Links</th>'
        f'</tr></thead><tbody>{rows}</tbody></table></div>'
        f'<p style="font-size:11px;color:#888780;margin-top:14px;text-align:center">'
        f'RE Bot · Strip mall tracker · 200mi of ZIP 29609</p>'
        f'</div></body></html>'
    )


def send_email(subject, html_body):
    if not SENDER_EMAIL or not SENDER_PASSWORD:
        print("  [WARN] No credentials.")
        return
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"], msg["From"], msg["To"] = subject, SENDER_EMAIL, RECIPIENT_EMAIL
        msg.attach(MIMEText(html_body, "html"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(SENDER_EMAIL, SENDER_PASSWORD)
            s.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, msg.as_string())
        print(f"  Email sent to {RECIPIENT_EMAIL}")
    except Exception as e:
        print(f"  [ERROR] {e}")


def update_html(current, new_ids, drop_ids):
    if not HTML_TRACKER_FILE.exists():
        print("  [WARN] HTML file not found.")
        return
    today = datetime.date.today().isoformat()
    entries = []
    for item in sorted(current, key=lambda x: x.get("dist",999)):
        lid   = item.get("id", make_id(item.get("address",""), item.get("city","")))
        flag  = item.get("flag","")
        first = item.get("first_seen", today)
        cap_v = str(item["cap"]) if item.get("cap") is not None else "null"
        sq_v  = str(item["sqft"]) if item.get("sqft") else "null"
        note  = f"Auto-scraped {item.get('source','')} — FOR SALE {fp(item.get('price'))}"
        if flag == "drop":
            note += f" (was {fp(item.get('prev_price'))})"
        a = item.get("address","").replace('"', '\\"').replace("'","&#39;")
        c = item.get("city","").replace('"', '\\"')
        u = item.get("url","").replace('"', '\\"')
        b = item.get("source","")
        n = f"{a} — {c}, {item.get('state','')}"
        entries.append(
            f'  {{f:"{flag}",id:"{lid}",n:"{n}",a:"{a}",c:"{c}",'
            f's:"{item.get("state","")}",d:{item.get("dist",999)},'
            f'p:{item.get("price",0)},sq:{sq_v},cap:{cap_v},cE:0,'
            f'nnn:null,nE:1,o:null,aadt:null,ar:"",u:"{u}",'
            f'om:null,b:"{b}",nt:"{note}",first:"{first}"}}'
        )
    new_seed = "const SEED = [\n" + ",\n".join(entries) + "\n];"
    html = HTML_TRACKER_FILE.read_text(encoding="utf-8")
    html = re.sub(r"const SEED = \[[\s\S]*?\];", new_seed, html)
    html = re.sub(r'id="b-run">[^<]*<',
                  f'id="b-run">Last run: {datetime.date.today().strftime("%b %d, %Y")}<', html)
    html = re.sub(r'Last updated: <span id="last-updated">[^<]*<',
                  f'Last updated: <span id="last-updated">{datetime.date.today().strftime("%b %d, %Y")}<', html)
    HTML_TRACKER_FILE.write_text(html, encoding="utf-8")
    print(f"  HTML updated — {len(current)} listings.")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*60}\nRE Bot — {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*60}")
    session = requests.Session()
    session.headers.update(HEADERS)

    print("\n[1] Scraping sources...")
    raw = scrape_all(session)
    print(f"  Total raw: {len(raw)}")

    print("\n[2] Filtering + dedup...")
    filtered = filter_listings(raw)
    current  = dedup(filtered)
    for item in current:
        item["id"] = make_id(item.get("address",""), item.get("city",""))
    print(f"  After filter+dedup: {len(current)}")

    print("\n[3] Comparing with known...")
    known = load_known()
    new_l, drops, active_ids = compare(current, known)
    print(f"  New: {len(new_l)} | Drops: {len(drops)}")
    for i in new_l:
        print(f"    NEW:  {i.get('address')} — {i.get('city')}, {i.get('state')} "
              f"— {fp(i.get('price'))} [{i.get('source')}]")
    for i in drops:
        print(f"    DROP: {i.get('address')} {fp(i.get('prev_price'))} → {fp(i.get('price'))}")

    save_known(update_known(known, current, active_ids))

    print("\n[4] Updating HTML tracker...")
    update_html(current, {i["id"] for i in new_l}, {i["id"] for i in drops})

    today_s = datetime.date.today().strftime("%B %d, %Y")
    subject = (
        f"RE Bot · {len(new_l)} NEW listing(s) · {today_s}" if new_l else
        f"RE Bot · {len(drops)} price drop(s) · {today_s}" if drops else
        f"RE Bot · Daily update · {today_s} · {len(current)} listings"
    )
    print(f"\n[5] Sending: {subject}")
    send_email(subject, build_email(current, new_l, drops))
    print(f"\n{'='*60}\nDone — {len(current)} listings tracked.\n{'='*60}\n")


if __name__ == "__main__":
    main()
