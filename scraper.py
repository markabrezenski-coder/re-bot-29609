"""
RE Bot — Strip Mall Tracker for ZIP 29609
Playwright headless browser scraper.
Sources: LoopNet (Playwright), Crexi (Playwright + login), CityFeet (Playwright),
         CommercialSearch (Playwright).
All sources use Playwright to bypass 403s from GitHub Actions IPs.
"""

import json, os, re, smtplib, datetime, time, math
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

RECIPIENT_EMAIL = "markbrezenski@yahoo.com"
SENDER_EMAIL    = os.environ.get("GMAIL_USER", "")
SENDER_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
CREXI_EMAIL     = os.environ.get("CREXI_EMAIL", "")
CREXI_PASSWORD  = os.environ.get("CREXI_PASSWORD", "")

CRITERIA = {"min_price": 1_000_000, "max_price": 5_000_000, "max_miles": 200}

KNOWN_LISTINGS_FILE = Path("known_listings.json")
HTML_TRACKER_FILE   = Path("re_bot_29609.html")

VALID_STATES = {"SC", "NC", "GA", "TN", "VA"}
HOME_LAT, HOME_LON = 34.8526, -82.3940

EXCLUDE_KW = [
    "dollar general","dollar tree","family dollar","walgreens","cvs",
    "autozone","o'reilly","advance auto","single tenant nnn","single-tenant",
    "cannabis","dispensary","trulieve","curaleaf","church","warehouse",
    "industrial","self storage","car wash","hotel","apartment","multifamily",
    "mobile home","vacant land","ground lease only",
]

DISTANCE_MAP = {
    "Greenville":5,"Simpsonville":12,"Mauldin":10,"Taylors":8,"Easley":18,
    "Greer":18,"Lyman":22,"Duncan":24,"Spartanburg":26,"Moore":24,
    "Boiling Springs":28,"Anderson":32,"Belton":36,"Abbeville":38,
    "Laurens":30,"Clinton":42,"Union":42,"Gaffney":45,"Greenwood":48,
    "Seneca":48,"Clemson":38,"Walhalla":52,"Pickens":28,"Liberty":24,
    "Powdersville":14,"Fountain Inn":18,"Woodruff":28,"Inman":30,
    "Landrum":35,"Aiken":75,"Newberry":65,"Lexington":95,"Irmo":90,
    "Columbia":100,"Lugoff":110,"Camden":115,"Sumter":130,"Florence":150,
    "Beaufort":170,"Rock Hill":75,"Fort Mill":72,"York":70,"Chester":75,
    "Asheville":65,"Hendersonville":55,"Brevard":60,"Waynesville":75,
    "Canton":70,"Black Mountain":70,"Morganton":85,"Hickory":95,
    "Statesville":100,"Concord":100,"Charlotte":95,"Monroe":100,
    "Gastonia":85,"Shelby":80,"Rutherfordton":70,"Forest City":72,
    "Kings Mountain":78,"High Point":115,"Greensboro":130,"Winston-Salem":125,
    "Euharlee":88,"Cartersville":90,"Rome":95,"Gainesville":110,
    "Cornelia":95,"Toccoa":90,"Marietta":145,"Kennesaw":140,
    "Smyrna":148,"Atlanta":150,"Alpharetta":155,"Duluth":160,
    "Lawrenceville":158,"Johns Creek":165,"Roswell":152,
    "Woodstock":140,"Augusta":145,"Athens":130,
    "Knoxville":110,"Maryville":115,"Cleveland":95,"Chattanooga":110,
    "Johnson City":140,"Kingsport":145,
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def haversine(lat2, lon2):
    dlat = math.radians(lat2 - HOME_LAT)
    dlon = math.radians(lon2 - HOME_LON)
    a = (math.sin(dlat/2)**2 +
         math.cos(math.radians(HOME_LAT)) *
         math.cos(math.radians(lat2)) *
         math.sin(dlon/2)**2)
    return int(3958.8 * 2 * math.asin(math.sqrt(a)) * 1.15)

def get_distance(city, state=""):
    city = city.strip(); state = state.strip().upper()
    if not city:
        return {"SC":80,"NC":120,"GA":140,"TN":120,"VA":180}.get(state, 999)
    for k, v in DISTANCE_MAP.items():
        if k.lower() == city.lower(): return v
    for k, v in DISTANCE_MAP.items():
        if k.lower() in city.lower(): return v
    try:
        q = requests.utils.quote(f"{city}, {state}")
        r = requests.get(
            f"https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
            f"?address={q}&benchmark=2020&format=json",
            timeout=8, headers={"User-Agent":"REBot/1.0"})
        if r.status_code == 200:
            m = r.json().get("result",{}).get("addressMatches",[])
            if m:
                c = m[0]["coordinates"]
                return haversine(float(c["y"]), float(c["x"]))
    except Exception:
        pass
    return {"SC":80,"NC":120,"GA":140,"TN":120,"VA":180}.get(state, 999)

def maps_url(address, city, state):
    q = requests.utils.quote(f"{address}, {city}, {state}")
    return f"https://www.google.com/maps/search/?api=1&query={q}"

def parse_price(text):
    if not text: return None
    text = str(text).replace(",","").strip()
    m = re.search(r'([\d.]+)\s*[Mm](?:illion)?', text)
    if m:
        v = int(float(m.group(1)) * 1_000_000)
        if 500_000 <= v <= 50_000_000: return v
    digits = re.sub(r'[^\d]','',text)
    if 7 <= len(digits) <= 8:
        v = int(digits)
        if 500_000 <= v <= 50_000_000: return v
    return None

def parse_cap(text):
    if not text: return None
    m = re.search(r'(\d+\.?\d*)\s*%', str(text))
    if m:
        v = float(m.group(1))
        if 2.0 <= v <= 20.0: return v
    return None

def parse_noi(text):
    if not text: return None
    text = str(text).replace(",","")
    m = re.search(r'([\d.]+)\s*[Kk]', text)
    if m: return int(float(m.group(1)) * 1000)
    digits = re.sub(r'[^\d]','',text)
    if 4 <= len(digits) <= 8:
        v = int(digits)
        if 10_000 <= v <= 10_000_000: return v
    return None

def parse_sqft(text):
    if not text: return None
    m = re.search(r'([\d,]+)\s*(?:SF|sq\.?\s*ft)', str(text), re.I)
    if m: return int(m.group(1).replace(",",""))
    return None

def parse_aadt(text):
    if not text: return None
    m = re.search(r'([\d,\.]+)\s*[Kk]?\s*(?:VPD|AADT|vehicles|cars)', str(text), re.I)
    if not m: return None
    raw = m.group(1).replace(",","")
    if 'k' in m.group(0).lower():
        try: return int(float(raw) * 1000)
        except: pass
    try:
        v = int(float(raw))
        if 1000 <= v <= 500_000: return v
    except: pass
    return None

def make_id(address, city):
    return re.sub(r'[^a-z0-9]+',' ', f"{address} {city}".lower()).strip().replace(' ','-')[:80]

def is_valid(item):
    state = item.get("state","").upper()
    if state and state not in VALID_STATES: return False
    price = item.get("price")
    if not price or not (CRITERIA["min_price"] <= price <= CRITERIA["max_price"]): return False
    chk = " ".join([item.get("address",""), item.get("name",""), item.get("notes","")]).lower()
    return not any(kw in chk for kw in EXCLUDE_KW)

def new_browser_context(playwright, stealth=True):
    """Create a browser context that mimics a real user."""
    browser = playwright.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
        ]
    )
    ctx = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 900},
        locale="en-US",
        timezone_id="America/New_York",
        extra_http_headers={
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    if stealth:
        ctx.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3]});
            window.chrome = {runtime: {}};
        """)
    return browser, ctx

def safe_goto(page, url, wait=5):
    try:
        page.goto(url, timeout=30000, wait_until="domcontentloaded")
        time.sleep(wait)
        return True
    except Exception as e:
        print(f"    goto error {url[:60]}: {e}")
        return False

def extract_listing_detail(soup, url):
    """Extract all data fields from a fully-rendered listing page."""
    text = soup.get_text(" ", strip=True)
    d = {"url": url}

    # Price
    pm = re.search(r'\$[\d,]+(?:\.\d+)?(?:\s*(?:,\d{3})*)', text)
    if pm: d["price"] = parse_price(pm.group())

    # CAP
    d["cap"] = parse_cap(text)

    # NOI
    noi_m = re.search(r'(?:NOI|Net Operating Income)[^\$\n]{0,30}\$([\d,]+)', text, re.I)
    if noi_m: d["noi"] = parse_noi(noi_m.group(1))

    # NNN rent per SF
    nnn_m = re.search(r'(?:NNN|Triple Net|Base Rent)[^\$\n]{0,20}\$([\d,\.]+)\s*(?:PSF|per SF|\/SF|\/sqft)', text, re.I)
    if nnn_m:
        try: d["nnn"] = float(nnn_m.group(1).replace(",",""))
        except: pass

    # Occupancy
    occ_m = re.search(r'(\d{1,3})\s*%\s*(?:Occupied|Leased|Occupancy)', text, re.I)
    if occ_m: d["occ"] = int(occ_m.group(1))

    # AADT
    d["aadt"] = parse_aadt(text)

    # Year built
    yr_m = re.search(r'(?:Year Built|Built in|Built)[:\s]+(\d{4})', text, re.I)
    if yr_m: d["year_built"] = int(yr_m.group(1))

    # Sqft
    d["sqft"] = parse_sqft(text)

    # OM / brochure link
    om = soup.find("a", href=re.compile(r'\.pdf', re.I))
    if not om:
        om = soup.find("a", string=re.compile(r'(?:OM|Offering Memo|Brochure|Download)', re.I))
    if om: d["om_url"] = om.get("href","")

    # Tenants
    t_m = re.search(r'(?:Tenants?|Anchor)[:\s]+([A-Za-z0-9\s,&\-]+?)(?:\.|$|\n)', text, re.I)
    if t_m: d["tenants"] = t_m.group(1).strip()[:120]

    # Address block
    addr_m = re.search(
        r'(\d+[\w\s\-\.]+(?:St|Ave|Blvd|Hwy|Dr|Rd|Way|Pkwy|Ln|Ct|Sq|Gtwy))'
        r'[,\s]+([A-Z][a-zA-Z\s]+),\s*([A-Z]{2})',
        text)
    if addr_m:
        d["address"] = addr_m.group(1).strip()
        d["city"]    = addr_m.group(2).strip()
        d["state"]   = addr_m.group(3).strip()

    return d


# ── SCRAPERS ──────────────────────────────────────────────────────────────────

def scrape_cityfeet(playwright):
    print("\n[CityFeet — Playwright]")
    out = []
    pages = [
        ("SC","https://www.cityfeet.com/cont/south-carolina/shopping-centers-for-sale"),
        ("NC","https://www.cityfeet.com/cont/north-carolina/shopping-centers-for-sale"),
        ("GA","https://www.cityfeet.com/cont/georgia/shopping-centers-for-sale"),
        ("TN","https://www.cityfeet.com/cont/tennessee/shopping-centers-for-sale"),
        ("VA","https://www.cityfeet.com/cont/virginia/shopping-centers-for-sale"),
    ]
    browser, ctx = new_browser_context(playwright)
    page = ctx.new_page()
    for state, url in pages:
        if not safe_goto(page, url, wait=4): continue
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(page.content(), "html.parser")
        seen = set()
        count = 0
        for card in soup.select("a[href*='/cont/listing/']"):
            href = card.get("href","")
            if href in seen or not href.startswith("/cont/listing/"): continue
            seen.add(href)
            lines = [l.strip() for l in card.get_text("\n").split("\n") if l.strip()]
            address, city, price, cap, sqft = "","",None,None,None
            for line in lines:
                if not address and re.match(r'^\d+\s+[\w]',line) and re.search(
                        r'\b(St|Ave|Blvd|Hwy|Dr|Rd|Way|Pkwy|Ln|Ct|Sq|Gtwy)\b',line,re.I):
                    address = line[:100]
                if not price and re.search(r'\$[\d,]+',line):
                    p = parse_price(re.search(r'\$[\d,]+(?:,\d+)*',line).group())
                    if p: price = p
                if cap is None and re.search(r'\d+\.?\d*\s*%\s*Cap',line,re.I):
                    cap = parse_cap(line)
                if sqft is None: sqft = parse_sqft(line)
            # City from slug
            slug = href.replace("/cont/listing/","").split("/")[0]
            m = re.search(r'-([a-z][a-z\-]+)-(sc|nc|ga|tn|va)(?:-\d+)?$', slug)
            if m: city = m.group(1).replace("-"," ").title()
            if not address and lines: address = lines[0][:100]
            if price and address:
                item = {"address":address,"city":city,"state":state,"price":price,
                        "cap":cap,"sqft":sqft,"noi":None,"nnn":None,"occ":None,
                        "aadt":None,"year_built":None,"tenants":None,"om_url":None,
                        "url":"https://www.cityfeet.com"+href,"source":"CityFeet"}
                if is_valid(item):
                    out.append(item); count += 1
        print(f"  {state}: {count}")
    ctx.close(); browser.close()
    return out


def scrape_loopnet(playwright):
    print("\n[LoopNet — Playwright]")
    from bs4 import BeautifulSoup
    out = []
    search_urls = [
        ("SC","https://www.loopnet.com/search/strip-malls/sc/for-sale/"),
        ("NC","https://www.loopnet.com/search/strip-malls/nc/for-sale/"),
        ("GA","https://www.loopnet.com/search/strip-malls/ga/for-sale/"),
        ("TN","https://www.loopnet.com/search/strip-malls/tn/for-sale/"),
        ("SC","https://www.loopnet.com/search/retail-space/greenville-sc/for-sale/"),
        ("SC","https://www.loopnet.com/search/retail-space/spartanburg-sc/for-sale/"),
        ("SC","https://www.loopnet.com/search/retail-space/anderson-sc/for-sale/"),
        ("NC","https://www.loopnet.com/search/retail-space/asheville-nc/for-sale/"),
        ("GA","https://www.loopnet.com/search/retail-space/gainesville-ga/for-sale/"),
        ("GA","https://www.loopnet.com/search/retail-space/cartersville-ga/for-sale/"),
    ]
    browser, ctx = new_browser_context(playwright)
    page = ctx.new_page()

    listing_urls = []
    seen_urls = set()

    for state, url in search_urls:
        print(f"  Searching {state}: {url.split('/')[-3]}")
        if not safe_goto(page, url, wait=5): continue
        # Scroll to load lazy content
        for _ in range(3):
            page.keyboard.press("End")
            time.sleep(2)
        soup = BeautifulSoup(page.content(), "html.parser")
        for a in soup.find_all("a", href=re.compile(r'/Listing/')):
            href = a.get("href","")
            full = "https://www.loopnet.com" + href if href.startswith("/") else href
            if full not in seen_urls:
                seen_urls.add(full)
                # Quick price filter from surrounding text
                parent_text = (a.parent.get_text(" ") if a.parent else "") + " " + a.get_text(" ")
                pm = re.search(r'\$[\d,]+', parent_text)
                price_est = parse_price(pm.group()) if pm else None
                listing_urls.append((state, full, price_est))

    print(f"  Found {len(listing_urls)} listing URLs — fetching details...")

    for state, url, est_price in listing_urls[:80]:
        try:
            if not safe_goto(page, url, wait=4): continue
            soup = BeautifulSoup(page.content(), "html.parser")
            d = extract_listing_detail(soup, url)
            price = d.get("price") or est_price
            if not price or not (CRITERIA["min_price"] <= price <= CRITERIA["max_price"]):
                continue
            addr  = d.get("address","")
            city  = d.get("city","")
            lstate = d.get("state", state)
            if not addr: continue
            item = {
                "address":addr,"city":city,"state":lstate,
                "price":price,"cap":d.get("cap"),"noi":d.get("noi"),
                "nnn":d.get("nnn"),"occ":d.get("occ"),"sqft":d.get("sqft"),
                "aadt":d.get("aadt"),"year_built":d.get("year_built"),
                "tenants":d.get("tenants"),"om_url":d.get("om_url"),
                "url":url,"source":"LoopNet",
            }
            if is_valid(item):
                out.append(item)
                print(f"    ✓ {addr}, {city} {lstate} — ${price:,.0f}"
                      + (f" CAP {item['cap']:.1f}%" if item.get("cap") else "")
                      + (f" NOI ${item['noi']:,.0f}" if item.get("noi") else ""))
        except Exception as e:
            print(f"    error {url[:60]}: {e}")

    ctx.close(); browser.close()
    print(f"  LoopNet total: {len(out)}")
    return out


def scrape_crexi(playwright):
    print("\n[Crexi — Playwright + Login]")
    from bs4 import BeautifulSoup
    out = []
    if not CREXI_EMAIL or not CREXI_PASSWORD:
        print("  No Crexi credentials — skipping.")
        return out

    browser, ctx = new_browser_context(playwright)
    page = ctx.new_page()

    # Login
    try:
        print("  Logging in...")
        page.goto("https://www.crexi.com/login", timeout=30000, wait_until="domcontentloaded")
        time.sleep(4)
        # Try multiple possible selectors for email/password fields
        for sel in ['input[type="email"]', 'input[name="email"]', 'input[placeholder*="Email"]']:
            try: page.fill(sel, CREXI_EMAIL); break
            except: pass
        time.sleep(1)
        for sel in ['input[type="password"]', 'input[name="password"]', 'input[placeholder*="Password"]']:
            try: page.fill(sel, CREXI_PASSWORD); break
            except: pass
        time.sleep(1)
        for sel in ['button[type="submit"]', 'button:has-text("Sign In")', 'button:has-text("Log In")', 'button:has-text("Continue")']:
            try: page.click(sel, timeout=3000); break
            except: pass
        time.sleep(6)
        current_url = page.url
        if "login" in current_url.lower():
            print("  Login may have failed — continuing anyway")
        else:
            print(f"  Login OK — on {current_url[:60]}")
    except Exception as e:
        print(f"  Login error: {e}")

    searches = [
        ("SC","https://www.crexi.com/properties/SC/Shopping-Centers?forSale=true&minPrice=1000000&maxPrice=5000000"),
        ("SC","https://www.crexi.com/properties/SC/Retail?forSale=true&minPrice=1000000&maxPrice=5000000"),
        ("NC","https://www.crexi.com/properties/NC/Shopping-Centers?forSale=true&minPrice=1000000&maxPrice=5000000"),
        ("NC","https://www.crexi.com/properties/NC/Retail?forSale=true&minPrice=1000000&maxPrice=5000000"),
        ("GA","https://www.crexi.com/properties/GA/Shopping-Centers?forSale=true&minPrice=1000000&maxPrice=5000000"),
        ("GA","https://www.crexi.com/properties/GA/Retail?forSale=true&minPrice=1000000&maxPrice=5000000"),
        ("TN","https://www.crexi.com/properties/TN/Shopping-Centers?forSale=true&minPrice=1000000&maxPrice=5000000"),
    ]

    listing_urls = []
    seen_urls = set()
    for state, url in searches:
        print(f"  Searching Crexi {state}...")
        if not safe_goto(page, url, wait=6): continue
        # Scroll down to load all cards
        for _ in range(6):
            page.keyboard.press("End")
            time.sleep(2)
        soup = BeautifulSoup(page.content(), "html.parser")
        for a in soup.find_all("a", href=re.compile(r'/properties/\d+')):
            href = a.get("href","")
            full = "https://www.crexi.com" + href if href.startswith("/") else href
            if full not in seen_urls:
                seen_urls.add(full)
                listing_urls.append((state, full))

    print(f"  Found {len(listing_urls)} Crexi listing URLs — fetching details...")

    for state, url in listing_urls[:100]:
        try:
            if not safe_goto(page, url, wait=4): continue
            soup = BeautifulSoup(page.content(), "html.parser")
            d = extract_listing_detail(soup, url)
            price = d.get("price")
            if not price or not (CRITERIA["min_price"] <= price <= CRITERIA["max_price"]):
                continue
            addr  = d.get("address","")
            city  = d.get("city","")
            lstate = d.get("state", state)
            if not addr: continue
            item = {
                "address":addr,"city":city,"state":lstate,
                "price":price,"cap":d.get("cap"),"noi":d.get("noi"),
                "nnn":d.get("nnn"),"occ":d.get("occ"),"sqft":d.get("sqft"),
                "aadt":d.get("aadt"),"year_built":d.get("year_built"),
                "tenants":d.get("tenants"),"om_url":d.get("om_url"),
                "url":url,"source":"Crexi",
            }
            if is_valid(item):
                out.append(item)
                print(f"    ✓ {addr}, {city} {lstate} — ${price:,.0f}"
                      + (f" CAP {item['cap']:.1f}%" if item.get("cap") else "")
                      + (f" NOI ${item['noi']:,.0f}" if item.get("noi") else ""))
        except Exception as e:
            print(f"    error {url[:60]}: {e}")

    ctx.close(); browser.close()
    print(f"  Crexi total: {len(out)}")
    return out


# ── Filter / dedup / compare ─────────────────────────────────────────────────

def filter_listings(raw):
    from bs4 import BeautifulSoup
    out = []
    for item in raw:
        if not is_valid(item): continue
        dist = get_distance(item.get("city",""), item.get("state",""))
        if dist > CRITERIA["max_miles"]: continue
        item["dist"] = dist
        item["maps_url"] = maps_url(item.get("address",""), item.get("city",""), item.get("state",""))
        out.append(item)
    return out

def dedup(listings):
    seen = {}
    for item in listings:
        lid = make_id(item.get("address",""), item.get("city",""))
        if lid not in seen:
            seen[lid] = item
        else:
            merged = dict(seen[lid])
            for k, v in item.items():
                if v is not None and merged.get(k) is None:
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
            item["flag"] = "new"; item["first_seen"] = today
            new_l.append(item)
        else:
            prev = known[lid].get("price")
            if prev and item.get("price") and item["price"] < prev:
                item["flag"] = "drop"; item["prev_price"] = prev
                item["first_seen"] = known[lid].get("first_seen", today)
                drops.append(item)
            else:
                item["flag"] = ""; item["first_seen"] = known[lid].get("first_seen", today)
    active = {make_id(i.get("address",""), i.get("city","")) for i in current}
    return new_l, drops, active

def update_known(known, current, active_ids):
    today = datetime.date.today().isoformat()
    for item in current:
        known[item["id"]] = {
            k: item.get(k) for k in
            ["price","cap","noi","nnn","occ","sqft","address","city","state","url","source"]
        }
        known[item["id"]]["last_seen"] = today
        known[item["id"]]["first_seen"] = item.get("first_seen", today)
    for lid in known:
        if lid not in active_ids:
            known[lid]["off_market"] = True
    return known


# ── Email ─────────────────────────────────────────────────────────────────────

def fp(p): return f"${p:,.0f}" if p else "—"
def fc(c): return f"{c:.2f}%" if c else "—"
def fn(n): return f"${n:,.0f}" if n else "—"
def fq(q): return f"${q:.2f}/SF" if q else "—"
def fo(o): return f"{o}%" if o is not None else "—"

def build_email(current, new_l, drops):
    today   = datetime.date.today().strftime("%B %d, %Y")
    sources = sorted(set(i.get("source","") for i in current if i.get("source")))
    rows = ""
    hs = "padding:9px 10px;text-align:left;font-size:11px;font-weight:500;color:#888780;border-bottom:1px solid #e8e7df;white-space:nowrap"
    td = "padding:8px 10px;border-bottom:1px solid #f0efe8;font-size:12px;vertical-align:top"
    for item in sorted(current, key=lambda x: x.get("dist",999)):
        flag = item.get("flag","")
        style, badge = "", ""
        if flag == "new":
            style = "background:#fffbf2;"
            badge = ' <span style="font-size:10px;background:#FAEEDA;color:#633806;padding:1px 6px;border-radius:4px;font-weight:bold">NEW</span>'
        elif flag == "drop":
            style = "background:#eef6fd;"
            badge = (f' <span style="font-size:10px;background:#E6F1FB;color:#0C447C;'
                     f'padding:1px 6px;border-radius:4px;font-weight:bold">'
                     f'PRICE DROP ▼ was {fp(item.get("prev_price"))}</span>')
        addr   = item.get("address","—")
        city   = item.get("city","")
        state  = item.get("state","")
        gmap   = item.get("maps_url", maps_url(addr, city, state))
        src    = f'<span style="font-size:10px;background:#f1f0e8;color:#5f5e5a;padding:1px 5px;border-radius:3px">{item.get("source","")}</span>'
        yr     = f" · Built {item['year_built']}" if item.get("year_built") else ""
        ten    = f'<br><span style="font-size:10px;color:#888780">Tenants: {item["tenants"]}</span>' if item.get("tenants") else ""
        om     = f' · <a href="{item["om_url"]}" style="color:#185FA5">OM</a>' if item.get("om_url") else ""
        sqft_s = f"{item['sqft']:,} SF" if item.get("sqft") else "—"
        aadt_s = f"{item['aadt']:,} VPD" if item.get("aadt") else "—"
        rows += (
            f'<tr style="{style}">'
            f'<td style="{td}"><strong>{addr}</strong>{badge}<br>'
            f'<span style="color:#888780">{city}, {state}{yr}</span><br>{src}{ten}</td>'
            f'<td style="{td};white-space:nowrap">{item.get("dist","—")} mi</td>'
            f'<td style="{td};white-space:nowrap">{fp(item.get("price"))}</td>'
            f'<td style="{td}">{fc(item.get("cap"))}</td>'
            f'<td style="{td}">{fn(item.get("noi"))}</td>'
            f'<td style="{td}">{fq(item.get("nnn"))}</td>'
            f'<td style="{td}">{fo(item.get("occ"))}</td>'
            f'<td style="{td}">{sqft_s}</td>'
            f'<td style="{td}">{aadt_s}</td>'
            f'<td style="{td}">'
            f'<a href="{item.get("url","#")}" style="color:#185FA5">Listing</a>{om}&nbsp;'
            f'<a href="{gmap}" style="color:#185FA5">Map</a></td>'
            f'</tr>'
        )
    sc = "#0F6E56" if new_l else "#888780"
    st = f"{len(new_l)} new listing(s) found" if new_l else "No new listings today"
    if drops: st += f" · {len(drops)} price drop(s)"
    headers = ["Property","Dist","Price","CAP","NOI","NNN/SF","Occ","Sqft","Traffic","Links"]
    return (
        f'<!DOCTYPE html><html><head><meta charset="UTF-8"></head>'
        f'<body style="font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',sans-serif;background:#f5f5f3;margin:0;padding:20px">'
        f'<div style="max-width:1020px;margin:0 auto">'
        f'<div style="background:#fff;border-radius:10px;padding:20px 24px;border:1px solid #e0dfd8;margin-bottom:16px">'
        f'<h1 style="font-size:16px;font-weight:500;margin:0 0 6px">RE Bot — Strip Mall Tracker · 29609</h1>'
        f'<div style="font-size:11px;color:#888780;margin-bottom:10px">{today} · {", ".join(sources)} · $1M–$5M · 200mi · SC/NC/GA/TN</div>'
        f'<p style="font-size:13px;color:{sc};margin:0;font-weight:500">{st}</p></div>'
        f'<div style="background:#fff;border-radius:10px;border:1px solid #e0dfd8;overflow:hidden">'
        f'<table style="width:100%;border-collapse:collapse;font-size:12px">'
        f'<thead><tr style="background:#f8f7f2">'
        + "".join(f'<th style="{hs}">{h}</th>' for h in headers)
        + f'</tr></thead><tbody>{rows}</tbody></table></div>'
        f'<p style="font-size:11px;color:#888780;margin-top:14px;text-align:center">'
        f'RE Bot · Playwright · LoopNet + Crexi + CityFeet · 200mi of 29609</p>'
        f'</div></body></html>'
    )

def send_email(subject, html_body):
    if not SENDER_EMAIL or not SENDER_PASSWORD:
        print("  [WARN] No credentials."); return
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
        print("  [WARN] HTML file not found."); return
    today = datetime.date.today().isoformat()
    entries = []
    for item in sorted(current, key=lambda x: x.get("dist",999)):
        lid   = item.get("id", make_id(item.get("address",""), item.get("city","")))
        flag  = item.get("flag","")
        first = item.get("first_seen", today)
        def jv(v): return str(v) if v is not None else "null"
        a = item.get("address","").replace('"','\\"').replace("'","&#39;")
        c = item.get("city","").replace('"','\\"')
        u = item.get("url","").replace('"','\\"')
        om = (item.get("om_url","") or "").replace('"','\\"')
        b = item.get("source","")
        n = f"{a} — {c}, {item.get('state','')}"
        note = f"{b} — FOR SALE {fp(item.get('price'))}"
        if item.get("cap"): note += f" CAP {item['cap']:.1f}%"
        if item.get("noi"): note += f" NOI {fn(item['noi'])}"
        if flag == "drop": note += f" (was {fp(item.get('prev_price'))})"
        entries.append(
            f'  {{f:"{flag}",id:"{lid}",n:"{n}",a:"{a}",c:"{c}",'
            f's:"{item.get("state","")}",d:{item.get("dist",999)},'
            f'p:{item.get("price",0)},sq:{jv(item.get("sqft"))},'
            f'cap:{jv(item.get("cap"))},cE:0,'
            f'nnn:{jv(item.get("nnn"))},nE:0,o:{jv(item.get("occ"))},'
            f'aadt:{jv(item.get("aadt"))},ar:"",'
            f'u:"{u}",om:{json.dumps(om) if om else "null"},'
            f'b:"{b}",nt:"{note}",first:"{first}"}}'
        )
    new_seed = "const SEED = [\n" + ",\n".join(entries) + "\n];"
    html = HTML_TRACKER_FILE.read_text(encoding="utf-8")
    html = re.sub(r"const SEED = \[[\s\S]*?\];", new_seed, html)
    html = re.sub(r'id="b-run">[^<]*<',
                  f'id="b-run">Last run: {datetime.date.today().strftime("%b %d, %Y")}<', html)
    HTML_TRACKER_FILE.write_text(html, encoding="utf-8")
    print(f"  HTML updated — {len(current)} listings.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*60}\nRE Bot — {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*60}")
    all_raw = []
    with sync_playwright() as pw:
        all_raw += scrape_cityfeet(pw)
        all_raw += scrape_loopnet(pw)
        all_raw += scrape_crexi(pw)

    print(f"\nTotal raw: {len(all_raw)}")
    filtered = filter_listings(all_raw)
    current  = dedup(filtered)
    for item in current:
        item["id"] = make_id(item.get("address",""), item.get("city",""))
    print(f"After filter+dedup: {len(current)}")

    known = load_known()
    new_l, drops, active_ids = compare(current, known)
    print(f"New: {len(new_l)} | Drops: {len(drops)}")
    for i in new_l:
        print(f"  NEW: {i.get('address')} {i.get('city')},{i.get('state')} {fp(i.get('price'))} [{i.get('source')}]")
    for i in drops:
        print(f"  DROP: {i.get('address')} {fp(i.get('prev_price'))}→{fp(i.get('price'))}")

    save_known(update_known(known, current, active_ids))
    update_html(current, {i["id"] for i in new_l}, {i["id"] for i in drops})

    today_s = datetime.date.today().strftime("%B %d, %Y")
    subject = (
        f"RE Bot · {len(new_l)} NEW listing(s) · {today_s}" if new_l else
        f"RE Bot · {len(drops)} price drop(s) · {today_s}" if drops else
        f"RE Bot · Daily update · {today_s} · {len(current)} listings"
    )
    print(f"\nSending: {subject}")
    send_email(subject, build_email(current, new_l, drops))
    print(f"\n{'='*60}\nDone — {len(current)} listings.\n{'='*60}\n")

if __name__ == "__main__":
    main()
