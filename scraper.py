"""
RE Bot — Strip Mall Tracker for ZIP 29609
Expanded multi-source scraper: CityFeet, CommercialSearch, Showcase,
BizBuySell, LoopNet (public), SVN, Marcus & Millichap, NAI Global,
Matthews, Bull Realty, Sands, Wilson Kibler, Reedy Property Group + more.
"""

import json, os, re, smtplib, datetime, time
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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

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
    "Kings Mountain":78,"High Point":115,"Greensboro":130,
    "Euharlee":88,"Cartersville":90,"Rome":95,"Gainesville":110,
    "Cornelia":95,"Toccoa":90,"Marietta":145,"Kennesaw":140,
    "Smyrna":148,"Atlanta":150,"Alpharetta":155,"Duluth":160,
    "Lawrenceville":158,"Johns Creek":165,"Roswell":152,
    "Woodstock":140,"Augusta":145,"Athens":130,"Covington":148,
    "Knoxville":110,"Maryville":115,"Cleveland TN":95,"Chattanooga":110,
}

def get_distance(city, state=""):
    for k, v in DISTANCE_MAP.items():
        if k.lower() in city.lower():
            return v
    return {"SC":80,"NC":120,"GA":140,"TN":120,"VA":180}.get(state.upper(), 999)

def parse_price(text):
    if not text:
        return None
    text = str(text).replace(",","")
    if re.search(r'[Mm]', text):
        m = re.search(r'([\d\.]+)\s*[Mm]', text)
        if m:
            v = int(float(m.group(1)) * 1_000_000)
            if 500_000 <= v <= 50_000_000:
                return v
    nums = re.findall(r'\d+', text.replace("$",""))
    if nums:
        joined = "".join(nums[:3])
        for candidate in [joined, nums[0]]:
            try:
                v = int(candidate)
                if 500_000 <= v <= 50_000_000:
                    return v
            except:
                pass
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

def extract_cards(soup, price_required=True):
    """Generic card extractor — works across many CRE sites."""
    results = []
    selectors = [
        "[class*='listing-card']","[class*='property-card']",
        "[class*='listing-item']","[class*='property-item']",
        "[class*='search-result']","article","[data-price]",
    ]
    for sel in selectors:
        cards = soup.select(sel)
        if len(cards) > 2:
            for card in cards:
                text  = card.get_text(" ", strip=True)
                pm    = re.search(r'\$[\d,]+', text)
                cm    = re.search(r'(\d+\.?\d*)\s*%\s*(?:cap|CAP|Cap)', text)
                sm    = re.search(r'([\d,]+)\s*(?:SF|sq\.?\s*ft)', text, re.I)
                am    = re.search(r'\d+[\w\s\-]+(?:St|Ave|Blvd|Hwy|Dr|Rd|Way|Pkwy|Ln|Ct)', text, re.I)
                link  = card.find("a", href=True)
                price = parse_price(pm.group()) if pm else None
                if price_required and not price:
                    continue
                results.append({
                    "address": am.group()[:80] if am else text[:60],
                    "price": price,
                    "cap": parse_cap(cm.group()) if cm else None,
                    "sqft": int(sm.group(1).replace(",","")) if sm else None,
                    "link": link["href"] if link else None,
                })
            if results:
                break
    return results

# ── SCRAPERS ────────────────────────────────────────────────────────────────

def scrape_cityfeet(session):
    pages = [
        ("SC","https://www.cityfeet.com/cont/south-carolina/shopping-centers-for-sale"),
        ("NC","https://www.cityfeet.com/cont/north-carolina/shopping-centers-for-sale"),
        ("GA","https://www.cityfeet.com/cont/georgia/shopping-centers-for-sale"),
        ("TN","https://www.cityfeet.com/cont/tennessee/shopping-centers-for-sale"),
        ("VA","https://www.cityfeet.com/cont/virginia/shopping-centers-for-sale"),
    ]
    out = []
    for state, url in pages:
        r = safe_get(url, session)
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        seen = set()
        for card in soup.select("a[href*='/cont/listing/']"):
            href = card.get("href","")
            if href in seen or not href.startswith("/cont/listing/"):
                continue
            seen.add(href)
            lines = [l.strip() for l in card.get_text("\n").split("\n") if l.strip()]
            address,city,price,cap,sqft = "","",None,None,None
            for line in lines:
                if re.match(r'^\d+[\w\s\-\.]+(?:St|Ave|Blvd|Hwy|Dr|Rd|Way|Pkwy|Ln|Ct|Sq|Gtwy)',line,re.I):
                    address = line
                if re.search(r'\$[\d,]+', line):
                    p = parse_price(re.search(r'\$[\d,]+', line).group())
                    if p: price = p
                if 'Cap Rate' in line or 'cap rate' in line:
                    cap = parse_cap(line)
                if re.search(r'[\d,]+\s*SF', line, re.I):
                    m = re.search(r'([\d,]+)\s*SF', line, re.I)
                    if m: sqft = int(m.group(1).replace(",",""))
            slug = href.replace("/cont/listing/","").split("/")[0]
            parts = slug.rsplit("-",2)
            if len(parts) >= 2 and not city:
                city = parts[-2].replace("-"," ").title()
            if not address and lines:
                address = lines[0]
            if price:
                out.append({"address":address,"city":city,"state":state,"price":price,
                            "cap":cap,"sqft":sqft,"url":"https://www.cityfeet.com"+href,"source":"CityFeet"})
        n = len([x for x in out if x["state"]==state])
        print(f"  CityFeet {state}: {n}")
    return out

def scrape_site(session, urls, source, state_override=None):
    out = []
    for item in urls:
        if isinstance(item, tuple):
            url, state = item
        else:
            url, state = item, state_override or "?"
        r = safe_get(url, session)
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        domain = re.match(r'https?://[^/]+', url)
        base = domain.group() if domain else ""
        for card in extract_cards(soup):
            href = card.get("link","")
            full_url = base + href if href and href.startswith("/") else (href or url)
            out.append({"address":card["address"],"city":"","state":state,
                        "price":card["price"],"cap":card["cap"],"sqft":card["sqft"],
                        "url":full_url,"source":source})
    print(f"  {source}: {len(out)}")
    return out

def scrape_all(session):
    out = []
    out += scrape_cityfeet(session)
    out += scrape_site(session, [
        ("https://www.commercialsearch.com/listings/for-sale/?q=strip+center&location=Greenville%2C+SC&radius=200&propertyType=retail","SC"),
        ("https://www.commercialsearch.com/listings/for-sale/?q=strip+mall&location=Charlotte%2C+NC&radius=150&propertyType=retail","NC"),
        ("https://www.commercialsearch.com/listings/for-sale/?q=strip+center&location=Atlanta%2C+GA&radius=120&propertyType=retail","GA"),
    ], "CommercialSearch")
    out += scrape_site(session, [
        ("https://www.showcase.com/commercial-real-estate/for-sale/sc/?type=shopping-center","SC"),
        ("https://www.showcase.com/commercial-real-estate/for-sale/nc/?type=shopping-center","NC"),
        ("https://www.showcase.com/commercial-real-estate/for-sale/ga/?type=shopping-center","GA"),
    ], "Showcase")
    out += scrape_site(session, [
        ("https://www.bizbuysell.com/commercial-real-estate/south-carolina/retail/","SC"),
        ("https://www.bizbuysell.com/commercial-real-estate/north-carolina/retail/","NC"),
        ("https://www.bizbuysell.com/commercial-real-estate/georgia/retail/","GA"),
    ], "BizBuySell")
    out += scrape_site(session, [
        ("https://www.svn.com/properties/?property_type=retail&transaction_type=sale&state=SC","SC"),
        ("https://www.svn.com/properties/?property_type=retail&transaction_type=sale&state=NC","NC"),
        ("https://www.svn.com/properties/?property_type=retail&transaction_type=sale&state=GA","GA"),
    ], "SVN")
    out += scrape_site(session, [
        ("https://www.marcusmillichap.com/properties/forsale?propertyType=Retail&stateCode=SC","SC"),
        ("https://www.marcusmillichap.com/properties/forsale?propertyType=Retail&stateCode=NC","NC"),
        ("https://www.marcusmillichap.com/properties/forsale?propertyType=Retail&stateCode=GA","GA"),
    ], "Marcus & Millichap")
    out += scrape_site(session, [
        ("https://www.naiglobal.com/properties?propertyType=Retail&transactionType=Sale&stateCode=SC","SC"),
        ("https://www.naiglobal.com/properties?propertyType=Retail&transactionType=Sale&stateCode=NC","NC"),
        ("https://www.naiglobal.com/properties?propertyType=Retail&transactionType=Sale&stateCode=GA","GA"),
    ], "NAI Global")
    out += scrape_site(session, [
        ("https://www.matthews.com/listings/?type=retail&state=SC&status=for-sale","SC"),
        ("https://www.matthews.com/listings/?type=retail&state=NC&status=for-sale","NC"),
        ("https://www.matthews.com/listings/?type=retail&state=GA&status=for-sale","GA"),
    ], "Matthews Real Estate")
    out += scrape_site(session, [
        ("https://bullrealty.com/commercial-real-estate-for-sale/georgia/retail/","GA"),
        ("https://bullrealty.com/commercial-real-estate-for-sale/south-carolina/retail/","SC"),
    ], "Bull Realty")
    out += scrape_site(session, [
        ("https://www.sandsinvestmentgroup.com/listings/?state=SC&type=retail","SC"),
        ("https://www.sandsinvestmentgroup.com/listings/?state=NC&type=retail","NC"),
        ("https://www.sandsinvestmentgroup.com/listings/?state=GA&type=retail","GA"),
    ], "Sands Investment Group")
    out += scrape_site(session, [
        "https://www.wilsonkibler.com/properties/?type=retail&status=for-sale",
    ], "Wilson Kibler", "SC")
    out += scrape_site(session, [
        "https://www.reedypropertygroup.com/commercial/for-sale/",
    ], "Reedy Property Group", "SC")
    out += scrape_site(session, [
        ("https://www.loopnet.com/search/strip-malls/sc/for-sale/","SC"),
        ("https://www.loopnet.com/search/strip-malls/nc/for-sale/","NC"),
        ("https://www.loopnet.com/search/strip-malls/ga/for-sale/","GA"),
    ], "LoopNet")
    return out

# ── FILTER / DEDUP / COMPARE ────────────────────────────────────────────────

def filter_listings(raw):
    out = []
    for item in raw:
        p = item.get("price")
        if not p or not (CRITERIA["min_price"] <= p <= CRITERIA["max_price"]):
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
            if (item.get("cap") and not ex.get("cap")) or (item.get("sqft") and not ex.get("sqft")):
                seen[lid] = {**ex, **{k:v for k,v in item.items() if v}}
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
        known[lid] = {"price":item.get("price"),"cap":item.get("cap"),
                      "first_seen":item.get("first_seen",today),"last_seen":today,
                      "address":item.get("address"),"city":item.get("city"),
                      "state":item.get("state"),"url":item.get("url"),
                      "source":item.get("source")}
    for lid in known:
        if lid not in active_ids:
            known[lid]["off_market"] = True
    return known

# ── EMAIL & HTML ─────────────────────────────────────────────────────────────

def fp(p): return f"${p:,.0f}" if p else "—"
def fc(c): return f"{c:.2f}%" if c else "—"

def build_email(current, new_l, drops):
    today  = datetime.date.today().strftime("%B %d, %Y")
    sources = sorted(set(i.get("source","") for i in current if i.get("source")))
    rows = ""
    for item in sorted(current, key=lambda x: x.get("dist",999)):
        flag = item.get("flag","")
        style, badge = "", ""
        if flag == "new":
            style = "background:#fffbf2;"
            badge = ' <span style="font-size:10px;background:#FAEEDA;color:#633806;padding:1px 6px;border-radius:4px;font-weight:bold">NEW</span>'
        elif flag == "drop":
            style = "background:#eef6fd;"
            badge = f' <span style="font-size:10px;background:#E6F1FB;color:#0C447C;padding:1px 6px;border-radius:4px;font-weight:bold">PRICE DROP ▼ was {fp(item.get("prev_price"))}</span>'
        addr  = item.get("address","—")
        city  = item.get("city","")
        state = item.get("state","")
        maps  = f"https://www.google.com/maps/search/?api=1&query={requests.utils.quote(addr+', '+city+', '+state)}"
        src   = f'<span style="font-size:10px;background:#f1f0e8;color:#5f5e5a;padding:1px 5px;border-radius:3px">{item.get("source","")}</span>'
        rows += f'<tr style="{style}"><td style="padding:8px 10px;border-bottom:1px solid #f0efe8;font-size:12px;vertical-align:top"><strong>{addr}</strong>{badge}<br><span style="color:#888780">{city}, {state}</span><br>{src}</td><td style="padding:8px 10px;border-bottom:1px solid #f0efe8;font-size:12px;white-space:nowrap">{item.get("dist","—")} mi</td><td style="padding:8px 10px;border-bottom:1px solid #f0efe8;font-size:12px;white-space:nowrap">{fp(item.get("price"))}</td><td style="padding:8px 10px;border-bottom:1px solid #f0efe8;font-size:12px">{fc(item.get("cap"))}</td><td style="padding:8px 10px;border-bottom:1px solid #f0efe8;font-size:12px">{str(item["sqft"])+" SF" if item.get("sqft") else "—"}</td><td style="padding:8px 10px;border-bottom:1px solid #f0efe8;font-size:12px"><a href="{item.get("url","#")}" style="color:#185FA5">Listing</a> &nbsp; <a href="{maps}" style="color:#185FA5">Map</a></td></tr>'
    sc = "#0F6E56" if new_l else "#888780"
    st = f"{len(new_l)} new listing(s) found" if new_l else "No new listings today"
    if drops: st += f" · {len(drops)} price drop(s)"
    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f5f3;margin:0;padding:20px"><div style="max-width:860px;margin:0 auto"><div style="background:#fff;border-radius:10px;padding:20px 24px;border:1px solid #e0dfd8;margin-bottom:16px"><h1 style="font-size:16px;font-weight:500;margin:0 0 6px">RE Bot — Strip Mall Tracker · 29609</h1><div style="font-size:11px;color:#888780;margin-bottom:10px">{today} · Sources: {", ".join(sources)} · $1M–$5M · 200mi · SC/NC/GA/TN</div><p style="font-size:13px;color:{sc};margin:0;font-weight:500">{st}</p></div><div style="background:#fff;border-radius:10px;border:1px solid #e0dfd8;overflow:hidden"><table style="width:100%;border-collapse:collapse"><thead><tr style="background:#f8f7f2"><th style="padding:9px 10px;text-align:left;font-size:11px;font-weight:500;color:#888780;border-bottom:1px solid #e8e7df">Property</th><th style="padding:9px 10px;text-align:left;font-size:11px;font-weight:500;color:#888780;border-bottom:1px solid #e8e7df">Dist</th><th style="padding:9px 10px;text-align:left;font-size:11px;font-weight:500;color:#888780;border-bottom:1px solid #e8e7df">Price</th><th style="padding:9px 10px;text-align:left;font-size:11px;font-weight:500;color:#888780;border-bottom:1px solid #e8e7df">CAP</th><th style="padding:9px 10px;text-align:left;font-size:11px;font-weight:500;color:#888780;border-bottom:1px solid #e8e7df">Size</th><th style="padding:9px 10px;text-align:left;font-size:11px;font-weight:500;color:#888780;border-bottom:1px solid #e8e7df">Links</th></tr></thead><tbody>{rows}</tbody></table></div><p style="font-size:11px;color:#888780;margin-top:14px;text-align:center">RE Bot · CityFeet · CommercialSearch · Showcase · BizBuySell · LoopNet · SVN · Marcus &amp; Millichap · NAI · Matthews · Bull Realty · Sands · Wilson Kibler · Reedy</p></div></body></html>"""

def send_email(subject, html_body):
    if not SENDER_EMAIL or not SENDER_PASSWORD:
        print("  [WARN] No email credentials — skipping.")
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
        print(f"  [ERROR] Email: {e}")

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
        name  = f"{item.get('address','Unknown')} — {item.get('city','')}, {item.get('state','')}"
        note  = f"Auto-scraped {item.get('source','')} — FOR SALE {fp(item.get('price'))}"
        if flag == "drop":
            note += f" (was {fp(item.get('prev_price'))})"
        a = item.get("address","").replace('"','\\"')
        c = item.get("city","").replace('"','\\"')
        u = item.get("url","").replace('"','\\"')
        b = item.get("source","").replace('"','\\"')
        entries.append(f'  {{f:"{flag}",id:"{lid}",n:"{name}",a:"{a}",c:"{c}",s:"{item.get("state","")}",d:{item.get("dist",999)},p:{item.get("price",0)},sq:{sq_v},cap:{cap_v},cE:0,nnn:null,nE:1,o:null,aadt:null,ar:"",u:"{u}",om:null,b:"{b}",nt:"{note}",first:"{first}"}}')
    new_seed = "const SEED = [\n" + ",\n".join(entries) + "\n];"
    html = HTML_TRACKER_FILE.read_text(encoding="utf-8")
    html = re.sub(r"const SEED = \[[\s\S]*?\];", new_seed, html)
    html = re.sub(r'id="b-run">[^<]*<', f'id="b-run">Last run: {datetime.date.today().strftime("%b %d, %Y")}<', html)
    html = re.sub(r'Last updated: <span id="last-updated">[^<]*<', f'Last updated: <span id="last-updated">{datetime.date.today().strftime("%b %d, %Y")}<', html)
    HTML_TRACKER_FILE.write_text(html, encoding="utf-8")
    print(f"  HTML updated — {len(current)} listings.")

# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*60}\nRE Bot — {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*60}")
    session = requests.Session()
    session.headers.update(HEADERS)

    print("\n[1] Scraping all sources...")
    raw = scrape_all(session)
    print(f"  Total raw: {len(raw)}")

    print("\n[2] Filtering + dedup...")
    filtered = filter_listings(raw)
    current  = dedup(filtered)
    for item in current:
        item["id"] = make_id(item.get("address",""), item.get("city",""))
    print(f"  After filter+dedup: {len(current)}")

    print("\n[3] Comparing...")
    known = load_known()
    new_l, drops, active_ids = compare(current, known)
    print(f"  New: {len(new_l)} | Drops: {len(drops)}")
    for i in new_l:  print(f"    NEW:  {i.get('address')} {i.get('city')},{i.get('state')} {fp(i.get('price'))} [{i.get('source')}]")
    for i in drops:  print(f"    DROP: {i.get('address')} {fp(i.get('prev_price'))}→{fp(i.get('price'))}")

    save_known(update_known(known, current, active_ids))

    print("\n[4] Updating HTML tracker...")
    update_html(current, {i["id"] for i in new_l}, {i["id"] for i in drops})

    today_s = datetime.date.today().strftime("%B %d, %Y")
    subject = (f"RE Bot · {len(new_l)} NEW listing(s) · {today_s}" if new_l else
               f"RE Bot · {len(drops)} price drop(s) · {today_s}" if drops else
               f"RE Bot · Daily update · {today_s} · {len(current)} listings")
    print(f"\n[5] Sending: {subject}")
    send_email(subject, build_email(current, new_l, drops))

    print(f"\n{'='*60}\nDone — {len(current)} listings tracked.\n{'='*60}\n")

if __name__ == "__main__":
    main()
