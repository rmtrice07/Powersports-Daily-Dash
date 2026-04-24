#!/usr/bin/env python3
"""
Daily refresh script for UTV / Powersports Intelligence Dashboard.
Fetches live stock prices and RSS news, then injects into index.html.
Run automatically via GitHub Actions or manually: python refresh.py
"""

import re
import time
import html as html_lib
import json
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import quote_plus

try:
    import yfinance as yf
    HAS_YF = True
except ImportError:
    HAS_YF = False
    print("WARNING: yfinance not installed — stock data skipped")

try:
    import feedparser
    HAS_FP = True
except ImportError:
    HAS_FP = False
    print("WARNING: feedparser not installed — RSS feeds skipped")

# ── Paths ─────────────────────────────────────────────────────────────────────

ROOT         = Path(__file__).parent
DASHBOARD    = ROOT / "index.html"
MA_DATA      = ROOT / "ma_data.json"
HISTORY_FILE = ROOT / "news_history.json"

# ── Stock Tickers ─────────────────────────────────────────────────────────────
# (yfinance_symbol, display_symbol, name, exchange, hex_color, css_tag_class)

TICKERS = [
    ("PII",     "PII",    "Polaris",      "NYSE", "#93c5fd", "tag-polaris",  "polaris"),
    ("DOOO.TO", "DOOO",   "BRP/Can-Am",   "TSX",  "#fca5a5", "tag-canam",    "canam"),
    ("DE",      "DE",     "John Deere",   "NYSE", "#86efac", "tag-deere",    "deere"),
    ("HMC",     "HMC",    "Honda",        "NYSE", "#fca5a5", "tag-honda",    "honda"),
    ("7272.T",  "7272.T", "Yamaha Motor", "TYO",  "#93c5fd", "tag-yamaha",   "yamaha"),
    ("6326.T",  "6326.T", "Kubota",       "TYO",  "#fca5a5", "tag-kubota",   "kubota"),
    ("7012.T",  "7012.T", "Kawasaki HI",  "TYO",  "#86efac", "tag-kawasaki", "kawasaki"),
]

# ── RSS Feeds ─────────────────────────────────────────────────────────────────

DIRECT_FEEDS = [
    {
        "url":   "https://powersportsbusiness.com/category/news/feed/",
        "label": "Powersports Business",
        "badge": "trade",
    },
    {
        "url":   "https://www.utvdriver.com/feed/",
        "label": "UTV Driver",
        "badge": "editorial",
    },
]

# (google_query, oem_key, oem_display, css_tag_class)
GNEWS_QUERIES = [
    ('Polaris RZR OR "Polaris Ranger" OR "Polaris XPEDITION"', "polaris",  "Polaris",   "tag-polaris"),
    ('"Can-Am" Maverick OR Defender OR Commander',             "canam",    "Can-Am",    "tag-canam"),
    ('Kawasaki Teryx OR "Kawasaki Mule" OR "Kawasaki RIDGE"', "kawasaki", "Kawasaki",  "tag-kawasaki"),
    ('Yamaha RMAX OR "Yamaha Wolverine" OR "Yamaha Viking"',  "yamaha",   "Yamaha",    "tag-yamaha"),
    ('CFMOTO ZFORCE OR UFORCE',                               "cfmoto",   "CFMOTO",    "tag-cfmoto"),
    ('"Speed UTV" OR "El Jefe UTV"',                          "speedutv", "Speed UTV", "tag-speedutv"),
    ('"John Deere" Gator utility vehicle',                    "deere",    "John Deere","tag-deere"),
    ('Kubota RTV utility vehicle',                            "kubota",   "Kubota",    "tag-kubota"),
    ('"Massimo Motor" OR "Massimo UTV"',                      "massimo",  "Massimo",   "tag-massimo"),
    ('Honda Pioneer OR "Honda Talon" side-by-side',           "honda",    "Honda",     "tag-honda"),
    ('UTV acquisition OR powersports merger OR "powersports" investment', "market", "M&A",        "tag-market"),
    ('UTV NHTSA OR powersports tariff OR "off-road vehicle" regulation',  "market", "Regulatory", "tag-reg"),
]

# ── Classification Maps ───────────────────────────────────────────────────────

OEM_KEYWORDS = {
    "polaris":  ["polaris", "rzr", "polaris ranger", "polaris general", "xpedition"],
    "canam":    ["can-am", "canam", "brp", "maverick r", "maverick x3", "can-am defender", "brp defender", "can-am commander"],
    "kawasaki": ["kawasaki", "teryx", "kawasaki mule", "kawasaki ridge"],
    "yamaha":   ["yamaha", "rmax", "yamaha wolverine", "yamaha viking", "yxz"],
    "cfmoto":   ["cfmoto", "cf moto", "zforce", "uforce"],
    "speedutv": ["speed utv", "el jefe"],
    "deere":    ["john deere", "deere gator", "gator utv"],
    "kubota":   ["kubota", "kubota rtv"],
    "massimo":  ["massimo"],
    "honda":    ["honda pioneer", "honda talon"],
}

CAT_KEYWORDS = {
    "financial":  ["earnings", "revenue", "profit", "stock", "guidance", "quarterly",
                   "fiscal", "acquisition", "merger", "invest"],
    "product":    ["launch", "reveal", "new model", "announces", "specs", "price",
                   "msrp", "horsepower", "engine", "debut"],
    "strategy":   ["strategy", "partnership", "dealer", "expansion", "market share",
                   "compete", "distribution"],
    "sentiment":  ["review", "test drive", "owner", "love", "hate", "problems",
                   "issues", "complaint", "opinion"],
    "regulatory": ["nhtsa", "epa", "regulation", "tariff", "safety", "recall",
                   "compliance", "trail access", "blm"],
    "dealer":     ["dealer", "retail", "floor plan", "inventory", "financing",
                   "apr", "incentive"],
}

OEM_TAG_MAP = {
    "polaris":  ("Polaris",   "tag-polaris"),
    "canam":    ("Can-Am",    "tag-canam"),
    "kawasaki": ("Kawasaki",  "tag-kawasaki"),
    "yamaha":   ("Yamaha",    "tag-yamaha"),
    "cfmoto":   ("CFMOTO",    "tag-cfmoto"),
    "speedutv": ("Speed UTV", "tag-speedutv"),
    "deere":    ("John Deere","tag-deere"),
    "kubota":   ("Kubota",    "tag-kubota"),
    "massimo":  ("Massimo",   "tag-massimo"),
    "honda":    ("Honda",     "tag-honda"),
    "market":   ("Market",    "tag-market"),
}

BADGE_STYLES = {
    "trade":    ("rgba(6,182,212,0.15)",    "var(--cyan)",       "PSB"),
    "editorial":("rgba(168,85,247,0.15)",   "var(--purple)",     "UTV Driver"),
    "news":     ("rgba(139,148,158,0.15)",  "var(--text-muted)", "News"),
}

MA_TYPE_STYLES = {
    "Acquisition":      ("rgba(239,68,68,0.15)",   "var(--red)"),
    "Investment":       ("rgba(59,130,246,0.15)",   "var(--accent2)"),
    "Manufacturing JV": ("rgba(34,197,94,0.15)",    "var(--green)"),
    "R&D Consortium":   ("rgba(234,179,8,0.15)",    "var(--yellow)"),
    "Partnership":      ("rgba(168,85,247,0.15)",   "var(--purple)"),
}

# ── SEC EDGAR ─────────────────────────────────────────────────────────────────
# CIKs verified against sec.gov search April 2026

SEC_FILERS = [
    # (padded_cik, display_name, oem_key)
    ("0000931015", "Polaris Inc.",    "polaris"),
    ("0000315189", "Deere & Company", "deere"),
    ("0000715153", "Honda Motor Co.", "honda"),
]

SEC_NO_EDGAR = [
    # (display_name, exchange_note, oem_key)
    ("BRP Inc.",         "SEDAR · TSX",  "canam"),
    ("Yamaha Motor Co.", "TDnet · TSE",  "yamaha"),
]

SEC_USER_AGENT = "Powersports Dashboard rich.macauleyiii@gmail.com"

SEC_EXCLUDED_FORMS = {
    "4", "4/A", "3", "3/A", "5", "5/A",
    "SC 13G", "SC 13G/A", "SC 13D", "SC 13D/A",
    "8-A12B", "8-A12G", "S-8", "S-8 POS",
    "FWP", "424B2", "424B3", "424B5",
    "DEFA14A", "ARS", "SD", "CORRESP", "UPLOAD",
}

FORM_COLORS = {
    "10-K":    ("rgba(59,130,246,0.15)",  "var(--accent2)"),
    "10-Q":    ("rgba(6,182,212,0.15)",   "var(--cyan)"),
    "8-K":     ("rgba(234,179,8,0.15)",   "var(--yellow)"),
    "DEF 14A": ("rgba(168,85,247,0.15)", "var(--purple)"),
    "20-F":    ("rgba(59,130,246,0.15)",  "var(--accent2)"),
    "6-K":     ("rgba(34,197,94,0.15)",   "var(--green)"),
}

# ── News Volume Chart ─────────────────────────────────────────────────────────

OEM_BAR_COLORS = {
    "polaris":  "#93c5fd",
    "canam":    "#fca5a5",
    "kawasaki": "#86efac",
    "yamaha":   "#93c5fd",
    "cfmoto":   "#c4b5fd",
    "speedutv": "#fdba74",
    "deere":    "#86efac",
    "kubota":   "#fca5a5",
    "massimo":  "#bfdbfe",
    "honda":    "#fca5a5",
}

OEM_CHART_ORDER = [
    "polaris", "canam", "kawasaki", "yamaha", "cfmoto",
    "speedutv", "deere", "kubota", "massimo", "honda",
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def strip_html(text):
    return re.sub(r"<[^>]+>", " ", text or "").strip()

def relative_time(dt):
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = now - dt
    if delta.total_seconds() < 0:
        return "just now"
    if delta.days > 6:
        return dt.strftime("%b %-d")
    if delta.days >= 1:
        return f"{delta.days}d ago"
    hours = delta.seconds // 3600
    if hours >= 1:
        return f"{hours}h ago"
    mins = delta.seconds // 60
    return f"{mins}m ago" if mins > 0 else "just now"

def struct_to_dt(struct):
    if struct is None:
        return datetime.now(timezone.utc)
    try:
        return datetime(*struct[:6], tzinfo=timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)

def detect_oem(text):
    t = text.lower()
    for oem, kws in OEM_KEYWORDS.items():
        if any(kw in t for kw in kws):
            return oem
    return "market"

def detect_cat(text):
    t = text.lower()
    for cat, kws in CAT_KEYWORDS.items():
        if any(kw in t for kw in kws):
            return cat
    return "product"

def truncate(text, n=200):
    text = " ".join(strip_html(text).split())
    return text[:n].rsplit(" ", 1)[0] + "…" if len(text) > n else text

def fmt_price(symbol, value):
    if value is None:
        return "N/A"
    if ".T" in symbol and "DOOO" not in symbol:
        return f"¥{value:,.0f}"
    if "DOOO" in symbol:
        return f"C${value:,.2f}"
    return f"${value:,.2f}"

# ── Fetchers ──────────────────────────────────────────────────────────────────

def fetch_stocks():
    if not HAS_YF:
        return []
    rows = []
    for sym, disp, name, exch, color, tag_cls, oem_key in TICKERS:
        try:
            fi    = yf.Ticker(sym).fast_info
            price = fi.last_price
            prev  = fi.previous_close
            if price is None or prev is None:
                raise ValueError("missing price data")
            change = price - prev
            pct    = (change / prev) * 100
            rows.append({
                "sym":     disp,
                "name":    name,
                "exch":    exch,
                "oem_key": oem_key,
                "price":   fmt_price(sym, price),
                "chg":     fmt_price(sym, abs(change)),
                "pct":     abs(pct),
                "range":   f"{fmt_price(sym, fi.year_low)}–{fmt_price(sym, fi.year_high)}",
                "color":   color,
                "dir":     "up" if change >= 0 else "down",
                "arrow":   "▲" if change >= 0 else "▼",
            })
            time.sleep(0.4)
        except Exception as e:
            print(f"  ✗ {sym}: {e}")
    return rows

def fetch_direct_feeds(max_per=12):
    if not HAS_FP:
        return []
    articles = []
    for cfg in DIRECT_FEEDS:
        try:
            print(f"  Fetching {cfg['label']}...")
            d = feedparser.parse(cfg["url"])
            for e in d.entries[:max_per]:
                title   = strip_html(e.get("title", ""))
                summary = e.get("summary") or e.get("description", "")
                link    = e.get("link", "#")
                dt      = struct_to_dt(e.get("published_parsed"))
                body    = f"{title} {summary}"
                articles.append({
                    "title":   title,
                    "snippet": truncate(summary),
                    "link":    link,
                    "dt":      dt,
                    "oem":     detect_oem(body),
                    "cat":     detect_cat(body),
                    "label":   cfg["label"],
                    "badge":   cfg["badge"],
                })
        except Exception as e:
            print(f"  ✗ {cfg['label']}: {e}")
    return articles

def fetch_gnews(max_per=5):
    if not HAS_FP:
        return []
    articles, seen = [], set()
    for query, oem_key, _, _ in GNEWS_QUERIES:
        try:
            url = (f"https://news.google.com/rss/search"
                   f"?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en")
            d, n = feedparser.parse(url), 0
            for e in d.entries:
                if n >= max_per:
                    break
                title = strip_html(e.get("title", ""))
                # Strip trailing " - Source Name" Google appends
                title = re.sub(r"\s*[-–]\s*\S[^-–]{0,40}$", "", title).strip()
                key = title[:55]
                if not title or key in seen:
                    continue
                seen.add(key)
                summary = e.get("summary") or e.get("description", "")
                body    = f"{title} {summary}"
                # Drop articles from OEM-specific queries that don't actually
                # mention any of that OEM's keywords — catches Google's imprecise
                # boolean matching which returns off-topic results.
                if oem_key != "market" and OEM_KEYWORDS.get(oem_key):
                    if not any(kw in body.lower() for kw in OEM_KEYWORDS[oem_key]):
                        continue
                articles.append({
                    "title":   title,
                    "snippet": truncate(summary),
                    "link":    e.get("link", "#"),
                    "dt":      struct_to_dt(e.get("published_parsed")),
                    "oem":     oem_key,
                    "cat":     detect_cat(body),
                    "label":   "Google News",
                    "badge":   "news",
                })
                n += 1
            time.sleep(1.2)
        except Exception as e:
            print(f"  ✗ GNews '{query[:40]}…': {e}")
    return articles

# ── HTML Builders ─────────────────────────────────────────────────────────────

def build_sidebar_tickers(stocks):
    if not stocks:
        return "<script>const SIDEBAR_PRICES = {};</script>"
    entries = []
    for s in stocks:
        entries.append(
            f'  "{s["oem_key"]}": {{'
            f'"exch":"{s["exch"]}", "sym":"{s["sym"]}", "price":"{s["price"]}", '
            f'"arrow":"{s["arrow"]}", "pct":"{s["pct"]:.1f}", "dir":"{s["dir"]}"'
            f'}}'
        )
    inner = ",\n".join(entries)
    return f"<script>\nconst SIDEBAR_PRICES = {{\n{inner}\n}};\n</script>"

def build_stock_rows(stocks):
    if not stocks:
        return ('      <tr><td colspan="4" style="color:var(--text-muted);'
                'text-align:center;padding:16px;font-size:12px;">'
                'Stock data unavailable — check network</td></tr>')
    parts = []
    for s in stocks:
        c = "var(--green)" if s["dir"] == "up" else "var(--red)"
        parts.append(
            f'      <tr>\n'
            f'        <td><span class="ticker-cell" style="color:{s["color"]};">{s["sym"]}</span>'
            f'<br><span style="font-size:10px;color:var(--text-muted);">{s["name"]}</span></td>\n'
            f'        <td class="price-cell">{s["price"]}</td>\n'
            f'        <td class="change-cell" style="color:{c};">{s["arrow"]}{s["chg"]}'
            f'<br><span style="font-size:10px;">{s["arrow"]}{s["pct"]:.1f}%</span></td>\n'
            f'        <td style="font-size:10px;color:var(--text-muted);">{s["range"]}</td>\n'
            f'      </tr>'
        )
    return "\n".join(parts)

def build_news_cards(articles, max_items=35):
    if not articles:
        return ('      <div style="color:var(--text-muted);padding:20px;'
                'text-align:center;font-size:12px;">'
                'No articles fetched — check RSS feeds or network.</div>')
    seen, unique = set(), []
    for a in sorted(articles, key=lambda x: x["dt"], reverse=True):
        k = a["title"][:55]
        if k not in seen:
            seen.add(k)
            unique.append(a)
    parts = []
    for a in unique[:max_items]:
        oem_label, tag_cls = OEM_TAG_MAP.get(a["oem"], ("Market", "tag-market"))
        bg, fg, badge_text = BADGE_STYLES.get(a["badge"], BADGE_STYLES["news"])
        rel     = relative_time(a["dt"])
        snippet = html_lib.escape(truncate(a["snippet"], 180))
        parts.append(
            f'      <div class="news-card" data-cat="{a["cat"]}" data-oem="{a["oem"]}">\n'
            f'        <div class="news-meta">\n'
            f'          <span class="news-oem-tag {tag_cls}">{html_lib.escape(oem_label)}</span>\n'
            f'          <span style="background:{bg};color:{fg};font-size:10px;font-weight:700;'
            f'padding:1px 5px;border-radius:3px;">{badge_text}</span>\n'
            f'          <span class="news-cat">{a["cat"].title()}</span>\n'
            f'          <span class="news-time">{rel}</span>\n'
            f'        </div>\n'
            f'        <div class="news-headline">'
            f'<a href="{html_lib.escape(a["link"])}" target="_blank" rel="noopener" '
            f'style="color:inherit;text-decoration:none;">'
            f'{html_lib.escape(a["title"])}</a></div>\n'
            f'        <div class="news-snippet">{snippet}</div>\n'
            f'      </div>'
        )
    return "\n".join(parts)

def build_ma_section():
    if not MA_DATA.exists():
        return ('      <div style="color:var(--text-muted);font-size:12px;">'
                'ma_data.json not found.</div>')
    data  = json.loads(MA_DATA.read_text(encoding="utf-8"))
    parts = []

    for deal in data.get("deals", []):
        acq_key  = deal.get("acquirer_key", "")
        _, tag   = OEM_TAG_MAP.get(acq_key, ("", "tag-market"))
        bg, fg   = MA_TYPE_STYLES.get(deal.get("type", ""), ("rgba(139,148,158,0.15)", "var(--text-muted)"))
        val_str  = (f'&nbsp;·&nbsp;{html_lib.escape(deal["value"])}'
                    if deal.get("value") and deal["value"] != "Undisclosed" else "")
        parts.append(
            f'      <div class="ma-item">\n'
            f'        <div class="ma-header">\n'
            f'          <span class="news-oem-tag {tag}">{html_lib.escape(deal["acquirer"])}</span>\n'
            f'          <span style="background:{bg};color:{fg};font-size:10px;font-weight:700;'
            f'padding:1px 6px;border-radius:3px;">{html_lib.escape(deal["type"])}</span>\n'
            f'          <span class="ma-date">{html_lib.escape(deal["date"])}{val_str}</span>\n'
            f'        </div>\n'
            f'        <div class="ma-target">→ {html_lib.escape(deal["target"])}</div>\n'
            f'        <div class="ma-intent">{html_lib.escape(deal["intent"])}</div>\n'
            f'      </div>'
        )

    for p in data.get("partnerships", []):
        keys    = p.get("party_keys", [])
        names   = p.get("parties", [])
        tags_html = " &amp; ".join(
            (f'<span class="news-oem-tag {OEM_TAG_MAP[k][1]}">{html_lib.escape(n)}</span>'
             if k and k in OEM_TAG_MAP else html_lib.escape(n))
            for k, n in zip(keys, names)
        )
        bg, fg = MA_TYPE_STYLES.get(p.get("type", ""), ("rgba(139,148,158,0.15)", "var(--text-muted)"))
        sc     = "var(--green)" if p.get("status") == "Active" else "var(--text-muted)"
        parts.append(
            f'      <div class="ma-item">\n'
            f'        <div class="ma-header">\n'
            f'          {tags_html}\n'
            f'          <span style="background:{bg};color:{fg};font-size:10px;font-weight:700;'
            f'padding:1px 6px;border-radius:3px;">{html_lib.escape(p["type"])}</span>\n'
            f'          <span class="ma-date">{html_lib.escape(p["date"])}</span>\n'
            f'          <span style="font-size:10px;color:{sc};font-weight:600;">'
            f'{html_lib.escape(p.get("status",""))}</span>\n'
            f'        </div>\n'
            f'        <div class="ma-intent">{html_lib.escape(p["intent"])}</div>\n'
            f'      </div>'
        )
    return "\n".join(parts)

def fetch_sec_filings(days_back=90, max_per_cik=10, top_n=15):
    results = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    for padded_cik, display_name, oem_key in SEC_FILERS:
        url = f"https://data.sec.gov/submissions/CIK{padded_cik}.json"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": SEC_USER_AGENT})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            recent       = data.get("filings", {}).get("recent", {})
            forms        = recent.get("form", [])
            dates        = recent.get("filingDate", [])
            accnos       = recent.get("accessionNumber", [])
            descriptions = recent.get("primaryDocDescription", [])
            count = 0
            for form, date_str, accno, desc in zip(forms, dates, accnos, descriptions):
                if count >= max_per_cik:
                    break
                if form in SEC_EXCLUDED_FORMS:
                    continue
                try:
                    filing_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
                if filing_dt < cutoff:
                    break  # EDGAR returns newest-first
                cik_int        = int(padded_cik)
                accno_nodashes = accno.replace("-", "")
                filing_url = (
                    f"https://www.sec.gov/Archives/edgar/data/{cik_int}/"
                    f"{accno_nodashes}/{accno}-index.htm"
                )
                results.append({
                    "oem_key":  oem_key,
                    "company":  display_name,
                    "form":     form,
                    "date":     filing_dt,
                    "date_str": filing_dt.strftime("%b %-d, %Y"),
                    "desc":     (desc or "").strip(),
                    "url":      filing_url,
                })
                count += 1
            time.sleep(0.2)
            print(f"  ✓ {display_name}: {count} filings in window")
        except Exception as e:
            print(f"  ✗ {display_name}: {e}")
    results.sort(key=lambda x: x["date"], reverse=True)
    return results[:top_n]

def build_sec_section(filings):
    parts = []
    if not filings:
        parts.append(
            '      <div style="color:var(--text-muted);font-size:12px;padding:16px;'
            'text-align:center;border:1px dashed var(--border);border-radius:var(--radius);">'
            'SEC filings unavailable — check connection or run refresh again.</div>'
        )
    else:
        for f in filings:
            _, tag_cls = OEM_TAG_MAP.get(f["oem_key"], ("Market", "tag-market"))
            bg, fg     = FORM_COLORS.get(f["form"], ("rgba(139,148,158,0.15)", "var(--text-muted)"))
            desc_part  = (f' <span style="color:var(--text-muted);">· {html_lib.escape(f["desc"])}</span>'
                          if f["desc"] else "")
            parts.append(
                f'      <div class="sec-item">\n'
                f'        <div class="sec-header">\n'
                f'          <span class="news-oem-tag {tag_cls}">{html_lib.escape(f["company"])}</span>\n'
                f'          <span style="background:{bg};color:{fg};font-size:10px;font-weight:700;'
                f'padding:1px 6px;border-radius:3px;">{html_lib.escape(f["form"])}</span>\n'
                f'          <span class="sec-date">{f["date_str"]}{desc_part}</span>\n'
                f'          <a href="{html_lib.escape(f["url"])}" target="_blank" rel="noopener" '
                f'class="sec-link">View →</a>\n'
                f'        </div>\n'
                f'      </div>'
            )
    if SEC_NO_EDGAR:
        rows = []
        for name, exchange, oem_key in SEC_NO_EDGAR:
            _, tag_cls = OEM_TAG_MAP.get(oem_key, ("", "tag-market"))
            rows.append(
                f'        <div style="display:flex;align-items:center;gap:6px;padding:4px 0;">\n'
                f'          <span class="news-oem-tag {tag_cls}">{html_lib.escape(name)}</span>\n'
                f'          <span style="font-size:11px;color:var(--text-muted);">'
                f'Files on {html_lib.escape(exchange)} — not on EDGAR</span>\n'
                f'        </div>'
            )
        parts.append(
            '      <div style="margin-top:8px;padding-top:8px;'
            'border-top:1px solid rgba(48,54,61,0.5);">\n'
            + "\n".join(rows) + '\n      </div>'
        )
    return "\n".join(parts)

def update_news_history(articles):
    now           = datetime.now(timezone.utc)
    prune_cutoff  = (now - timedelta(days=14)).strftime("%Y-%m-%d")
    if HISTORY_FILE.exists():
        try:
            stored  = json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
            history = stored.get("articles", [])
        except Exception:
            history = []
    else:
        history = []
    existing_keys = {a["title"][:55] for a in history}
    for a in articles:
        key = a["title"][:55]
        if key not in existing_keys and a["title"]:
            history.append({
                "title":   a["title"][:200],
                "link":    a.get("link", ""),
                "date":    a["dt"].strftime("%Y-%m-%d"),
                "oem_key": a["oem"],
            })
            existing_keys.add(key)
    history      = [a for a in history if a.get("date", "") >= prune_cutoff]
    window_start = min((a["date"] for a in history), default=now.strftime("%Y-%m-%d"))
    HISTORY_FILE.write_text(
        json.dumps({"_window_start": window_start, "articles": history},
                   indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return history

def build_news_volume_chart(history=None):
    now          = datetime.now(timezone.utc)
    seven_ago    = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    window_start = seven_ago
    if history is None:
        if not HISTORY_FILE.exists():
            return ('      <div style="color:var(--text-muted);font-size:12px;padding:12px;'
                    'text-align:center;">Chart populates after first week of refresh runs.</div>')
        try:
            stored       = json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
            history      = stored.get("articles", [])
            window_start = stored.get("_window_start", seven_ago)
        except Exception:
            history = []
    else:
        window_start = min((a["date"] for a in history), default=seven_ago)
    counts = {oem: 0 for oem in OEM_CHART_ORDER}
    for a in history:
        if a.get("date", "") >= seven_ago:
            oem = a.get("oem_key", "market")
            if oem in counts:
                counts[oem] += 1
    sorted_oems = sorted(OEM_CHART_ORDER, key=lambda o: (-counts[o], OEM_CHART_ORDER.index(o)))
    max_count   = max(counts.values(), default=1) or 1
    parts = []
    for oem in sorted_oems:
        count   = counts[oem]
        label, _= OEM_TAG_MAP.get(oem, (oem.title(), ""))
        color   = OEM_BAR_COLORS.get(oem, "#8b949e")
        bar_pct = (count / max_count) * 100
        parts.append(
            f'      <div style="display:flex;align-items:center;gap:8px;margin-bottom:7px;">\n'
            f'        <div style="width:68px;flex-shrink:0;font-size:11px;font-weight:600;'
            f'color:var(--text);text-align:right;white-space:nowrap;">{html_lib.escape(label)}</div>\n'
            f'        <div style="flex:1;background:var(--border);border-radius:3px;'
            f'height:7px;overflow:hidden;">\n'
            f'          <div style="width:{bar_pct:.1f}%;height:100%;background:{color};'
            f'border-radius:3px;"></div>\n'
            f'        </div>\n'
            f'        <div style="width:20px;flex-shrink:0;font-size:11px;font-weight:700;'
            f'color:{color};text-align:right;">{count}</div>\n'
            f'      </div>'
        )
    try:
        ws_dt          = datetime.strptime(window_start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        days_collected = max(0, (now - ws_dt).days + 1)
        ws_fmt         = ws_dt.strftime("%b %-d, %Y")
    except Exception:
        days_collected = 7
        ws_fmt         = window_start
    if days_collected < 7:
        parts.append(
            f'      <div style="font-size:10px;color:var(--text-muted);font-style:italic;margin-top:4px;">'
            f'Showing {days_collected} day{"s" if days_collected != 1 else ""} of data · '
            f'7-day window begins {ws_fmt}</div>'
        )
    return "\n".join(parts)

def build_timestamp():
    et = datetime.now(timezone.utc) - timedelta(hours=4)  # EDT; close enough year-round for daily brief
    return f'📅 {et.strftime("%a, %B %-d, %Y")} &nbsp;·&nbsp; Refreshed {et.strftime("%-I:%M %p ET")}'

# ── Inject & Write ────────────────────────────────────────────────────────────

def inject(html, marker, content):
    start   = f"<!-- REFRESH:{marker}:START -->"
    end     = f"<!-- REFRESH:{marker}:END -->"
    pattern = re.compile(
        re.escape(start) + r".*?" + re.escape(end), re.DOTALL
    )
    replacement = f"{start}\n{content}\n      {end}"
    result, n = pattern.subn(lambda _: replacement, html)
    if n == 0:
        print(f"  WARNING: marker REFRESH:{marker} not found in template")
    return result

def main():
    print("=" * 52)
    print("  Powersports Dashboard — Daily Refresh")
    print("=" * 52)

    print("\n[1/6] Fetching stock prices...")
    stocks = fetch_stocks()
    print(f"      {len(stocks)}/{len(TICKERS)} tickers OK")

    print("\n[2/6] Fetching direct RSS feeds...")
    direct = fetch_direct_feeds()
    print(f"      {len(direct)} articles")

    print("\n[3/6] Fetching Google News RSS...")
    gnews = fetch_gnews()
    print(f"      {len(gnews)} articles")

    all_articles = direct + gnews

    print("\n[4/6] Updating news history...")
    history = update_news_history(all_articles)
    print(f"      {len(history)} articles in rolling history")

    print("\n[5/6] Fetching SEC EDGAR filings...")
    filings = fetch_sec_filings()
    print(f"      {len(filings)} filings fetched")

    print(f"\n[6/6] Building dashboard ({len(all_articles)} articles)...")
    template = DASHBOARD.read_text(encoding="utf-8")
    out = template
    out = inject(out, "TIMESTAMP",       build_timestamp())
    out = inject(out, "STOCKS",          build_stock_rows(stocks))
    out = inject(out, "SIDEBAR_TICKERS", build_sidebar_tickers(stocks))
    out = inject(out, "NEWS",            build_news_cards(all_articles))
    out = inject(out, "MA_DEALS",        build_ma_section())
    out = inject(out, "SEC_FILINGS",     build_sec_section(filings))
    out = inject(out, "NEWS_VOLUME",     build_news_volume_chart(history))
    DASHBOARD.write_text(out, encoding="utf-8")

    print(f"      Written → {DASHBOARD.name}")
    print("\n✓ Done.\n")

if __name__ == "__main__":
    main()
