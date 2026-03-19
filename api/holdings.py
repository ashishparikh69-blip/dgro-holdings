# Vercel Python Serverless Function — /api/holdings
# Price/52W data from Stooq (no auth, no cloud IP blocking).
# Dividend yield from Yahoo Finance v7 via cookie+crumb session.
from http.server import BaseHTTPRequestHandler
import json, time, urllib.request, urllib.parse, http.cookiejar
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# DGRO Top 100 Holdings (source: iShares, approximate weights)
DGRO_HOLDINGS = [
    {"ticker": "AAPL",  "name": "Apple Inc",                     "weight": 3.18},
    {"ticker": "MSFT",  "name": "Microsoft Corp",                "weight": 3.05},
    {"ticker": "JPM",   "name": "JPMorgan Chase & Co",           "weight": 2.89},
    {"ticker": "ABBV",  "name": "AbbVie Inc",                    "weight": 2.72},
    {"ticker": "AVGO",  "name": "Broadcom Inc",                  "weight": 2.55},
    {"ticker": "HD",    "name": "Home Depot Inc",                "weight": 2.21},
    {"ticker": "JNJ",   "name": "Johnson & Johnson",             "weight": 2.15},
    {"ticker": "PG",    "name": "Procter & Gamble Co",           "weight": 1.98},
    {"ticker": "XOM",   "name": "Exxon Mobil Corp",              "weight": 1.85},
    {"ticker": "CVX",   "name": "Chevron Corp",                  "weight": 1.62},
    {"ticker": "MRK",   "name": "Merck & Co Inc",                "weight": 1.58},
    {"ticker": "PFE",   "name": "Pfizer Inc",                    "weight": 1.52},
    {"ticker": "CSCO",  "name": "Cisco Systems Inc",             "weight": 1.48},
    {"ticker": "KO",    "name": "Coca-Cola Co",                  "weight": 1.45},
    {"ticker": "PEP",   "name": "PepsiCo Inc",                   "weight": 1.42},
    {"ticker": "VZ",    "name": "Verizon Communications",        "weight": 1.38},
    {"ticker": "CMCSA", "name": "Comcast Corp",                  "weight": 1.35},
    {"ticker": "TXN",   "name": "Texas Instruments Inc",         "weight": 1.32},
    {"ticker": "PM",    "name": "Philip Morris International",   "weight": 1.28},
    {"ticker": "BMY",   "name": "Bristol-Myers Squibb",          "weight": 1.25},
    {"ticker": "UNP",   "name": "Union Pacific Corp",            "weight": 1.22},
    {"ticker": "QCOM",  "name": "Qualcomm Inc",                  "weight": 1.18},
    {"ticker": "RTX",   "name": "RTX Corp",                      "weight": 1.15},
    {"ticker": "LOW",   "name": "Lowe's Companies Inc",          "weight": 1.12},
    {"ticker": "MDT",   "name": "Medtronic PLC",                 "weight": 1.08},
    {"ticker": "MS",    "name": "Morgan Stanley",                "weight": 1.05},
    {"ticker": "BLK",   "name": "BlackRock Inc",                 "weight": 1.02},
    {"ticker": "SCHW",  "name": "Charles Schwab Corp",           "weight": 0.98},
    {"ticker": "C",     "name": "Citigroup Inc",                 "weight": 0.95},
    {"ticker": "CB",    "name": "Chubb Ltd",                     "weight": 0.92},
    {"ticker": "GS",    "name": "Goldman Sachs Group",           "weight": 0.87},
    {"ticker": "ADI",   "name": "Analog Devices Inc",            "weight": 0.85},
    {"ticker": "DE",    "name": "Deere & Co",                    "weight": 0.83},
    {"ticker": "SO",    "name": "Southern Company",              "weight": 0.81},
    {"ticker": "DUK",   "name": "Duke Energy Corp",              "weight": 0.79},
    {"ticker": "ITW",   "name": "Illinois Tool Works",           "weight": 0.77},
    {"ticker": "CI",    "name": "Cigna Group",                   "weight": 0.75},
    {"ticker": "USB",   "name": "U.S. Bancorp",                  "weight": 0.73},
    {"ticker": "PNC",   "name": "PNC Financial Services",        "weight": 0.71},
    {"ticker": "ADP",   "name": "Automatic Data Processing",     "weight": 0.69},
    {"ticker": "TGT",   "name": "Target Corp",                   "weight": 0.67},
    {"ticker": "MMM",   "name": "3M Company",                    "weight": 0.65},
    {"ticker": "EMR",   "name": "Emerson Electric Co",           "weight": 0.63},
    {"ticker": "FIS",   "name": "Fidelity National Info",        "weight": 0.61},
    {"ticker": "APD",   "name": "Air Products & Chemicals",      "weight": 0.59},
    {"ticker": "NSC",   "name": "Norfolk Southern Corp",         "weight": 0.57},
    {"ticker": "CME",   "name": "CME Group Inc",                 "weight": 0.56},
    {"ticker": "ICE",   "name": "Intercontinental Exchange",     "weight": 0.55},
    {"ticker": "EOG",   "name": "EOG Resources Inc",             "weight": 0.54},
    {"ticker": "CL",    "name": "Colgate-Palmolive Co",          "weight": 0.53},
    {"ticker": "WMB",   "name": "Williams Companies Inc",        "weight": 0.52},
    {"ticker": "F",     "name": "Ford Motor Co",                 "weight": 0.51},
    {"ticker": "GM",    "name": "General Motors Co",             "weight": 0.50},
    {"ticker": "MET",   "name": "MetLife Inc",                   "weight": 0.49},
    {"ticker": "PRU",   "name": "Prudential Financial",          "weight": 0.48},
    {"ticker": "AIG",   "name": "American Intl Group",           "weight": 0.47},
    {"ticker": "TRV",   "name": "Travelers Companies",           "weight": 0.46},
    {"ticker": "ALL",   "name": "Allstate Corp",                 "weight": 0.45},
    {"ticker": "D",     "name": "Dominion Energy Inc",           "weight": 0.44},
    {"ticker": "SRE",   "name": "Sempra",                        "weight": 0.43},
    {"ticker": "AEP",   "name": "American Electric Power",       "weight": 0.42},
    {"ticker": "WEC",   "name": "WEC Energy Group",              "weight": 0.41},
    {"ticker": "XEL",   "name": "Xcel Energy Inc",               "weight": 0.40},
    {"ticker": "ETR",   "name": "Entergy Corp",                  "weight": 0.39},
    {"ticker": "PPL",   "name": "PPL Corp",                      "weight": 0.38},
    {"ticker": "ED",    "name": "Consolidated Edison",           "weight": 0.37},
    {"ticker": "FITB",  "name": "Fifth Third Bancorp",           "weight": 0.36},
    {"ticker": "KEY",   "name": "KeyCorp",                       "weight": 0.35},
    {"ticker": "RF",    "name": "Regions Financial Corp",        "weight": 0.34},
    {"ticker": "CFG",   "name": "Citizens Financial Group",      "weight": 0.33},
    {"ticker": "HBAN",  "name": "Huntington Bancshares",         "weight": 0.32},
    {"ticker": "NTRS",  "name": "Northern Trust Corp",           "weight": 0.31},
    {"ticker": "STT",   "name": "State Street Corp",             "weight": 0.30},
    {"ticker": "IP",    "name": "International Paper",           "weight": 0.29},
    {"ticker": "NUE",   "name": "Nucor Corp",                    "weight": 0.28},
    {"ticker": "PAYX",  "name": "Paychex Inc",                   "weight": 0.27},
    {"ticker": "FAST",  "name": "Fastenal Co",                   "weight": 0.26},
    {"ticker": "GPC",   "name": "Genuine Parts Co",              "weight": 0.25},
    {"ticker": "OMC",   "name": "Omnicom Group Inc",             "weight": 0.24},
    {"ticker": "HPQ",   "name": "HP Inc",                        "weight": 0.23},
    {"ticker": "KMB",   "name": "Kimberly-Clark Corp",           "weight": 0.22},
    {"ticker": "SYY",   "name": "Sysco Corp",                    "weight": 0.21},
    {"ticker": "CAH",   "name": "Cardinal Health Inc",           "weight": 0.20},
    {"ticker": "TROW",  "name": "T. Rowe Price Group",           "weight": 0.19},
    {"ticker": "BEN",   "name": "Franklin Resources",            "weight": 0.18},
    {"ticker": "LEN",   "name": "Lennar Corp",                   "weight": 0.16},
    {"ticker": "DHI",   "name": "D.R. Horton Inc",               "weight": 0.15},
    {"ticker": "PHM",   "name": "PulteGroup Inc",                "weight": 0.14},
    {"ticker": "OKE",   "name": "ONEOK Inc",                     "weight": 0.13},
    {"ticker": "KMI",   "name": "Kinder Morgan Inc",             "weight": 0.12},
    {"ticker": "CINF",  "name": "Cincinnati Financial",          "weight": 0.11},
    {"ticker": "AMCR",  "name": "Amcor PLC",                     "weight": 0.10},
    {"ticker": "FNF",   "name": "Fidelity National Financial",   "weight": 0.09},
    {"ticker": "CMA",   "name": "Comerica Inc",                  "weight": 0.08},
    {"ticker": "ZION",  "name": "Zions Bancorporation",          "weight": 0.07},
    {"ticker": "OGN",   "name": "Organon & Co",                  "weight": 0.06},
    {"ticker": "UGI",   "name": "UGI Corp",                      "weight": 0.05},
    {"ticker": "FAF",   "name": "First American Financial",      "weight": 0.04},
]

# Module-level cache (reused across warm Vercel invocations)
_cache = {"data": None, "timestamp": 0}
CACHE_DURATION = 600  # 10 minutes


def fetch_stooq(ticker):
    """Fetch 1 year of daily closes from Stooq for a US-listed ticker."""
    d2 = date.today().strftime("%Y%m%d")
    d1 = (date.today() - timedelta(days=366)).strftime("%Y%m%d")
    stooq_sym = ticker.lower() + ".us"
    url = (
        f"https://stooq.com/q/d/l/?s={stooq_sym}&i=d"
        f"&d1={d1}&d2={d2}"
    )
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            text = resp.read().decode("utf-8").strip()

        lines = text.split("\n")
        if len(lines) < 2:
            print(f"{ticker}: no data rows")
            return ticker, None

        # CSV header: Date,Open,High,Low,Close,Volume
        closes = []
        for line in lines[1:]:
            parts = line.split(",")
            if len(parts) >= 5:
                try:
                    closes.append(float(parts[4]))
                except ValueError:
                    pass

        if not closes:
            print(f"{ticker}: could not parse closes")
            return ticker, None

        price  = closes[-1]
        low52  = min(closes)
        high52 = max(closes)
        return ticker, {
            "price":            round(price,  2),
            "fiftyTwoWeekLow":  round(low52,  2),
            "fiftyTwoWeekHigh": round(high52, 2),
        }
    except Exception as e:
        print(f"{ticker} stooq error: {e}")
        return ticker, None


def fetch_yields(tickers):
    """
    Fetch trailing dividend yields from Yahoo Finance v7 using a
    cookie+crumb session. Returns {ticker: yield_pct} or {} on failure.
    """
    try:
        jar    = http.cookiejar.CookieJar()
        opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(jar)
        )
        opener.addheaders = [
            ("User-Agent",
             "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
             "AppleWebKit/537.36 (KHTML, like Gecko) "
             "Chrome/122.0.0.0 Safari/537.36"),
            ("Accept", "text/html,application/json,*/*"),
            ("Accept-Language", "en-US,en;q=0.9"),
        ]

        # Step 1: prime the cookie jar
        opener.open("https://fc.yahoo.com/", timeout=10)

        # Step 2: get crumb
        r = opener.open(
            "https://query2.finance.yahoo.com/v1/test/getcrumb",
            timeout=10
        )
        crumb = r.read().decode().strip()
        if not crumb or len(crumb) > 20:
            print(f"Bad crumb: {crumb[:40]}")
            return {}
        print(f"Got crumb: {crumb}")

        # Step 3: batch quote request in chunks of 50
        yields = {}
        for i in range(0, len(tickers), 50):
            batch   = tickers[i:i + 50]
            params  = urllib.parse.urlencode({
                "symbols":   ",".join(batch),
                "fields":    "trailingAnnualDividendYield",
                "formatted": "false",
                "crumb":     crumb,
            })
            url = f"https://query2.finance.yahoo.com/v7/finance/quote?{params}"
            resp = opener.open(url, timeout=15)
            data = json.loads(resp.read().decode())
            for q in data.get("quoteResponse", {}).get("result", []):
                dy = q.get("trailingAnnualDividendYield")
                if dy is not None:
                    yields[q["symbol"]] = round(dy * 100, 2)
        print(f"Fetched yields for {len(yields)} tickers")
        return yields
    except Exception as e:
        print(f"yield fetch error: {e}")
        return {}


def get_holdings_data():
    now = time.time()
    if _cache["data"] and (now - _cache["timestamp"]) < CACHE_DURATION:
        return _cache["data"], _cache["timestamp"]

    tickers = [h["ticker"] for h in DGRO_HOLDINGS]
    quotes  = {}

    # 5 concurrent workers per batch + 0.5s pause = stays under Stooq rate limits
    # 20 batches × ~1s each ≈ 20s total, well within the 60s function timeout
    batch_size = 5
    for batch_start in range(0, len(tickers), batch_size):
        batch = tickers[batch_start:batch_start + batch_size]
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = {executor.submit(fetch_stooq, t): t for t in batch}
            for future in as_completed(futures):
                ticker, data = future.result()
                if data:
                    quotes[ticker] = data
        # Pause between batches to respect Stooq's rate limit
        if batch_start + batch_size < len(tickers):
            time.sleep(0.5)

    print(f"Fetched {len(quotes)}/{len(tickers)} tickers from Stooq")

    # Fetch yields in parallel with the last Stooq batch already done
    yields = fetch_yields(tickers)

    results = []
    for i, holding in enumerate(DGRO_HOLDINGS):
        ticker = holding["ticker"]
        q      = quotes.get(ticker, {})
        price  = q.get("price")
        low52  = q.get("fiftyTwoWeekLow")
        high52 = q.get("fiftyTwoWeekHigh")
        variance = (
            round((price - low52) / low52 * 100, 2)
            if price and low52 and low52 > 0
            else None
        )
        results.append({
            "rank":            i + 1,
            "ticker":          ticker,
            "name":            holding["name"],
            "weight":          holding["weight"],
            "price":           price,
            "yield":           yields.get(ticker),
            "fiftyTwoWeekLow": low52,
            "fiftyTwoWeekHigh":high52,
            "varianceFromLow": variance,
        })

    _cache["data"]      = results
    _cache["timestamp"] = now
    return results, now


class handler(BaseHTTPRequestHandler):
    """Vercel Python serverless handler."""

    def do_GET(self):
        try:
            data, timestamp = get_holdings_data()
            body = json.dumps({
                "holdings":    data,
                "lastUpdated": time.strftime(
                    "%Y-%m-%dT%H:%M:%S", time.gmtime(timestamp)
                ),
            }).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        except Exception as e:
            import traceback
            body = json.dumps({
                "error": str(e),
                "trace": traceback.format_exc()
            }).encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass  # suppress request logs
