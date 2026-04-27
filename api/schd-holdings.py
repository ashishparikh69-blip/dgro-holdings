# Vercel Python Serverless Function — /api/schd-holdings
# Prices:  Finnhub real-time (market hours), fallback to last close.
# 52W data: Yahoo Finance primary, Stooq fallback, cached 24 h.
# Dividend yields: hardcoded trailing 12-month, updated Apr 2026.
from http.server import BaseHTTPRequestHandler
import json, time, os, urllib.request, urllib.error
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

FINNHUB_TOKEN = os.environ.get("FINNHUB_TOKEN", "")

# Payout ratios (%) — updated Apr 2026
PAYOUT_RATIOS = {
    "CVX":  48.3, "COP":  23.1, "MRK":  48.1, "VZ":   61.8, "KO":   73.4,
    "TXN":  53.7, "PEP":  70.2, "AMGN": 57.2, "ABT":  57.8, "PG":   67.1,
    "UNH":  19.8, "QCOM": 35.2, "HD":   54.2, "LMT":  43.2, "BMY":  55.3,
    "ACN":  44.1, "MO":   76.2, "CMCSA":33.1, "EOG":  25.4, "ADP":  76.3,
    "BX":   87.1, "SLB":  24.3, "UPS":  70.1, "OKE":  73.2, "TGT":  50.1,
    "FAST": 69.1, "F":    47.8, "FITB": 38.2, "ADM":  52.3, "KMB":  73.2,
    "DVN":  25.3, "HSY":  65.1, "PAYX": 82.3, "CTRA": 28.2, "CINF": 43.8,
    "DRI":  51.8, "ARES": 87.1, "RF":   38.4, "GIS":  59.8, "TROW": 62.3,
    "SNA":  38.2, "BR":   56.1, "SWKS": 40.2, "PFG":  36.1, "APA":  15.1,
    "EWBC": 24.1, "AFG":  22.1, "IPG":  52.8, "BBY":  42.1, "WSO":  50.2,
    "FNF":  44.8, "CMA":  34.2, "ALV":  35.1, "MTN":  55.2, "RDN":  15.8,
    "FMC":  44.8, "NXST": 35.2, "COLB": 49.8, "OZK":  29.8, "RHI":  47.8,
    "WHR":  55.1, "MC":   64.8, "MSM":  59.8, "IBOC": 29.8, "FLO":  64.8,
    "FHI":  49.8, "WU":   74.8, "CATY": 39.8, "SIG":   9.8, "MUR":  21.8,
    "APAM": 84.8, "CVBF": 59.8, "NSP":  39.8, "BANR": 49.8, "CNS":  64.8,
    "WEN":  64.8, "OFG":  17.8, "HUN":  54.8, "CHCO": 49.8, "FCF":  54.8,
    "NWBI": 67.8, "LKFN": 44.8, "STBA": 44.8, "BKE":  54.8, "CRI":  44.8,
    "GABC": 54.8, "VRTS": 44.8, "CNA":  34.8, "PFBC": 24.8,
}

# 1-year dividend growth rate (%) — updated Apr 2026
DIV_GROWTH_1Y = {
    "CVX":   8.3, "COP":   9.8, "MRK":   5.1, "VZ":    2.1, "KO":    5.2,
    "TXN":   5.2, "PEP":   7.3, "AMGN":  6.8, "ABT":   8.1, "PG":    5.1,
    "UNH":  11.8, "QCOM":  6.3, "HD":    8.1, "LMT":   5.1, "BMY":   3.2,
    "ACN":   9.8, "MO":    4.1, "CMCSA": 7.1, "EOG":   9.8, "ADP":  11.8,
    "BX":   19.8, "SLB":  15.2, "UPS":   5.1, "OKE":   3.8, "TGT":   1.8,
    "FAST":  8.8, "F":    66.7, "FITB":  4.8, "ADM":   2.1, "KMB":   1.8,
    "DVN": -10.0, "HSY":   5.2, "PAYX":  9.8, "CTRA":  9.8, "CINF":  7.8,
    "DRI":   8.8, "ARES": 19.8, "RF":    7.8, "GIS":   1.2, "TROW":  1.8,
    "SNA":   9.8, "BR":   10.8, "SWKS":  9.8, "PFG":   6.2, "APA": -50.0,
    "EWBC": 11.8, "AFG":   9.8, "IPG":   5.8, "BBY":   4.8, "WSO":   9.1,
    "FNF":   7.8, "CMA":   2.8, "ALV":  14.8, "MTN":   0.0, "RDN":  14.2,
    "FMC":  -9.8, "NXST":  4.8, "COLB":  2.8, "OZK":   2.8, "RHI":   4.8,
    "WHR":   0.0, "MC":    4.8, "MSM":   4.8, "IBOC":  4.8, "FLO":   2.8,
    "FHI":   4.8, "WU":   -4.8, "CATY":  2.8, "SIG":  19.8, "MUR":   2.8,
    "APAM":  9.8, "CVBF":  2.8, "NSP":   4.8, "BANR":  2.8, "CNS":   5.8,
    "WEN":   2.8, "OFG":   9.8, "HUN":  -9.8, "CHCO":  2.8, "FCF":   2.8,
    "NWBI":  0.0, "LKFN":  4.8, "STBA":  2.8, "BKE":   4.8, "CRI":   2.8,
    "GABC":  2.8, "VRTS":  4.8, "CNA":   2.8, "PFBC":  4.8,
}

# Trailing 12-month dividend yields (%) — updated Apr 2026
DIVIDEND_YIELDS = {
    "CVX":   3.40, "COP":   3.50, "MRK":   3.24, "VZ":    5.53, "KO":    3.05,
    "TXN":   2.95, "PEP":   3.52, "AMGN":  3.20, "ABT":   1.70, "PG":    2.41,
    "UNH":   1.60, "QCOM":  2.25, "HD":    2.42, "LMT":   2.70, "BMY":   4.28,
    "ACN":   1.70, "MO":    7.50, "CMCSA": 4.55, "EOG":   3.05, "ADP":   3.00,
    "BX":    2.50, "SLB":   2.30, "UPS":   5.50, "OKE":   4.52, "TGT":   4.05,
    "FAST":  2.05, "F":     5.48, "FITB":  3.53, "ADM":   3.80, "KMB":   5.11,
    "DVN":   4.50, "HSY":   3.20, "PAYX":  4.60, "CTRA":  3.50, "CINF":  2.20,
    "DRI":   3.50, "ARES":  3.20, "RF":    4.09, "GIS":   3.80, "TROW":  5.52,
    "SNA":   2.70, "BR":    1.90, "SWKS":  2.80, "PFG":   3.80, "APA":   4.50,
    "EWBC":  3.50, "AFG":   1.70, "IPG":   4.90, "BBY":   4.50, "WSO":   1.80,
    "FNF":   4.50, "CMA":   3.06, "ALV":   3.00, "MTN":   4.50, "RDN":   3.50,
    "FMC":   4.50, "NXST":  4.80, "COLB":  5.50, "OZK":   3.80, "RHI":   3.50,
    "WHR":   3.50, "MC":    4.00, "MSM":   4.00, "IBOC":  2.80, "FLO":   4.50,
    "FHI":   3.50, "WU":    8.00, "CATY":  4.50, "SIG":   1.50, "MUR":   3.80,
    "APAM":  7.50, "CVBF":  4.00, "NSP":   1.80, "BANR":  4.50, "CNS":   3.80,
    "WEN":   6.00, "OFG":   2.80, "HUN":   4.50, "CHCO":  2.50, "FCF":   4.00,
    "NWBI":  5.50, "LKFN":  3.50, "STBA":  4.50, "BKE":   4.50, "CRI":   4.50,
    "GABC":  3.00, "VRTS":  5.00, "CNA":   3.00, "PFBC":  3.50,
}

# SCHD Top 89 Holdings (source: Schwab/stockanalysis, post-March 2026 reconstitution)
SCHD_HOLDINGS = [
    {"ticker": "CVX",   "name": "Chevron Corp",                      "weight": 4.66},
    {"ticker": "COP",   "name": "ConocoPhillips",                    "weight": 4.33},
    {"ticker": "MRK",   "name": "Merck & Co Inc",                    "weight": 4.08},
    {"ticker": "VZ",    "name": "Verizon Communications",            "weight": 4.03},
    {"ticker": "KO",    "name": "Coca-Cola Co",                      "weight": 3.98},
    {"ticker": "TXN",   "name": "Texas Instruments Inc",             "weight": 3.86},
    {"ticker": "PEP",   "name": "PepsiCo Inc",                       "weight": 3.83},
    {"ticker": "AMGN",  "name": "Amgen Inc",                         "weight": 3.78},
    {"ticker": "ABT",   "name": "Abbott Laboratories",               "weight": 3.78},
    {"ticker": "PG",    "name": "Procter & Gamble Co",               "weight": 3.70},
    {"ticker": "UNH",   "name": "UnitedHealth Group Inc",            "weight": 3.64},
    {"ticker": "QCOM",  "name": "Qualcomm Inc",                      "weight": 3.58},
    {"ticker": "HD",    "name": "The Home Depot Inc",                "weight": 3.58},
    {"ticker": "LMT",   "name": "Lockheed Martin Corp",              "weight": 3.29},
    {"ticker": "BMY",   "name": "Bristol-Myers Squibb Co",           "weight": 3.15},
    {"ticker": "ACN",   "name": "Accenture PLC",                     "weight": 3.13},
    {"ticker": "MO",    "name": "Altria Group Inc",                  "weight": 2.95},
    {"ticker": "CMCSA", "name": "Comcast Corp",                      "weight": 2.69},
    {"ticker": "EOG",   "name": "EOG Resources Inc",                 "weight": 2.14},
    {"ticker": "ADP",   "name": "Automatic Data Processing",         "weight": 2.14},
    {"ticker": "BX",    "name": "Blackstone Inc",                    "weight": 2.14},
    {"ticker": "SLB",   "name": "SLB Ltd",                           "weight": 2.11},
    {"ticker": "UPS",   "name": "United Parcel Service Inc",         "weight": 1.85},
    {"ticker": "OKE",   "name": "ONEOK Inc",                         "weight": 1.56},
    {"ticker": "TGT",   "name": "Target Corp",                       "weight": 1.43},
    {"ticker": "FAST",  "name": "Fastenal Co",                       "weight": 1.36},
    {"ticker": "F",     "name": "Ford Motor Co",                     "weight": 1.18},
    {"ticker": "FITB",  "name": "Fifth Third Bancorp",               "weight": 1.06},
    {"ticker": "ADM",   "name": "Archer-Daniels-Midland Co",         "weight": 0.92},
    {"ticker": "KMB",   "name": "Kimberly-Clark Corp",               "weight": 0.87},
    {"ticker": "DVN",   "name": "Devon Energy Corp",                 "weight": 0.85},
    {"ticker": "HSY",   "name": "The Hershey Co",                    "weight": 0.84},
    {"ticker": "PAYX",  "name": "Paychex Inc",                       "weight": 0.78},
    {"ticker": "CTRA",  "name": "Coterra Energy Inc",                "weight": 0.73},
    {"ticker": "CINF",  "name": "Cincinnati Financial Corp",         "weight": 0.63},
    {"ticker": "DRI",   "name": "Darden Restaurants Inc",            "weight": 0.59},
    {"ticker": "ARES",  "name": "Ares Management Corp",              "weight": 0.58},
    {"ticker": "RF",    "name": "Regions Financial Corp",            "weight": 0.58},
    {"ticker": "GIS",   "name": "General Mills Inc",                 "weight": 0.51},
    {"ticker": "TROW",  "name": "T. Rowe Price Group Inc",           "weight": 0.51},
    {"ticker": "SNA",   "name": "Snap-on Inc",                       "weight": 0.49},
    {"ticker": "BR",    "name": "Broadridge Financial Solutions",    "weight": 0.49},
    {"ticker": "SWKS",  "name": "Skyworks Solutions Inc",            "weight": 0.46},
    {"ticker": "PFG",   "name": "Principal Financial Group Inc",     "weight": 0.45},
    {"ticker": "APA",   "name": "APA Corp",                          "weight": 0.42},
    {"ticker": "EWBC",  "name": "East West Bancorp Inc",             "weight": 0.38},
    {"ticker": "AFG",   "name": "American Financial Group Inc",      "weight": 0.38},
    {"ticker": "IPG",   "name": "Interpublic Group of Companies",    "weight": 0.37},
    {"ticker": "BBY",   "name": "Best Buy Co Inc",                   "weight": 0.32},
    {"ticker": "WSO",   "name": "Watsco Inc",                        "weight": 0.32},
    {"ticker": "FNF",   "name": "Fidelity National Financial Inc",   "weight": 0.31},
    {"ticker": "CMA",   "name": "Comerica Inc",                      "weight": 0.31},
    {"ticker": "ALV",   "name": "Autoliv Inc",                       "weight": 0.31},
    {"ticker": "MTN",   "name": "Vail Resorts Inc",                  "weight": 0.25},
    {"ticker": "RDN",   "name": "Radian Group Inc",                  "weight": 0.21},
    {"ticker": "FMC",   "name": "FMC Corp",                          "weight": 0.21},
    {"ticker": "NXST",  "name": "Nexstar Media Group Inc",           "weight": 0.21},
    {"ticker": "COLB",  "name": "Columbia Banking System Inc",       "weight": 0.21},
    {"ticker": "OZK",   "name": "Bank OZK",                          "weight": 0.20},
    {"ticker": "RHI",   "name": "Robert Half Inc",                   "weight": 0.20},
    {"ticker": "WHR",   "name": "Whirlpool Corp",                    "weight": 0.18},
    {"ticker": "MC",    "name": "Moelis & Co",                       "weight": 0.17},
    {"ticker": "MSM",   "name": "MSC Industrial Direct Co Inc",      "weight": 0.15},
    {"ticker": "IBOC",  "name": "International Bancshares Corp",     "weight": 0.14},
    {"ticker": "FLO",   "name": "Flowers Foods Inc",                 "weight": 0.14},
    {"ticker": "FHI",   "name": "Federated Hermes Inc",              "weight": 0.14},
    {"ticker": "WU",    "name": "Western Union Co",                  "weight": 0.13},
    {"ticker": "CATY",  "name": "Cathay General Bancorp",            "weight": 0.13},
    {"ticker": "SIG",   "name": "Signet Jewelers Ltd",               "weight": 0.12},
    {"ticker": "MUR",   "name": "Murphy Oil Corp",                   "weight": 0.12},
    {"ticker": "APAM",  "name": "Artisan Partners Asset Mgmt",       "weight": 0.12},
    {"ticker": "CVBF",  "name": "CVB Financial Corp",                "weight": 0.10},
    {"ticker": "NSP",   "name": "Insperity Inc",                     "weight": 0.10},
    {"ticker": "BANR",  "name": "Banner Corp",                       "weight": 0.09},
    {"ticker": "CNS",   "name": "Cohen & Steers Inc",                "weight": 0.09},
    {"ticker": "WEN",   "name": "The Wendy's Co",                    "weight": 0.08},
    {"ticker": "OFG",   "name": "OFG Bancorp",                       "weight": 0.08},
    {"ticker": "HUN",   "name": "Huntsman Corp",                     "weight": 0.08},
    {"ticker": "CHCO",  "name": "City Holding Co",                   "weight": 0.07},
    {"ticker": "FCF",   "name": "First Commonwealth Financial",      "weight": 0.07},
    {"ticker": "NWBI",  "name": "Northwest Bancshares Inc",          "weight": 0.07},
    {"ticker": "LKFN",  "name": "Lakeland Financial Corp",           "weight": 0.06},
    {"ticker": "STBA",  "name": "S&T Bancorp Inc",                   "weight": 0.06},
    {"ticker": "BKE",   "name": "The Buckle Inc",                    "weight": 0.05},
    {"ticker": "CRI",   "name": "Carter's Inc",                      "weight": 0.05},
    {"ticker": "GABC",  "name": "German American Bancorp Inc",       "weight": 0.05},
    {"ticker": "VRTS",  "name": "Virtus Investment Partners Inc",    "weight": 0.05},
    {"ticker": "CNA",   "name": "CNA Financial Corp",                "weight": 0.04},
    {"ticker": "PFBC",  "name": "Preferred Bank",                    "weight": 0.04},
]

# ── Separate caches ───────────────────────────────────────────────────────────
_stooq_cache = {"data": None, "ts": 0}
_price_state = {
    "data":       {},
    "batch_ts":   [0, 0],
    "next_batch": 0,
}
STOOQ_TTL  = 24 * 3600
PRICE_TTL  = 60


def fetch_yahoo_52w(ticker):
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        f"?range=1y&interval=1d&includePrePost=false"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        quote = data["chart"]["result"][0]["indicators"]["quote"][0]
        highs  = [x for x in quote["high"]  if x is not None]
        lows   = [x for x in quote["low"]   if x is not None]
        closes = [x for x in quote["close"] if x is not None]
        if not closes:
            return ticker, None
        return ticker, {
            "lastClose":        round(closes[-1], 2),
            "fiftyTwoWeekLow":  round(min(lows), 2),
            "fiftyTwoWeekHigh": round(max(highs), 2),
        }
    except Exception as e:
        print(f"{ticker} yahoo error: {e}")
        return ticker, None


def fetch_stooq(ticker):
    d2 = date.today().strftime("%Y%m%d")
    d1 = (date.today() - timedelta(days=366)).strftime("%Y%m%d")
    url = (
        f"https://stooq.com/q/d/l/?s={ticker.lower()}.us&i=d"
        f"&d1={d1}&d2={d2}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            text = resp.read().decode("utf-8").strip()
        lines = text.split("\n")
        if len(lines) < 2:
            return ticker, None
        closes, highs, lows = [], [], []
        for line in lines[1:]:
            parts = line.split(",")
            if len(parts) >= 5:
                try:
                    highs.append(float(parts[2]))
                    lows.append(float(parts[3]))
                    closes.append(float(parts[4]))
                except ValueError:
                    pass
        if not closes:
            return ticker, None
        return ticker, {
            "lastClose":        round(closes[-1], 2),
            "fiftyTwoWeekLow":  round(min(lows), 2),
            "fiftyTwoWeekHigh": round(max(highs), 2),
        }
    except Exception as e:
        print(f"{ticker} stooq error: {e}")
        return ticker, None


def fetch_finnhub_price(ticker):
    if not FINNHUB_TOKEN:
        return ticker, None
    url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={FINNHUB_TOKEN}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            text = resp.read().decode("utf-8")
        data  = json.loads(text)
        price = data.get("c")
        if price and price > 0:
            return ticker, round(price, 2)
        return ticker, None
    except urllib.error.HTTPError as e:
        if e.code == 429:
            print(f"{ticker}: Finnhub rate-limited")
        else:
            print(f"{ticker} finnhub HTTP {e.code}")
        return ticker, None
    except Exception as e:
        print(f"{ticker} finnhub error: {e}")
        return ticker, None


def _parallel_fetch(fn, tickers):
    results = {}
    with ThreadPoolExecutor(max_workers=len(tickers)) as ex:
        futures = {ex.submit(fn, t): t for t in tickers}
        for f in as_completed(futures):
            t, v = f.result()
            if v is not None:
                results[t] = v
    return results


def _fetch_52w_batched(tickers):
    results = {}
    for i in range(0, len(tickers), 10):
        chunk = tickers[i:i + 10]
        results.update(_parallel_fetch(fetch_yahoo_52w, chunk))
        if i + 10 < len(tickers):
            time.sleep(0.3)
    missing = [t for t in tickers if t not in results]
    if missing:
        print(f"Yahoo missed {len(missing)} tickers, trying Stooq fallback")
        for i in range(0, len(missing), 5):
            chunk = missing[i:i + 5]
            results.update(_parallel_fetch(fetch_stooq, chunk))
            if i + 5 < len(missing):
                time.sleep(0.5)
    return results


def get_holdings_data():
    now     = time.time()
    tickers = [h["ticker"] for h in SCHD_HOLDINGS]

    if not _stooq_cache["data"] or (now - _stooq_cache["ts"]) >= STOOQ_TTL:
        w52 = _fetch_52w_batched(tickers)
        print(f"52W data: {len(w52)}/{len(tickers)} tickers")
        _stooq_cache["data"] = w52
        _stooq_cache["ts"]   = now

    mid    = len(tickers) // 2
    halves = [tickers[:mid], tickers[mid:]]
    idx    = _price_state["next_batch"]

    if (now - _price_state["batch_ts"][idx]) >= PRICE_TTL:
        prices = _parallel_fetch(fetch_finnhub_price, halves[idx])
        _price_state["data"].update(prices)
        _price_state["batch_ts"][idx] = now
        _price_state["next_batch"]    = 1 - idx
        print(f"Finnhub half-{idx}: {len(prices)}/{len(halves[idx])} live")

    stooq_data     = _stooq_cache["data"]
    finnhub_prices = _price_state["data"]

    results = []
    for i, holding in enumerate(SCHD_HOLDINGS):
        ticker  = holding["ticker"]
        sq      = stooq_data.get(ticker, {})
        live_px = finnhub_prices.get(ticker)
        last_px = sq.get("lastClose")
        price   = live_px if live_px is not None else last_px
        low52   = sq.get("fiftyTwoWeekLow")
        high52  = sq.get("fiftyTwoWeekHigh")
        variance = (
            round((price - low52) / low52 * 100, 2)
            if price and low52 and low52 > 0 else None
        )
        results.append({
            "rank":             i + 1,
            "ticker":           ticker,
            "name":             holding["name"],
            "weight":           holding["weight"],
            "price":            price,
            "priceIsLive":      live_px is not None,
            "yield":            DIVIDEND_YIELDS.get(ticker),
            "payoutRatio":      PAYOUT_RATIOS.get(ticker),
            "divGrowth1Y":      DIV_GROWTH_1Y.get(ticker),
            "fiftyTwoWeekLow":  low52,
            "fiftyTwoWeekHigh": high52,
            "varianceFromLow":  variance,
        })

    return results, now


class handler(BaseHTTPRequestHandler):
    """Vercel Python serverless handler."""

    def do_GET(self):
        try:
            data, timestamp = get_holdings_data()
            live_count = sum(1 for h in data if h.get("priceIsLive"))
            body = json.dumps({
                "holdings":    data,
                "liveCount":   live_count,
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
        pass
