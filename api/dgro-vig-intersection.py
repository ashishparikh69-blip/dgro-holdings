# Vercel Python Serverless Function — /api/dgro-vig-intersection
# Stocks present in both DGRO and VIG top-100 holdings.
from http.server import BaseHTTPRequestHandler
import json, time, os, urllib.request, urllib.error
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

FINNHUB_TOKEN = os.environ.get("FINNHUB_TOKEN", "")

# Payout ratios (%) — updated Oct 2026
PAYOUT_RATIOS = {
    "MSFT": 24.8, "AAPL": 15.2, "AVGO": 46.1, "JPM": 25.2, "LLY": 15.0,
    "XOM": 43.2, "JNJ": 46.3, "ABBV": 86.5, "CSCO": 50.2, "BAC": 24.0,
    "LRCX": 20.0, "MRK": 48.1, "UNH": 19.8, "KO": 73.4, "PG": 67.1,
    "HD": 54.2, "AMGN": 57.2, "KLAC": 20.0, "IBM": 65.0, "APH": 20.0,
    "PEP": 70.2, "DHR": 15.0, "SPGI": 35.0, "CB": 21.3, "SYK": 20.0,
    "BNY": 30.0, "INTU": 15.0, "GD": 42.0, "CSX": 40.0, "ICE": 28.1,
    "EMR": 44.2, "ELV": 25.0, "WM": 45.0, "MSI": 30.0, "CMI": 35.0,
    "TRV": 26.4, "SHW": 25.0, "ECL": 35.0, "NOC": 28.0, "AON": 20.0,
    "AJG": 15.0, "CTAS": 45.0, "APD": 62.1, "HON": 60.0, "ALL": 18.2,
    "TEL": 30.0, "FAST": 69.1, "STT": 29.2, "SRE": 55.1, "AFL": 35.0,
    "BDX": 30.0, "MET": 25.3, "ETR": 47.2, "LHX": 28.0, "FITB": 38.2,
    "ROK": 40.0, "XEL": 71.4,
}

# 1-year dividend growth rate (%) — updated Oct 2026
DIV_GROWTH_1Y = {
    "MSFT": 10.2, "AAPL": 4.0, "AVGO": 14.2, "JPM": 8.9, "LLY": 15.0,
    "XOM": 4.2, "JNJ": 5.3, "ABBV": 4.8, "CSCO": 3.1, "BAC": 9.0,
    "LRCX": 15.0, "MRK": 5.1, "UNH": 11.8, "KO": 5.2, "PG": 5.1,
    "HD": 8.1, "AMGN": 6.8, "KLAC": 15.0, "IBM": 0.5, "APH": 30.0,
    "PEP": 7.3, "DHR": 10.0, "SPGI": 10.0, "CB": 5.0, "SYK": 12.0,
    "BNY": 12.0, "INTU": 15.0, "GD": 5.0, "CSX": 8.0, "ICE": 9.8,
    "EMR": 0.8, "ELV": 8.0, "WM": 8.0, "MSI": 12.0, "CMI": 8.0,
    "TRV": 4.8, "SHW": 10.0, "ECL": 10.0, "NOC": 10.0, "AON": 10.0,
    "AJG": 12.0, "CTAS": 18.0, "APD": 1.2, "HON": 5.0, "ALL": 4.8,
    "TEL": 8.0, "FAST": 8.8, "STT": 10.0, "SRE": 4.8, "AFL": 16.0,
    "BDX": 4.0, "MET": 4.8, "ETR": 3.8, "LHX": 5.0, "FITB": 4.8,
    "ROK": 5.0, "XEL": 6.1,
}

# 5-year dividend growth rate CAGR (%) — updated Oct 2026
DIV_GROWTH_5Y = {
    "MSFT": 10.0, "AAPL": 5.0, "AVGO": 14.0, "JPM": 15.0, "LLY": 15.0,
    "XOM": 3.0, "JNJ": 6.0, "ABBV": 8.0, "CSCO": 7.0, "BAC": 10.0,
    "LRCX": 15.0, "MRK": 9.1, "UNH": 18.2, "KO": 4.8, "PG": 5.8,
    "HD": 13.8, "AMGN": 8.8, "KLAC": 18.0, "IBM": 0.0, "APH": 25.0,
    "PEP": 7.2, "DHR": 12.0, "SPGI": 12.0, "CB": 8.0, "SYK": 15.0,
    "BNY": 8.0, "INTU": 15.0, "GD": 7.0, "CSX": 10.0, "ICE": 12.0,
    "EMR": 8.0, "ELV": 15.0, "WM": 10.0, "MSI": 12.0, "CMI": 10.0,
    "TRV": 6.0, "SHW": 15.0, "ECL": 10.0, "NOC": 9.0, "AON": 15.0,
    "AJG": 15.0, "CTAS": 18.0, "APD": 6.0, "HON": 3.0, "ALL": 10.0,
    "TEL": 8.0, "FAST": 11.8, "STT": 8.0, "SRE": 6.0, "AFL": 16.0,
    "BDX": 8.0, "MET": 8.0, "ETR": 4.0, "LHX": 8.0, "FITB": 7.8,
    "ROK": 8.0, "XEL": 6.0,
}

# Trailing 12-month dividend yields (%) — updated Oct 2026
DIVIDEND_YIELDS = {
    "MSFT": 0.80, "AAPL": 0.50, "AVGO": 1.40, "JPM": 2.30, "LLY": 0.70,
    "XOM": 3.40, "JNJ": 3.00, "ABBV": 3.60, "CSCO": 2.80, "BAC": 2.40,
    "LRCX": 1.00, "MRK": 3.24, "UNH": 1.60, "KO": 3.05, "PG": 2.41,
    "HD": 2.42, "AMGN": 3.20, "KLAC": 1.00, "IBM": 2.80, "APH": 0.90,
    "PEP": 3.52, "DHR": 0.50, "SPGI": 0.90, "CB": 1.40, "SYK": 1.00,
    "BNY": 2.80, "INTU": 0.70, "GD": 2.20, "CSX": 1.30, "ICE": 1.20,
    "EMR": 2.00, "ELV": 1.50, "WM": 1.40, "MSI": 1.00, "CMI": 2.50,
    "TRV": 2.00, "SHW": 1.00, "ECL": 1.20, "NOC": 1.70, "AON": 0.80,
    "AJG": 0.80, "CTAS": 1.00, "APD": 2.50, "HON": 2.30, "ALL": 2.00,
    "TEL": 1.50, "FAST": 2.10, "STT": 3.10, "SRE": 3.10, "AFL": 2.50,
    "BDX": 1.80, "MET": 3.00, "ETR": 2.70, "LHX": 2.10, "FITB": 3.53,
    "ROK": 1.80, "XEL": 3.50,
}

# 57 stocks in both DGRO top-100 and VIG top-100, sorted by VIG weight desc (as of Oct 9 2026)
INTERSECTION_HOLDINGS = [
    {"ticker": "MSFT",  "name": "Microsoft Corp",                    "dgroWeight": 3.26, "vigWeight": 4.68},
    {"ticker": "AAPL",  "name": "Apple Inc",                         "dgroWeight": 3.12, "vigWeight": 4.51},
    {"ticker": "AVGO",  "name": "Broadcom Inc",                      "dgroWeight": 2.92, "vigWeight": 4.35},
    {"ticker": "JPM",   "name": "JPMorgan Chase & Co",               "dgroWeight": 2.76, "vigWeight": 4.07},
    {"ticker": "LLY",   "name": "Eli Lilly and Co",                  "dgroWeight": 1.28, "vigWeight": 3.90},
    {"ticker": "XOM",   "name": "Exxon Mobil Corp",                  "dgroWeight": 3.09, "vigWeight": 2.85},
    {"ticker": "JNJ",   "name": "Johnson & Johnson",                 "dgroWeight": 2.74, "vigWeight": 2.73},
    {"ticker": "ABBV",  "name": "AbbVie Inc",                        "dgroWeight": 2.92, "vigWeight": 1.93},
    {"ticker": "CSCO",  "name": "Cisco Systems Inc",                 "dgroWeight": 1.64, "vigWeight": 1.86},
    {"ticker": "BAC",   "name": "Bank of America Corp",              "dgroWeight": 1.44, "vigWeight": 1.73},
    {"ticker": "LRCX",  "name": "Lam Research Corp",                 "dgroWeight": 0.32, "vigWeight": 1.61},
    {"ticker": "MRK",   "name": "Merck & Co Inc",                    "dgroWeight": 1.82, "vigWeight": 1.56},
    {"ticker": "UNH",   "name": "UnitedHealth Group Inc",            "dgroWeight": 1.81, "vigWeight": 1.51},
    {"ticker": "KO",    "name": "Coca-Cola Co",                      "dgroWeight": 1.86, "vigWeight": 1.47},
    {"ticker": "PG",    "name": "Procter & Gamble Co",               "dgroWeight": 2.35, "vigWeight": 1.44},
    {"ticker": "HD",    "name": "The Home Depot Inc",                "dgroWeight": 1.90, "vigWeight": 1.39},
    {"ticker": "AMGN",  "name": "Amgen Inc",                         "dgroWeight": 1.15, "vigWeight": 0.99},
    {"ticker": "KLAC",  "name": "KLA Corp",                          "dgroWeight": 0.29, "vigWeight": 0.98},
    {"ticker": "IBM",   "name": "IBM Corp",                          "dgroWeight": 1.38, "vigWeight": 0.94},
    {"ticker": "APH",   "name": "Amphenol Corp",                     "dgroWeight": 0.30, "vigWeight": 0.83},
    {"ticker": "PEP",   "name": "PepsiCo Inc",                       "dgroWeight": 1.69, "vigWeight": 0.82},
    {"ticker": "DHR",   "name": "Danaher Corp",                      "dgroWeight": 0.24, "vigWeight": 0.57},
    {"ticker": "SPGI",  "name": "S&P Global Inc",                    "dgroWeight": 0.23, "vigWeight": 0.56},
    {"ticker": "CB",    "name": "Chubb Ltd",                         "dgroWeight": 0.32, "vigWeight": 0.52},
    {"ticker": "SYK",   "name": "Stryker Corp",                      "dgroWeight": 0.27, "vigWeight": 0.48},
    {"ticker": "BNY",   "name": "Bank of New York Mellon Corp",      "dgroWeight": 0.34, "vigWeight": 0.47},
    {"ticker": "INTU",  "name": "Intuit Inc",                        "dgroWeight": 0.26, "vigWeight": 0.42},
    {"ticker": "GD",    "name": "General Dynamics Corp",             "dgroWeight": 0.33, "vigWeight": 0.40},
    {"ticker": "CSX",   "name": "CSX Corp",                          "dgroWeight": 0.23, "vigWeight": 0.40},
    {"ticker": "ICE",   "name": "Intercontinental Exchange Inc",     "dgroWeight": 0.26, "vigWeight": 0.39},
    {"ticker": "EMR",   "name": "Emerson Electric Co",               "dgroWeight": 0.30, "vigWeight": 0.36},
    {"ticker": "ELV",   "name": "Elevance Health Inc",               "dgroWeight": 0.32, "vigWeight": 0.35},
    {"ticker": "WM",    "name": "Waste Management Inc",              "dgroWeight": 0.31, "vigWeight": 0.35},
    {"ticker": "MSI",   "name": "Motorola Solutions Inc",            "dgroWeight": 0.18, "vigWeight": 0.34},
    {"ticker": "CMI",   "name": "Cummins Inc",                       "dgroWeight": 0.26, "vigWeight": 0.33},
    {"ticker": "TRV",   "name": "The Travelers Companies Inc",       "dgroWeight": 0.23, "vigWeight": 0.33},
    {"ticker": "SHW",   "name": "Sherwin-Williams Co",               "dgroWeight": 0.16, "vigWeight": 0.33},
    {"ticker": "ECL",   "name": "Ecolab Inc",                        "dgroWeight": 0.17, "vigWeight": 0.31},
    {"ticker": "NOC",   "name": "Northrop Grumman Corp",             "dgroWeight": 0.30, "vigWeight": 0.31},
    {"ticker": "AON",   "name": "Aon PLC",                           "dgroWeight": 0.14, "vigWeight": 0.29},
    {"ticker": "AJG",   "name": "Arthur J. Gallagher & Co",          "dgroWeight": 0.14, "vigWeight": 0.29},
    {"ticker": "CTAS",  "name": "Cintas Corp",                       "dgroWeight": 0.16, "vigWeight": 0.29},
    {"ticker": "APD",   "name": "Air Products and Chemicals Inc",    "dgroWeight": 0.34, "vigWeight": 0.29},
    {"ticker": "HON",   "name": "Honeywell International Inc",       "dgroWeight": 0.21, "vigWeight": 0.29},
    {"ticker": "ALL",   "name": "Allstate Corp",                     "dgroWeight": 0.22, "vigWeight": 0.28},
    {"ticker": "TEL",   "name": "TE Connectivity PLC",               "dgroWeight": 0.22, "vigWeight": 0.26},
    {"ticker": "FAST",  "name": "Fastenal Co",                       "dgroWeight": 0.27, "vigWeight": 0.24},
    {"ticker": "STT",   "name": "State Street Corp",                 "dgroWeight": 0.19, "vigWeight": 0.23},
    {"ticker": "SRE",   "name": "Sempra",                            "dgroWeight": 0.37, "vigWeight": 0.23},
    {"ticker": "AFL",   "name": "Aflac Inc",                         "dgroWeight": 0.24, "vigWeight": 0.23},
    {"ticker": "BDX",   "name": "Becton Dickinson & Co",             "dgroWeight": 0.26, "vigWeight": 0.22},
    {"ticker": "MET",   "name": "MetLife Inc",                       "dgroWeight": 0.29, "vigWeight": 0.22},
    {"ticker": "ETR",   "name": "Entergy Corp",                      "dgroWeight": 0.26, "vigWeight": 0.21},
    {"ticker": "LHX",   "name": "L3Harris Technologies Inc",         "dgroWeight": 0.20, "vigWeight": 0.21},
    {"ticker": "FITB",  "name": "Fifth Third Bancorp",               "dgroWeight": 0.31, "vigWeight": 0.21},
    {"ticker": "ROK",   "name": "Rockwell Automation Inc",           "dgroWeight": 0.15, "vigWeight": 0.20},
    {"ticker": "XEL",   "name": "Xcel Energy Inc",                   "dgroWeight": 0.33, "vigWeight": 0.20},
]

_stooq_cache = {"data": None, "ts": 0}
_price_state = {
    "data":       {},
    "batch_ts":   [0, 0],
    "next_batch": 0,
}
STOOQ_TTL = 24 * 3600
PRICE_TTL = 60


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
    url = f"https://stooq.com/q/d/l/?s={ticker.lower()}.us&i=d&d1={d1}&d2={d2}"
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
            data = json.loads(resp.read().decode("utf-8"))
        price = data.get("c")
        if price and price > 0:
            return ticker, round(price, 2)
        return ticker, None
    except urllib.error.HTTPError as e:
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


def get_holdings_data():
    now     = time.time()
    tickers = [h["ticker"] for h in INTERSECTION_HOLDINGS]

    if not _stooq_cache["data"] or (now - _stooq_cache["ts"]) >= STOOQ_TTL:
        results = {}
        for i in range(0, len(tickers), 10):
            chunk = tickers[i:i + 10]
            results.update(_parallel_fetch(fetch_yahoo_52w, chunk))
            if i + 10 < len(tickers):
                time.sleep(0.3)
        missing = [t for t in tickers if t not in results]
        if missing:
            results.update(_parallel_fetch(fetch_stooq, missing))
        _stooq_cache["data"] = results
        _stooq_cache["ts"]   = now

    mid    = len(tickers) // 2
    halves = [tickers[:mid], tickers[mid:]]
    idx    = _price_state["next_batch"]

    if (now - _price_state["batch_ts"][idx]) >= PRICE_TTL:
        prices = _parallel_fetch(fetch_finnhub_price, halves[idx])
        _price_state["data"].update(prices)
        _price_state["batch_ts"][idx] = now
        _price_state["next_batch"]    = 1 - idx

    stooq_data     = _stooq_cache["data"]
    finnhub_prices = _price_state["data"]

    results = []
    for i, holding in enumerate(INTERSECTION_HOLDINGS):
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
            "dgroWeight":       holding["dgroWeight"],
            "vigWeight":        holding["vigWeight"],
            "price":            price,
            "priceIsLive":      live_px is not None,
            "yield":            DIVIDEND_YIELDS.get(ticker),
            "payoutRatio":      PAYOUT_RATIOS.get(ticker),
            "divGrowth1Y":      DIV_GROWTH_1Y.get(ticker),
            "divGrowth5Y":      DIV_GROWTH_5Y.get(ticker),
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
