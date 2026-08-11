# Vercel Python Serverless Function — /api/intersection-holdings
# Stocks present in both DGRO and SCHD holdings.
from http.server import BaseHTTPRequestHandler
import json, time, os, urllib.request, urllib.error
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

FINNHUB_TOKEN = os.environ.get("FINNHUB_TOKEN", "")

# Payout ratios (%) — updated Aug 2026
PAYOUT_RATIOS = {
    "AMGN": 53.1, "MRK":  48.1, "KO":   73.4, "UNH":  20.0, "HD":   54.2,
    "PG":   67.1, "PEP":  70.2, "FAST": 69.1, "FITB": 38.2, "ADM":  52.3,
    "ARES": 87.1, "CINF": 43.8, "DRI":  51.8, "PFG":  36.1,
}

# 1-year dividend growth rate (%) — updated Aug 2026
DIV_GROWTH_1Y = {
    "AMGN":  6.5, "MRK":   5.1, "KO":   5.2, "UNH":  12.0, "HD":   8.1,
    "PG":    5.1, "PEP":   7.3, "FAST": 8.8, "FITB": 4.8,  "ADM":  2.1,
    "ARES": 19.8, "CINF":  7.8, "DRI":  8.8, "PFG":  6.2,
}

# 5-year dividend growth rate CAGR (%) — updated Aug 2026
DIV_GROWTH_5Y = {
    "AMGN":  7.8, "MRK":   9.1, "KO":   4.8, "UNH":  18.2, "HD":  13.8,
    "PG":    5.8, "PEP":   7.2, "FAST":11.8, "FITB": 7.8,  "ADM":  5.8,
    "ARES": 25.0, "CINF":  7.8, "DRI": 11.8, "PFG":  6.2,
}

# Trailing 12-month dividend yields (%) — updated Aug 2026
DIVIDEND_YIELDS = {
    "AMGN":  3.20, "MRK":   3.24, "KO":   3.05, "UNH":  1.60, "HD":   2.42,
    "PG":    2.41, "PEP":   3.52, "FAST": 2.05, "FITB": 3.53, "ADM":  3.80,
    "ARES":  3.20, "CINF":  2.20, "DRI":  3.50, "PFG":  3.80,
}

# 14 stocks in both DGRO top-100 and SCHD, sorted by SCHD weight desc (Aug 2026)
INTERSECTION_HOLDINGS = [
    {"ticker": "AMGN",  "name": "Amgen Inc",               "dgroWeight": 1.30, "schdWeight": 4.30},
    {"ticker": "MRK",   "name": "Merck & Co Inc",          "dgroWeight": 1.85, "schdWeight": 4.27},
    {"ticker": "KO",    "name": "Coca-Cola Co",            "dgroWeight": 1.91, "schdWeight": 4.23},
    {"ticker": "UNH",   "name": "UnitedHealth Group Inc",  "dgroWeight": 1.70, "schdWeight": 4.22},
    {"ticker": "HD",    "name": "The Home Depot Inc",      "dgroWeight": 2.21, "schdWeight": 4.17},
    {"ticker": "PG",    "name": "Procter & Gamble Co",     "dgroWeight": 2.19, "schdWeight": 3.97},
    {"ticker": "PEP",   "name": "PepsiCo Inc",             "dgroWeight": 1.63, "schdWeight": 3.76},
    {"ticker": "FAST",  "name": "Fastenal Co",             "dgroWeight": 0.24, "schdWeight": 1.40},
    {"ticker": "FITB",  "name": "Fifth Third Bancorp",     "dgroWeight": 0.34, "schdWeight": 1.30},
    {"ticker": "ADM",   "name": "Archer-Daniels-Midland",  "dgroWeight": 0.20, "schdWeight": 0.95},
    {"ticker": "ARES",  "name": "Ares Management Corp",    "dgroWeight": 0.27, "schdWeight": 0.74},
    {"ticker": "CINF",  "name": "Cincinnati Financial",    "dgroWeight": 0.14, "schdWeight": 0.70},
    {"ticker": "DRI",   "name": "Darden Restaurants Inc",  "dgroWeight": 0.16, "schdWeight": 0.60},
    {"ticker": "PFG",   "name": "Principal Financial Group","dgroWeight": 0.16, "schdWeight": 0.57},
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
        results.update(_parallel_fetch(fetch_yahoo_52w, tickers))
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

    holdings = []
    for holding in INTERSECTION_HOLDINGS:
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
        holdings.append({
            "ticker":           ticker,
            "name":             holding["name"],
            "dgroWeight":       holding["dgroWeight"],
            "schdWeight":       holding["schdWeight"],
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

    return holdings, now


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            data, timestamp = get_holdings_data()
            body = json.dumps({
                "holdings":    data,
                "lastUpdated": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(timestamp)),
            }).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        except Exception as e:
            import traceback
            body = json.dumps({"error": str(e), "trace": traceback.format_exc()}).encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass
