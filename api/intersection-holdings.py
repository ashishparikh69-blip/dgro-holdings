# Vercel Python Serverless Function — /api/intersection-holdings
# Stocks present in both DGRO and SCHD holdings.
from http.server import BaseHTTPRequestHandler
import json, time, os, urllib.request, urllib.error
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

FINNHUB_TOKEN = os.environ.get("FINNHUB_TOKEN", "")

PAYOUT_RATIOS = {
    "CVX":  48.3, "MRK":  48.1, "VZ":   61.8, "KO":   73.4, "TXN":  53.7,
    "PEP":  70.2, "PG":   67.1, "QCOM": 35.2, "HD":   54.2, "BMY":  55.3,
    "CMCSA":33.1, "EOG":  25.4, "ADP":  76.3, "OKE":  73.2, "TGT":  50.1,
    "FAST": 69.1, "F":    47.8, "FITB": 38.2, "KMB":  73.2, "PAYX": 82.3,
    "CINF": 43.8, "RF":   38.4, "TROW": 62.3, "FNF":  44.8, "CMA":  34.2,
}

DIV_GROWTH_1Y = {
    "CVX":   8.3, "MRK":   5.1, "VZ":   2.1, "KO":   5.2, "TXN":   5.2,
    "PEP":   7.3, "PG":    5.1, "QCOM": 6.3, "HD":   8.1, "BMY":   3.2,
    "CMCSA": 7.1, "EOG":   9.8, "ADP": 11.8, "OKE":  3.8, "TGT":   1.8,
    "FAST":  8.8, "F":    66.7, "FITB": 4.8, "KMB":  1.8, "PAYX":  9.8,
    "CINF":  7.8, "RF":    7.8, "TROW": 1.8, "FNF":  7.8, "CMA":   2.8,
}

DIVIDEND_YIELDS = {
    "CVX":   3.40, "MRK":   3.24, "VZ":    5.53, "KO":    3.05, "TXN":   2.95,
    "PEP":   3.52, "PG":    2.41, "QCOM":  2.25, "HD":    2.42, "BMY":   4.28,
    "CMCSA": 4.55, "EOG":   3.05, "ADP":   3.00, "OKE":   4.52, "TGT":   4.05,
    "FAST":  2.05, "F":     5.48, "FITB":  3.53, "KMB":   5.11, "PAYX":  4.60,
    "CINF":  2.20, "RF":    4.09, "TROW":  5.52, "FNF":   4.50, "CMA":   3.06,
}

# 25 stocks in both DGRO and SCHD, sorted by SCHD weight desc
INTERSECTION_HOLDINGS = [
    {"ticker": "CVX",   "name": "Chevron Corp",               "dgroWeight": 1.62, "schdWeight": 4.66},
    {"ticker": "MRK",   "name": "Merck & Co Inc",             "dgroWeight": 1.58, "schdWeight": 4.08},
    {"ticker": "VZ",    "name": "Verizon Communications",     "dgroWeight": 1.38, "schdWeight": 4.03},
    {"ticker": "KO",    "name": "Coca-Cola Co",               "dgroWeight": 1.45, "schdWeight": 3.98},
    {"ticker": "TXN",   "name": "Texas Instruments Inc",      "dgroWeight": 1.32, "schdWeight": 3.86},
    {"ticker": "PEP",   "name": "PepsiCo Inc",                "dgroWeight": 1.42, "schdWeight": 3.83},
    {"ticker": "PG",    "name": "Procter & Gamble Co",        "dgroWeight": 1.98, "schdWeight": 3.70},
    {"ticker": "QCOM",  "name": "Qualcomm Inc",               "dgroWeight": 1.18, "schdWeight": 3.58},
    {"ticker": "HD",    "name": "The Home Depot Inc",         "dgroWeight": 2.21, "schdWeight": 3.58},
    {"ticker": "BMY",   "name": "Bristol-Myers Squibb Co",    "dgroWeight": 1.25, "schdWeight": 3.15},
    {"ticker": "CMCSA", "name": "Comcast Corp",               "dgroWeight": 1.35, "schdWeight": 2.69},
    {"ticker": "EOG",   "name": "EOG Resources Inc",          "dgroWeight": 0.54, "schdWeight": 2.14},
    {"ticker": "ADP",   "name": "Automatic Data Processing",  "dgroWeight": 0.69, "schdWeight": 2.14},
    {"ticker": "OKE",   "name": "ONEOK Inc",                  "dgroWeight": 0.13, "schdWeight": 1.56},
    {"ticker": "TGT",   "name": "Target Corp",                "dgroWeight": 0.67, "schdWeight": 1.43},
    {"ticker": "FAST",  "name": "Fastenal Co",                "dgroWeight": 0.26, "schdWeight": 1.36},
    {"ticker": "F",     "name": "Ford Motor Co",              "dgroWeight": 0.51, "schdWeight": 1.18},
    {"ticker": "FITB",  "name": "Fifth Third Bancorp",        "dgroWeight": 0.36, "schdWeight": 1.06},
    {"ticker": "KMB",   "name": "Kimberly-Clark Corp",        "dgroWeight": 0.22, "schdWeight": 0.87},
    {"ticker": "PAYX",  "name": "Paychex Inc",                "dgroWeight": 0.27, "schdWeight": 0.78},
    {"ticker": "CINF",  "name": "Cincinnati Financial Corp",  "dgroWeight": 0.11, "schdWeight": 0.63},
    {"ticker": "RF",    "name": "Regions Financial Corp",     "dgroWeight": 0.34, "schdWeight": 0.58},
    {"ticker": "TROW",  "name": "T. Rowe Price Group Inc",    "dgroWeight": 0.19, "schdWeight": 0.51},
    {"ticker": "FNF",   "name": "Fidelity National Financial","dgroWeight": 0.09, "schdWeight": 0.31},
    {"ticker": "CMA",   "name": "Comerica Inc",               "dgroWeight": 0.08, "schdWeight": 0.31},
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
