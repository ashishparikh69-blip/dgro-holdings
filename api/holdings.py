# Vercel Python Serverless Function — /api/holdings
# Prices:  Finnhub real-time (market hours), fallback to Stooq last close.
# 52W data: Stooq end-of-day, cached 24 h.
# Dividend yields: hardcoded trailing 12-month, updated Mar 2026.
from http.server import BaseHTTPRequestHandler
import json, time, os, urllib.request, urllib.error
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

FINNHUB_TOKEN = os.environ.get("FINNHUB_TOKEN", "")

# Trailing 12-month dividend yields (%) — updated Mar 2026 via Yahoo Finance trailingAnnualDividendYield
DIVIDEND_YIELDS = {
    "AAPL":  0.52, "MSFT":  0.83, "JPM":   2.28, "ABBV":  3.55, "AVGO":  1.23,
    "HD":    2.42, "JNJ":   2.16, "PG":    2.41, "XOM":   2.53, "CVX":   3.40,
    "MRK":   3.24, "PFE":   6.28, "CSCO":  2.09, "KO":    3.05, "PEP":   3.52,
    "VZ":    5.53, "CMCSA": 4.55, "TXN":   2.95, "PM":    3.45, "BMY":   4.28,
    "UNP":   2.45, "QCOM":  2.25, "RTX":   1.33, "LOW":   2.18, "MDT":   3.62,
    "MS":    2.48, "BLK":   2.55, "SCHW":  1.15, "C":     2.11, "CB":    1.44,
    "GS":    1.73, "ADI":   1.31, "DE":    1.14, "SO":    3.45, "DUK":   3.25,
    "ITW":   2.55, "CI":    1.78, "USB":   3.98, "PNC":   3.27, "ADP":   3.00,
    "TGT":   4.05, "MMM":   2.25, "EMR":   2.05, "FIS":   3.25, "APD":   2.55,
    "NSC":   1.94, "CME":   2.05, "ICE":   1.32, "EOG":   3.05, "CL":    2.25,
    "WMB":   2.70, "F":     5.48, "GM":    1.02, "MET":   3.25, "PRU":   5.84,
    "AIG":   2.05, "TRV":   1.82, "ALL":   2.05, "D":     4.85, "SRE":   2.71,
    "AEP":   2.91, "WEC":   3.52, "XEL":   2.87, "ETR":   2.35, "PPL":   3.25,
    "ED":    3.45, "FITB":  3.53, "KEY":   4.25, "RF":    4.09, "CFG":   3.01,
    "HBAN":  4.09, "NTRS":  2.28, "STT":   2.65, "IP":    5.44, "NUE":   1.36,
    "PAYX":  4.60, "FAST":  2.05, "GPC":   4.16, "OMC":   3.82, "HPQ":   6.32,
    "KMB":   5.11, "SYY":   2.82, "CAH":   0.97, "TROW":  5.52, "BEN":   5.52,
    "LEN":   2.13, "DHI":   1.05, "PHM":   0.82, "OKE":   4.52, "KMI":   3.50,
    "CINF":  2.20, "AMCR":  6.66, "FNF":   4.50, "CMA":   3.06, "ZION":  3.52,
    "OGN":   5.52, "UGI":   4.07, "FAF":   4.05,
}

# DGRO Top 100 Holdings (source: iShares, approximate weights) — sorted by weight desc
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

# ── Separate caches ───────────────────────────────────────────────────────────
# Stooq:   52W low/high + last-close fallback — refresh every 24 h
# Finnhub: real-time price, staggered batches — each half refreshes every 60 s,
#          alternating so we never fire >49 calls at once (well within 60/min limit).
#          Each individual ticker is guaranteed fresh within ~120 s.
_stooq_cache = {"data": None, "ts": 0}
_price_state = {
    "data":       {},     # accumulated {ticker: price} for all tickers
    "batch_ts":   [0, 0], # last fetch time for half-0 and half-1
    "next_batch": 0,      # which half to refresh on the next stale tick
}
STOOQ_TTL  = 24 * 3600  # 24 hours
PRICE_TTL  = 60          # each half refreshes every 60 s (so each ticker every ~120 s)


def fetch_stooq(ticker):
    """Fetch 1 year of daily closes from Stooq (52W range + last-close fallback)."""
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
        closes = []
        for line in lines[1:]:
            parts = line.split(",")
            if len(parts) >= 5:
                try:
                    closes.append(float(parts[4]))
                except ValueError:
                    pass
        if not closes:
            return ticker, None
        return ticker, {
            "lastClose":        round(closes[-1], 2),
            "fiftyTwoWeekLow":  round(min(closes), 2),
            "fiftyTwoWeekHigh": round(max(closes), 2),
        }
    except Exception as e:
        print(f"{ticker} stooq error: {e}")
        return ticker, None


def fetch_finnhub_price(ticker):
    """Fetch real-time quote from Finnhub. Returns (ticker, price|None)."""
    if not FINNHUB_TOKEN:
        return ticker, None
    url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={FINNHUB_TOKEN}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            text = resp.read().decode("utf-8")
        data  = json.loads(text)
        price = data.get("c")   # current price (0 when market closed / no data)
        if price and price > 0:
            return ticker, round(price, 2)
        # Finnhub returns c=0 outside market hours — treat as no live price
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
    """Fire all ticker requests concurrently. Returns dict {ticker: result}."""
    results = {}
    with ThreadPoolExecutor(max_workers=len(tickers)) as ex:
        futures = {ex.submit(fn, t): t for t in tickers}
        for f in as_completed(futures):
            t, v = f.result()
            if v is not None:
                results[t] = v
    return results


def _stooq_batched(tickers):
    """Stooq needs gentle rate-limiting: batches of 5 with 0.5 s pauses."""
    results = {}
    for i in range(0, len(tickers), 5):
        chunk = tickers[i:i + 5]
        results.update(_parallel_fetch(fetch_stooq, chunk))
        if i + 5 < len(tickers):
            time.sleep(0.5)
    return results


def get_holdings_data():
    now     = time.time()
    tickers = [h["ticker"] for h in DGRO_HOLDINGS]

    # ── Stooq 52W data (refresh every 24 h) ──────────────────────────────────
    if not _stooq_cache["data"] or (now - _stooq_cache["ts"]) >= STOOQ_TTL:
        stooq = _stooq_batched(tickers)
        print(f"Stooq: {len(stooq)}/{len(tickers)} tickers")
        _stooq_cache["data"] = stooq
        _stooq_cache["ts"]   = now

    # ── Finnhub staggered batch prices ────────────────────────────────────────
    # Split tickers into two halves (~49 each).  On each 60-second tick we fetch
    # only ONE half, alternating between them.  This keeps every Finnhub call
    # burst well under the 60-calls/minute free-tier limit, and guarantees every
    # ticker gets a fresh price within ~120 seconds.
    mid  = len(tickers) // 2          # 49
    halves = [tickers[:mid], tickers[mid:]]
    idx  = _price_state["next_batch"]

    if (now - _price_state["batch_ts"][idx]) >= PRICE_TTL:
        prices = _parallel_fetch(fetch_finnhub_price, halves[idx])
        _price_state["data"].update(prices)
        _price_state["batch_ts"][idx] = now
        _price_state["next_batch"]    = 1 - idx   # alternate
        print(f"Finnhub half-{idx}: {len(prices)}/{len(halves[idx])} live")

    stooq_data     = _stooq_cache["data"]
    finnhub_prices = _price_state["data"]

    # ── Combine ───────────────────────────────────────────────────────────────
    results = []
    for i, holding in enumerate(DGRO_HOLDINGS):
        ticker  = holding["ticker"]
        sq      = stooq_data.get(ticker, {})
        live_px = finnhub_prices.get(ticker)          # real-time (may be None)
        last_px = sq.get("lastClose")                 # Stooq last close fallback
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
        pass  # suppress request logs
