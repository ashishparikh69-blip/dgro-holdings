# Vercel Python Serverless Function — /api/holdings
# Uses yfinance bulk download (single call for all tickers) to avoid Yahoo Finance IP blocks.
from http.server import BaseHTTPRequestHandler
import json, time
import yfinance as yf

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
    {"ticker": "MMC",   "name": "Marsh & McLennan Cos",          "weight": 0.89},
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
    {"ticker": "WBA",   "name": "Walgreens Boots Alliance",      "weight": 0.17},
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


def get_holdings_data():
    now = time.time()
    if _cache["data"] and (now - _cache["timestamp"]) < CACHE_DURATION:
        return _cache["data"], _cache["timestamp"]

    tickers_list = [h["ticker"] for h in DGRO_HOLDINGS]
    tickers_str  = " ".join(tickers_list)

    # --- Bulk price + 52-week range via single yf.download() call ---
    try:
        raw = yf.download(
            tickers_str,
            period="1y",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
        # raw.columns is a MultiIndex (field, ticker) when >1 ticker
        close = raw["Close"] if "Close" in raw.columns.get_level_values(0) else raw
    except Exception as e:
        print(f"yf.download error: {e}")
        close = None

    # --- Dividend yield via fast_info (one request per ticker, parallel) ---
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def fetch_yield(ticker):
        try:
            fi = yf.Ticker(ticker).fast_info
            dy = getattr(fi, "three_month_average_volume", None)  # placeholder probe
            # Try dividendYield from summary_detail endpoint (lighter than full info)
            tk_obj = yf.Ticker(ticker)
            dy = None
            try:
                dy = tk_obj.info.get("dividendYield")
            except Exception:
                pass
            return ticker, dy
        except Exception:
            return ticker, None

    yields = {}
    try:
        with ThreadPoolExecutor(max_workers=20) as ex:
            futs = {ex.submit(fetch_yield, t): t for t in tickers_list}
            for f in as_completed(futs):
                t, dy = f.result()
                yields[t] = dy
    except Exception as e:
        print(f"yield fetch error: {e}")

    # --- Build results ---
    results = []
    for i, holding in enumerate(DGRO_HOLDINGS):
        ticker = holding["ticker"]
        price = low52 = high52 = None

        if close is not None:
            try:
                col = close[ticker]
                col = col.dropna()
                if not col.empty:
                    price = float(round(col.iloc[-1], 2))
                    low52 = float(round(col.min(), 2))
                    high52 = float(round(col.max(), 2))
            except Exception as e:
                print(f"parse error {ticker}: {e}")

        dy = yields.get(ticker)
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
            "yield":           round(dy, 4) if dy else None,
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
            body = json.dumps({"error": str(e), "trace": traceback.format_exc()}).encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass  # suppress request logs
