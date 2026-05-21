# Vercel Python Serverless Function — /api/holdings
# Prices:  Finnhub real-time (market hours), fallback to last close.
# 52W data: Yahoo Finance primary, Stooq fallback, cached 24 h.
# Dividend yields: hardcoded trailing 12-month, updated May 2026.
from http.server import BaseHTTPRequestHandler
import json, time, os, urllib.request, urllib.error
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

FINNHUB_TOKEN = os.environ.get("FINNHUB_TOKEN", "")

# Payout ratios (%) — updated May 2026
PAYOUT_RATIOS = {
    "AVGO": 46.1, "AAPL": 15.2, "MSFT": 24.8, "JPM":  25.2, "XOM":  43.2,
    "JNJ":  46.3, "ABBV": 86.5, "UNH":  24.5, "PM":   94.2, "CSCO": 50.2,
    "PG":   67.1, "KO":   73.4, "MRK":  48.1, "HD":   54.2, "BAC":  27.3,
    "PEP":  70.2, "MS":   27.8, "GS":   19.8, "QCOM": 35.2, "LLY":  10.2,
    "IBM":  68.5, "NEE":  58.2, "WFC":  26.1, "C":    27.6, "AMGN": 53.2,
    "V":    20.1, "MCD":  55.8, "ORCL": 22.4, "WMT":  37.2, "COP":  40.1,
    "CAT":  26.3, "GILD": 52.8, "UNP":  41.8, "BLK":  52.3, "ABT":  46.2,
    "DUK":  71.2, "ACN":  43.1, "SO":   71.8, "RTX":  26.4, "MDT":  55.2,
    "LIN":  38.4, "PNC":  41.1, "HON":  46.3, "MDLZ": 50.2, "MA":   18.9,
    "ADI":  49.1, "ADP":  76.3, "LOW":  38.1, "COST": 22.3, "EOG":  25.4,
    "PSX":  55.1, "ELV":  23.8, "LMT":  50.2, "AEP":  67.3, "AMAT": 16.8,
    "ETN":  32.1, "LRCX": 27.8, "AXP":  24.2, "TJX":  37.4, "APD":  62.1,
    "CI":   21.3, "BK":   33.1, "ITW":  63.1, "CME":  57.3, "SRE":  55.1,
    "CL":   65.2, "MRSH": 39.8, "DE":   15.8, "MCHP": 69.8, "FITB": 38.2,
    "CMI":  41.8, "NXPI": 39.2, "MET":  25.3, "CB":   21.3, "GD":   43.8,
    "FDX":  35.8, "KLAC": 24.1, "XEL":  71.4, "ETR":  47.2, "ED":   79.8,
    "EMR":  44.2, "AFL":  26.8, "STT":  29.2, "CSX":  31.8, "PEG":  64.2,
    "APH":  22.1, "KDP":  61.8, "WEC":  68.2, "ADM":  64.8, "ALL":  18.2,
    "WM":   43.8, "ARES": 88.2, "JCI":  47.8, "SYK":  24.2, "SPGI": 24.8,
    "BDX":  56.8, "INTU": 29.8, "ICE":  28.1, "FAST": 69.1, "CRH":  27.2,
}

# 1-year dividend growth rate (%) — updated May 2026
DIV_GROWTH_1Y = {
    "AVGO": 14.2, "AAPL":  4.0, "MSFT": 10.2, "JPM":   8.9, "XOM":   4.2,
    "JNJ":   5.3, "ABBV":  4.8, "UNH":  12.1, "PM":    3.1, "CSCO":  3.1,
    "PG":    5.1, "KO":    5.2, "MRK":   5.1, "HD":    8.1, "BAC":   8.3,
    "PEP":   7.3, "MS":    9.8, "GS":    9.8, "QCOM":  6.3, "LLY":  15.2,
    "IBM":   0.6, "NEE":  10.2, "WFC":  17.4, "C":     8.2, "AMGN":  6.1,
    "V":    17.2, "MCD":   6.8, "ORCL": 25.0, "WMT":   9.1, "COP":  12.2,
    "CAT":   8.1, "GILD":  2.5, "UNP":   5.1, "BLK":   9.8, "ABT":   5.1,
    "DUK":   2.1, "ACN":  15.2, "SO":    3.1, "RTX":   7.2, "MDT":   1.8,
    "LIN":   8.1, "PNC":   5.2, "HON":   5.2, "MDLZ": 10.8, "MA":   15.8,
    "ADI":   8.1, "ADP":  11.8, "LOW":   4.8, "COST": 13.8, "EOG":   9.8,
    "PSX":  10.2, "ELV":  12.8, "LMT":   5.2, "AEP":   5.8, "AMAT": 25.1,
    "ETN":  10.1, "LRCX": 17.2, "AXP":  17.1, "TJX":  13.2, "APD":   1.2,
    "CI":    8.1, "BK":   12.8, "ITW":   7.2, "CME":   9.8, "SRE":   4.8,
    "CL":    3.8, "MRSH": 10.2, "DE":    9.2, "MCHP":  0.0, "FITB":  4.8,
    "CMI":   8.2, "NXPI": 10.8, "MET":   4.8, "CB":    4.8, "GD":    5.1,
    "FDX":  10.2, "KLAC": 24.8, "XEL":   6.1, "ETR":   3.8, "ED":    2.8,
    "EMR":   0.8, "AFL":  17.1, "STT":   9.8, "CSX":   8.8, "PEG":   5.1,
    "APH":  23.8, "KDP":   7.2, "WEC":   6.8, "ADM":  12.2, "ALL":   4.8,
    "WM":    7.8, "ARES": 20.1, "JCI":   9.8, "SYK":  12.1, "SPGI": 12.2,
    "BDX":   5.1, "INTU": 15.2, "ICE":   9.8, "FAST":  8.8, "CRH":  12.1,
}

# 5-year dividend growth rate CAGR (%) — updated May 2026
DIV_GROWTH_5Y = {
    "AVGO": 17.5, "AAPL":  5.8, "MSFT": 10.7, "JPM":  14.2, "XOM":   3.5,
    "JNJ":   5.8, "ABBV":  8.4, "UNH":  14.8, "PM":    3.5, "CSCO":  3.1,
    "PG":    5.8, "KO":    4.8, "MRK":   9.1, "HD":   13.8, "BAC":  10.2,
    "PEP":   7.2, "MS":   16.8, "GS":   25.0, "QCOM":  7.2, "LLY":  15.2,
    "IBM":   1.0, "NEE":  10.2, "WFC":   4.8, "C":    20.0, "AMGN": 13.2,
    "V":    18.1, "MCD":   9.2, "ORCL": 20.1, "WMT":   2.1, "COP":  17.2,
    "CAT":   8.8, "GILD":  4.1, "UNP":  12.1, "BLK":   9.8, "ABT":  12.1,
    "DUK":   2.5, "ACN":  14.2, "SO":    3.2, "RTX":   7.8, "MDT":   4.1,
    "LIN":   8.2, "PNC":   8.2, "HON":   5.2, "MDLZ": 11.2, "MA":   17.8,
    "ADI":  12.8, "ADP":  13.2, "LOW":  19.2, "COST": 13.2, "EOG":  22.8,
    "PSX":  14.2, "ELV":  13.8, "LMT":   7.2, "AEP":   5.8, "AMAT": 22.1,
    "ETN":   9.2, "LRCX": 22.1, "AXP":  17.2, "TJX":  15.1, "APD":   8.2,
    "CI":   12.8, "BK":    8.2, "ITW":   7.8, "CME":  10.2, "SRE":   4.8,
    "CL":    4.8, "MRSH": 11.2, "DE":   14.8, "MCHP":  8.2, "FITB":  7.8,
    "CMI":   8.2, "NXPI": 15.1, "MET":   9.8, "CB":    4.8, "GD":    8.1,
    "FDX":  12.1, "KLAC": 22.1, "XEL":   6.1, "ETR":   3.8, "ED":    2.8,
    "EMR":   2.8, "AFL":  11.8, "STT":   7.8, "CSX":   9.2, "PEG":   3.2,
    "APH":  17.2, "KDP":   7.2, "WEC":   6.8, "ADM":   8.2, "ALL":   4.8,
    "WM":    8.1, "ARES": 25.1, "JCI":   6.2, "SYK":  12.2, "SPGI": 13.2,
    "BDX":   5.2, "INTU": 18.2, "ICE":  11.2, "FAST": 11.8, "CRH":  10.2,
}

# Trailing 12-month dividend yields (%) — updated May 2026 via iShares/Yahoo Finance
DIVIDEND_YIELDS = {
    "AVGO":  1.23, "AAPL":  0.52, "MSFT":  0.83, "JPM":   2.28, "XOM":   2.53,
    "JNJ":   2.16, "ABBV":  3.55, "UNH":   1.82, "PM":    3.45, "CSCO":  2.09,
    "PG":    2.41, "KO":    3.05, "MRK":   3.24, "HD":    2.42, "BAC":   2.38,
    "PEP":   3.52, "MS":    2.48, "GS":    1.73, "QCOM":  2.25, "LLY":   0.62,
    "IBM":   2.82, "NEE":   2.88, "WFC":   2.18, "C":     2.11, "AMGN":  3.22,
    "V":     0.71, "MCD":   2.42, "ORCL":  0.92, "WMT":   0.88, "COP":   3.22,
    "CAT":   1.52, "GILD":  3.38, "UNP":   2.45, "BLK":   2.55, "ABT":   1.62,
    "DUK":   3.25, "ACN":   1.88, "SO":    3.45, "RTX":   1.33, "MDT":   3.62,
    "LIN":   1.22, "PNC":   3.27, "HON":   2.38, "MDLZ":  3.12, "MA":    0.62,
    "ADI":   1.31, "ADP":   3.00, "LOW":   2.18, "COST":  0.52, "EOG":   3.05,
    "PSX":   3.72, "ELV":   2.18, "LMT":   2.82, "AEP":   2.91, "AMAT":  1.02,
    "ETN":   1.22, "LRCX":  1.38, "AXP":   1.38, "TJX":   1.52, "APD":   2.55,
    "CI":    1.78, "BK":    2.82, "ITW":   2.55, "CME":   2.05, "SRE":   2.71,
    "CL":    2.25, "MRSH":  1.95, "DE":    1.14, "MCHP":  3.48, "FITB":  3.53,
    "CMI":   2.78, "NXPI":  2.18, "MET":   3.25, "CB":    1.44, "GD":    2.28,
    "FDX":   2.28, "KLAC":  1.08, "XEL":   2.87, "ETR":   2.35, "ED":    3.45,
    "EMR":   2.05, "AFL":   2.38, "STT":   2.65, "CSX":   1.48, "PEG":   3.38,
    "APH":   0.92, "KDP":   3.22, "WEC":   3.52, "ADM":   4.18, "ALL":   2.05,
    "WM":    1.52, "ARES":  2.78, "JCI":   2.38, "SYK":   1.02, "SPGI":  0.82,
    "BDX":   1.98, "INTU":  0.72, "ICE":   1.32, "FAST":  2.05, "CRH":   1.62,
}

# DGRO Top 100 Holdings (source: iShares, May 14 2026) — sorted by weight desc
DGRO_HOLDINGS = [
    {"ticker": "AVGO",  "name": "Broadcom Inc",                     "weight": 3.42},
    {"ticker": "AAPL",  "name": "Apple Inc",                        "weight": 3.22},
    {"ticker": "MSFT",  "name": "Microsoft Corp",                   "weight": 2.92},
    {"ticker": "JPM",   "name": "JPMorgan Chase & Co",              "weight": 2.91},
    {"ticker": "XOM",   "name": "Exxon Mobil Corp",                 "weight": 2.87},
    {"ticker": "JNJ",   "name": "Johnson & Johnson",                "weight": 2.65},
    {"ticker": "ABBV",  "name": "AbbVie Inc",                       "weight": 2.52},
    {"ticker": "UNH",   "name": "UnitedHealth Group Inc",           "weight": 2.49},
    {"ticker": "PM",    "name": "Philip Morris International",      "weight": 2.25},
    {"ticker": "CSCO",  "name": "Cisco Systems Inc",                "weight": 2.18},
    {"ticker": "PG",    "name": "Procter & Gamble Co",              "weight": 2.01},
    {"ticker": "KO",    "name": "Coca-Cola Co",                     "weight": 1.83},
    {"ticker": "MRK",   "name": "Merck & Co Inc",                   "weight": 1.82},
    {"ticker": "HD",    "name": "Home Depot Inc",                   "weight": 1.75},
    {"ticker": "BAC",   "name": "Bank of America Corp",             "weight": 1.71},
    {"ticker": "PEP",   "name": "PepsiCo Inc",                      "weight": 1.59},
    {"ticker": "MS",    "name": "Morgan Stanley",                   "weight": 1.30},
    {"ticker": "GS",    "name": "Goldman Sachs Group Inc",          "weight": 1.24},
    {"ticker": "QCOM",  "name": "Qualcomm Inc",                     "weight": 1.22},
    {"ticker": "LLY",   "name": "Eli Lilly and Co",                 "weight": 1.22},
    {"ticker": "IBM",   "name": "International Business Machines",  "weight": 1.20},
    {"ticker": "NEE",   "name": "NextEra Energy Inc",               "weight": 1.20},
    {"ticker": "WFC",   "name": "Wells Fargo & Co",                 "weight": 1.16},
    {"ticker": "C",     "name": "Citigroup Inc",                    "weight": 1.09},
    {"ticker": "AMGN",  "name": "Amgen Inc",                        "weight": 1.07},
    {"ticker": "V",     "name": "Visa Inc",                         "weight": 1.02},
    {"ticker": "MCD",   "name": "McDonald's Corp",                  "weight": 0.98},
    {"ticker": "ORCL",  "name": "Oracle Corp",                      "weight": 0.97},
    {"ticker": "WMT",   "name": "Walmart Inc",                      "weight": 0.96},
    {"ticker": "COP",   "name": "ConocoPhillips",                   "weight": 0.94},
    {"ticker": "CAT",   "name": "Caterpillar Inc",                  "weight": 0.82},
    {"ticker": "GILD",  "name": "Gilead Sciences Inc",              "weight": 0.78},
    {"ticker": "UNP",   "name": "Union Pacific Corp",               "weight": 0.77},
    {"ticker": "BLK",   "name": "BlackRock Inc",                    "weight": 0.74},
    {"ticker": "ABT",   "name": "Abbott Laboratories",              "weight": 0.73},
    {"ticker": "DUK",   "name": "Duke Energy Corp",                 "weight": 0.70},
    {"ticker": "ACN",   "name": "Accenture PLC",                    "weight": 0.70},
    {"ticker": "SO",    "name": "Southern Co",                      "weight": 0.70},
    {"ticker": "RTX",   "name": "RTX Corp",                         "weight": 0.68},
    {"ticker": "MDT",   "name": "Medtronic PLC",                    "weight": 0.68},
    {"ticker": "LIN",   "name": "Linde PLC",                        "weight": 0.66},
    {"ticker": "PNC",   "name": "PNC Financial Services",           "weight": 0.63},
    {"ticker": "HON",   "name": "Honeywell International",          "weight": 0.62},
    {"ticker": "MDLZ",  "name": "Mondelez International",           "weight": 0.60},
    {"ticker": "MA",    "name": "Mastercard Inc",                   "weight": 0.59},
    {"ticker": "ADI",   "name": "Analog Devices Inc",               "weight": 0.57},
    {"ticker": "ADP",   "name": "Automatic Data Processing",        "weight": 0.57},
    {"ticker": "LOW",   "name": "Lowe's Companies Inc",             "weight": 0.53},
    {"ticker": "COST",  "name": "Costco Wholesale Corp",            "weight": 0.53},
    {"ticker": "EOG",   "name": "EOG Resources Inc",                "weight": 0.51},
    {"ticker": "PSX",   "name": "Phillips 66",                      "weight": 0.48},
    {"ticker": "ELV",   "name": "Elevance Health Inc",              "weight": 0.48},
    {"ticker": "LMT",   "name": "Lockheed Martin Corp",             "weight": 0.47},
    {"ticker": "AEP",   "name": "American Electric Power",          "weight": 0.42},
    {"ticker": "AMAT",  "name": "Applied Materials Inc",            "weight": 0.42},
    {"ticker": "ETN",   "name": "Eaton Corp PLC",                   "weight": 0.41},
    {"ticker": "LRCX",  "name": "Lam Research Corp",                "weight": 0.41},
    {"ticker": "AXP",   "name": "American Express Co",              "weight": 0.40},
    {"ticker": "TJX",   "name": "TJX Companies Inc",                "weight": 0.39},
    {"ticker": "APD",   "name": "Air Products & Chemicals",         "weight": 0.39},
    {"ticker": "CI",    "name": "Cigna Group",                      "weight": 0.39},
    {"ticker": "BK",    "name": "Bank of New York Mellon",          "weight": 0.38},
    {"ticker": "ITW",   "name": "Illinois Tool Works",              "weight": 0.37},
    {"ticker": "CME",   "name": "CME Group Inc",                    "weight": 0.37},
    {"ticker": "SRE",   "name": "Sempra",                           "weight": 0.37},
    {"ticker": "CL",    "name": "Colgate-Palmolive Co",             "weight": 0.36},
    {"ticker": "MRSH",  "name": "Marsh & McLennan Cos",             "weight": 0.35},
    {"ticker": "DE",    "name": "Deere & Co",                       "weight": 0.34},
    {"ticker": "MCHP",  "name": "Microchip Technology Inc",         "weight": 0.33},
    {"ticker": "FITB",  "name": "Fifth Third Bancorp",              "weight": 0.33},
    {"ticker": "CMI",   "name": "Cummins Inc",                      "weight": 0.32},
    {"ticker": "NXPI",  "name": "NXP Semiconductors NV",           "weight": 0.31},
    {"ticker": "MET",   "name": "MetLife Inc",                      "weight": 0.31},
    {"ticker": "CB",    "name": "Chubb Ltd",                        "weight": 0.31},
    {"ticker": "GD",    "name": "General Dynamics Corp",            "weight": 0.30},
    {"ticker": "FDX",   "name": "FedEx Corp",                       "weight": 0.30},
    {"ticker": "KLAC",  "name": "KLA Corp",                         "weight": 0.29},
    {"ticker": "XEL",   "name": "Xcel Energy Inc",                  "weight": 0.29},
    {"ticker": "ETR",   "name": "Entergy Corp",                     "weight": 0.27},
    {"ticker": "ED",    "name": "Consolidated Edison",              "weight": 0.27},
    {"ticker": "EMR",   "name": "Emerson Electric Co",              "weight": 0.27},
    {"ticker": "AFL",   "name": "Aflac Inc",                        "weight": 0.27},
    {"ticker": "STT",   "name": "State Street Corp",                "weight": 0.26},
    {"ticker": "CSX",   "name": "CSX Corp",                         "weight": 0.26},
    {"ticker": "PEG",   "name": "Public Service Enterprise Group",  "weight": 0.26},
    {"ticker": "APH",   "name": "Amphenol Corp",                    "weight": 0.26},
    {"ticker": "KDP",   "name": "Keurig Dr Pepper Inc",             "weight": 0.26},
    {"ticker": "WEC",   "name": "WEC Energy Group Inc",             "weight": 0.26},
    {"ticker": "ADM",   "name": "Archer Daniels Midland Co",        "weight": 0.25},
    {"ticker": "ALL",   "name": "Allstate Corp",                    "weight": 0.24},
    {"ticker": "WM",    "name": "Waste Management Inc",             "weight": 0.24},
    {"ticker": "ARES",  "name": "Ares Management Corp",             "weight": 0.24},
    {"ticker": "JCI",   "name": "Johnson Controls International",   "weight": 0.24},
    {"ticker": "SYK",   "name": "Stryker Corp",                     "weight": 0.24},
    {"ticker": "SPGI",  "name": "S&P Global Inc",                   "weight": 0.23},
    {"ticker": "BDX",   "name": "Becton Dickinson and Co",          "weight": 0.23},
    {"ticker": "INTU",  "name": "Intuit Inc",                       "weight": 0.23},
    {"ticker": "ICE",   "name": "Intercontinental Exchange",        "weight": 0.23},
    {"ticker": "FAST",  "name": "Fastenal Co",                      "weight": 0.23},
    {"ticker": "CRH",   "name": "CRH PLC",                          "weight": 0.22},
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


def fetch_yahoo_52w(ticker):
    """Fetch 1 year of daily OHLC from Yahoo Finance (52W range + last-close fallback)."""
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
    """Fallback: Fetch 1 year of daily data from Stooq."""
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


def _fetch_52w_batched(tickers):
    """Yahoo Finance primary, Stooq fallback. Batches of 10 with 0.3 s pauses."""
    results = {}
    # Primary: Yahoo Finance (reliable from cloud IPs)
    for i in range(0, len(tickers), 10):
        chunk = tickers[i:i + 10]
        results.update(_parallel_fetch(fetch_yahoo_52w, chunk))
        if i + 10 < len(tickers):
            time.sleep(0.3)
    # Fallback: Stooq for any tickers Yahoo missed
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
    tickers = [h["ticker"] for h in DGRO_HOLDINGS]

    # ── Stooq 52W data (refresh every 24 h) ──────────────────────────────────
    if not _stooq_cache["data"] or (now - _stooq_cache["ts"]) >= STOOQ_TTL:
        w52 = _fetch_52w_batched(tickers)
        print(f"52W data: {len(w52)}/{len(tickers)} tickers")
        _stooq_cache["data"] = w52
        _stooq_cache["ts"]   = now

    # ── Finnhub staggered batch prices ────────────────────────────────────────
    # Split tickers into two halves (~50 each).  On each 60-second tick we fetch
    # only ONE half, alternating between them.  This keeps every Finnhub call
    # burst well under the 60-calls/minute free-tier limit, and guarantees every
    # ticker gets a fresh price within ~120 seconds.
    mid  = len(tickers) // 2
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
        pass  # suppress request logs
