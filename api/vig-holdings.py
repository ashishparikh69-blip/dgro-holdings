# Vercel Python Serverless Function — /api/vig-holdings
# Prices:  Finnhub real-time (market hours), fallback to last close.
# 52W data: Yahoo Finance primary, Stooq fallback, cached 24 h.
# Dividend yields: hardcoded trailing 12-month, updated Aug 2026.
from http.server import BaseHTTPRequestHandler
import json, time, os, urllib.request, urllib.error
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

FINNHUB_TOKEN = os.environ.get("FINNHUB_TOKEN", "")

# Payout ratios (%) — updated Aug 2026
PAYOUT_RATIOS = {
    "AVGO": 46.1, "AAPL": 15.2, "MSFT": 24.8, "JPM":  25.2, "LLY":  15.0,
    "XOM":  43.2, "JNJ":  46.3, "V":    22.0,  "WMT":  30.0, "MA":   18.0,
    "CSCO": 50.2, "ABBV": 86.5, "COST": 30.0,  "BAC":  24.0, "CAT":  35.0,
    "UNH":  19.8, "LRCX": 20.0, "KO":   73.4,  "PG":   67.1, "HD":   54.2,
    "MRK":  48.1, "GS":   19.8, "MS":   27.8,  "TXN":  53.7, "KLAC": 20.0,
    "ORCL": 30.0, "LIN":  35.0, "IBM":  65.0,  "AMGN": 57.2, "APH":  20.0,
    "MCD":  73.0, "PEP":  70.2, "ABT":  57.8,  "NEE":  55.0, "ADI":  49.1,
    "UNP":  41.8, "ETN":  25.0, "GILD": 45.0,  "BLK":  52.3, "QCOM": 35.2,
    "CB":   21.3, "SPGI": 35.0, "DHR":  15.0,  "SBUX": 80.0, "LMT":  43.2,
    "LOW":  38.1, "SYK":  20.0, "MDT":  55.2,  "BNY":  30.0, "ADP":  76.3,
    "ACN":  44.1, "MCK":  10.0, "PNC":  41.1,  "GD":   42.0, "CME":  57.3,
    "CSX":  40.0, "MMC":  35.0, "CMI":  35.0,  "INTU": 15.0, "PSX":  55.0,
    "ICE":  28.1, "WM":   45.0, "EMR":  44.2,  "ELV":  25.0, "TRV":  26.4,
    "MDLZ": 50.0, "SHW":  25.0, "HON":  60.0,  "AON":  20.0, "CL":   65.2,
    "ITW":  63.1, "NOC":  28.0, "MSI":  30.0,  "MCO":  35.0, "AEP":  67.3,
    "ECL":  35.0, "CTAS": 45.0, "ALL":  18.2,  "AJG":  15.0, "APD":  62.1,
    "FIX":  10.0, "COR":   5.0, "TEL":  30.0,  "GWW":  18.0, "NUE":  10.8,
    "AFL":  35.0, "SRE":  55.1, "FAST": 69.1,  "CAH":  19.8, "ROK":  40.0,
    "MET":  25.3, "LHX":  28.0, "NKE":  45.0,  "FITB": 38.2, "STT":  29.2,
    "XEL":  71.4, "ETR":  47.2, "AMP":  20.0,  "BDX":  30.0, "SYY":  61.4,
}

# 1-year dividend growth rate (%) — updated Aug 2026
DIV_GROWTH_1Y = {
    "AVGO": 14.2, "AAPL":  4.0, "MSFT": 10.2, "JPM":   8.9, "LLY":  15.0,
    "XOM":   4.2, "JNJ":   5.3, "V":    15.0,  "WMT":  13.0, "MA":   15.0,
    "CSCO":  3.1, "ABBV":  4.8, "COST": 15.0,  "BAC":   9.0, "CAT":   8.0,
    "UNH":  11.8, "LRCX": 15.0, "KO":   5.2,   "PG":    5.1, "HD":    8.1,
    "MRK":   5.1, "GS":    9.8, "MS":   9.8,   "TXN":   5.2, "KLAC": 15.0,
    "ORCL": 25.0, "LIN":   8.0, "IBM":  0.5,   "AMGN":  6.8, "APH":  30.0,
    "MCD":   2.0, "PEP":   7.3, "ABT":  8.1,   "NEE":  10.0, "ADI":   8.1,
    "UNP":   5.1, "ETN":  12.0, "GILD":  5.0,  "BLK":  10.0, "QCOM":  6.3,
    "CB":    5.0, "SPGI": 10.0, "DHR":  10.0,  "SBUX":  8.0, "LMT":   5.1,
    "LOW":   4.8, "SYK":  12.0, "MDT":   1.8,  "BNY":  12.0, "ADP":  11.8,
    "ACN":   9.8, "MCK":  15.0, "PNC":   5.2,  "GD":    5.0, "CME":   9.8,
    "CSX":   8.0, "MMC":  10.0, "CMI":   8.0,  "INTU": 15.0, "PSX":  10.0,
    "ICE":   9.8, "WM":    8.0, "EMR":   0.8,  "ELV":   8.0, "TRV":   4.8,
    "MDLZ": 10.0, "SHW":  10.0, "HON":   5.0,  "AON":  10.0, "CL":    3.8,
    "ITW":   7.2, "NOC":  10.0, "MSI":  12.0,  "MCO":  12.0, "AEP":   5.8,
    "ECL":  10.0, "CTAS": 18.0, "ALL":   4.8,  "AJG":  12.0, "APD":   1.2,
    "FIX":  20.0, "COR":  15.0, "TEL":   8.0,  "GWW":  15.0, "NUE":   5.0,
    "AFL":  16.0, "SRE":   4.8, "FAST":  8.8,  "CAH":  18.0, "ROK":   5.0,
    "MET":   4.8, "LHX":   5.0, "NKE":   8.0,  "FITB":  4.8, "STT":  10.0,
    "XEL":   6.1, "ETR":   3.8, "AMP":  15.0,  "BDX":   4.0, "SYY":   5.0,
}

# 5-year dividend growth rate CAGR (%) — updated Aug 2026
DIV_GROWTH_5Y = {
    "AVGO": 14.0, "AAPL":  5.0, "MSFT": 10.0, "JPM":  15.0, "LLY":  15.0,
    "XOM":   3.0, "JNJ":   6.0, "V":    18.0,  "WMT":   5.0, "MA":   16.0,
    "CSCO":  7.0, "ABBV":  8.0, "COST": 12.0,  "BAC":  10.0, "CAT":  10.0,
    "UNH":  18.2, "LRCX": 15.0, "KO":   4.8,   "PG":    5.8, "HD":   13.8,
    "MRK":   9.1, "GS":   25.0, "MS":   15.0,  "TXN":  12.2, "KLAC": 18.0,
    "ORCL": 15.0, "LIN":   8.0, "IBM":   0.0,  "AMGN":  8.8, "APH":  25.0,
    "MCD":   8.0, "PEP":   7.2, "ABT":  10.2,  "NEE":  11.0, "ADI":  12.0,
    "UNP":  10.0, "ETN":  18.0, "GILD":  5.0,  "BLK":  12.0, "QCOM":  7.2,
    "CB":    8.0, "SPGI": 12.0, "DHR":  12.0,  "SBUX": 10.0, "LMT":   7.8,
    "LOW":  15.0, "SYK":  15.0, "MDT":   5.0,  "BNY":   8.0, "ADP":  13.2,
    "ACN":  10.8, "MCK":  20.0, "PNC":   8.0,  "GD":    7.0, "CME":  12.0,
    "CSX":  10.0, "MMC":  10.0, "CMI":  10.0,  "INTU": 15.0, "PSX":   8.0,
    "ICE":  12.0, "WM":   10.0, "EMR":   8.0,  "ELV":  15.0, "TRV":   6.0,
    "MDLZ": 11.0, "SHW":  15.0, "HON":   3.0,  "AON":  15.0, "CL":    4.0,
    "ITW":   7.2, "NOC":   9.0, "MSI":  12.0,  "MCO":  12.0, "AEP":   6.0,
    "ECL":  10.0, "CTAS": 18.0, "ALL":  10.0,  "AJG":  15.0, "APD":   6.0,
    "FIX":  25.0, "COR":  12.0, "TEL":   8.0,  "GWW":  12.0, "NUE":  15.0,
    "AFL":  16.0, "SRE":   6.0, "FAST": 11.8,  "CAH":  12.0, "ROK":   8.0,
    "MET":   8.0, "LHX":   8.0, "NKE":  10.0,  "FITB":  7.8, "STT":   8.0,
    "XEL":   6.0, "ETR":   4.0, "AMP":  15.0,  "BDX":   8.0, "SYY":   5.0,
}

# Trailing 12-month dividend yields (%) — updated Aug 2026
DIVIDEND_YIELDS = {
    "AVGO":  1.40, "AAPL":  0.50, "MSFT":  0.80, "JPM":  2.30, "LLY":  0.70,
    "XOM":   3.40, "JNJ":   3.00, "V":     0.80,  "WMT":  1.00, "MA":   0.60,
    "CSCO":  2.80, "ABBV":  3.60, "COST":  0.60,  "BAC":  2.40, "CAT":  1.50,
    "UNH":   1.60, "LRCX":  1.00, "KO":    3.05,  "PG":   2.41, "HD":   2.42,
    "MRK":   3.24, "GS":    2.00, "MS":    2.80,  "TXN":  2.95, "KLAC": 1.00,
    "ORCL":  1.50, "LIN":   1.50, "IBM":   2.80,  "AMGN": 3.20, "APH":  0.90,
    "MCD":   2.50, "PEP":   3.52, "ABT":   1.70,  "NEE":  2.80, "ADI":  1.70,
    "UNP":   2.30, "ETN":   1.00, "GILD":  3.30,  "BLK":  2.50, "QCOM": 2.25,
    "CB":    1.40, "SPGI":  0.90, "DHR":   0.50,  "SBUX": 3.00, "LMT":  2.70,
    "LOW":   1.80, "SYK":   1.00, "MDT":   3.70,  "BNY":  2.80, "ADP":  3.00,
    "ACN":   1.70, "MCK":   0.50, "PNC":   3.30,  "GD":   2.20, "CME":  4.50,
    "CSX":   1.30, "MMC":   1.40, "CMI":   2.50,  "INTU": 0.70, "PSX":  3.50,
    "ICE":   1.20, "WM":    1.40, "EMR":   2.00,  "ELV":  1.50, "TRV":  2.00,
    "MDLZ":  2.80, "SHW":   1.00, "HON":   2.30,  "AON":  0.80, "CL":   2.20,
    "ITW":   2.30, "NOC":   1.70, "MSI":   1.00,  "MCO":  0.90, "AEP":  3.80,
    "ECL":   1.20, "CTAS":  1.00, "ALL":   2.00,  "AJG":  0.80, "APD":  2.50,
    "FIX":   0.30, "COR":   0.50, "TEL":   1.50,  "GWW":  0.80, "NUE":  1.10,
    "AFL":   2.50, "SRE":   3.10, "FAST":  2.10,  "CAH":  1.90, "ROK":  1.80,
    "MET":   3.00, "LHX":   2.10, "NKE":   2.10,  "FITB": 3.53, "STT":  3.10,
    "XEL":   3.50, "ETR":   2.70, "AMP":   1.30,  "BDX":  1.80, "SYY":  2.40,
}

# VIG Top 100 Holdings (source: Vanguard/AlphaVantage, Aug 2026)
VIG_HOLDINGS = [
    {"ticker": "AVGO",  "name": "Broadcom Inc",                         "weight": 4.62},
    {"ticker": "AAPL",  "name": "Apple Inc",                            "weight": 4.44},
    {"ticker": "MSFT",  "name": "Microsoft Corp",                       "weight": 4.33},
    {"ticker": "JPM",   "name": "JPMorgan Chase & Co",                  "weight": 4.06},
    {"ticker": "LLY",   "name": "Eli Lilly and Co",                     "weight": 3.92},
    {"ticker": "XOM",   "name": "Exxon Mobil Corp",                     "weight": 2.78},
    {"ticker": "JNJ",   "name": "Johnson & Johnson",                    "weight": 2.66},
    {"ticker": "V",     "name": "Visa Inc",                             "weight": 2.44},
    {"ticker": "WMT",   "name": "Walmart Inc",                          "weight": 2.10},
    {"ticker": "MA",    "name": "Mastercard Inc",                       "weight": 1.99},
    {"ticker": "CSCO",  "name": "Cisco Systems Inc",                    "weight": 1.98},
    {"ticker": "ABBV",  "name": "AbbVie Inc",                           "weight": 1.91},
    {"ticker": "COST",  "name": "Costco Wholesale Corp",                "weight": 1.82},
    {"ticker": "BAC",   "name": "Bank of America Corp",                 "weight": 1.74},
    {"ticker": "CAT",   "name": "Caterpillar Inc",                      "weight": 1.62},
    {"ticker": "UNH",   "name": "UnitedHealth Group Inc",               "weight": 1.62},
    {"ticker": "LRCX",  "name": "Lam Research Corp",                    "weight": 1.58},
    {"ticker": "KO",    "name": "Coca-Cola Co",                         "weight": 1.46},
    {"ticker": "PG",    "name": "Procter & Gamble Co",                  "weight": 1.45},
    {"ticker": "HD",    "name": "The Home Depot Inc",                   "weight": 1.43},
    {"ticker": "MRK",   "name": "Merck & Co Inc",                       "weight": 1.39},
    {"ticker": "GS",    "name": "The Goldman Sachs Group Inc",          "weight": 1.23},
    {"ticker": "MS",    "name": "Morgan Stanley",                       "weight": 1.09},
    {"ticker": "TXN",   "name": "Texas Instruments Inc",                "weight": 1.08},
    {"ticker": "KLAC",  "name": "KLA Corp",                             "weight": 1.03},
    {"ticker": "ORCL",  "name": "Oracle Corp",                          "weight": 0.95},
    {"ticker": "LIN",   "name": "Linde PLC",                            "weight": 0.95},
    {"ticker": "IBM",   "name": "IBM Corp",                             "weight": 0.91},
    {"ticker": "AMGN",  "name": "Amgen Inc",                            "weight": 0.90},
    {"ticker": "APH",   "name": "Amphenol Corp",                        "weight": 0.85},
    {"ticker": "MCD",   "name": "McDonald's Corp",                      "weight": 0.83},
    {"ticker": "PEP",   "name": "PepsiCo Inc",                          "weight": 0.82},
    {"ticker": "ABT",   "name": "Abbott Laboratories",                  "weight": 0.79},
    {"ticker": "NEE",   "name": "NextEra Energy Inc",                   "weight": 0.78},
    {"ticker": "ADI",   "name": "Analog Devices Inc",                   "weight": 0.77},
    {"ticker": "UNP",   "name": "Union Pacific Corp",                   "weight": 0.75},
    {"ticker": "ETN",   "name": "Eaton Corp PLC",                       "weight": 0.70},
    {"ticker": "GILD",  "name": "Gilead Sciences Inc",                  "weight": 0.70},
    {"ticker": "BLK",   "name": "BlackRock Inc",                        "weight": 0.68},
    {"ticker": "QCOM",  "name": "Qualcomm Inc",                         "weight": 0.67},
    {"ticker": "CB",    "name": "Chubb Ltd",                            "weight": 0.55},
    {"ticker": "SPGI",  "name": "S&P Global Inc",                       "weight": 0.54},
    {"ticker": "DHR",   "name": "Danaher Corp",                         "weight": 0.53},
    {"ticker": "SBUX",  "name": "Starbucks Corp",                       "weight": 0.52},
    {"ticker": "LMT",   "name": "Lockheed Martin Corp",                 "weight": 0.51},
    {"ticker": "LOW",   "name": "Lowe's Companies Inc",                 "weight": 0.50},
    {"ticker": "SYK",   "name": "Stryker Corp",                         "weight": 0.48},
    {"ticker": "MDT",   "name": "Medtronic PLC",                        "weight": 0.47},
    {"ticker": "BNY",   "name": "Bank of New York Mellon Corp",         "weight": 0.46},
    {"ticker": "ADP",   "name": "Automatic Data Processing Inc",        "weight": 0.46},
    {"ticker": "ACN",   "name": "Accenture PLC",                        "weight": 0.44},
    {"ticker": "MCK",   "name": "McKesson Corp",                        "weight": 0.44},
    {"ticker": "PNC",   "name": "PNC Financial Services Group Inc",     "weight": 0.42},
    {"ticker": "GD",    "name": "General Dynamics Corp",                "weight": 0.42},
    {"ticker": "CME",   "name": "CME Group Inc",                        "weight": 0.42},
    {"ticker": "CSX",   "name": "CSX Corp",                             "weight": 0.40},
    {"ticker": "MMC",   "name": "Marsh & McLennan Companies Inc",       "weight": 0.39},
    {"ticker": "CMI",   "name": "Cummins Inc",                          "weight": 0.38},
    {"ticker": "INTU",  "name": "Intuit Inc",                           "weight": 0.38},
    {"ticker": "PSX",   "name": "Phillips 66",                          "weight": 0.37},
    {"ticker": "ICE",   "name": "Intercontinental Exchange Inc",        "weight": 0.37},
    {"ticker": "WM",    "name": "Waste Management Inc",                 "weight": 0.36},
    {"ticker": "EMR",   "name": "Emerson Electric Co",                  "weight": 0.36},
    {"ticker": "ELV",   "name": "Elevance Health Inc",                  "weight": 0.34},
    {"ticker": "TRV",   "name": "The Travelers Companies Inc",          "weight": 0.34},
    {"ticker": "MDLZ",  "name": "Mondelez International Inc",           "weight": 0.34},
    {"ticker": "SHW",   "name": "Sherwin-Williams Co",                  "weight": 0.34},
    {"ticker": "HON",   "name": "Honeywell International Inc",          "weight": 0.33},
    {"ticker": "AON",   "name": "Aon PLC",                              "weight": 0.33},
    {"ticker": "CL",    "name": "Colgate-Palmolive Co",                 "weight": 0.32},
    {"ticker": "ITW",   "name": "Illinois Tool Works Inc",              "weight": 0.32},
    {"ticker": "NOC",   "name": "Northrop Grumman Corp",                "weight": 0.31},
    {"ticker": "MSI",   "name": "Motorola Solutions Inc",               "weight": 0.31},
    {"ticker": "MCO",   "name": "Moody's Corp",                         "weight": 0.31},
    {"ticker": "AEP",   "name": "American Electric Power Co Inc",       "weight": 0.30},
    {"ticker": "ECL",   "name": "Ecolab Inc",                           "weight": 0.30},
    {"ticker": "CTAS",  "name": "Cintas Corp",                          "weight": 0.30},
    {"ticker": "ALL",   "name": "Allstate Corp",                        "weight": 0.29},
    {"ticker": "AJG",   "name": "Arthur J. Gallagher & Co",             "weight": 0.28},
    {"ticker": "APD",   "name": "Air Products and Chemicals Inc",       "weight": 0.28},
    {"ticker": "FIX",   "name": "Comfort Systems USA Inc",              "weight": 0.26},
    {"ticker": "COR",   "name": "Cencora Inc",                          "weight": 0.26},
    {"ticker": "TEL",   "name": "TE Connectivity PLC",                  "weight": 0.26},
    {"ticker": "GWW",   "name": "W.W. Grainger Inc",                    "weight": 0.26},
    {"ticker": "NUE",   "name": "Nucor Corp",                           "weight": 0.25},
    {"ticker": "AFL",   "name": "Aflac Inc",                            "weight": 0.25},
    {"ticker": "SRE",   "name": "Sempra",                               "weight": 0.25},
    {"ticker": "FAST",  "name": "Fastenal Co",                          "weight": 0.24},
    {"ticker": "CAH",   "name": "Cardinal Health Inc",                  "weight": 0.23},
    {"ticker": "ROK",   "name": "Rockwell Automation Inc",              "weight": 0.23},
    {"ticker": "MET",   "name": "MetLife Inc",                          "weight": 0.22},
    {"ticker": "LHX",   "name": "L3Harris Technologies Inc",            "weight": 0.22},
    {"ticker": "NKE",   "name": "Nike Inc",                             "weight": 0.22},
    {"ticker": "FITB",  "name": "Fifth Third Bancorp",                  "weight": 0.22},
    {"ticker": "STT",   "name": "State Street Corp",                    "weight": 0.22},
    {"ticker": "XEL",   "name": "Xcel Energy Inc",                      "weight": 0.21},
    {"ticker": "ETR",   "name": "Entergy Corp",                         "weight": 0.21},
    {"ticker": "AMP",   "name": "Ameriprise Financial Inc",             "weight": 0.21},
    {"ticker": "BDX",   "name": "Becton Dickinson & Co",                "weight": 0.20},
    {"ticker": "SYY",   "name": "Sysco Corp",                           "weight": 0.18},
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
    tickers = [h["ticker"] for h in VIG_HOLDINGS]

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
    for i, holding in enumerate(VIG_HOLDINGS):
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
