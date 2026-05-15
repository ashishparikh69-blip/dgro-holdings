# Vercel Python Serverless Function — /api/schd-holdings
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
    "CVX":  48.3, "COP":  23.1, "MRK":  48.1, "VZ":   61.8, "KO":   73.4,
    "TXN":  53.7, "PEP":  70.2, "AMGN": 57.2, "ABT":  57.8, "PG":   67.1,
    "UNH":  19.8, "QCOM": 35.2, "HD":   54.2, "LMT":  43.2, "BMY":  55.3,
    "ACN":  44.1, "MO":   76.2, "CMCSA":33.1, "EOG":  25.4, "ADP":  76.3,
    "BX":   87.1, "SLB":  24.3, "UPS":  70.1, "OKE":  73.2, "TGT":  50.1,
    "FAST": 69.1, "F":    47.8, "FITB": 38.2, "ADM":  52.3, "KMB":  73.2,
    "DVN":  25.3, "HSY":  65.1, "PAYX": 82.3, "CINF": 43.8, "ARES": 87.1,
    "DRI":  51.8, "RF":   38.4, "GIS":  59.8, "TROW": 62.3, "SNA":  38.2,
    "BR":   56.1, "SWKS": 40.2, "PFG":  36.1, "APA":  15.1, "EWBC": 24.1,
    "AFG":  22.1, "BBY":  42.1, "WSO":  50.2, "FNF":  44.8, "ALV":  35.1,
    "MTN":  55.2, "NXST": 35.2, "COLB": 49.8, "OZK":  29.8, "RHI":  47.8,
    "MC":   64.8, "MSM":  59.8, "FLO":  64.8, "FHI":  49.8, "WU":   74.8,
    "MUR":  21.8, "APAM": 84.8, "CVBF": 59.8, "NSP":  39.8, "BANR": 49.8,
    "CNS":  64.8, "WEN":  64.8, "OFG":  17.8, "CHCO": 49.8, "LKFN": 44.8,
    "STBA": 44.8, "BKE":  54.8, "GABC": 54.8, "VRTS": 44.8, "CNA":  34.8,
    "PFBC": 24.8,
    # New May 2026 additions
    "DINO": 25.0, "ORI":  45.2, "BAH":  35.0, "ERIE": 72.0, "M":    28.0,
    "KFY":  18.0, "PAG":  18.0, "MZTI": 40.0, "NBHC": 30.0, "AGM":  35.0,
    "IPAR": 34.0, "CWEN": 65.0, "SRCE": 35.0, "HAFC": 38.0, "THFF": 32.0,
    "ORRF": 35.0, "IBCP": 43.0, "CCBG": 38.0, "AMSF": 30.0, "OXM":  25.0,
    "EBF":  67.0, "ETD":  57.0, "CPF":  42.0,
}

# 1-year dividend growth rate (%) — updated May 2026
DIV_GROWTH_1Y = {
    "CVX":   8.3, "COP":   9.8, "MRK":   5.1, "VZ":    2.1, "KO":    5.2,
    "TXN":   5.2, "PEP":   7.3, "AMGN":  6.8, "ABT":   8.1, "PG":    5.1,
    "UNH":  11.8, "QCOM":  6.3, "HD":    8.1, "LMT":   5.1, "BMY":   3.2,
    "ACN":   9.8, "MO":    4.1, "CMCSA": 7.1, "EOG":   9.8, "ADP":  11.8,
    "BX":   19.8, "SLB":  15.2, "UPS":   5.1, "OKE":   3.8, "TGT":   1.8,
    "FAST":  8.8, "F":    66.7, "FITB":  4.8, "ADM":   2.1, "KMB":   1.8,
    "DVN": -10.0, "HSY":   5.2, "PAYX":  9.8, "CINF":  7.8, "ARES": 19.8,
    "DRI":   8.8, "RF":    7.8, "GIS":   1.2, "TROW":  1.8, "SNA":   9.8,
    "BR":   10.8, "SWKS":  9.8, "PFG":   6.2, "APA": -50.0, "EWBC": 11.8,
    "AFG":   9.8, "BBY":   4.8, "WSO":   9.1, "FNF":   7.8, "ALV":  14.8,
    "MTN":   0.0, "NXST":  4.8, "COLB":  2.8, "OZK":   2.8, "RHI":   4.8,
    "MC":    4.8, "MSM":   4.8, "FLO":   2.8, "FHI":   4.8, "WU":   -4.8,
    "MUR":   2.8, "APAM":  9.8, "CVBF":  2.8, "NSP":   4.8, "BANR":  2.8,
    "CNS":   5.8, "WEN":   2.8, "OFG":   9.8, "CHCO":  2.8, "LKFN":  4.8,
    "STBA":  2.8, "BKE":   4.8, "GABC":  2.8, "VRTS":  4.8, "CNA":   2.8,
    "PFBC":  4.8,
    # New May 2026 additions
    "DINO":  2.0, "ORI":   3.0, "BAH":   8.0, "ERIE": 11.0, "M":     5.0,
    "KFY":   5.0, "PAG":   8.0, "MZTI":  3.0, "NBHC":  3.0, "AGM":  15.0,
    "IPAR": 15.0, "CWEN":  5.0, "SRCE":  4.0, "HAFC":  5.0, "THFF":  4.0,
    "ORRF":  5.0, "IBCP":  5.0, "CCBG":  5.0, "AMSF":  4.0, "OXM":   4.0,
    "EBF":   3.0, "ETD":   5.0, "CPF":   5.0,
}

# 5-year dividend growth rate CAGR (%) — updated May 2026
DIV_GROWTH_5Y = {
    "CVX":   6.2, "COP":  15.8, "MRK":   9.1, "VZ":    2.0, "KO":    4.8,
    "TXN":  12.2, "PEP":   7.2, "AMGN":  8.8, "ABT":  10.2, "PG":    5.8,
    "UNH":  18.2, "QCOM":  7.2, "HD":   13.8, "LMT":   7.8, "BMY":   9.8,
    "ACN":  10.8, "MO":    5.8, "CMCSA":12.1, "EOG":  22.8, "ADP":  13.2,
    "BX":   25.0, "SLB":   4.8, "UPS":  12.8, "OKE":   3.8, "TGT":   9.2,
    "FAST": 11.8, "F":     0.0, "FITB":  7.8, "ADM":   5.8, "KMB":   3.8,
    "DVN":  38.0, "HSY":   8.2, "PAYX": 12.8, "CINF":  7.8, "ARES": 25.0,
    "DRI":  11.8, "RF":    9.8, "GIS":   4.8, "TROW":  4.8, "SNA":  11.8,
    "BR":   10.8, "SWKS": 13.8, "PFG":   6.2, "APA":   0.0, "EWBC": 14.8,
    "AFG":  12.8, "BBY":  14.8, "WSO":  12.8, "FNF":   9.8, "ALV":   8.8,
    "MTN":   9.8, "NXST": 15.8, "COLB":  4.8, "OZK":   4.8, "RHI":   9.8,
    "MC":    8.8, "MSM":   9.8, "FLO":   4.8, "FHI":   8.8, "WU":   -5.0,
    "MUR":   4.8, "APAM": 12.8, "CVBF":  4.8, "NSP":   9.8, "BANR":  4.8,
    "CNS":   9.8, "WEN":   5.8, "OFG":  14.8, "CHCO":  4.8, "LKFN":  7.8,
    "STBA":  4.8, "BKE":   8.8, "GABC":  5.8, "VRTS":  9.8, "CNA":   3.8,
    "PFBC":  7.8,
    # New May 2026 additions
    "DINO": 15.0, "ORI":   4.0, "BAH":  12.0, "ERIE":  8.0, "M":     0.0,
    "KFY":   8.0, "PAG":  12.0, "MZTI":  4.0, "NBHC":  5.0, "AGM":  12.0,
    "IPAR": 18.0, "CWEN":  4.0, "SRCE":  6.0, "HAFC":  4.0, "THFF":  4.0,
    "ORRF":  5.0, "IBCP":  5.0, "CCBG":  4.0, "AMSF":  4.0, "OXM":   8.0,
    "EBF":   3.0, "ETD":   8.0, "CPF":   3.0,
}

# Trailing 12-month dividend yields (%) — updated May 2026
DIVIDEND_YIELDS = {
    "CVX":   3.40, "COP":   3.50, "MRK":   3.24, "VZ":    5.53, "KO":    3.05,
    "TXN":   2.95, "PEP":   3.52, "AMGN":  3.20, "ABT":   1.70, "PG":    2.41,
    "UNH":   1.60, "QCOM":  2.25, "HD":    2.42, "LMT":   2.70, "BMY":   4.28,
    "ACN":   1.70, "MO":    7.50, "CMCSA": 4.55, "EOG":   3.05, "ADP":   3.00,
    "BX":    2.50, "SLB":   2.30, "UPS":   5.50, "OKE":   4.52, "TGT":   4.05,
    "FAST":  2.05, "F":     5.48, "FITB":  3.53, "ADM":   3.80, "KMB":   5.11,
    "DVN":   4.50, "HSY":   3.20, "PAYX":  4.60, "CINF":  2.20, "ARES":  3.20,
    "DRI":   3.50, "RF":    4.09, "GIS":   3.80, "TROW":  5.52, "SNA":   2.70,
    "BR":    1.90, "SWKS":  2.80, "PFG":   3.80, "APA":   4.50, "EWBC":  3.50,
    "AFG":   1.70, "BBY":   4.50, "WSO":   1.80, "FNF":   4.50, "ALV":   3.00,
    "MTN":   4.50, "NXST":  4.80, "COLB":  5.50, "OZK":   3.80, "RHI":   3.50,
    "MC":    4.00, "MSM":   4.00, "FLO":   4.50, "FHI":   3.50, "WU":    8.00,
    "MUR":   3.80, "APAM":  7.50, "CVBF":  4.00, "NSP":   1.80, "BANR":  4.50,
    "CNS":   3.80, "WEN":   6.00, "OFG":   2.80, "CHCO":  2.50, "LKFN":  3.50,
    "STBA":  4.50, "BKE":   4.50, "GABC":  3.00, "VRTS":  5.00, "CNA":   3.00,
    "PFBC":  3.50,
    # New May 2026 additions
    "DINO":  3.50, "ORI":   4.50, "BAH":   1.70, "ERIE":  1.30, "M":     5.50,
    "KFY":   1.40, "PAG":   2.00, "MZTI":  3.50, "NBHC":  2.50, "AGM":   3.80,
    "IPAR":  2.00, "CWEN":  5.50, "SRCE":  2.20, "HAFC":  4.50, "THFF":  2.80,
    "ORRF":  2.80, "IBCP":  3.80, "CCBG":  2.80, "AMSF":  2.50, "OXM":   2.50,
    "EBF":   4.00, "ETD":   4.50, "CPF":   3.80,
}

# SCHD Top 99 Holdings (source: Schwab/stockanalysis, May 2026 reconstitution)
SCHD_HOLDINGS = [
    {"ticker": "TXN",   "name": "Texas Instruments Inc",             "weight": 5.82},
    {"ticker": "QCOM",  "name": "Qualcomm Inc",                      "weight": 5.76},
    {"ticker": "UNH",   "name": "UnitedHealth Group Inc",            "weight": 5.42},
    {"ticker": "KO",    "name": "Coca-Cola Co",                      "weight": 4.09},
    {"ticker": "CVX",   "name": "Chevron Corp",                      "weight": 3.99},
    {"ticker": "VZ",    "name": "Verizon Communications",            "weight": 3.74},
    {"ticker": "MRK",   "name": "Merck & Co Inc",                    "weight": 3.73},
    {"ticker": "COP",   "name": "ConocoPhillips",                    "weight": 3.71},
    {"ticker": "PEP",   "name": "PepsiCo Inc",                       "weight": 3.70},
    {"ticker": "PG",    "name": "Procter & Gamble Co",               "weight": 3.63},
    {"ticker": "AMGN",  "name": "Amgen Inc",                         "weight": 3.54},
    {"ticker": "HD",    "name": "The Home Depot Inc",                "weight": 3.36},
    {"ticker": "MO",    "name": "Altria Group Inc",                  "weight": 3.01},
    {"ticker": "ABT",   "name": "Abbott Laboratories",               "weight": 2.98},
    {"ticker": "BMY",   "name": "Bristol-Myers Squibb Co",           "weight": 2.95},
    {"ticker": "LMT",   "name": "Lockheed Martin Corp",              "weight": 2.71},
    {"ticker": "ACN",   "name": "Accenture PLC",                     "weight": 2.68},
    {"ticker": "BX",    "name": "Blackstone Inc",                    "weight": 2.36},
    {"ticker": "CMCSA", "name": "Comcast Corp",                      "weight": 2.30},
    {"ticker": "ADP",   "name": "Automatic Data Processing",         "weight": 2.21},
    {"ticker": "SLB",   "name": "SLB Ltd",                           "weight": 2.14},
    {"ticker": "EOG",   "name": "EOG Resources Inc",                 "weight": 1.87},
    {"ticker": "UPS",   "name": "United Parcel Service Inc",         "weight": 1.87},
    {"ticker": "OKE",   "name": "ONEOK Inc",                         "weight": 1.43},
    {"ticker": "TGT",   "name": "Target Corp",                       "weight": 1.42},
    {"ticker": "DVN",   "name": "Devon Energy Corp",                 "weight": 1.38},
    {"ticker": "FAST",  "name": "Fastenal Co",                       "weight": 1.28},
    {"ticker": "F",     "name": "Ford Motor Co",                     "weight": 1.21},
    {"ticker": "FITB",  "name": "Fifth Third Bancorp",               "weight": 1.12},
    {"ticker": "ADM",   "name": "Archer-Daniels-Midland Co",         "weight": 1.00},
    {"ticker": "KMB",   "name": "Kimberly-Clark Corp",               "weight": 0.83},
    {"ticker": "PAYX",  "name": "Paychex Inc",                       "weight": 0.78},
    {"ticker": "HSY",   "name": "The Hershey Co",                    "weight": 0.73},
    {"ticker": "CINF",  "name": "Cincinnati Financial Corp",         "weight": 0.66},
    {"ticker": "ARES",  "name": "Ares Management Corp",              "weight": 0.65},
    {"ticker": "RF",    "name": "Regions Financial Corp",            "weight": 0.61},
    {"ticker": "DRI",   "name": "Darden Restaurants Inc",            "weight": 0.59},
    {"ticker": "TROW",  "name": "T. Rowe Price Group Inc",           "weight": 0.58},
    {"ticker": "PFG",   "name": "Principal Financial Group Inc",     "weight": 0.51},
    {"ticker": "SNA",   "name": "Snap-on Inc",                       "weight": 0.49},
    {"ticker": "GIS",   "name": "General Mills Inc",                 "weight": 0.46},
    {"ticker": "BR",    "name": "Broadridge Financial Solutions",    "weight": 0.45},
    {"ticker": "EWBC",  "name": "East West Bancorp Inc",             "weight": 0.43},
    {"ticker": "WSO",   "name": "Watsco Inc",                        "weight": 0.37},
    {"ticker": "APA",   "name": "APA Corp",                          "weight": 0.34},
    {"ticker": "FNF",   "name": "Fidelity National Financial Inc",   "weight": 0.31},
    {"ticker": "DINO",  "name": "HF Sinclair Corp",                  "weight": 0.29},
    {"ticker": "BBY",   "name": "Best Buy Co Inc",                   "weight": 0.29},
    {"ticker": "SWKS",  "name": "Skyworks Solutions Inc",            "weight": 0.26},
    {"ticker": "AFG",   "name": "American Financial Group Inc",      "weight": 0.24},
    {"ticker": "ORI",   "name": "Old Republic International Corp",   "weight": 0.23},
    {"ticker": "BAH",   "name": "Booz Allen Hamilton Holding Corp",  "weight": 0.22},
    {"ticker": "COLB",  "name": "Columbia Banking System Inc",       "weight": 0.22},
    {"ticker": "ALV",   "name": "Autoliv Inc",                       "weight": 0.21},
    {"ticker": "NXST",  "name": "Nexstar Media Group Inc",           "weight": 0.14},
    {"ticker": "ERIE",  "name": "Erie Indemnity Co",                 "weight": 0.14},
    {"ticker": "MUR",   "name": "Murphy Oil Corp",                   "weight": 0.13},
    {"ticker": "M",     "name": "Macy's Inc",                        "weight": 0.13},
    {"ticker": "MSM",   "name": "MSC Industrial Direct Co Inc",      "weight": 0.12},
    {"ticker": "OZK",   "name": "Bank OZK",                          "weight": 0.12},
    {"ticker": "MC",    "name": "Moelis & Co",                       "weight": 0.12},
    {"ticker": "MTN",   "name": "Vail Resorts Inc",                  "weight": 0.11},
    {"ticker": "FHI",   "name": "Federated Hermes Inc",              "weight": 0.11},
    {"ticker": "KFY",   "name": "Korn Ferry",                        "weight": 0.09},
    {"ticker": "PAG",   "name": "Penske Automotive Group Inc",       "weight": 0.08},
    {"ticker": "WU",    "name": "Western Union Co",                  "weight": 0.07},
    {"ticker": "APAM",  "name": "Artisan Partners Asset Mgmt",       "weight": 0.07},
    {"ticker": "CVBF",  "name": "CVB Financial Corp",                "weight": 0.06},
    {"ticker": "RHI",   "name": "Robert Half Inc",                   "weight": 0.06},
    {"ticker": "MZTI",  "name": "Mesquite Financial Inc",            "weight": 0.06},
    {"ticker": "BANR",  "name": "Banner Corp",                       "weight": 0.05},
    {"ticker": "CNS",   "name": "Cohen & Steers Inc",                "weight": 0.05},
    {"ticker": "OFG",   "name": "OFG Bancorp",                       "weight": 0.05},
    {"ticker": "NBHC",  "name": "National Bank Holdings Corp",       "weight": 0.05},
    {"ticker": "CHCO",  "name": "City Holding Co",                   "weight": 0.04},
    {"ticker": "STBA",  "name": "S&T Bancorp Inc",                   "weight": 0.04},
    {"ticker": "FLO",   "name": "Flowers Foods Inc",                 "weight": 0.04},
    {"ticker": "IPAR",  "name": "Inter Parfums Inc",                 "weight": 0.04},
    {"ticker": "AGM",   "name": "Federal Agricultural Mortgage Corp","weight": 0.04},
    {"ticker": "GABC",  "name": "German American Bancorp Inc",       "weight": 0.04},
    {"ticker": "LKFN",  "name": "Lakeland Financial Corp",           "weight": 0.04},
    {"ticker": "BKE",   "name": "The Buckle Inc",                    "weight": 0.03},
    {"ticker": "CWEN",  "name": "Clearway Energy Inc",               "weight": 0.03},
    {"ticker": "SRCE",  "name": "1st Source Corp",                   "weight": 0.03},
    {"ticker": "WEN",   "name": "The Wendy's Co",                    "weight": 0.03},
    {"ticker": "NSP",   "name": "Insperity Inc",                     "weight": 0.03},
    {"ticker": "PFBC",  "name": "Preferred Bank",                    "weight": 0.02},
    {"ticker": "CNA",   "name": "CNA Financial Corp",                "weight": 0.02},
    {"ticker": "VRTS",  "name": "Virtus Investment Partners Inc",    "weight": 0.02},
    {"ticker": "CPF",   "name": "Central Pacific Financial Corp",    "weight": 0.02},
    {"ticker": "HAFC",  "name": "Hanmi Financial Corp",              "weight": 0.02},
    {"ticker": "THFF",  "name": "First Financial Corp",              "weight": 0.02},
    {"ticker": "ORRF",  "name": "Orrstown Financial Services Inc",   "weight": 0.02},
    {"ticker": "IBCP",  "name": "Independent Bank Corp",             "weight": 0.02},
    {"ticker": "CCBG",  "name": "Capital City Holding Co",           "weight": 0.01},
    {"ticker": "AMSF",  "name": "AMERITAS Life Partners Corp",       "weight": 0.01},
    {"ticker": "OXM",   "name": "Oxford Industries Inc",             "weight": 0.01},
    {"ticker": "EBF",   "name": "Ennis Inc",                         "weight": 0.01},
    {"ticker": "ETD",   "name": "Ethan Allen Interiors Inc",         "weight": 0.01},
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
