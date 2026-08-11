# Vercel Python Serverless Function — /api/holdings
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
    # Top 20 (>1% weight)
    "MSFT": 24.8, "JPM":  25.2, "JNJ":  46.3, "AAPL": 15.2, "ABBV": 86.5,
    "XOM":  43.2, "AVGO": 46.1, "HD":   54.2, "PG":   67.1, "PM":   94.2,
    "KO":   73.4, "MRK":  48.1, "BAC":  29.0, "UNH":  20.0, "PEP":  70.2,
    "CSCO": 50.2, "AMGN": 53.1, "WFC":  30.1, "LLY":  12.8, "NEE":  57.8,
    # Holdings 21-100
    "HON":  50.2, "DE":   15.8, "APD":  62.1, "GD":   38.2, "SRE":  55.1,
    "FITB": 38.2, "CI":   21.3, "BNY":  25.3, "WM":   47.8, "XEL":  71.4,
    "CB":   21.3, "SYK":  16.8, "MET":  25.3, "APH":  35.8, "EMR":  44.2,
    "ED":   79.8, "ALL":  18.2, "INTU": 21.2, "BDX":  44.2, "PEG":  68.2,
    "ELV":  15.8, "NOC":  37.2, "ICE":  28.1, "ARES": 87.1, "KDP":  52.3,
    "WEC":  68.2, "AFL":  25.8, "FDX":  55.2, "ETR":  47.2, "LRCX": 42.3,
    "TRV":  26.4, "SPGI": 32.1, "SYY":  61.4, "DHR":  32.8, "FAST": 69.1,
    "CSX":  31.2, "PH":   18.2, "CMI":  22.8, "STT":  29.2, "KLAC": 42.1,
    "CRH":  15.8, "JCI":  42.8, "MTB":  42.8, "ADM":  52.3, "TEL":  35.8,
    "TT":   30.2, "DTE":  70.2, "ZTS":  24.8, "MSI":  36.2, "SHW":  25.2,
    "LHX":  38.2, "TMO":   8.2, "AJG":  26.2, "AEE":  72.8, "AON":  12.2,
    "YUM":  65.2, "ECL":  29.8, "FANG": 25.3, "GPC":  55.2, "MCHP": 65.8,
    "KR":   20.8, "HIG":  19.8, "NXPI": 38.2, "PCAR": 18.2, "PFG":  36.1,
    "DRI":  51.8, "GRMN": 40.2, "CTAS": 40.8, "FERG": 30.2, "AMP":  15.2,
    "OTIS": 32.8, "MCO":  25.8, "CARR": 26.2, "AWK":  55.2, "CMS":  72.8,
    "CINF": 43.8, "PPG":  38.2, "ROST": 22.8, "EVRG": 72.8, "ROK":  50.8,
}

# 1-year dividend growth rate (%) — updated Aug 2026
DIV_GROWTH_1Y = {
    # Top 20
    "MSFT": 10.2, "JPM":  10.0, "JNJ":   5.3, "AAPL":  4.0, "ABBV":  4.8,
    "XOM":   4.2, "AVGO": 14.2, "HD":    8.1, "PG":    5.1, "PM":    3.1,
    "KO":    5.2, "MRK":   5.1, "BAC":   8.3, "UNH":  12.0, "PEP":   7.3,
    "CSCO":  3.1, "AMGN":  6.5, "WFC":  25.0, "LLY":  15.0, "NEE":  10.0,
    # Holdings 21-100
    "HON":   5.0, "DE":    9.2, "APD":   1.2, "GD":    5.8, "SRE":   4.8,
    "FITB":  4.8, "CI":    8.1, "BNY":  12.5, "WM":    9.8, "XEL":   6.1,
    "CB":    4.8, "SYK":   6.8, "MET":   4.8, "APH":  10.8, "EMR":   0.8,
    "ED":    2.8, "ALL":   4.8, "INTU": 15.0, "BDX":   4.8, "PEG":   5.2,
    "ELV":   5.1, "NOC":   9.8, "ICE":   9.8, "ARES": 19.8, "KDP":   6.8,
    "WEC":   6.8, "AFL":  16.2, "FDX":  10.0, "ETR":   3.8, "LRCX": 15.8,
    "TRV":   4.8, "SPGI": 25.0, "SYY":   4.8, "DHR":   3.8, "FAST":  8.8,
    "CSX":   6.8, "PH":   14.8, "CMI":   7.8, "STT":   9.8, "KLAC": 15.8,
    "CRH":  20.0, "JCI":   5.8, "MTB":   3.8, "ADM":   2.1, "TEL":  10.0,
    "TT":   10.8, "DTE":   5.8, "ZTS":  15.0, "MSI":  10.8, "SHW":  10.8,
    "LHX":   5.8, "TMO":   5.8, "AJG":  12.8, "AEE":   5.8, "AON":  10.0,
    "YUM":   6.8, "ECL":   5.8, "FANG":  5.8, "GPC":   4.8, "MCHP": -8.0,
    "KR":    9.8, "HIG":  10.8, "NXPI": 11.8, "PCAR":  5.8, "PFG":   6.2,
    "DRI":   8.8, "GRMN":  9.8, "CTAS": 14.8, "FERG":  8.8, "AMP":  15.8,
    "OTIS": 12.8, "MCO":  15.8, "CARR": 10.8, "AWK":   8.8, "CMS":   6.8,
    "CINF":  7.8, "PPG":   5.8, "ROST": 10.8, "EVRG":  3.8, "ROK":   5.8,
}

# 5-year dividend growth rate CAGR (%) — updated Aug 2026
DIV_GROWTH_5Y = {
    # Top 20
    "MSFT": 10.7, "JPM":  14.2, "JNJ":   5.8, "AAPL":  5.8, "ABBV":  8.4,
    "XOM":   3.5, "AVGO": 17.5, "HD":   13.8, "PG":    5.8, "PM":    3.5,
    "KO":    4.8, "MRK":   9.1, "BAC":  12.8, "UNH":  18.2, "PEP":   7.2,
    "CSCO":  3.1, "AMGN":  7.8, "WFC":   0.0, "LLY":  15.0, "NEE":  10.0,
    # Holdings 21-100
    "HON":   5.0, "DE":   14.8, "APD":   8.2, "GD":    7.8, "SRE":   4.8,
    "FITB":  7.8, "CI":   12.8, "BNY":   8.8, "WM":   10.8, "XEL":   6.1,
    "CB":    4.8, "SYK":   6.8, "MET":   9.8, "APH":  10.8, "EMR":   2.8,
    "ED":    2.8, "ALL":   4.8, "INTU": 15.0, "BDX":   5.8, "PEG":   5.8,
    "ELV":   9.8, "NOC":   9.8, "ICE":  11.2, "ARES": 25.0, "KDP":   5.8,
    "WEC":   6.8, "AFL":  11.8, "FDX":  12.8, "ETR":   3.8, "LRCX": 25.0,
    "TRV":   4.8, "SPGI": 20.0, "SYY":   5.8, "DHR":  18.8, "FAST": 11.8,
    "CSX":  10.8, "PH":   12.8, "CMI":   8.8, "STT":   7.8, "KLAC": 25.0,
    "CRH":  15.0, "JCI":   4.8, "MTB":   3.8, "ADM":   5.8, "TEL":  12.8,
    "TT":   14.8, "DTE":   7.8, "ZTS":  28.0, "MSI":  10.8, "SHW":  15.8,
    "LHX":   9.8, "TMO":  11.8, "AJG":  14.8, "AEE":   6.8, "AON":   9.8,
    "YUM":   8.8, "ECL":   7.8, "FANG": 12.8, "GPC":   5.8, "MCHP": 18.8,
    "KR":   12.8, "HIG":  15.8, "NXPI": 18.8, "PCAR": 10.0, "PFG":   6.2,
    "DRI":  11.8, "GRMN": 18.8, "CTAS": 18.8, "FERG":  8.8, "AMP":  18.8,
    "OTIS": 15.8, "MCO":  18.8, "CARR": 15.8, "AWK":   9.8, "CMS":   6.8,
    "CINF":  7.8, "PPG":   5.8, "ROST": 15.8, "EVRG":  5.8, "ROK":   8.8,
}

# Trailing 12-month dividends per share ($) — updated Aug 2026 via Yahoo Finance chart events.
# Yield is computed live as annual_div / current_price so it stays accurate as prices move.
ANNUAL_DIVIDENDS = {
    # Top 20
    "MSFT":  3.56,  "JPM":   6.52,  "JNJ":   5.24,  "AAPL":  1.05,  "ABBV":  7.12,
    "XOM":   3.96,  "AVGO":  2.36,  "HD":    9.26,  "PG":    4.26,  "PM":    5.76,
    "KO":    2.06,  "MRK":   3.32,  "BAC":   1.04,  "UNH":   8.80,  "PEP":   5.60,
    "CSCO":  1.65,  "AMGN":  9.52,  "WFC":   1.40,  "LLY":   5.96,  "NEE":   2.22,
    # Holdings 21-100
    "HON":   4.52,  "DE":    6.48,  "APD":   7.18,  "GD":    5.88,  "SRE":   2.59,
    "FITB":  1.57,  "CI":    6.14,  "BNY":   1.80,  "WM":    3.36,  "XEL":   2.30,
    "CB":    1.94,  "SYK":   3.68,  "MET":   2.30,  "APH":   1.00,  "EMR":   2.19,
    "ED":    3.48,  "ALL":   5.16,  "INTU":  4.20,  "BDX":   4.04,  "PEG":   2.52,
    "ELV":   7.40,  "NOC":   9.28,  "ICE":   1.96,  "ARES":  3.20,  "KDP":   0.92,
    "WEC":   3.69,  "AFL":   2.00,  "FDX":   6.00,  "ETR":   2.52,  "LRCX": 10.00,
    "TRV":   4.40,  "SPGI":  3.80,  "SYY":   2.16,  "DHR":   1.44,  "FAST":  0.92,
    "CSX":   0.64,  "PH":    8.00,  "CMI":   9.60,  "STT":   3.28,  "KLAC":  6.64,
    "CRH":   1.60,  "JCI":   1.64,  "MTB":   5.60,  "ADM":   2.04,  "TEL":   2.64,
    "TT":    4.60,  "DTE":   4.24,  "ZTS":   1.60,  "MSI":   4.32,  "SHW":   3.24,
    "LHX":   5.48,  "TMO":   4.08,  "AJG":   2.80,  "AEE":   3.40,  "AON":   2.88,
    "YUM":   2.64,  "ECL":   2.44,  "FANG":  5.00,  "GPC":   4.19,  "MCHP":  1.72,
    "KR":    1.28,  "HIG":   2.52,  "NXPI":  4.56,  "PCAR":  4.00,  "PFG":   4.36,
    "DRI":   5.60,  "GRMN":  3.80,  "CTAS":  6.40,  "FERG":  2.80,  "AMP":   6.36,
    "OTIS":  1.44,  "MCO":   4.00,  "CARR":  1.16,  "AWK":   2.80,  "CMS":   2.36,
    "CINF":  3.55,  "PPG":   2.52,  "ROST":  1.36,  "EVRG":  2.52,  "ROK":   5.24,
}

# DGRO Top 100 Holdings (source: iShares / Alpha Vantage, Aug 2026) — sorted by weight desc
DGRO_HOLDINGS = [
    # ── Top 20 (>1% weight) ───────────────────────────────────────────────────
    {"ticker": "MSFT",  "name": "Microsoft Corp",                  "weight": 3.39},
    {"ticker": "JPM",   "name": "JPMorgan Chase & Co",             "weight": 3.17},
    {"ticker": "JNJ",   "name": "Johnson & Johnson",               "weight": 3.01},
    {"ticker": "AAPL",  "name": "Apple Inc",                       "weight": 2.83},
    {"ticker": "ABBV",  "name": "AbbVie Inc",                      "weight": 2.83},
    {"ticker": "XOM",   "name": "Exxon Mobil Corp",                "weight": 2.80},
    {"ticker": "AVGO",  "name": "Broadcom Inc",                    "weight": 2.70},
    {"ticker": "HD",    "name": "The Home Depot Inc",              "weight": 2.21},
    {"ticker": "PG",    "name": "Procter & Gamble Co",             "weight": 2.19},
    {"ticker": "PM",    "name": "Philip Morris International",     "weight": 2.06},
    {"ticker": "KO",    "name": "Coca-Cola Co",                    "weight": 1.91},
    {"ticker": "MRK",   "name": "Merck & Co Inc",                  "weight": 1.85},
    {"ticker": "BAC",   "name": "Bank of America Corp",            "weight": 1.83},
    {"ticker": "UNH",   "name": "UnitedHealth Group Inc",          "weight": 1.70},
    {"ticker": "PEP",   "name": "PepsiCo Inc",                     "weight": 1.63},
    {"ticker": "CSCO",  "name": "Cisco Systems Inc",               "weight": 1.38},
    {"ticker": "AMGN",  "name": "Amgen Inc",                       "weight": 1.30},
    {"ticker": "WFC",   "name": "Wells Fargo & Co",                "weight": 1.28},
    {"ticker": "LLY",   "name": "Eli Lilly and Co",                "weight": 1.14},
    {"ticker": "NEE",   "name": "NextEra Energy Inc",              "weight": 1.14},
    # ── Holdings 21–100 (<0.4% weight each) ──────────────────────────────────
    {"ticker": "HON",   "name": "Honeywell International Inc",     "weight": 0.38},
    {"ticker": "DE",    "name": "Deere & Co",                      "weight": 0.37},
    {"ticker": "APD",   "name": "Air Products & Chemicals",        "weight": 0.37},
    {"ticker": "GD",    "name": "General Dynamics Corp",           "weight": 0.37},
    {"ticker": "SRE",   "name": "Sempra",                          "weight": 0.36},
    {"ticker": "FITB",  "name": "Fifth Third Bancorp",             "weight": 0.34},
    {"ticker": "CI",    "name": "Cigna Group",                     "weight": 0.33},
    {"ticker": "BNY",   "name": "Bank of New York Mellon Corp",    "weight": 0.33},
    {"ticker": "WM",    "name": "Waste Management Inc",            "weight": 0.32},
    {"ticker": "XEL",   "name": "Xcel Energy Inc",                 "weight": 0.32},
    {"ticker": "CB",    "name": "Chubb Ltd",                       "weight": 0.32},
    {"ticker": "SYK",   "name": "Stryker Corp",                    "weight": 0.31},
    {"ticker": "MET",   "name": "MetLife Inc",                     "weight": 0.31},
    {"ticker": "APH",   "name": "Amphenol Corp",                   "weight": 0.30},
    {"ticker": "EMR",   "name": "Emerson Electric Co",             "weight": 0.30},
    {"ticker": "ED",    "name": "Consolidated Edison Inc",         "weight": 0.29},
    {"ticker": "ALL",   "name": "Allstate Corp",                   "weight": 0.29},
    {"ticker": "INTU",  "name": "Intuit Inc",                      "weight": 0.29},
    {"ticker": "BDX",   "name": "Becton Dickinson and Co",         "weight": 0.28},
    {"ticker": "PEG",   "name": "Public Service Enterprise Group", "weight": 0.28},
    {"ticker": "ELV",   "name": "Elevance Health Inc",             "weight": 0.28},
    {"ticker": "NOC",   "name": "Northrop Grumman Corp",           "weight": 0.28},
    {"ticker": "ICE",   "name": "Intercontinental Exchange",       "weight": 0.28},
    {"ticker": "ARES",  "name": "Ares Management Corp",            "weight": 0.27},
    {"ticker": "KDP",   "name": "Keurig Dr Pepper Inc",            "weight": 0.27},
    {"ticker": "WEC",   "name": "WEC Energy Group Inc",            "weight": 0.26},
    {"ticker": "AFL",   "name": "Aflac Inc",                       "weight": 0.26},
    {"ticker": "FDX",   "name": "FedEx Corp",                      "weight": 0.26},
    {"ticker": "ETR",   "name": "Entergy Corp",                    "weight": 0.25},
    {"ticker": "LRCX",  "name": "Lam Research Corp",               "weight": 0.25},
    {"ticker": "TRV",   "name": "Travelers Companies Inc",         "weight": 0.25},
    {"ticker": "SPGI",  "name": "S&P Global Inc",                  "weight": 0.25},
    {"ticker": "SYY",   "name": "Sysco Corp",                      "weight": 0.25},
    {"ticker": "DHR",   "name": "Danaher Corp",                    "weight": 0.24},
    {"ticker": "FAST",  "name": "Fastenal Co",                     "weight": 0.24},
    {"ticker": "CSX",   "name": "CSX Corp",                        "weight": 0.24},
    {"ticker": "PH",    "name": "Parker Hannifin Corp",            "weight": 0.23},
    {"ticker": "CMI",   "name": "Cummins Inc",                     "weight": 0.23},
    {"ticker": "STT",   "name": "State Street Corp",               "weight": 0.22},
    {"ticker": "KLAC",  "name": "KLA Corp",                        "weight": 0.22},
    {"ticker": "CRH",   "name": "CRH PLC",                         "weight": 0.22},
    {"ticker": "JCI",   "name": "Johnson Controls International",  "weight": 0.21},
    {"ticker": "MTB",   "name": "M&T Bank Corp",                   "weight": 0.21},
    {"ticker": "ADM",   "name": "Archer-Daniels-Midland Co",       "weight": 0.20},
    {"ticker": "TEL",   "name": "TE Connectivity Ltd",             "weight": 0.20},
    {"ticker": "TT",    "name": "Trane Technologies PLC",          "weight": 0.20},
    {"ticker": "DTE",   "name": "DTE Energy Co",                   "weight": 0.20},
    {"ticker": "ZTS",   "name": "Zoetis Inc",                      "weight": 0.19},
    {"ticker": "MSI",   "name": "Motorola Solutions Inc",          "weight": 0.19},
    {"ticker": "SHW",   "name": "Sherwin-Williams Co",             "weight": 0.19},
    {"ticker": "LHX",   "name": "L3Harris Technologies Inc",       "weight": 0.18},
    {"ticker": "TMO",   "name": "Thermo Fisher Scientific Inc",    "weight": 0.18},
    {"ticker": "AJG",   "name": "Arthur J. Gallagher & Co",        "weight": 0.18},
    {"ticker": "AEE",   "name": "Ameren Corp",                     "weight": 0.18},
    {"ticker": "AON",   "name": "Aon PLC",                         "weight": 0.17},
    {"ticker": "YUM",   "name": "Yum! Brands Inc",                 "weight": 0.17},
    {"ticker": "ECL",   "name": "Ecolab Inc",                      "weight": 0.17},
    {"ticker": "FANG",  "name": "Diamondback Energy Inc",          "weight": 0.17},
    {"ticker": "GPC",   "name": "Genuine Parts Co",                "weight": 0.17},
    {"ticker": "MCHP",  "name": "Microchip Technology Inc",        "weight": 0.17},
    {"ticker": "KR",    "name": "Kroger Co",                       "weight": 0.16},
    {"ticker": "HIG",   "name": "Hartford Financial Services",     "weight": 0.16},
    {"ticker": "NXPI",  "name": "NXP Semiconductors NV",           "weight": 0.16},
    {"ticker": "PCAR",  "name": "Paccar Inc",                      "weight": 0.16},
    {"ticker": "PFG",   "name": "Principal Financial Group Inc",   "weight": 0.16},
    {"ticker": "DRI",   "name": "Darden Restaurants Inc",          "weight": 0.16},
    {"ticker": "GRMN",  "name": "Garmin Ltd",                      "weight": 0.16},
    {"ticker": "CTAS",  "name": "Cintas Corp",                     "weight": 0.16},
    {"ticker": "FERG",  "name": "Ferguson Enterprises Inc",        "weight": 0.16},
    {"ticker": "AMP",   "name": "Ameriprise Financial Inc",        "weight": 0.16},
    {"ticker": "OTIS",  "name": "Otis Worldwide Corp",             "weight": 0.15},
    {"ticker": "MCO",   "name": "Moody's Corp",                    "weight": 0.15},
    {"ticker": "CARR",  "name": "Carrier Global Corp",             "weight": 0.15},
    {"ticker": "AWK",   "name": "American Water Works Co",         "weight": 0.15},
    {"ticker": "CMS",   "name": "CMS Energy Corp",                 "weight": 0.15},
    {"ticker": "CINF",  "name": "Cincinnati Financial Corp",       "weight": 0.14},
    {"ticker": "PPG",   "name": "PPG Industries Inc",              "weight": 0.14},
    {"ticker": "ROST",  "name": "Ross Stores Inc",                 "weight": 0.14},
    {"ticker": "EVRG",  "name": "Evergy Inc",                      "weight": 0.14},
    {"ticker": "ROK",   "name": "Rockwell Automation Inc",         "weight": 0.14},
]

# ── Separate caches ───────────────────────────────────────────────────────────
# Stooq:   52W low/high + last-close fallback — refresh every 24 h
# Finnhub: real-time price, staggered batches — each half refreshes every 60 s,
#          alternating so we never fire >50 calls at once (well within 60/min limit).
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
            "yield":            round(ANNUAL_DIVIDENDS.get(ticker, 0) / price * 100, 2) if price else None,
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
