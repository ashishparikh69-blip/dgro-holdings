from flask import Flask, jsonify, send_from_directory
import yfinance as yf
import time
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__, static_folder='public')

# DGRO Top 100 Holdings (source: iShares, approximate weights)
DGRO_HOLDINGS = [
    {"ticker": "AAPL", "name": "Apple Inc", "weight": 3.18},
    {"ticker": "MSFT", "name": "Microsoft Corp", "weight": 3.05},
    {"ticker": "JPM", "name": "JPMorgan Chase & Co", "weight": 2.89},
    {"ticker": "ABBV", "name": "AbbVie Inc", "weight": 2.72},
    {"ticker": "AVGO", "name": "Broadcom Inc", "weight": 2.55},
    {"ticker": "HD", "name": "Home Depot Inc", "weight": 2.21},
    {"ticker": "JNJ", "name": "Johnson & Johnson", "weight": 2.15},
    {"ticker": "PG", "name": "Procter & Gamble Co", "weight": 1.98},
    {"ticker": "XOM", "name": "Exxon Mobil Corp", "weight": 1.85},
    {"ticker": "CVX", "name": "Chevron Corp", "weight": 1.62},
    {"ticker": "MRK", "name": "Merck & Co Inc", "weight": 1.58},
    {"ticker": "PFE", "name": "Pfizer Inc", "weight": 1.52},
    {"ticker": "CSCO", "name": "Cisco Systems Inc", "weight": 1.48},
    {"ticker": "KO", "name": "Coca-Cola Co", "weight": 1.45},
    {"ticker": "PEP", "name": "PepsiCo Inc", "weight": 1.42},
    {"ticker": "VZ", "name": "Verizon Communications", "weight": 1.38},
    {"ticker": "CMCSA", "name": "Comcast Corp", "weight": 1.35},
    {"ticker": "TXN", "name": "Texas Instruments Inc", "weight": 1.32},
    {"ticker": "PM", "name": "Philip Morris International", "weight": 1.28},
    {"ticker": "BMY", "name": "Bristol-Myers Squibb", "weight": 1.25},
    {"ticker": "UNP", "name": "Union Pacific Corp", "weight": 1.22},
    {"ticker": "QCOM", "name": "Qualcomm Inc", "weight": 1.18},
    {"ticker": "RTX", "name": "RTX Corp", "weight": 1.15},
    {"ticker": "LOW", "name": "Lowe's Companies Inc", "weight": 1.12},
    {"ticker": "MDT", "name": "Medtronic PLC", "weight": 1.08},
    {"ticker": "MS", "name": "Morgan Stanley", "weight": 1.05},
    {"ticker": "BLK", "name": "BlackRock Inc", "weight": 1.02},
    {"ticker": "SCHW", "name": "Charles Schwab Corp", "weight": 0.98},
    {"ticker": "C", "name": "Citigroup Inc", "weight": 0.95},
    {"ticker": "CB", "name": "Chubb Ltd", "weight": 0.92},
    {"ticker": "GS", "name": "Goldman Sachs Group", "weight": 0.87},
    {"ticker": "ADI", "name": "Analog Devices Inc", "weight": 0.85},
    {"ticker": "DE", "name": "Deere & Co", "weight": 0.83},
    {"ticker": "SO", "name": "Southern Company", "weight": 0.81},
    {"ticker": "DUK", "name": "Duke Energy Corp", "weight": 0.79},
    {"ticker": "ITW", "name": "Illinois Tool Works", "weight": 0.77},
    {"ticker": "CI", "name": "Cigna Group", "weight": 0.75},
    {"ticker": "USB", "name": "U.S. Bancorp", "weight": 0.73},
    {"ticker": "PNC", "name": "PNC Financial Services", "weight": 0.71},
    {"ticker": "ADP", "name": "Automatic Data Processing", "weight": 0.69},
    {"ticker": "TGT", "name": "Target Corp", "weight": 0.67},
    {"ticker": "MMM", "name": "3M Company", "weight": 0.65},
    {"ticker": "EMR", "name": "Emerson Electric Co", "weight": 0.63},
    {"ticker": "FIS", "name": "Fidelity National Info", "weight": 0.61},
    {"ticker": "APD", "name": "Air Products & Chemicals", "weight": 0.59},
    {"ticker": "NSC", "name": "Norfolk Southern Corp", "weight": 0.57},
    {"ticker": "CME", "name": "CME Group Inc", "weight": 0.56},
    {"ticker": "ICE", "name": "Intercontinental Exchange", "weight": 0.55},
    {"ticker": "EOG", "name": "EOG Resources Inc", "weight": 0.54},
    {"ticker": "CL", "name": "Colgate-Palmolive Co", "weight": 0.53},
    {"ticker": "WMB", "name": "Williams Companies Inc", "weight": 0.52},
    {"ticker": "F", "name": "Ford Motor Co", "weight": 0.51},
    {"ticker": "GM", "name": "General Motors Co", "weight": 0.50},
    {"ticker": "MET", "name": "MetLife Inc", "weight": 0.49},
    {"ticker": "PRU", "name": "Prudential Financial", "weight": 0.48},
    {"ticker": "AIG", "name": "American Intl Group", "weight": 0.47},
    {"ticker": "TRV", "name": "Travelers Companies", "weight": 0.46},
    {"ticker": "ALL", "name": "Allstate Corp", "weight": 0.45},
    {"ticker": "D", "name": "Dominion Energy Inc", "weight": 0.44},
    {"ticker": "SRE", "name": "Sempra", "weight": 0.43},
    {"ticker": "AEP", "name": "American Electric Power", "weight": 0.42},
    {"ticker": "WEC", "name": "WEC Energy Group", "weight": 0.41},
    {"ticker": "XEL", "name": "Xcel Energy Inc", "weight": 0.40},
    {"ticker": "ETR", "name": "Entergy Corp", "weight": 0.39},
    {"ticker": "PPL", "name": "PPL Corp", "weight": 0.38},
    {"ticker": "ED", "name": "Consolidated Edison", "weight": 0.37},
    {"ticker": "FITB", "name": "Fifth Third Bancorp", "weight": 0.36},
    {"ticker": "KEY", "name": "KeyCorp", "weight": 0.35},
    {"ticker": "RF", "name": "Regions Financial Corp", "weight": 0.34},
    {"ticker": "CFG", "name": "Citizens Financial Group", "weight": 0.33},
    {"ticker": "HBAN", "name": "Huntington Bancshares", "weight": 0.32},
    {"ticker": "NTRS", "name": "Northern Trust Corp", "weight": 0.31},
    {"ticker": "STT", "name": "State Street Corp", "weight": 0.30},
    {"ticker": "IP", "name": "International Paper", "weight": 0.29},
    {"ticker": "NUE", "name": "Nucor Corp", "weight": 0.28},
    {"ticker": "PAYX", "name": "Paychex Inc", "weight": 0.27},
    {"ticker": "FAST", "name": "Fastenal Co", "weight": 0.26},
    {"ticker": "GPC", "name": "Genuine Parts Co", "weight": 0.25},
    {"ticker": "OMC", "name": "Omnicom Group Inc", "weight": 0.24},
    {"ticker": "HPQ", "name": "HP Inc", "weight": 0.23},
    {"ticker": "KMB", "name": "Kimberly-Clark Corp", "weight": 0.22},
    {"ticker": "SYY", "name": "Sysco Corp", "weight": 0.21},
    {"ticker": "CAH", "name": "Cardinal Health Inc", "weight": 0.20},
    {"ticker": "TROW", "name": "T. Rowe Price Group", "weight": 0.19},
    {"ticker": "BEN", "name": "Franklin Resources", "weight": 0.18},
    {"ticker": "LEN", "name": "Lennar Corp", "weight": 0.16},
    {"ticker": "DHI", "name": "D.R. Horton Inc", "weight": 0.15},
    {"ticker": "PHM", "name": "PulteGroup Inc", "weight": 0.14},
    {"ticker": "OKE", "name": "ONEOK Inc", "weight": 0.13},
    {"ticker": "KMI", "name": "Kinder Morgan Inc", "weight": 0.12},
    {"ticker": "CINF", "name": "Cincinnati Financial", "weight": 0.11},
    {"ticker": "AMCR", "name": "Amcor PLC", "weight": 0.10},
    {"ticker": "FNF", "name": "Fidelity National Financial", "weight": 0.09},
    {"ticker": "CMA", "name": "Comerica Inc", "weight": 0.08},
    {"ticker": "ZION", "name": "Zions Bancorporation", "weight": 0.07},
    {"ticker": "OGN", "name": "Organon & Co", "weight": 0.06},
    {"ticker": "UGI", "name": "UGI Corp", "weight": 0.05},
    {"ticker": "FAF", "name": "First American Financial", "weight": 0.04},
]

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

# Cache
cache = {"data": None, "timestamp": 0}
schd_cache = {"data": None, "timestamp": 0}
intersection_cache = {"data": None, "timestamp": 0}
CACHE_DURATION = 600  # 10 minutes
cache_lock = threading.Lock()
schd_cache_lock = threading.Lock()
intersection_cache_lock = threading.Lock()


def fetch_single_ticker(ticker):
    try:
        tk = yf.Ticker(ticker)
        fi = tk.fast_info
        price = getattr(fi, "last_price", None)
        low52 = getattr(fi, "year_low", None)
        high52 = getattr(fi, "year_high", None)
        # Get dividend yield from info (not available in fast_info)
        info = tk.info
        # trailingAnnualDividendYield is consistently a decimal (0.0306 = 3.06%)
        raw_yield = info.get("trailingAnnualDividendYield")
        div_yield = round(raw_yield * 100, 2) if raw_yield else None
        return ticker, {
            "price": round(price, 2) if price else None,
            "yield": div_yield,
            "fiftyTwoWeekLow": round(low52, 2) if low52 else None,
            "fiftyTwoWeekHigh": round(high52, 2) if high52 else None,
        }
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return ticker, None


def fetch_fund_data(holdings_list, fund_cache, fund_cache_lock):
    now = time.time()
    with fund_cache_lock:
        if fund_cache["data"] and (now - fund_cache["timestamp"]) < CACHE_DURATION:
            return fund_cache["data"]

    tickers = [h["ticker"] for h in holdings_list]
    infos = {}

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_single_ticker, t): t for t in tickers}
        for future in as_completed(futures):
            ticker, data = future.result()
            if data:
                infos[ticker] = data

    results = []
    for i, holding in enumerate(holdings_list):
        ticker = holding["ticker"]
        info = infos.get(ticker)
        if info:
            price = info["price"]
            low52 = info["fiftyTwoWeekLow"]
            variance = None
            if price and low52 and low52 > 0:
                variance = round((price - low52) / low52 * 100, 2)
            results.append({
                "rank": i + 1,
                "ticker": ticker,
                "name": holding["name"],
                "weight": holding["weight"],
                "price": info["price"],
                "yield": info["yield"],
                "fiftyTwoWeekLow": info["fiftyTwoWeekLow"],
                "fiftyTwoWeekHigh": info["fiftyTwoWeekHigh"],
                "varianceFromLow": variance,
            })
        else:
            results.append({
                "rank": i + 1,
                "ticker": ticker,
                "name": holding["name"],
                "weight": holding["weight"],
                "price": None,
                "yield": None,
                "fiftyTwoWeekLow": None,
                "fiftyTwoWeekHigh": None,
                "varianceFromLow": None,
            })

    with fund_cache_lock:
        fund_cache["data"] = results
        fund_cache["timestamp"] = time.time()

    return results


def fetch_holdings_data():
    return fetch_fund_data(DGRO_HOLDINGS, cache, cache_lock)


def fetch_schd_data():
    return fetch_fund_data(SCHD_HOLDINGS, schd_cache, schd_cache_lock)


# 25 stocks present in both DGRO and SCHD
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


def fetch_intersection_data():
    now = time.time()
    with intersection_cache_lock:
        if intersection_cache["data"] and (now - intersection_cache["timestamp"]) < CACHE_DURATION:
            return intersection_cache["data"]

    tickers = [h["ticker"] for h in INTERSECTION_HOLDINGS]
    infos = {}
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_single_ticker, t): t for t in tickers}
        for future in as_completed(futures):
            ticker, data = future.result()
            if data:
                infos[ticker] = data

    results = []
    for holding in INTERSECTION_HOLDINGS:
        ticker = holding["ticker"]
        info = infos.get(ticker)
        if info:
            price = info["price"]
            low52 = info["fiftyTwoWeekLow"]
            variance = round((price - low52) / low52 * 100, 2) if price and low52 and low52 > 0 else None
            results.append({
                "ticker":          ticker,
                "name":            holding["name"],
                "dgroWeight":      holding["dgroWeight"],
                "schdWeight":      holding["schdWeight"],
                "price":           info["price"],
                "yield":           info["yield"],
                "fiftyTwoWeekLow": info["fiftyTwoWeekLow"],
                "fiftyTwoWeekHigh":info["fiftyTwoWeekHigh"],
                "varianceFromLow": variance,
            })
        else:
            results.append({
                "ticker":          ticker,
                "name":            holding["name"],
                "dgroWeight":      holding["dgroWeight"],
                "schdWeight":      holding["schdWeight"],
                "price":           None,
                "yield":           None,
                "fiftyTwoWeekLow": None,
                "fiftyTwoWeekHigh":None,
                "varianceFromLow": None,
            })

    with intersection_cache_lock:
        intersection_cache["data"] = results
        intersection_cache["timestamp"] = time.time()

    return results


@app.route("/")
def index():
    return send_from_directory("public", "index.html")


@app.route("/schd")
def schd():
    return send_from_directory("public", "schd.html")


@app.route("/intersection")
def intersection():
    return send_from_directory("public", "intersection.html")


@app.route("/api/holdings")
def get_holdings():
    try:
        data = fetch_holdings_data()
        return jsonify({
            "holdings": data,
            "lastUpdated": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(cache["timestamp"]))
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/schd-holdings")
def get_schd_holdings():
    try:
        data = fetch_schd_data()
        return jsonify({
            "holdings": data,
            "lastUpdated": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(schd_cache["timestamp"]))
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/intersection-holdings")
def get_intersection_holdings():
    try:
        data = fetch_intersection_data()
        return jsonify({
            "holdings": data,
            "lastUpdated": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(intersection_cache["timestamp"]))
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, debug=False)
