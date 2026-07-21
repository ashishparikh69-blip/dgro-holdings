from http.server import BaseHTTPRequestHandler
import json, urllib.request, urllib.error, re
from datetime import datetime, timezone, date

TREASURY_BONDS = [
    {"cusip": "912810UV8", "type": "20Y Bond", "coupon": 5.000, "maturityDate": "2046-05-15"},
    {"cusip": "912810UT3", "type": "20Y Bond", "coupon": 4.625, "maturityDate": "2046-02-15"},
    {"cusip": "912810UQ9", "type": "20Y Bond", "coupon": 4.625, "maturityDate": "2045-11-15"},
    {"cusip": "912810UN6", "type": "20Y Bond", "coupon": 4.875, "maturityDate": "2045-08-15"},
    {"cusip": "912810UL0", "type": "20Y Bond", "coupon": 5.000, "maturityDate": "2045-05-15"},
    {"cusip": "91282CQQ7", "type": "10Y Note", "coupon": 4.375, "maturityDate": "2036-05-15"},
    {"cusip": "91282CPZ8", "type": "10Y Note", "coupon": 4.125, "maturityDate": "2036-02-15"},
    {"cusip": "91282CPJ4", "type": "10Y Note", "coupon": 4.000, "maturityDate": "2035-11-15"},
    {"cusip": "91282CNT4", "type": "10Y Note", "coupon": 4.250, "maturityDate": "2035-08-15"},
    {"cusip": "91282CNC1", "type": "10Y Note", "coupon": 4.250, "maturityDate": "2035-05-15"},
    {"cusip": "91282CQT1", "type": "7Y Note",  "coupon": 4.250, "maturityDate": "2033-05-31"},
    {"cusip": "91282CQN4", "type": "7Y Note",  "coupon": 4.125, "maturityDate": "2033-04-30"},
    {"cusip": "91282CQF1", "type": "7Y Note",  "coupon": 4.250, "maturityDate": "2033-03-31"},
    {"cusip": "91282CQC8", "type": "7Y Note",  "coupon": 3.750, "maturityDate": "2033-02-28"},
    {"cusip": "91282CPY1", "type": "7Y Note",  "coupon": 4.000, "maturityDate": "2033-01-31"},
    {"cusip": "91282CPQ8", "type": "7Y Note",  "coupon": 3.875, "maturityDate": "2032-12-31"},
    {"cusip": "91282CPM7", "type": "7Y Note",  "coupon": 3.750, "maturityDate": "2032-11-30"},
    {"cusip": "91282CPF2", "type": "7Y Note",  "coupon": 3.750, "maturityDate": "2032-10-31"},
    {"cusip": "91282CNZ0", "type": "7Y Note",  "coupon": 3.875, "maturityDate": "2032-09-30"},
    {"cusip": "91282CNW7", "type": "7Y Note",  "coupon": 3.875, "maturityDate": "2032-08-31"},
    {"cusip": "91282CNR8", "type": "7Y Note",  "coupon": 4.000, "maturityDate": "2032-07-31"},
    {"cusip": "91282CNJ6", "type": "7Y Note",  "coupon": 4.000, "maturityDate": "2032-06-30"},
    {"cusip": "91282CNF4", "type": "7Y Note",  "coupon": 4.125, "maturityDate": "2032-05-31"},
]

# Updated 2026-07-17 from CNBC markets/bonds
FALLBACK_CURVE = {2: 4.18, 3: 4.21, 5: 4.28, 7: 4.41, 10: 4.55, 20: 5.09, 30: 5.06}

_CNBC_SYMBOL_TO_YEARS = {
    "US1M": 1/12, "US3M": 3/12, "US6M": 6/12,
    "US1Y": 1, "US2Y": 2, "US3Y": 3, "US5Y": 5,
    "US7Y": 7, "US10Y": 10, "US20Y": 20, "US30Y": 30,
}

_XML_TAG_TO_YEARS = {
    "BC_1MONTH": 1/12, "BC_2MONTH": 2/12, "BC_3MONTH": 3/12, "BC_4MONTH": 4/12,
    "BC_6MONTH": 6/12, "BC_1YEAR": 1, "BC_2YEAR": 2, "BC_3YEAR": 3, "BC_5YEAR": 5,
    "BC_7YEAR": 7, "BC_10YEAR": 10, "BC_20YEAR": 20, "BC_30YEAR": 30,
}


def _fetch_cnbc():
    """Fetch Treasury yields from CNBC's quote webservice (powers markets/bonds/ page)."""
    symbols = "|".join(_CNBC_SYMBOL_TO_YEARS.keys())
    url = (
        "https://quote.cnbc.com/quote-html-webservice/quote.htm"
        f"?symbols={symbols}&requestMethod=itv&noform=1"
        "&partnerId=2&fund=1&exthrs=1&output=json&events=1"
    )
    req = urllib.request.Request(url, headers={
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/javascript, */*",
        "Referer": "https://www.cnbc.com/",
        "Origin": "https://www.cnbc.com",
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode("utf-8", errors="replace"))

    quotes = data.get("QuickQuoteResult", {}).get("QuickQuote", [])
    if isinstance(quotes, dict):
        quotes = [quotes]

    curve = {}
    curve_date = None
    for q in quotes:
        sym = (q.get("symbol") or q.get("symb") or "").upper()
        years = _CNBC_SYMBOL_TO_YEARS.get(sym)
        if years is None:
            continue
        last = q.get("last") or q.get("last_price")
        if last:
            try:
                curve[years] = float(str(last).replace(",", ""))
            except (ValueError, TypeError):
                pass
        if not curve_date:
            ts = q.get("last_time") or q.get("time")
            if ts:
                curve_date = str(ts)[:10]

    return (curve if len(curve) >= 3 else None), curve_date


def _fetch_xml(year_month):
    """Try the Treasury XML feed for a given YYYYMM string."""
    url = (
        "https://home.treasury.gov/resource-center/data-chart-center/"
        f"interest-rates/pages/xmlview?data=daily_treasury_yield_curve"
        f"&field_tdr_date_value_month={year_month}"
    )
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; bond-tracker/1.0)",
        "Accept": "application/xml,text/xml",
    })
    with urllib.request.urlopen(req, timeout=20) as resp:
        xml = resp.read().decode("utf-8", errors="replace")
    # Each trading day is wrapped in <G_NEW_DATE>...</G_NEW_DATE>; take the last one
    blocks = re.findall(r'<G_NEW_DATE>(.*?)</G_NEW_DATE>', xml, re.DOTALL)
    if not blocks:
        return None, None
    block = blocks[-1]
    date_m = re.search(r'<NEW_DATE>(\d{4}-\d{2}-\d{2})', block)
    curve_date = date_m.group(1) if date_m else None
    curve = {}
    for tag, years in _XML_TAG_TO_YEARS.items():
        m = re.search(rf'<{tag}>([0-9.]+)</{tag}>', block)
        if m:
            try:
                curve[years] = float(m.group(1))
            except ValueError:
                pass
    return curve if len(curve) >= 3 else None, curve_date


def _fetch_csv(year):
    """Try the Treasury CSV download for a given year."""
    url = (
        "https://home.treasury.gov/resource-center/data-chart-center/"
        f"interest-rates/daily-treasury-rates.csv/{year}/all"
        f"?field_tdr_date_value={year}&type=daily_treasury_yield_curve&page&_format=csv"
    )
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; bond-tracker/1.0)",
        "Accept": "text/csv",
    })
    with urllib.request.urlopen(req, timeout=20) as resp:
        csv_text = resp.read().decode("utf-8", errors="replace")
    lines = csv_text.strip().split("\n")
    if len(lines) < 2:
        return None, None
    header = [h.strip().strip('"') for h in lines[0].split(",")]
    first_row = [c.strip().strip('"') for c in lines[1].split(",")]
    if not first_row or not re.match(r'\d{2}/\d{2}/\d{4}', first_row[0]):
        return None, None
    curve_date = first_row[0]
    col_years = {
        "1 Mo": 1/12, "2 Mo": 2/12, "3 Mo": 3/12, "4 Mo": 4/12,
        "6 Mo": 6/12, "1 Yr": 1, "2 Yr": 2, "3 Yr": 3, "5 Yr": 5,
        "7 Yr": 7, "10 Yr": 10, "20 Yr": 20, "30 Yr": 30,
    }
    curve = {}
    for i, col_name in enumerate(header):
        if col_name in col_years and i < len(first_row) and first_row[i]:
            try:
                curve[col_years[col_name]] = float(first_row[i])
            except ValueError:
                pass
    return (curve if len(curve) >= 3 else None), curve_date


def fetch_yield_curve():
    """Fetch live Treasury yield curve from CNBC, then treasury.gov, then hardcoded fallback."""
    today = date.today()

    # 1. CNBC quote webservice (powers cnbc.com/markets/bonds/)
    try:
        curve, curve_date = _fetch_cnbc()
        if curve:
            return {"curve": curve, "date": curve_date or today.isoformat(), "source": "cnbc.com"}
    except Exception:
        pass

    # 2. Treasury XML feed for current month
    year_month = today.strftime("%Y%m")
    try:
        curve, curve_date = _fetch_xml(year_month)
        if curve:
            return {"curve": curve, "date": curve_date or today.isoformat(), "source": "treasury.gov/xml"}
    except Exception:
        pass

    # 3. Treasury XML for prior month (first days of new month before new data publishes)
    try:
        prev_ym = f"{today.year - 1}12" if today.month == 1 else f"{today.year}{today.month - 1:02d}"
        curve, curve_date = _fetch_xml(prev_ym)
        if curve:
            return {"curve": curve, "date": curve_date or today.isoformat(), "source": "treasury.gov/xml"}
    except Exception:
        pass

    # 4. Treasury CSV download
    try:
        curve, curve_date = _fetch_csv(today.year)
        if curve:
            return {"curve": curve, "date": curve_date or today.isoformat(), "source": "treasury.gov/csv"}
    except Exception:
        pass

    return {"curve": FALLBACK_CURVE, "date": today.isoformat(), "source": "fallback"}


def next_coupon(maturity_str, today):
    """Treasury bonds pay semiannually on maturity month/day and 6 months offset."""
    mat = date.fromisoformat(maturity_str)
    m1, d1 = mat.month, mat.day
    m2 = m1 + 6 if m1 <= 6 else m1 - 6
    d2 = min(d1, 28 if m2 == 2 else 30 if m2 in (4, 6, 9, 11) else 31)
    d1 = min(d1, 28 if m1 == 2 else 30 if m1 in (4, 6, 9, 11) else 31)
    dates = []
    for y in (today.year, today.year + 1):
        for mo, dy in [(m1, d1), (m2, d2)]:
            try:
                dates.append(date(y, mo, dy))
            except ValueError:
                pass
    future = sorted(d for d in dates if d > today)
    return future[0].isoformat() if future else mat.isoformat()


def interpolate_yield(curve, years):
    """Linear interpolation on the yield curve for a given maturity in years."""
    keys = sorted(curve.keys())
    if years <= keys[0]:
        return curve[keys[0]]
    if years >= keys[-1]:
        return curve[keys[-1]]
    for i in range(len(keys) - 1):
        if keys[i] <= years <= keys[i + 1]:
            t = (years - keys[i]) / (keys[i + 1] - keys[i])
            return curve[keys[i]] * (1 - t) + curve[keys[i + 1]] * t
    return curve[keys[-1]]


def bond_price(coupon_rate, ytm, years_to_maturity):
    """Price per $100 face value using semiannual compounding."""
    if years_to_maturity <= 0 or ytm <= 0:
        return 100.0
    c = coupon_rate / 200
    y = ytm / 200
    n = max(1, round(years_to_maturity * 2))
    if abs(y) < 1e-10:
        return c * n * 100 + 100
    pv_coupons = c * 100 * (1 - (1 + y) ** (-n)) / y
    pv_face = 100 / (1 + y) ** n
    return pv_coupons + pv_face


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        today = date.today()
        curve_data = fetch_yield_curve()
        curve = curve_data["curve"]
        curve_date = curve_data["date"]
        curve_source = curve_data.get("source", "")

        results = []
        for bond in TREASURY_BONDS:
            mat = date.fromisoformat(bond["maturityDate"])
            years_left = (mat - today).days / 365.25
            if years_left <= 0:
                continue
            ytm = interpolate_yield(curve, years_left)
            price = bond_price(bond["coupon"], ytm, years_left)
            current_yield = (bond["coupon"] / price) * 100 if price > 0 else 0

            results.append({
                "rank": 0,
                "cusip": bond["cusip"],
                "type": bond["type"],
                "coupon": bond["coupon"],
                "maturityDate": bond["maturityDate"],
                "nextCouponDate": next_coupon(bond["maturityDate"], today),
                "yearsLeft": round(years_left, 1),
                "ytm": round(ytm, 3),
                "price": round(price, 2),
                "currentYield": round(current_yield, 2),
            })

        results.sort(key=lambda x: -x["ytm"])
        for i, r in enumerate(results):
            r["rank"] = i + 1

        now = datetime.now(timezone.utc)
        payload = {
            "bonds": results,
            "curveDate": curve_date,
            "curveSource": curve_source,
            "lastUpdated": now.strftime("%Y-%m-%dT%H:%M:%S"),
            "yieldCurve": {str(k): v for k, v in curve.items()},
        }
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "public, max-age=300")
        self.end_headers()
        self.wfile.write(json.dumps(payload).encode())
