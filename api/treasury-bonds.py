from http.server import BaseHTTPRequestHandler
import json, urllib.request, urllib.error, re
from datetime import datetime, timezone, date

TREASURY_BONDS = [
    {"cusip": "912810UT3", "type": "20Y Bond", "coupon": 4.625, "maturityDate": "2046-02-15"},
    {"cusip": "912810UQ9", "type": "20Y Bond", "coupon": 4.625, "maturityDate": "2045-11-15"},
    {"cusip": "912810UN6", "type": "20Y Bond", "coupon": 4.875, "maturityDate": "2045-08-15"},
    {"cusip": "912810UL0", "type": "20Y Bond", "coupon": 5.000, "maturityDate": "2045-05-15"},
    {"cusip": "91282CPZ8", "type": "10Y Note", "coupon": 4.125, "maturityDate": "2036-02-15"},
    {"cusip": "91282CPJ4", "type": "10Y Note", "coupon": 4.000, "maturityDate": "2035-11-15"},
    {"cusip": "91282CNT4", "type": "10Y Note", "coupon": 4.250, "maturityDate": "2035-08-15"},
    {"cusip": "91282CNC1", "type": "10Y Note", "coupon": 4.250, "maturityDate": "2035-05-15"},
    {"cusip": "91282CQN4", "type": "7Y Note",  "coupon": 4.125, "maturityDate": "2033-04-30"},
    {"cusip": "91282CQF1", "type": "7Y Note",  "coupon": 4.250, "maturityDate": "2033-03-31"},
    {"cusip": "91282CPY1", "type": "7Y Note",  "coupon": 4.000, "maturityDate": "2033-01-31"},
    {"cusip": "91282CPQ8", "type": "7Y Note",  "coupon": 3.875, "maturityDate": "2032-12-31"},
    {"cusip": "91282CQC8", "type": "7Y Note",  "coupon": 3.750, "maturityDate": "2033-02-28"},
    {"cusip": "91282CPM7", "type": "7Y Note",  "coupon": 3.750, "maturityDate": "2032-11-30"},
    {"cusip": "91282CPF2", "type": "7Y Note",  "coupon": 3.750, "maturityDate": "2032-10-31"},
    {"cusip": "91282CNZ0", "type": "7Y Note",  "coupon": 3.875, "maturityDate": "2032-09-30"},
    {"cusip": "91282CNW7", "type": "7Y Note",  "coupon": 3.875, "maturityDate": "2032-08-31"},
    {"cusip": "91282CNR8", "type": "7Y Note",  "coupon": 4.000, "maturityDate": "2032-07-31"},
    {"cusip": "91282CNJ6", "type": "7Y Note",  "coupon": 4.000, "maturityDate": "2032-06-30"},
    {"cusip": "91282CNF4", "type": "7Y Note",  "coupon": 4.125, "maturityDate": "2032-05-31"},
]

FALLBACK_CURVE = {2: 4.17, 3: 4.22, 5: 4.29, 7: 4.41, 10: 4.55, 20: 5.03, 30: 5.01}

def fetch_yield_curve():
    """Fetch latest Treasury yield curve from treasury.gov CSV API."""
    try:
        today = date.today()
        url = (
            "https://home.treasury.gov/resource-center/data-chart-center/"
            "interest-rates/daily-treasury-rates.csv/"
            f"{today.year}/all?field_tdr_date_value={today.year}"
            "&type=daily_treasury_yield_curve&page&_format=csv"
        )
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (compatible; bond-tracker/1.0)",
            "Accept": "text/csv",
        })
        with urllib.request.urlopen(req, timeout=20) as resp:
            csv_text = resp.read().decode("utf-8", errors="replace")
        lines = csv_text.strip().split("\n")
        if len(lines) < 2:
            return {"curve": FALLBACK_CURVE, "date": today.isoformat(), "source": "fallback"}
        header = [h.strip().strip('"') for h in lines[0].split(",")]
        first_row = [c.strip().strip('"') for c in lines[1].split(",")]
        if not first_row or not re.match(r'\d{2}/\d{2}/\d{4}', first_row[0]):
            return {"curve": FALLBACK_CURVE, "date": today.isoformat(), "source": "fallback"}
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
        if len(curve) < 3:
            return {"curve": FALLBACK_CURVE, "date": today.isoformat(), "source": "fallback"}
        return {"curve": curve, "date": curve_date, "source": "treasury.gov"}
    except Exception:
        return {"curve": FALLBACK_CURVE, "date": date.today().isoformat(), "source": "fallback"}


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
