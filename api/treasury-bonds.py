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

FALLBACK_CURVE = {2: 3.95, 3: 3.97, 5: 4.07, 7: 4.24, 10: 4.41, 20: 4.97, 30: 4.98}


def fetch_yield_curve():
    """Fetch latest Treasury yield curve from treasury.gov XML API."""
    try:
        today = date.today()
        url = (
            "https://home.treasury.gov/resource-center/data-chart-center/"
            "interest-rates/TextView?type=daily_treasury_yield_curve"
            f"&field_tdr_date_value={today.year}"
        )
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (compatible; bond-tracker/1.0)",
            "Accept": "text/html",
        })
        with urllib.request.urlopen(req, timeout=20) as resp:
            html = resp.read().decode("utf-8", errors="replace")
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL)
        if not rows:
            return {"curve": FALLBACK_CURVE, "date": today.isoformat(), "source": "fallback"}
        last_data_row = None
        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            cells = [c.strip() for c in cells]
            if cells and re.match(r'\d{2}/\d{2}/\d{4}', cells[0]):
                last_data_row = cells
        if not last_data_row or len(last_data_row) < 13:
            return {"curve": FALLBACK_CURVE, "date": today.isoformat(), "source": "fallback"}
        curve_date = last_data_row[0]
        col_map = [
            (1, 1/12), (2, 2/12), (3, 3/12), (4, 4/12),
            (5, 6/12), (6, 1), (7, 2), (8, 3), (9, 5),
            (10, 7), (11, 10), (12, 20), (13, 30),
        ]
        curve = {}
        for idx, years in col_map:
            if idx < len(last_data_row) and last_data_row[idx]:
                try:
                    curve[years] = float(last_data_row[idx])
                except ValueError:
                    pass
        if len(curve) < 3:
            return {"curve": FALLBACK_CURVE, "date": today.isoformat(), "source": "fallback"}
        return {"curve": curve, "date": curve_date, "source": "treasury.gov"}
    except Exception:
        return {"curve": FALLBACK_CURVE, "date": date.today().isoformat(), "source": "fallback"}


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
