from http.server import BaseHTTPRequestHandler
import json, urllib.request, urllib.error, math
from datetime import datetime, timezone

TREASURY_BONDS = [
    {"cusip": "912810UT3", "type": "20Y Bond", "coupon": 4.625, "maturityDate": "2046-02-15", "issueDate": "2026-03-02"},
    {"cusip": "912810UQ9", "type": "20Y Bond", "coupon": 4.625, "maturityDate": "2045-11-15", "issueDate": "2025-12-01"},
    {"cusip": "912810UN6", "type": "20Y Bond", "coupon": 4.875, "maturityDate": "2045-08-15", "issueDate": "2025-09-02"},
    {"cusip": "912810UL0", "type": "20Y Bond", "coupon": 5.000, "maturityDate": "2045-05-15", "issueDate": "2025-06-02"},
    {"cusip": "91282CPZ8", "type": "10Y Note", "coupon": 4.125, "maturityDate": "2036-02-15", "issueDate": "2026-02-17"},
    {"cusip": "91282CPJ4", "type": "10Y Note", "coupon": 4.000, "maturityDate": "2035-11-15", "issueDate": "2025-11-17"},
    {"cusip": "91282CNT4", "type": "10Y Note", "coupon": 4.250, "maturityDate": "2035-08-15", "issueDate": "2025-08-15"},
    {"cusip": "91282CNC1", "type": "10Y Note", "coupon": 4.250, "maturityDate": "2035-05-15", "issueDate": "2025-06-16"},
    {"cusip": "91282CQN4", "type": "7Y Note",  "coupon": 4.125, "maturityDate": "2033-04-30", "issueDate": "2026-04-30"},
    {"cusip": "91282CQF1", "type": "7Y Note",  "coupon": 4.250, "maturityDate": "2033-03-31", "issueDate": "2026-03-31"},
    {"cusip": "91282CPY1", "type": "7Y Note",  "coupon": 4.000, "maturityDate": "2033-01-31", "issueDate": "2026-02-02"},
    {"cusip": "91282CPQ8", "type": "7Y Note",  "coupon": 3.875, "maturityDate": "2032-12-31", "issueDate": "2025-12-31"},
    {"cusip": "91282CQC8", "type": "7Y Note",  "coupon": 3.750, "maturityDate": "2033-02-28", "issueDate": "2026-03-02"},
    {"cusip": "91282CPM7", "type": "7Y Note",  "coupon": 3.750, "maturityDate": "2032-11-30", "issueDate": "2025-12-01"},
    {"cusip": "91282CPF2", "type": "7Y Note",  "coupon": 3.750, "maturityDate": "2032-10-31", "issueDate": "2025-10-31"},
    {"cusip": "91282CNZ0", "type": "7Y Note",  "coupon": 3.875, "maturityDate": "2032-09-30", "issueDate": "2025-09-30"},
    {"cusip": "91282CNW7", "type": "7Y Note",  "coupon": 3.875, "maturityDate": "2032-08-31", "issueDate": "2025-09-02"},
    {"cusip": "91282CNR8", "type": "7Y Note",  "coupon": 4.000, "maturityDate": "2032-07-31", "issueDate": "2025-07-31"},
    {"cusip": "91282CNJ6", "type": "7Y Note",  "coupon": 4.000, "maturityDate": "2032-06-30", "issueDate": "2025-06-30"},
    {"cusip": "91282CNF4", "type": "7Y Note",  "coupon": 4.125, "maturityDate": "2032-05-31", "issueDate": "2025-06-02"},
]

YIELD_CURVE_URL = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/all/050126?type=daily_treasury_yield_curve&field_tdr_date_value=2026&page&_format=csv"


def fetch_yield_curve():
    """Fetch the latest Treasury yield curve from treasury.gov CSV."""
    try:
        req = urllib.request.Request(YIELD_CURVE_URL, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            text = resp.read().decode("utf-8")
        lines = text.strip().split("\n")
        if len(lines) < 2:
            return None
        headers = [h.strip().strip('"') for h in lines[0].split(",")]
        last_row = [v.strip().strip('"') for v in lines[-1].split(",")]
        row_dict = dict(zip(headers, last_row))
        curve = {}
        mapping = {
            "1 Mo": 1/12, "2 Mo": 2/12, "3 Mo": 0.25, "4 Mo": 4/12,
            "6 Mo": 0.5, "1 Yr": 1, "2 Yr": 2, "3 Yr": 3, "5 Yr": 5,
            "7 Yr": 7, "10 Yr": 10, "20 Yr": 20, "30 Yr": 30,
        }
        for label, years in mapping.items():
            val = row_dict.get(label, "")
            if val:
                try:
                    curve[years] = float(val)
                except ValueError:
                    pass
        date_str = row_dict.get("Date", "")
        return {"curve": curve, "date": date_str}
    except Exception:
        return None


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
        now = datetime.now(timezone.utc)
        curve_data = fetch_yield_curve()
        if not curve_data or not curve_data["curve"]:
            fallback = {2: 3.95, 3: 3.97, 5: 4.07, 7: 4.24, 10: 4.41, 20: 4.97, 30: 4.98}
            curve = fallback
            curve_date = now.strftime("%m/%d/%Y")
        else:
            curve = curve_data["curve"]
            curve_date = curve_data["date"]

        results = []
        for bond in TREASURY_BONDS:
            mat = datetime.strptime(bond["maturityDate"], "%Y-%m-%d")
            years_left = (mat - now).days / 365.25
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

        payload = {
            "bonds": results,
            "curveDate": curve_date,
            "lastUpdated": now.strftime("%Y-%m-%dT%H:%M:%S"),
            "yieldCurve": {str(k): v for k, v in curve.items()},
        }
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "public, max-age=300")
        self.end_headers()
        self.wfile.write(json.dumps(payload).encode())
