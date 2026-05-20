from __future__ import annotations

import io
import os
import random
import tomllib
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import pandas as pd
from botocore.client import Config
from faker import Faker

_CONFIG_PATH = Path(
    os.getenv("GENERATOR_CONFIG", "/opt/airflow/config/generator.toml")
)

# ISO-2 → (latitude, longitude) country centroids
_COUNTRY_COORDS: dict[str, tuple[float, float]] = {
    "US": ( 37.090240,  -95.712891),
    "CA": ( 56.130366, -106.346771),
    "BR": (-14.235004,  -51.925280),
    "MX": ( 23.634501, -102.552784),
    "DE": ( 51.165691,   10.451526),
    "GB": ( 55.378051,   -3.435973),
    "FR": ( 46.227638,    2.213749),
    "NL": ( 52.132633,    5.291266),
    "ES": ( 40.463667,   -3.749220),
    "IN": ( 20.593684,   78.962880),
    "SG": (  1.352083,  103.819836),
    "AE": ( 23.424076,   53.847818),
    "JP": ( 36.204824,  138.252924),
    "NG": (  9.081999,    8.675277),
    "GH": (  7.946527,   -1.023194),
    "KE": ( -0.023559,   37.906193),
    "ZA": (-30.559482,   22.937506),
    "EG": ( 26.820553,   30.802498),
    "RW": ( -1.940278,   29.873888),
    "SN": ( 14.497401,  -14.452362),
    "TZ": ( -6.369028,   34.888822),
    "AU": (-25.274398,  133.775136),
}

# Fixed product catalog: name → (brand, sku_prefix)
_PRODUCT_CATALOG: dict[str, tuple[str, str]] = {
    # Electronics
    "Laptop":        ("Dell",       "EL-LAP"),
    "Monitor":       ("Samsung",    "EL-MON"),
    "Keyboard":      ("Logitech",   "EL-KBD"),
    "Mouse":         ("Logitech",   "EL-MOU"),
    "Headset":       ("Bose",       "EL-HDS"),
    "Webcam":        ("Logitech",   "EL-WCM"),
    "Tablet":        ("Apple",      "EL-TAB"),
    "Smartphone":    ("Samsung",    "EL-PHN"),
    "Smartwatch":    ("Apple",      "EL-WCH"),
    "Printer":       ("HP",         "EL-PRT"),
    "SSD":           ("Samsung",    "EL-SSD"),
    "RAM":           ("Kingston",   "EL-RAM"),
    "Graphics Card": ("NVIDIA",     "EL-GPU"),
    "Microphone":    ("Blue",       "EL-MIC"),
    "LED Strip":     ("Govee",      "EL-LED"),
    # Furniture
    "Desk":          ("IKEA",       "FU-DSK"),
    "Chair":         ("Herman Miller", "FU-CHR"),
    "Bookshelf":     ("IKEA",       "FU-BSH"),
    "Filing Cabinet":("Bisley",     "FU-CAB"),
    "Standing Desk": ("Flexispot",  "FU-STD"),
    "Monitor Arm":   ("Ergotron",   "FU-ARM"),
    "Footrest":      ("Kensington", "FU-FTR"),
    "Whiteboard":    ("Quartet",    "FU-WBD"),
    # Software
    "Antivirus":     ("Norton",     "SW-AV"),
    "Office Suite":  ("Microsoft",  "SW-OFF"),
    "Design Tool":   ("Adobe",      "SW-DSN"),
    "Cloud Storage": ("Dropbox",    "SW-CLD"),
    "VPN License":   ("NordVPN",    "SW-VPN"),
    "Password Manager": ("1Password", "SW-PWD"),
    "Video Editor":  ("DaVinci",    "SW-VED"),
    # Accessories
    "USB Hub":       ("Anker",      "AC-USB"),
    "Charging Cable":("Belkin",     "AC-CBL"),
    "Laptop Bag":    ("Targus",     "AC-BAG"),
    "Power Bank":    ("Anker",      "AC-PWB"),
    "Screen Protector": ("Belkin",  "AC-SCR"),
    "Docking Station": ("CalDigit", "AC-DCK"),
    "Surge Protector": ("APC",      "AC-SRG"),
    "Webcam Cover":  ("Eyebloc",    "AC-WCC"),
    "Cable Organiser": ("Bluelounge", "AC-ORG"),
}

_CHANNEL_META = {
    "online":  "Direct Digital",
    "retail":  "Physical Store",
    "partner": "B2B Partner",
}

_PAYMENT_META = {
    "card":    True,
    "invoice": False,
    "wallet":  True,
    "crypto":  True,
}


def load_config() -> dict:
    with open(_CONFIG_PATH, "rb") as f:
        return tomllib.load(f)


def _weighted_choice(weights: dict[str, int]) -> str:
    keys    = list(weights.keys())
    counts  = list(weights.values())
    return random.choices(keys, weights=counts, k=1)[0]


def _customer_tier(customer_id: int, tiers: dict) -> str:
    if customer_id <= tiers["platinum"]:
        return "platinum"
    if customer_id <= tiers["gold"]:
        return "gold"
    if customer_id <= tiers["silver"]:
        return "silver"
    return "bronze"


def _discount(tier: str) -> float:
    return {
        "platinum": round(random.uniform(0.10, 0.25), 2),
        "gold":     round(random.uniform(0.05, 0.15), 2),
        "silver":   round(random.uniform(0.00, 0.08), 2),
        "bronze":   0.0,
    }[tier]


def _get_customer_profile(customer_id: int, tiers: dict) -> dict:
    """Deterministic customer profile — same ID always yields same name/email."""
    f = Faker()
    f.seed_instance(customer_id * 7919)
    return {
        "customer_id":          customer_id,
        "customer_full_name":   f.name(),
        "customer_email":       f.email(),
        "customer_signup_date": f.date_between(start_date="-5y", end_date="-30d").isoformat(),
        "customer_tier":        _customer_tier(customer_id, tiers),
    }


def _realistic_ts(start_dt: datetime) -> datetime:
    """Bias toward business hours Mon–Fri."""
    fake = Faker()
    ts = fake.date_time_between(start_date=start_dt, end_date="now", tzinfo=timezone.utc)
    if random.random() < 0.70:
        days_back = random.randint(0, (datetime.now(tz=timezone.utc) - start_dt).days)
        ts = datetime.now(tz=timezone.utc) - timedelta(days=days_back)
        while ts.weekday() >= 5 and random.random() < 0.6:
            ts -= timedelta(days=1)
        ts = ts.replace(hour=random.randint(8, 18), minute=random.randint(0, 59))
    return ts


def _sku(product: str) -> str:
    prefix = _PRODUCT_CATALOG.get(product, ("Unknown", "XX-UNK"))[1]
    suffix = abs(hash(product)) % 10000
    return f"{prefix}-{suffix:04d}"


def generate_sales(n_rows: int | None = None) -> pd.DataFrame:
    cfg      = load_config()
    gcfg     = cfg["generator"]
    n_rows   = n_rows or gcfg["default_rows"]
    start_dt = datetime.now(tz=timezone.utc) - timedelta(days=gcfg["lookback_days"])

    all_products: list[tuple[str, str]] = [
        (item, category)
        for category, items in gcfg["products"].items()
        for item in items
    ]

    country_pool = [c for c in gcfg["countries"] for _ in range(c["weight"])]
    channel_w    = gcfg["channels"]["weights"]
    payment_w    = gcfg["payment_methods"]["weights"]
    pricing      = gcfg["pricing"]
    margins      = gcfg["category_margins"]
    tiers_cfg    = gcfg["customer_tiers"]
    max_cust     = gcfg["max_customer_id"]
    max_qty      = gcfg["max_qty"]

    def pick_customer() -> int:
        if random.random() < 0.40:
            return random.randint(1, max(1, max_cust // 10))
        return random.randint(1, max_cust)

    rows = []
    for _ in range(n_rows):
        product, category    = random.choice(all_products)
        price_min, price_max = pricing[category]
        unit_price           = round(random.uniform(price_min, price_max), 2)
        cost_price           = round(unit_price * margins[category], 2)
        qty                  = random.randint(1, max_qty)
        customer_id          = pick_customer()
        profile              = _get_customer_profile(customer_id, tiers_cfg)
        discount             = _discount(profile["customer_tier"])
        net_price            = round(unit_price * (1 - discount), 2)
        total                = round(qty * net_price, 2)
        gross_margin         = round(total - (cost_price * qty), 2)
        country              = random.choice(country_pool)
        channel              = _weighted_choice(channel_w)
        payment              = _weighted_choice(payment_w)
        brand                = _PRODUCT_CATALOG.get(product, ("Unknown", "XX"))[0]

        rows.append({
            # order fields
            "order_id":             str(uuid.uuid4()),
            "order_ts":             _realistic_ts(start_dt).isoformat(),
            # customer dim
            "customer_id":          profile["customer_id"],
            "customer_full_name":   profile["customer_full_name"],
            "customer_email":       profile["customer_email"],
            "customer_signup_date": profile["customer_signup_date"],
            "customer_tier":        profile["customer_tier"],
            # product dim
            "product_name":         product,
            "product_category":     category,
            "product_brand":        brand,
            "product_sku":          _sku(product),
            "product_cost_price":   cost_price,
            "product_list_price":   unit_price,
            # geography dim
            "country":              country["name"],
            "country_code":         country["code"],
            "region":               country["region"],
            "sub_region":           country["sub_region"],
            "latitude":             _COUNTRY_COORDS.get(country["code"], (None, None))[0],
            "longitude":            _COUNTRY_COORDS.get(country["code"], (None, None))[1],
            # channel + payment dims
            "channel":              channel,
            "channel_type":         _CHANNEL_META[channel],
            "payment_method":       payment,
            "payment_is_digital":   _PAYMENT_META[payment],
            # measures
            "qty":                  qty,
            "unit_price":           unit_price,
            "cost_price":           cost_price,
            "discount_pct":         round(discount * 100, 2),
            "total":                total,
            "gross_margin":         gross_margin,
            "is_returned":          random.random() < 0.04,
        })

    return pd.DataFrame(rows)


def upload_to_minio(
    df: pd.DataFrame,
    access_key: str = "minioadmin",
    secret_key: str = "minioadmin",
) -> str:
    cfg = load_config()["minio"]
    ts  = datetime.now(tz=timezone.utc)
    key = f"{ts.strftime('%Y/%m/%d')}/{ts.strftime('%H%M%S')}_{uuid.uuid4().hex[:8]}.csv"

    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    client = boto3.client(
        "s3",
        endpoint_url=cfg["endpoint_url"],
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    client.put_object(Bucket=cfg["bucket_raw"], Key=key, Body=csv_buffer.getvalue())
    return key


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=None)
    args = parser.parse_args()

    df  = generate_sales(args.rows)
    key = upload_to_minio(
        df,
        access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
    )
    cfg = load_config()
    print(f"Uploaded {len(df)} rows → s3://{cfg['minio']['bucket_raw']}/{key}")
