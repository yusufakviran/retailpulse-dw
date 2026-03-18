"""
RetailPulse — Demo Data Generator
Generates realistic Turkish retail chain data (150 stores, 3 years)
Kimball DW compatible: dict → dim → fact load order

Usage:
    pip install pandas numpy faker
    python generate_demo_data.py
"""

import random
import math
import hashlib
import os
from datetime import date, datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
RANDOM_SEED = 42
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

N_STORES         = 150
N_PRODUCTS       = 3_000
N_CUSTOMERS      = 80_000
DATE_START       = date(2023, 1, 1)
DATE_END         = date(2025, 12, 31)
OUTPUT_DIR       = "output_csv"

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("🚀 RetailPulse Demo Data Generator başlıyor...")

# ─────────────────────────────────────────
# REFERENCE DATA
# ─────────────────────────────────────────
CITIES = [
    ("İstanbul",    "IST",  "TR10", "Marmara",          "Metropolitan"),
    ("Ankara",      "ANK",  "TR51", "Orta Anadolu",     "Metropolitan"),
    ("İzmir",       "IZM",  "TR31", "Ege",              "Metropolitan"),
    ("Bursa",       "BRS",  "TR41", "Marmara",          "Urban"),
    ("Antalya",     "ANT",  "TR61", "Akdeniz",          "Urban"),
    ("Adana",       "ADA",  "TR62", "Akdeniz",          "Urban"),
    ("Konya",       "KNY",  "TR52", "Orta Anadolu",     "Urban"),
    ("Gaziantep",   "GAZ",  "TR90", "Güneydoğu",        "Urban"),
    ("Kayseri",     "KAY",  "TR72", "Orta Anadolu",     "Urban"),
    ("Mersin",      "MER",  "TR62", "Akdeniz",          "Urban"),
    ("Eskişehir",   "ESK",  "TR41", "Marmara",          "Urban"),
    ("Trabzon",     "TRB",  "TR90", "Karadeniz",        "Urban"),
    ("Diyarbakır",  "DIY",  "TR72", "Doğu Anadolu",     "Urban"),
    ("Samsun",      "SAM",  "TR83", "Karadeniz",        "Urban"),
    ("Denizli",     "DNZ",  "TR32", "Ege",              "Urban"),
]

FORMATS = [
    ("HYPER",   0.10, (5000,  12000), (18, 35)),
    ("SUPER",   0.45, (1000,   4999), (8,  22)),
    ("EXPRESS", 0.35, (150,     999), (4,  10)),
    ("OUTLET",  0.10, (500,    2500), (6,  14)),
]

DEPARTMENTS = [
    ("GDA", "Gıda",         [
        ("GDA-01", "Meyve Sebze",   [("MS-01","Meyveler"),("MS-02","Sebzeler"),("MS-03","Organik")]),
        ("GDA-02", "Et & Balık",    [("EB-01","Kırmızı Et"),("EB-02","Tavuk"),("EB-03","Balık")]),
        ("GDA-03", "Süt & Kahvaltı",[("SK-01","Süt Ürünleri"),("SK-02","Kahvaltılık"),("SK-03","Yumurta")]),
        ("GDA-04", "Temel Gıda",    [("TG-01","Un & Tahıl"),("TG-02","Makarna Pirinç"),("TG-03","Konserve")]),
        ("GDA-05", "Atıştırmalık",  [("AT-01","Çerez"),("AT-02","Çikolata"),("AT-03","Bisküvi")]),
    ]),
    ("ICK", "İçecek",       [
        ("ICK-01", "Alkolsüz",      [("ICA-01","Meyve Suyu"),("ICA-02","Gazlı İçecek"),("ICA-03","Su")]),
        ("ICK-02", "Sıcak İçecek",  [("ICS-01","Çay"),("ICS-02","Kahve"),("ICS-03","Bitki Çayı")]),
    ]),
    ("KIS", "Kişisel Bakım",[
        ("KIS-01", "Cilt Bakım",    [("CIL-01","Yüz Bakım"),("CIL-02","Vücut Bakım"),("CIL-03","Güneş")]),
        ("KIS-02", "Saç Bakım",     [("SAC-01","Şampuan"),("SAC-02","Saç Kremi"),("SAC-03","Fön")]),
        ("KIS-03", "Hijyen",        [("HIJ-01","Deodorant"),("HIJ-02","Sabun"),("HIJ-03","Diş Bakım")]),
    ]),
    ("EV",  "Ev & Yaşam",   [
        ("EV-01",  "Temizlik",      [("TEM-01","Deterjan"),("TEM-02","Yüzey Temizleyici"),("TEM-03","Kağıt")]),
        ("EV-02",  "Mutfak",        [("MUT-01","Pişirme"),("MUT-02","Saklama"),("MUT-03","Servis")]),
    ]),
    ("TEK", "Teknoloji",    [
        ("TEK-01", "Küçük Ev Al.",  [("KEA-01","Kahve Makinesi"),("KEA-02","Blender"),("KEA-03","Toaster")]),
        ("TEK-02", "Aksesuar",      [("AKS-01","Kablo"),("AKS-02","Kulaklık"),("AKS-03","Şarj")]),
    ]),
]

BRANDS = {
    "GDA": ["Koska","Ülker","Pınar","Sütaş","Torku","Tadım","ETi","Tukaş","Öncü","Dardanel"],
    "ICK": ["Fuse Tea","Coca-Cola","Pepsi","Doğadan","Çaykur","Nescafé","Lipton","Uludağ","Erikli","Beypazarı"],
    "KIS": ["Dove","L'Oréal","Nivea","Pantene","Elidor","Head&Shoulders","Gillette","Colgate","Oral-B","Arko"],
    "EV":  ["Omo","Persil","Fairy","Domestos","Vanish","Bref","Pril","Cam","Papia","Lotus"],
    "TEK": ["Philips","Tefal","Bosch","Braun","SteelSeries","Logitech","Xiaomi","Belkin","Anker","JBL"],
}

SUPPLIER_MAP = {
    "GDA": ["Metro Cash&Carry","Ekol Lojistik","Horoz Lojistik"],
    "ICK": ["Efes Dağıtım","Coca-Cola İçecek","Doğadan AŞ"],
    "KIS": ["P&G Türkiye","Unilever","Colgate-Palmolive"],
    "EV":  ["Henkel Türkiye","Reckitt","SC Johnson"],
    "TEK": ["Philips Türkiye","Arçelik","Teknosa Tedarik"],
}

# ─────────────────────────────────────────
# 1. dim_region
# ─────────────────────────────────────────
print("📍 dim_region oluşturuluyor...")
regions = []
for i, (city, code, nuts2, nuts2_name, pop_seg) in enumerate(CITIES, 1):
    regions.append({
        "region_id":        i,
        "region_code":      code,
        "region_name":      f"{city} Bölgesi",
        "city":             city,
        "city_code":        code,
        "district":         None,
        "nuts2_code":       nuts2,
        "nuts2_name":       nuts2_name,
        "latitude":         round(random.uniform(36.5, 42.0), 6),
        "longitude":        round(random.uniform(26.0, 44.5), 6),
        "timezone":         "Europe/Istanbul",
        "population_segment": pop_seg,
    })
df_region = pd.DataFrame(regions)
df_region.to_csv(f"{OUTPUT_DIR}/stg_region.csv", index=False)
print(f"   ✓ {len(df_region)} bölge")

# ─────────────────────────────────────────
# 2. dim_product_category
# ─────────────────────────────────────────
print("📦 dim_product_category oluşturuluyor...")
categories = []
cat_id = 1
cat_lookup = {}
for dept_code, dept_name, cat_list in DEPARTMENTS:
    for cat_code, cat_name, subcats in cat_list:
        for sub_code, sub_name in subcats:
            categories.append({
                "category_id":      cat_id,
                "department_code":  dept_code,
                "department_name":  dept_name,
                "category_code":    cat_code,
                "category_name":    cat_name,
                "subcategory_code": sub_code,
                "subcategory_name": sub_name,
                "is_food":          dept_code in ("GDA","ICK"),
                "is_private_label": False,
                "vat_rate":         1.0 if dept_code == "GDA" else 20.0,
                "shelf_life_days":  random.choice([None,3,7,14,30,90,180,365]) if dept_code=="GDA" else None,
            })
            cat_lookup[sub_code] = (cat_id, dept_code)
            cat_id += 1
df_category = pd.DataFrame(categories)
df_category.to_csv(f"{OUTPUT_DIR}/stg_product_category.csv", index=False)
print(f"   ✓ {len(df_category)} kategori")

# ─────────────────────────────────────────
# 3. dim_store
# ─────────────────────────────────────────
print("🏪 dim_store oluşturuluyor...")
stores = []
store_format_weights = [f[1] for f in FORMATS]
for i in range(1, N_STORES + 1):
    fmt = random.choices(FORMATS, weights=store_format_weights)[0]
    fmt_code, _, sqm_range, checkout_range = fmt
    city_row = random.choices(CITIES, weights=[
        5 if c[4]=="Metropolitan" else 2 if c[4]=="Urban" else 1 for c in CITIES
    ])[0]
    sqm = random.randint(*sqm_range)
    is_mall = fmt_code in ("HYPER","SUPER") and random.random() < 0.45
    cluster = random.choices(["A","B","C","D"], weights=[0.15,0.30,0.35,0.20])[0]
    open_date = date(random.randint(2010,2022), random.randint(1,12), random.randint(1,28))

    # SCD2 version 1 (some stores will change cluster/headcount later)
    stores.append({
        "store_id":         i,
        "store_code":       f"STR-{i:04d}",
        "store_name":       f"{city_row[0]} {['Merkez','Şehir','Batı','Kuzey','Güney','Doğu','Yeni','AVM'][i%8]} Mağazası",
        "store_short_name": f"{city_row[1]}-{i:03d}",
        "format_code":      fmt_code,
        "store_sqm":        sqm,
        "selling_sqm":      int(sqm * random.uniform(0.65, 0.80)),
        "checkout_count":   random.randint(*checkout_range),
        "region_id":        CITIES.index(city_row) + 1,
        "address":          f"{city_row[0]}, Örnek Mahalle No:{random.randint(1,200)}",
        "mall_name":        f"{city_row[0]} AVM" if is_mall else None,
        "is_mall_store":    is_mall,
        "opening_date":     open_date.isoformat(),
        "renovation_date":  None,
        "closing_date":     None,
        "is_franchise":     random.random() < 0.08,
        "cluster":          cluster,
        "headcount":        random.randint(8, sqm // 50),
        "scd_start_date":   "2020-01-01",
        "scd_end_date":     None,
        "is_current":       True,
        "scd_version":      1,
    })
    # ~15% of stores had a change mid-period (new cluster/renovation → SCD2 version 2)
    if random.random() < 0.15:
        change_date = date(2024, random.randint(1,6), 1)
        stores[-1]["scd_end_date"] = (change_date - timedelta(days=1)).isoformat()
        stores[-1]["is_current"]   = False
        new_store = stores[-1].copy()
        new_store["cluster"]        = random.choices(["A","B","C","D"], weights=[0.20,0.35,0.30,0.15])[0]
        new_store["headcount"]      = new_store["headcount"] + random.randint(-2, 5)
        new_store["renovation_date"]= change_date.isoformat()
        new_store["scd_start_date"] = change_date.isoformat()
        new_store["scd_end_date"]   = None
        new_store["is_current"]     = True
        new_store["scd_version"]    = 2
        stores.append(new_store)

df_store = pd.DataFrame(stores)
df_store.to_csv(f"{OUTPUT_DIR}/stg_store.csv", index=False)
print(f"   ✓ {len(df_store)} store SCD2 kaydı ({N_STORES} fiziksel mağaza)")

# ─────────────────────────────────────────
# 4. dim_product
# ─────────────────────────────────────────
print("🛒 dim_product oluşturuluyor...")
products = []
all_subcodes = [(k, v) for k, v in cat_lookup.items()]
for i in range(1, N_PRODUCTS + 1):
    sub_code, (cat_id_val, dept_code) = random.choice(all_subcodes)
    brand = random.choice(BRANDS.get(dept_code, ["Private Label","RetailBrand"]))
    supplier = random.choice(SUPPLIER_MAP.get(dept_code, ["Genel Tedarikçi"]))
    cost  = round(random.uniform(2, 800), 2)
    price = round(cost * random.uniform(1.20, 2.50), 2)
    is_pl = random.random() < 0.12
    is_per= dept_code == "GDA" and random.random() < 0.40
    products.append({
        "product_id":       i,
        "sku_code":         f"8690{random.randint(100000000, 999999999)}",
        "product_name":     f"{brand} Ürün {sub_code}-{i}",
        "product_short_name": f"{brand[:8]}-{i}",
        "brand":            brand if not is_pl else "RetailPulse Özel",
        "supplier_name":    supplier,
        "supplier_code":    f"SUP-{hash(supplier) % 9999:04d}",
        "category_id":      cat_id_val,
        "unit_of_measure":  random.choices(["ADET","KG","LT"], weights=[0.70,0.20,0.10])[0],
        "net_weight_gr":    round(random.uniform(50, 2000), 1) if dept_code=="GDA" else None,
        "package_size":     random.choice(["Tekli","İkili","6'lı","12'li","500g","1kg","1L","250ml"]),
        "list_price":       price,
        "cost_price":       cost,
        "status_code":      random.choices(
            ["ACTIVE","PASSIVE","SEASONAL","CLEARANCE","NEW"],
            weights=[0.70,0.05,0.10,0.05,0.10])[0],
        "is_private_label": is_pl,
        "is_perishable":    is_per,
        "scd_start_date":   "2020-01-01",
        "scd_end_date":     None,
        "is_current":       True,
        "scd_version":      1,
    })
df_product = pd.DataFrame(products)
df_product.to_csv(f"{OUTPUT_DIR}/stg_product.csv", index=False)
print(f"   ✓ {len(df_product)} ürün")

# ─────────────────────────────────────────
# 5. dim_customer
# ─────────────────────────────────────────
print("👥 dim_customer oluşturuluyor...")
segments = ["Gold","Silver","Bronze","Occasional","New"]
seg_weights = [0.05,0.15,0.30,0.35,0.15]
loyalty_tiers = ["Platin","Altın","Gümüş","Bronz",None]
age_bands = ["18-24","25-34","35-44","45-54","55-64","65+"]
customers = []
for i in range(1, N_CUSTOMERS + 1):
    seg = random.choices(segments, weights=seg_weights)[0]
    customers.append({
        "customer_id":          i,
        "customer_segment":     seg,
        "age_band":             random.choice(age_bands),
        "gender":               random.choices(["M","F","U"], weights=[0.42,0.52,0.06])[0],
        "city":                 random.choice([c[0] for c in CITIES]),
        "acquisition_channel":  random.choice(["Mağaza","Online","Kampanya","Referans","Sosyal Medya"]),
        "loyalty_enrolled":     random.random() < 0.65,
        "loyalty_tier":         random.choice(loyalty_tiers),
        "clv_segment":          random.choices(["High","Medium","Low"], weights=[0.15,0.45,0.40])[0],
        "scd_start_date":       "2020-01-01",
        "scd_end_date":         None,
        "is_current":           True,
        "scd_version":          1,
    })
df_customer = pd.DataFrame(customers)
df_customer.to_csv(f"{OUTPUT_DIR}/stg_customer.csv", index=False)
print(f"   ✓ {len(df_customer):,} müşteri")

# ─────────────────────────────────────────
# 6. fact.sales_transaction
# Realistic seasonality + store format differences
# ─────────────────────────────────────────
print("💰 fact_sales_transaction oluşturuluyor (bu biraz sürebilir)...")

# Pre-index for speed
product_list = df_product[df_product["is_current"]==True].to_dict("records")
customer_ids = df_customer["customer_id"].tolist()
store_ids    = df_store[df_store["is_current"]==True]["store_id"].tolist()

# Format → daily transaction count range
FORMAT_TX = {
    "HYPER":   (800,  2200),
    "SUPER":   (250,   800),
    "EXPRESS": (80,    300),
    "OUTLET":  (150,   500),
}

def get_seasonality(d: date) -> float:
    """Turkish retail seasonality multiplier"""
    m = d.month
    if m in (11, 12):   return 1.35   # Yılbaşı
    if m in (6, 7, 8):  return 1.10   # Yaz
    if m in (3, 4):     return 1.05   # Ramazan / Nevruz
    if m in (1, 2):     return 0.85   # Ocak Kasavet
    return 1.0

def get_day_multiplier(d: date) -> float:
    dow = d.weekday()  # 0=Mon
    if dow == 5: return 1.40   # Cumartesi
    if dow == 6: return 1.25   # Pazar
    if dow == 4: return 1.10   # Cuma
    return 1.0

all_sales = []
tx_id = 1_000_000
current_date = DATE_START

# Build store→format lookup
store_fmt_map = {}
for _, row in df_store[df_store["is_current"]==True].iterrows():
    store_fmt_map[row["store_id"]] = row["format_code"]

while current_date <= DATE_END:
    season_mult = get_seasonality(current_date)
    day_mult    = get_day_multiplier(current_date)
    date_id     = int(current_date.strftime("%Y%m%d"))

    # Sample a subset of stores each day (not all 150 stores need full detail)
    daily_stores = random.sample(store_ids, min(len(store_ids), 30))

    for store_id in daily_stores:
        fmt = store_fmt_map.get(store_id, "SUPER")
        tx_min, tx_max = FORMAT_TX.get(fmt, (200, 600))
        n_transactions = int(random.randint(tx_min, tx_max) * season_mult * day_mult)

        for _ in range(n_transactions):
            # Basket: 1-12 items
            basket_size = max(1, int(np.random.exponential(3)))
            hour   = random.choices(range(8,23), weights=[2,3,5,7,8,9,10,10,10,9,8,7,5,4,3])[0]
            minute = random.randint(0,59)
            tx_dt  = datetime(current_date.year, current_date.month, current_date.day, hour, minute)
            customer_id = random.choices(customer_ids + [-1], weights=[0.65]+[0.35/len(customer_ids)]*len(customer_ids))[0]
            payment_id  = random.choices([1,2,3,4,5,6,7,8], weights=[10,35,20,15,8,4,3,5])[0]
            is_return   = random.random() < 0.015
            tx_type     = 2 if is_return else 1
            campaign_id = random.choices([-1,1,2,3,4,5], weights=[0.55,0.15,0.12,0.10,0.05,0.03])[0]

            for line in range(1, basket_size + 1):
                prod = random.choice(product_list)
                qty  = round(random.choices([1,2,3,4,5], weights=[0.55,0.25,0.12,0.05,0.03])[0]
                             * (random.uniform(0.3,3.0) if prod["unit_of_measure"]=="KG" else 1), 3)
                list_p = float(prod["list_price"])
                disc_r = random.choices([0,0.05,0.10,0.15,0.20,0.30], weights=[0.50,0.20,0.15,0.08,0.05,0.02])[0]
                sell_p = round(list_p * (1 - disc_r), 2)
                cost   = float(prod["cost_price"])
                disc_a = round((list_p - sell_p) * qty, 2)
                vat_r  = 0.01 if prod["category_id"] <= 50 else 0.20
                vat_a  = round(sell_p * qty * vat_r / (1 + vat_r), 2)

                all_sales.append({
                    "transaction_id":       tx_id,
                    "line_number":          line,
                    "store_id":             store_id,
                    "product_id":           prod["product_id"],
                    "customer_id":          customer_id,
                    "employee_id":          random.randint(1, 500),
                    "campaign_id":          campaign_id,
                    "payment_method_id":    payment_id,
                    "transaction_type_id":  tx_type,
                    "quantity":             qty if not is_return else -qty,
                    "unit_list_price":      list_p,
                    "unit_selling_price":   sell_p,
                    "unit_cost":            cost,
                    "discount_amount":      disc_a,
                    "vat_amount":           vat_a,
                    "transaction_datetime": tx_dt.isoformat(),
                    "date_id":              date_id,
                })
            tx_id += 1

    current_date += timedelta(days=1)

    if current_date.day == 1:
        print(f"   ⏳ {current_date.strftime('%Y-%m')} işleniyor... ({len(all_sales):,} satır)")

df_sales = pd.DataFrame(all_sales)
print(f"   💾 CSV yazılıyor: {len(df_sales):,} satır...")
# Write in chunks to avoid memory issues
chunk_size = 500_000
for i, chunk_start in enumerate(range(0, len(df_sales), chunk_size)):
    chunk = df_sales.iloc[chunk_start:chunk_start+chunk_size]
    mode = 'w' if i == 0 else 'a'
    header = i == 0
    chunk.to_csv(f"{OUTPUT_DIR}/stg_sales_transaction.csv", mode=mode, header=header, index=False)
print(f"   ✓ {len(df_sales):,} satış işlemi")

# ─────────────────────────────────────────
# 7. fact.daily_store_kpi (pre-aggregated)
# ─────────────────────────────────────────
print("📊 fact_daily_store_kpi aggregate oluşturuluyor...")
df_kpi = df_sales[df_sales["transaction_type_id"]==1].groupby(["date_id","store_id"]).agg(
    gross_sales         =("unit_list_price",  lambda x: (x * df_sales.loc[x.index,"quantity"]).sum()),
    discount_amount     =("discount_amount",  "sum"),
    net_sales           =("unit_selling_price",lambda x: (x * df_sales.loc[x.index,"quantity"]).sum()),
    cogs                =("unit_cost",         lambda x: (x * df_sales.loc[x.index,"quantity"]).sum()),
    vat_collected       =("vat_amount",        "sum"),
    units_sold          =("quantity",          "sum"),
    transaction_count   =("transaction_id",    "nunique"),
    unique_customers    =("customer_id",       lambda x: (x[x!=-1]).nunique()),
).reset_index()
df_kpi["gross_margin"] = df_kpi["net_sales"] - df_kpi["cogs"]
df_kpi["avg_basket_size"] = df_kpi["net_sales"] / df_kpi["transaction_count"]
df_kpi["gross_margin_pct"] = (df_kpi["gross_margin"] / df_kpi["net_sales"].replace(0,np.nan) * 100).round(4)
df_kpi["return_amount"] = 0
df_kpi["units_returned"] = 0
df_kpi["return_transaction_count"] = 0
df_kpi["sales_target"] = (df_kpi["net_sales"] * random.uniform(1.03,1.12)).round(2)
df_kpi.to_csv(f"{OUTPUT_DIR}/fact_daily_store_kpi.csv", index=False)
print(f"   ✓ {len(df_kpi):,} günlük KPI kaydı")

# ─────────────────────────────────────────
# 8. Summary
# ─────────────────────────────────────────
print("\n" + "="*60)
print("✅ DEMO DATA GENERATION TAMAMLANDI")
print("="*60)
files = []
for f in os.listdir(OUTPUT_DIR):
    path = os.path.join(OUTPUT_DIR, f)
    size_mb = os.path.getsize(path) / 1024 / 1024
    rows = sum(1 for _ in open(path)) - 1
    files.append((f, rows, size_mb))
    print(f"   📄 {f:<45} {rows:>10,} rows  {size_mb:>6.1f} MB")

total_mb = sum(f[2] for f in files)
print(f"\n   💾 Toplam: {total_mb:.1f} MB")
print(f"\n📂 Dosyalar: ./{OUTPUT_DIR}/")
print("🚀 BigQuery'e yüklemek için: bq load --source_format=CSV dataset.table output_csv/file.csv")
