"""
RetailPulse — Demo Data Generator (Hızlı Versiyon)
~3 milyon satış işlemi, ~2-3 dakikada tamamlanır
"""

import random
import os
from datetime import date, timedelta

import numpy as np
import pandas as pd

RANDOM_SEED  = 42
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

N_STORES     = 150
N_PRODUCTS   = 3_000
N_CUSTOMERS  = 80_000
DATE_START   = date(2023, 1, 1)
DATE_END     = date(2025, 12, 31)
OUTPUT_DIR   = "output_csv"
DAILY_TX     = 2_800

os.makedirs(OUTPUT_DIR, exist_ok=True)
print("🚀 RetailPulse Demo Data Generator başlıyor...")

CITIES = [
    ("İstanbul","IST","TR10","Marmara","Metropolitan"),
    ("Ankara","ANK","TR51","Orta Anadolu","Metropolitan"),
    ("İzmir","IZM","TR31","Ege","Metropolitan"),
    ("Bursa","BRS","TR41","Marmara","Urban"),
    ("Antalya","ANT","TR61","Akdeniz","Urban"),
    ("Adana","ADA","TR62","Akdeniz","Urban"),
    ("Konya","KNY","TR52","Orta Anadolu","Urban"),
    ("Gaziantep","GAZ","TR90","Güneydoğu","Urban"),
    ("Kayseri","KAY","TR72","Orta Anadolu","Urban"),
    ("Mersin","MER","TR62","Akdeniz","Urban"),
    ("Eskişehir","ESK","TR41","Marmara","Urban"),
    ("Trabzon","TRB","TR90","Karadeniz","Urban"),
    ("Diyarbakır","DIY","TR72","Doğu Anadolu","Urban"),
    ("Samsun","SAM","TR83","Karadeniz","Urban"),
    ("Denizli","DNZ","TR32","Ege","Urban"),
]

DEPARTMENTS = [
    ("GDA","Gıda"),("ICK","İçecek"),("KIS","Kişisel Bakım"),
    ("EV","Ev & Yaşam"),("TEK","Teknoloji"),
]

BRANDS = [
    "Ülker","Pınar","Sütaş","ETi","Torku","Tadım","Koska","Tukaş",
    "Coca-Cola","Fuse Tea","Doğadan","Çaykur","Nescafé","Lipton",
    "Dove","Nivea","Pantene","Colgate","Arko","Oral-B",
    "Omo","Fairy","Domestos","Vanish","Pril",
    "Philips","Tefal","Bosch","Braun","Xiaomi","Private Label",
]

print("📍 dim_region oluşturuluyor...")
regions = []
for i, (city, code, nuts2, nuts2_name, pop_seg) in enumerate(CITIES, 1):
    regions.append({
        "region_id": i, "region_code": code, "region_name": f"{city} Bölgesi",
        "city": city, "city_code": code, "nuts2_code": nuts2,
        "nuts2_name": nuts2_name, "timezone": "Europe/Istanbul",
        "population_segment": pop_seg,
        "latitude": round(random.uniform(36.5, 42.0), 6),
        "longitude": round(random.uniform(26.0, 44.5), 6),
    })
df_region = pd.DataFrame(regions)
df_region.to_csv(f"{OUTPUT_DIR}/stg_region.csv", index=False)
print(f"   ✓ {len(df_region)} bölge")

print("📦 dim_product_category oluşturuluyor...")
categories = []
cat_id = 1
for dept_code, dept_name in DEPARTMENTS:
    for j in range(1, 4):
        for k in range(1, 4):
            categories.append({
                "category_id": cat_id,
                "department_code": dept_code, "department_name": dept_name,
                "category_code": f"{dept_code}-{j:02d}",
                "category_name": f"{dept_name} Kategori {j}",
                "subcategory_code": f"{dept_code}-{j:02d}-{k:02d}",
                "subcategory_name": f"{dept_name} Alt Kategori {j}-{k}",
                "is_food": dept_code in ("GDA","ICK"),
                "is_private_label": False,
                "vat_rate": 1.0 if dept_code == "GDA" else 20.0,
            })
            cat_id += 1
df_category = pd.DataFrame(categories)
df_category.to_csv(f"{OUTPUT_DIR}/stg_product_category.csv", index=False)
print(f"   ✓ {len(df_category)} kategori")

print("🏪 dim_store oluşturuluyor...")
FORMATS = [("HYPER",0.10,5000,12000),("SUPER",0.45,1000,4999),
           ("EXPRESS",0.35,150,999),("OUTLET",0.10,500,2500)]
fmt_weights = [f[1] for f in FORMATS]
stores = []
for i in range(1, N_STORES + 1):
    fmt = random.choices(FORMATS, weights=fmt_weights)[0]
    fmt_code, _, sqm_min, sqm_max = fmt
    city_row = random.choices(CITIES, weights=[5 if c[4]=="Metropolitan" else 2 for c in CITIES])[0]
    sqm = random.randint(sqm_min, sqm_max)
    cluster = random.choices(["A","B","C","D"], weights=[0.15,0.30,0.35,0.20])[0]
    stores.append({
        "store_id": i, "store_code": f"STR-{i:04d}",
        "store_name": f"{city_row[0]} {['Merkez','Şehir','Batı','Kuzey','Güney','AVM'][i%6]} Mağazası",
        "store_short_name": f"{city_row[1]}-{i:03d}",
        "format_code": fmt_code, "store_sqm": sqm,
        "selling_sqm": int(sqm * 0.72),
        "region_id": CITIES.index(city_row) + 1,
        "is_mall_store": fmt_code in ("HYPER","SUPER") and random.random() < 0.4,
        "is_franchise": random.random() < 0.08,
        "cluster": cluster,
        "headcount": random.randint(4, max(5, sqm // 50)),
        "opening_date": date(random.randint(2010,2022), random.randint(1,12), 1).isoformat(),
        "scd_start_date": "2020-01-01", "scd_end_date": None,
        "is_current": True, "scd_version": 1,
    })
df_store = pd.DataFrame(stores)
df_store.to_csv(f"{OUTPUT_DIR}/stg_store.csv", index=False)
print(f"   ✓ {len(df_store)} mağaza")

print("🛒 dim_product oluşturuluyor...")
cat_ids = df_category["category_id"].tolist()
dept_codes = df_category["department_code"].tolist()
products = []
for i in range(1, N_PRODUCTS + 1):
    idx = random.randint(0, len(cat_ids)-1)
    brand = random.choice(BRANDS)
    cost  = round(random.uniform(2, 500), 2)
    price = round(cost * random.uniform(1.25, 2.40), 2)
    products.append({
        "product_id": i,
        "sku_code": f"869{random.randint(1000000000, 9999999999)}",
        "product_name": f"{brand} Ürün {i:04d}",
        "brand": brand, "category_id": cat_ids[idx],
        "unit_of_measure": random.choices(["ADET","KG","LT"], weights=[0.70,0.20,0.10])[0],
        "list_price": price, "cost_price": cost,
        "status_code": random.choices(["ACTIVE","PASSIVE","SEASONAL","NEW"],
                                       weights=[0.75,0.05,0.10,0.10])[0],
        "is_private_label": brand == "Private Label",
        "is_perishable": dept_codes[idx] in ("GDA","ICK") and random.random() < 0.4,
        "scd_start_date": "2020-01-01", "scd_end_date": None,
        "is_current": True, "scd_version": 1,
    })
df_product = pd.DataFrame(products)
df_product.to_csv(f"{OUTPUT_DIR}/stg_product.csv", index=False)
print(f"   ✓ {len(df_product)} ürün")

print("👥 dim_customer oluşturuluyor...")
n = N_CUSTOMERS
df_customer = pd.DataFrame({
    "customer_id": range(1, n+1),
    "customer_segment": np.random.choice(["Gold","Silver","Bronze","Occasional","New"],
                                          n, p=[0.05,0.15,0.30,0.35,0.15]),
    "age_band": np.random.choice(["18-24","25-34","35-44","45-54","55-64","65+"], n),
    "gender": np.random.choice(["M","F","U"], n, p=[0.42,0.52,0.06]),
    "city": np.random.choice([c[0] for c in CITIES], n),
    "loyalty_enrolled": np.random.choice([True,False], n, p=[0.65,0.35]),
    "loyalty_tier": np.random.choice(["Platin","Altın","Gümüş","Bronz",""], n),
    "clv_segment": np.random.choice(["High","Medium","Low"], n, p=[0.15,0.45,0.40]),
    "scd_start_date": "2020-01-01", "scd_end_date": None,
    "is_current": True, "scd_version": 1,
})
df_customer.to_csv(f"{OUTPUT_DIR}/stg_customer.csv", index=False)
print(f"   ✓ {len(df_customer):,} müşteri")

print("💰 fact_sales_transaction oluşturuluyor...")
store_ids    = df_store["store_id"].values
product_ids  = df_product["product_id"].values
list_prices  = df_product["list_price"].values
cost_prices  = df_product["cost_price"].values
customer_ids = np.append(df_customer["customer_id"].values, np.full(5000, -1))

all_days = []
current = DATE_START
while current <= DATE_END:
    all_days.append(current)
    current += timedelta(days=1)

chunks = []
tx_id = 1_000_000

for day_idx, current_date in enumerate(all_days):
    m   = current_date.month
    dow = current_date.weekday()
    season = 1.35 if m in (11,12) else 1.10 if m in (6,7,8) else 1.05 if m in (3,4) else 0.85 if m in (1,2) else 1.0
    day_w  = 1.40 if dow==5 else 1.25 if dow==6 else 1.10 if dow==4 else 1.0
    n_tx   = int(DAILY_TX * season * day_w)
    date_id = int(current_date.strftime("%Y%m%d"))

    prod_idx  = np.random.randint(0, len(product_ids), n_tx)
    store_idx = np.random.randint(0, len(store_ids), n_tx)
    cust_idx  = np.random.randint(0, len(customer_ids), n_tx)
    qty       = np.random.choice([1,2,3,4,5], n_tx, p=[0.55,0.25,0.12,0.05,0.03]).astype(float)
    disc_r    = np.random.choice([0,0.05,0.10,0.15,0.20], n_tx, p=[0.50,0.20,0.15,0.10,0.05])
    tx_type   = np.random.choice([1,2], n_tx, p=[0.985,0.015])
    payment   = np.random.choice([1,2,3,4,5,6,7,8], n_tx, p=[0.10,0.35,0.20,0.15,0.08,0.04,0.03,0.05])
    campaign  = np.random.choice([-1,1,2,3,4,5], n_tx, p=[0.55,0.15,0.12,0.10,0.05,0.03])
    hours     = np.random.choice(range(8,23), n_tx)
    minutes   = np.random.randint(0, 60, n_tx)

    lp     = list_prices[prod_idx]
    cp     = cost_prices[prod_idx]
    sp     = np.round(lp * (1 - disc_r), 2)
    disc_a = np.round((lp - sp) * qty, 2)
    vat_a  = np.round(sp * qty * 0.10, 2)
    signed_qty = np.where(tx_type==2, -qty, qty)

    tx_datetimes = [f"{current_date} {h:02d}:{mn:02d}:00" for h, mn in zip(hours, minutes)]

    chunk = pd.DataFrame({
        "transaction_id":      range(tx_id, tx_id + n_tx),
        "line_number":         1,
        "store_id":            store_ids[store_idx],
        "product_id":          product_ids[prod_idx],
        "customer_id":         customer_ids[cust_idx],
        "employee_id":         np.random.randint(1, 500, n_tx),
        "campaign_id":         campaign,
        "payment_method_id":   payment,
        "transaction_type_id": tx_type,
        "quantity":            signed_qty,
        "unit_list_price":     lp,
        "unit_selling_price":  sp,
        "unit_cost":           cp,
        "discount_amount":     disc_a,
        "vat_amount":          vat_a,
        "transaction_datetime": tx_datetimes,
        "date_id":             date_id,
    })
    chunks.append(chunk)
    tx_id += n_tx

    if day_idx % 30 == 0:
        total_so_far = sum(len(c) for c in chunks)
        print(f"   ⏳ {current_date.strftime('%Y-%m')} tamamlandı... {total_so_far:,} satır")

print("   💾 CSV yazılıyor (büyük dosya, biraz sürebilir)...")
df_sales = pd.concat(chunks, ignore_index=True)
df_sales.to_csv(f"{OUTPUT_DIR}/stg_sales_transaction.csv", index=False)
print(f"   ✓ {len(df_sales):,} satış işlemi")

print("📊 fact_daily_store_kpi oluşturuluyor...")
df_s = df_sales[df_sales["transaction_type_id"]==1].copy()
df_s["net_sales"]   = df_s["unit_selling_price"] * df_s["quantity"] - df_s["discount_amount"]
df_s["gross_sales"] = df_s["unit_list_price"] * df_s["quantity"]
df_s["cogs"]        = df_s["unit_cost"] * df_s["quantity"]
df_kpi = df_s.groupby(["date_id","store_id"]).agg(
    gross_sales=("gross_sales","sum"), discount_amount=("discount_amount","sum"),
    net_sales=("net_sales","sum"), cogs=("cogs","sum"),
    vat_collected=("vat_amount","sum"), units_sold=("quantity","sum"),
    transaction_count=("transaction_id","nunique"),
    unique_customers=("customer_id","nunique"),
).reset_index()
df_kpi["gross_margin"]     = df_kpi["net_sales"] - df_kpi["cogs"]
df_kpi["avg_basket_size"]  = (df_kpi["net_sales"] / df_kpi["transaction_count"]).round(2)
df_kpi["gross_margin_pct"] = (df_kpi["gross_margin"] / df_kpi["net_sales"].replace(0,np.nan) * 100).round(4)
df_kpi["sales_target"]     = (df_kpi["net_sales"] * 1.08).round(2)
df_kpi.to_csv(f"{OUTPUT_DIR}/fact_daily_store_kpi.csv", index=False)
print(f"   ✓ {len(df_kpi):,} günlük KPI kaydı")

print("\n" + "="*55)
print("✅ DEMO VERİ OLUŞTURMA TAMAMLANDI")
print("="*55)
for f in sorted(os.listdir(OUTPUT_DIR)):
    path = os.path.join(OUTPUT_DIR, f)
    size_mb = os.path.getsize(path) / 1024 / 1024
    rows = sum(1 for _ in open(path, encoding="utf-8")) - 1
    print(f"   📄 {f:<45} {rows:>8,} satır  {size_mb:>5.1f} MB")
print(f"\n📂 Dosyalar: ./{OUTPUT_DIR}/")
print("🚀 Sonraki adım: PostgreSQL'e yükle!")
