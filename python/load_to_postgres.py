"""
RetailPulse — PostgreSQL Veri Yükleyici
CSV dosyalarını PostgreSQL'e yükler
"""

import os
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import numpy as np

# ─────────────────────────────────────────
# VERİTABANI BAĞLANTISI
# ─────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "retailpulse",
    "user":     "postgres",
    "password": "Ya6473..",  # ← kendi şifreni yaz
}

INPUT_DIR = "output_csv"

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def load_table(conn, table, df, batch_size=5000):
    """DataFrame'i PostgreSQL tablosuna yükle"""
    df = df.replace({np.nan: None, float('inf'): None, float('-inf'): None})
    cols = list(df.columns)
    values = [tuple(row) for row in df.itertuples(index=False)]
    
    col_str = ", ".join(cols)
    with conn.cursor() as cur:
        total = len(values)
        loaded = 0
        for i in range(0, total, batch_size):
            batch = values[i:i+batch_size]
            execute_values(
                cur,
                f"INSERT INTO {table} ({col_str}) VALUES %s ON CONFLICT DO NOTHING",
                batch
            )
            loaded += len(batch)
            if total > 10000:
                print(f"   ⏳ {loaded:,} / {total:,} satır yüklendi...", end="\r")
        conn.commit()
    print(f"   ✓ {total:,} satır → {table}          ")

print("🚀 RetailPulse PostgreSQL Yükleyici başlıyor...")
print(f"   Bağlanıyor: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")

try:
    conn = get_conn()
    print("   ✅ Bağlantı başarılı!\n")
except Exception as e:
    print(f"   ❌ Bağlantı hatası: {e}")
    print("   DB_CONFIG içindeki şifreyi kontrol et!")
    exit(1)

# ─────────────────────────────────────────
# 1. dim.date — Stored Procedure ile doldur
# ─────────────────────────────────────────
print("📅 dim.date yükleniyor (stored procedure)...")
with conn.cursor() as cur:
    cur.execute("CALL staging.sp_load_dim_date('2020-01-01', '2030-12-31');")
    conn.commit()
print("   ✓ dim.date → 2020-2030 takvim yüklendi")

# ─────────────────────────────────────────
# 2. dim.region
# ─────────────────────────────────────────
print("\n📍 dim.region yükleniyor...")
df = pd.read_csv(f"{INPUT_DIR}/stg_region.csv")
load_table(conn, "dim.region", df[[
    "region_id","region_code","region_name","city","city_code",
    "nuts2_code","nuts2_name","latitude","longitude","timezone","population_segment"
]])

# ─────────────────────────────────────────
# 3. dim.product_category
# ─────────────────────────────────────────
print("📦 dim.product_category yükleniyor...")
df = pd.read_csv(f"{INPUT_DIR}/stg_product_category.csv")
load_table(conn, "dim.product_category", df[[
    "category_id","department_code","department_name",
    "category_code","category_name","subcategory_code","subcategory_name",
    "is_food","is_private_label","vat_rate"
]])

# ─────────────────────────────────────────
# 4. dim.store
# ─────────────────────────────────────────
print("🏪 dim.store yükleniyor...")
df = pd.read_csv(f"{INPUT_DIR}/stg_store.csv")
# region_sk lookup
with conn.cursor() as cur:
    cur.execute("SELECT region_id, region_sk FROM dim.region")
    region_map = {row[0]: row[1] for row in cur.fetchall()}
df["region_sk"] = df["region_id"].map(region_map)
load_table(conn, "dim.store", df[[
    "store_id","store_code","store_name","store_short_name",
    "format_code","store_sqm","selling_sqm","region_sk",
    "is_mall_store","is_franchise","cluster","headcount","opening_date",
    "scd_start_date","scd_end_date","is_current","scd_version"
]])

# ─────────────────────────────────────────
# 5. dim.product
# ─────────────────────────────────────────
print("🛒 dim.product yükleniyor...")
df = pd.read_csv(f"{INPUT_DIR}/stg_product.csv")
with conn.cursor() as cur:
    cur.execute("SELECT category_id, category_sk FROM dim.product_category")
    cat_map = {row[0]: row[1] for row in cur.fetchall()}
df["category_sk"] = df["category_id"].map(cat_map)
load_table(conn, "dim.product", df[[
    "product_id","sku_code","product_name","brand","category_sk",
    "unit_of_measure","list_price","cost_price","status_code",
    "is_private_label","is_perishable",
    "scd_start_date","scd_end_date","is_current","scd_version"
]])

# ─────────────────────────────────────────
# 6. dim.customer
# ─────────────────────────────────────────
print("👥 dim.customer yükleniyor...")
df = pd.read_csv(f"{INPUT_DIR}/stg_customer.csv")
load_table(conn, "dim.customer", df[[
    "customer_id","customer_segment","age_band","gender","city",
    "loyalty_enrolled","loyalty_tier","clv_segment",
    "scd_start_date","scd_end_date","is_current","scd_version"
]])

# ─────────────────────────────────────────
# 7. dim.campaign — Varsayılan kayıtlar
# ─────────────────────────────────────────
print("🎯 dim.campaign yükleniyor...")
campaigns = pd.DataFrame([
    (1, "CAMP-001", "Yılbaşı İndirimi 2023",    "İndirim",   "Tüm Kanal", "2022-12-15", "2023-01-05"),
    (2, "CAMP-002", "Ramazan Kampanyası 2023",   "İndirim",   "Mağaza",    "2023-03-23", "2023-04-21"),
    (3, "CAMP-003", "Yaz Sezonu 2023",           "BOGO",      "Tüm Kanal", "2023-06-01", "2023-08-31"),
    (4, "CAMP-004", "Yılbaşı İndirimi 2024",     "İndirim",   "Tüm Kanal", "2023-12-15", "2024-01-05"),
    (5, "CAMP-005", "Yaz Sezonu 2024",           "BOGO",      "Online",    "2024-06-01", "2024-08-31"),
], columns=["campaign_id","campaign_code","campaign_name","campaign_type","channel","start_date","end_date"])
load_table(conn, "dim.campaign", campaigns)

# ─────────────────────────────────────────
# 8. fact.sales_transaction (En büyük tablo)
# ─────────────────────────────────────────
print("\n💰 fact.sales_transaction yükleniyor (3.5M satır - en uzun adım)...")

# SK lookup tabloları
with conn.cursor() as cur:
    cur.execute("SELECT store_id, store_sk FROM dim.store WHERE is_current=TRUE")
    store_map = {row[0]: row[1] for row in cur.fetchall()}
    cur.execute("SELECT product_id, product_sk FROM dim.product WHERE is_current=TRUE")
    product_map = {row[0]: row[1] for row in cur.fetchall()}
    cur.execute("SELECT customer_id, customer_sk FROM dim.customer WHERE is_current=TRUE")
    customer_map = {row[0]: row[1] for row in cur.fetchall()}
    customer_map[-1] = -1
    cur.execute("SELECT campaign_id, campaign_sk FROM dim.campaign")
    campaign_map = {row[0]: row[1] for row in cur.fetchall()}
    campaign_map[-1] = -1

chunk_size = 100_000
total_loaded = 0

for chunk in pd.read_csv(f"{INPUT_DIR}/stg_sales_transaction.csv", chunksize=chunk_size):
    chunk["store_sk"]    = chunk["store_id"].map(store_map)
    chunk["product_sk"]  = chunk["product_id"].map(product_map)
    chunk["customer_sk"] = chunk["customer_id"].map(customer_map).fillna(-1).astype(int)
    chunk["campaign_sk"] = chunk["campaign_id"].map(campaign_map).fillna(-1).astype(int)

    # Geçersiz SK'ları filtrele
    chunk = chunk.dropna(subset=["store_sk","product_sk"])
    chunk["store_sk"]   = chunk["store_sk"].astype(int)
    chunk["product_sk"] = chunk["product_sk"].astype(int)

    cols = [
        "transaction_id","line_number","date_id",
        "store_sk","product_sk","customer_sk","campaign_sk",
        "payment_method_id","transaction_type_id",
        "quantity","unit_list_price","unit_selling_price","unit_cost",
        "discount_amount","vat_amount","transaction_datetime"
    ]
    load_table(conn, "fact.sales_transaction", chunk[cols], batch_size=10_000)
    total_loaded += len(chunk)
    print(f"   📊 Toplam yüklenen: {total_loaded:,} satır")

# ─────────────────────────────────────────
# 9. fact.daily_store_kpi
# ─────────────────────────────────────────
print("\n📊 fact.daily_store_kpi yükleniyor...")
df = pd.read_csv(f"{INPUT_DIR}/fact_daily_store_kpi.csv")
df["store_sk"] = df["store_id"].map(store_map)
df = df.dropna(subset=["store_sk"])
df["store_sk"] = df["store_sk"].astype(int)
load_table(conn, "fact.daily_store_kpi", df[[
    "date_id","store_sk","gross_sales","discount_amount","net_sales",
    "cogs","gross_margin","vat_collected","units_sold",
    "transaction_count","unique_customers","avg_basket_size",
    "gross_margin_pct","sales_target"
]], batch_size=10_000)

conn.close()

print("\n" + "="*55)
print("✅ POSTGRESQL YÜKLEME TAMAMLANDI!")
print("="*55)
print("🚀 Sonraki adım: Power BI'a bağlan!")
