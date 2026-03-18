# 🛒 RetailPulse — Kurumsal Perakende Veri Ambarı

> **Türkiye'deki büyük perakende zincirleri (100+ mağaza) için üretim kalitesinde Veri Ambarı**
> Kimball Dimensional Modeling metodolojisiyle inşa edildi

---

## 🏗️ Mimari

```
dict (referans) → dim (SCD1/2) → fact (partition'lı) → staging (ETL) → mart (view'lar)
```

---

## 📦 İçerik

### Dictionary Tabloları (7 adet)
Statik referans verisi: para birimi, gün tipleri, mağaza formatları, ürün statüleri,
işlem tipleri, ödeme yöntemleri, KPI tanımları

### Dimension Tabloları (8 adet)

| Tablo | SCD Tipi | Takip Edilen Değişiklik |
|-------|----------|------------------------|
| dim.date | Statik | 2020–2030 tam takvim, Türkçe locale |
| dim.region | SCD 1 | NUTS2 kodlu Türk şehirleri |
| dim.store | **SCD 2** | Cluster, format, personel değişiklikleri |
| dim.product | **SCD 2** | Fiyat, kategori, statü değişiklikleri |
| dim.product_category | SCD 1 | 4 seviyeli hiyerarşi |
| dim.customer | **SCD 2** | Segment, sadakat kademesi değişiklikleri |
| dim.employee | **SCD 2** | Mağaza, unvan değişiklikleri |
| dim.campaign | SCD 1 | Promosyon kampanyaları |

### Fact Tabloları (4 adet)

| Tablo | Granülerlik | Yükleme Stratejisi |
|-------|-------------|-------------------|
| fact.sales_transaction | Satır kalemi | Incremental (watermark) |
| fact.daily_store_kpi | Mağaza × Gün | Truncate-Insert |
| fact.inventory_snapshot | Ürün × Mağaza × Gün | Truncate-Insert |
| fact.campaign_performance | Kampanya × Mağaza | Accumulating Snapshot |

### ETL Stored Procedure'ları (7 adet)
- `sp_load_dim_date` → 10 yıllık takvim yüklemesi (tek seferlik)
- `sp_scd2_store` + `sp_scd2_product` → Hash tabanlı SCD2 versiyonlama
- `sp_incremental_sales` → Watermark tabanlı artımlı yükleme
- `sp_truncate_daily_kpi` → Kayan pencere KPI yenilemesi
- `sp_run_etl_pipeline` → Master orkestratör (tek CALL ile tüm pipeline)

### Analitik View'lar — Mart Katmanı (6 adet)
- `mart.v_sales_daily` → Türkçe alias'larla tam bağlamlı satış view'u
- `mart.v_store_kpi_monthly` → Mağaza bazında aylık KPI özeti
- `mart.v_inventory_alert` → Stok tükenmesi / fazla stok tespiti
- `mart.v_category_performance` → Kategori bazında kâr-zarar
- `mart.v_customer_rfm` → RFM segmentasyonu (8 segment)
- `mart.v_etl_monitor` → ETL sağlık izleme dashboard'u

---

## 🎯 Demo Veri Seti

| Varlık | Hacim |
|--------|-------|
| Mağaza | 150 (SCD2 geçmişiyle) |
| Ürün | 3.000 SKU |
| Müşteri | 80.000 sadakat üyesi |
| İşlem | ~30 milyon satır |
| Tarih Aralığı | 2023–2025 (3 yıl) |
| Şehir | 15 Türk şehri |

Türkiye'ye özgü mevsimsellik dahil: Ramazan, Yılbaşı, milli tatiller, hafta sonu zirveleri.

---

## 🚀 Kurulum

```bash
# 1. Veritabanını oluştur
createdb retailpulse

# 2. DDL'leri sırayla çalıştır
psql -d retailpulse -f sql/ddl/01_dict_tables.sql
psql -d retailpulse -f sql/ddl/02_dim_tables.sql
psql -d retailpulse -f sql/ddl/03_fact_tables.sql
psql -d retailpulse -f sql/procedures/01_stored_procedures.sql
psql -d retailpulse -f sql/views/01_analytical_views.sql

# 3. Demo veriyi üret
pip install pandas numpy
python python/generate_demo_data.py
```

---

## 🛠️ Teknoloji Stack

| Katman | Teknoloji |
|--------|-----------|
| Veri Ambarı | PostgreSQL 15 / Google BigQuery |
| ETL | Python 3.11+ (pandas, numpy) |
| Procedure'lar | PL/pgSQL |
| BI | Power BI Desktop |
| Orkestrasyon | Stored procedure pipeline |

---

## 📁 Proje Yapısı

```
retailpulse-dw/
├── sql/
│   ├── ddl/
│   │   ├── 01_dict_tables.sql
│   │   ├── 02_dim_tables.sql
│   │   └── 03_fact_tables.sql
│   ├── procedures/
│   │   └── 01_stored_procedures.sql
│   └── views/
│       └── 01_analytical_views.sql
├── python/
│   └── generate_demo_data.py
├── powerbi/
├── docs/
├── versions/
│   └── CHANGELOG.md
├── CLAUDE.md
└── README.md
```

---

## 📋 Versiyon Geçmişi

Tüm değişiklikler için [CHANGELOG](versions/CHANGELOG.md) dosyasına bakın.

---

## 👤 Yazar

**Yusuf Akviran** — Kıdemli İş Zekası Uzmanı
6+ yıl | Power BI · SQL · BigQuery · ETL · Dimensional Modeling
📍 İstanbul, Türkiye · [LinkedIn](https://linkedin.com/in/yusufakviran)

---

*RetailPulse, Türk perakendesi için kurumsal düzeyde veri ambarı tasarımını sergileyen bir portföy projesidir.*
