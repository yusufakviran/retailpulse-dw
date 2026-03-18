# RetailPulse — Claude Code Proje Rehberi

## Proje Özeti
Türkiye'deki büyük perakende zincirleri (100+ mağaza) için kurumsal veri ambarı.
Kimball Dimensional Modeling | PostgreSQL | Python | Power BI

---

## 🚦 Çalışma Kuralları
- Her zaman Türkçe konuş
- Herhangi bir değişiklik yapmadan önce plan göster ve onay bekle
- Asla dosya silme — önce sor
- Her geliştirme sonrası versiyon numarasını artır
- Kod değişikliklerini her zaman `versions/` klasörüne de kaydet

---

## 📁 Klasör Yapısı
```
retailpulse-dw/
├── sql/
│   ├── ddl/              → Tablo tanımları (sırayla çalıştır: 01 → 02 → 03)
│   ├── procedures/       → ETL stored procedure'ları
│   └── views/            → Mart katmanı analitik view'ları
├── python/               → Veri üretimi ve ETL scriptleri
├── powerbi/              → Power BI dashboard dosyaları
├── docs/                 → Dokümantasyon
├── versions/             → Versiyon geçmişi (her değişiklik buraya)
├── CLAUDE.md             → Bu dosya
└── README.md             → Proje tanıtımı
```

---

## 🗂️ Versiyonlama Kuralları

### Klasör Yapısı
```
versions/
├── sql/
│   ├── v1.0.0_init_schema.sql
│   ├── v1.1.0_add_campaign_table.sql
│   └── v1.2.0_update_store_scd2.sql
├── python/
│   ├── v1.0.0_generate_demo_data.py
│   └── v1.1.0_etl_pipeline.py
└── CHANGELOG.md
```

### Versiyon Numarası Formatı
```
v[MAJOR].[MINOR].[PATCH]

MAJOR → Mimari değişiklik (yeni fact/dim tablosu, schema değişikliği)
MINOR → Yeni özellik (yeni view, yeni procedure, yeni Python scripti)
PATCH → Küçük düzeltme (bug fix, index ekleme, yorum güncelleme)
```

### Örnekler
```
v1.0.0 → İlk schema (dict + dim + fact)
v1.1.0 → Yeni mart view eklendi
v1.1.1 → View'da bug fix
v2.0.0 → BigQuery'e geçiş
```

### Versiyonlama Adımları
Her değişiklikten sonra şu adımları takip et:

1. Değişikliği ana dosyada yap
2. Kopyasını `versions/` klasörüne kaydet (versiyon numarasıyla)
3. `versions/CHANGELOG.md` dosyasını güncelle
4. Git commit mesajına versiyon numarasını yaz

### CHANGELOG Formatı
```markdown
## [v1.1.0] - 2025-03-18
### Eklendi
- mart.v_customer_rfm view'u eklendi
### Değiştirildi
- dim.store tablosuna cluster kolonu eklendi
### Düzeltildi
- fact.sales_transaction FK hatası giderildi
```

---

## 🏗️ Mimari

```
Kaynak Sistemler (POS / ERP / Excel)
          ↓
    staging schema  ←── ETL batch log (watermark)
          ↓
    dict schema     ←── Statik referans tabloları
          ↓
    dim schema      ←── Dimension'lar (SCD1 & SCD2)
          ↓
    fact schema     ←── Fact tabloları (partition'lı)
          ↓
    mart schema     ←── Analitik view'lar (Power BI bağlanır)
```

---

## 🔄 Yükleme Stratejileri
- `fact.sales_transaction`  → INCREMENTAL (watermark tabanlı)
- `fact.daily_store_kpi`    → TRUNCATE-INSERT (kayan pencere)
- `dim.store / dim.product` → SCD TYPE 2 (MD5 hash karşılaştırma)
- `dim.date`                → FULL LOAD (tek seferlik, 10 yıllık)

---

## 📐 Temel Kurallar
- Surrogate key'ler : `_sk` suffix (SERIAL)
- Business key'ler  : `_id` suffix
- SCD2 kolonları    : `scd_start_date`, `scd_end_date`, `is_current`, `scd_version`, `scd_hash`
- Tüm fact FK'larının -1 "bilinmeyen" dimension kaydı olmalı
- Mart view'larında Türkçe kolon alias'ları kullan (iş kullanıcıları için)
- Partition: `fact.sales_transaction` → `date_id` (YYYYMMDD INT) ile aylık

---

## 🛠️ Teknoloji Stack
- Veritabanı  : PostgreSQL 15 (lokal) / Google BigQuery (bulut)
- ETL         : Python 3.11+ (pandas, numpy)
- Procedure'lar: PL/pgSQL
- BI          : Power BI Desktop
- API         : FastAPI (planlandı)
- Frontend    : React + Recharts (planlandı)

---

## 🔌 Veritabanı Bağlantısı (Lokal Geliştirme)
```
Host     : localhost
Port     : 5432
Database : retailpulse
Şema sırası: dict → dim → fact → staging → mart
```

---

## ⚠️ SQL Değiştirirken Dikkat
- Tablo silmeden önce FK bağımlılıklarını kontrol et
- SCD2 tablolarında takip edilen kolonları direkt UPDATE etme — `sp_scd2_*` procedure'larını kullan
- Fact tablolarında partition sınırlarını kontrol etmeden DELETE yapma

---

## 🎲 Demo Veri
- 150 mağaza · 3.000 SKU · 80.000 müşteri · ~30 milyon işlem
- Tarih aralığı: 2023-01-01 → 2025-12-31
- Üretici: `python/generate_demo_data.py`
