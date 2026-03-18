# 📋 RetailPulse — Değişiklik Geçmişi

Tüm önemli değişiklikler bu dosyada belgelenir.
Format: [Semantic Versioning](https://semver.org/lang/tr/)

---

## [v1.0.0] - 2025-03-18

### 🎉 İlk Sürüm

#### Eklendi
- `dict` şeması: 7 referans tablo
  - d_currency, d_day_type, d_store_format
  - d_product_status, d_transaction_type
  - d_payment_method, d_kpi_definition
- `dim` şeması: 8 dimension tablosu
  - dim.date (statik, 2020–2030)
  - dim.region (SCD1)
  - dim.store (SCD2)
  - dim.product (SCD2)
  - dim.product_category (SCD1)
  - dim.customer (SCD2)
  - dim.employee (SCD2)
  - dim.campaign (SCD1)
- `fact` şeması: 4 fact tablosu
  - fact.sales_transaction (aylık partition, incremental)
  - fact.daily_store_kpi (truncate-insert)
  - fact.inventory_snapshot (truncate-insert)
  - fact.campaign_performance (accumulating snapshot)
- `staging` şeması: ETL kontrol tablosu
  - staging.etl_batch_log (watermark & audit)
- ETL Stored Procedure'ları (7 adet)
  - sp_load_dim_date
  - sp_scd2_store
  - sp_scd2_product
  - sp_incremental_sales
  - sp_truncate_daily_kpi
  - sp_run_etl_pipeline
- `mart` şeması: 6 analitik view
  - v_sales_daily
  - v_store_kpi_monthly
  - v_inventory_alert
  - v_category_performance
  - v_customer_rfm
  - v_etl_monitor
- Demo veri üretici (Python)
  - 150 mağaza · 3.000 SKU · 80.000 müşteri · ~30M işlem
  - Türkiye mevsimselliği dahil

---

*Sonraki sürüm: v1.1.0 — PostgreSQL kurulumu & demo veri yüklemesi*
