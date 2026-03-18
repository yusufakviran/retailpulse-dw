-- ============================================================
-- RetailPulse Data Warehouse
-- ANALYTICAL VIEWS (mart layer)
-- These views are what BI tools (Power BI, Looker) connect to
-- ============================================================

-- ─────────────────────────────────────────
-- v_sales_daily  — Main sales view with full context
-- ─────────────────────────────────────────
CREATE OR REPLACE VIEW mart.v_sales_daily AS
SELECT
    -- Date
    d.full_date,
    d.day_name_tr                   AS gun,
    d.week_of_year                  AS hafta,
    d.month_name_tr                 AS ay,
    d.month_number,
    d.quarter_name                  AS ceyrek,
    d.year_number                   AS yil,
    d.fiscal_year                   AS mali_yil,
    d.fiscal_month                  AS mali_ay,
    d.is_weekend                    AS hafta_sonu_mu,
    d.is_public_holiday             AS tatil_mi,
    d.season                        AS mevsim,
    -- Store
    st.store_id,
    st.store_code,
    st.store_name                   AS magaza_adi,
    sf.format_name                  AS magaza_formati,
    st.cluster                      AS magaza_segmenti,
    st.store_sqm                    AS magaza_m2,
    st.is_mall_store                AS avm_magaza_mi,
    -- Region
    r.city                          AS sehir,
    r.district                      AS ilce,
    r.nuts2_name                    AS bolge,
    r.population_segment            AS sehir_turu,
    -- Product
    p.sku_code,
    p.product_name                  AS urun_adi,
    p.brand                         AS marka,
    pc.department_name              AS departman,
    pc.category_name                AS kategori,
    pc.subcategory_name             AS alt_kategori,
    p.is_private_label              AS ozel_marka_mi,
    p.is_perishable                 AS bozulabilir_mi,
    -- Customer
    c.customer_segment              AS musteri_segmenti,
    c.loyalty_tier                  AS sadakat_turu,
    c.age_band                      AS yas_grubu,
    c.gender                        AS cinsiyet,
    -- Campaign
    camp.campaign_name              AS kampanya_adi,
    camp.campaign_type              AS kampanya_turu,
    -- Transaction
    s.transaction_id,
    s.transaction_type_id,
    tt.type_name                    AS islem_turu,
    pm.method_name                  AS odeme_yontemi,
    -- Measures
    s.quantity                      AS adet,
    s.unit_list_price               AS liste_fiyati,
    s.unit_selling_price            AS satis_fiyati,
    s.unit_cost                     AS maliyet,
    s.gross_sales_amount            AS brut_satis,
    s.discount_amount               AS indirim_tutari,
    s.net_sales_amount              AS net_satis,
    s.cogs_amount                   AS satis_maliyeti,
    s.gross_margin_amount           AS brut_kar,
    CASE WHEN s.net_sales_amount > 0
         THEN s.gross_margin_amount / s.net_sales_amount * 100
         ELSE 0 END                 AS brut_kar_marji_pct,
    -- M² verimlilik
    CASE WHEN st.store_sqm > 0
         THEN s.net_sales_amount / st.store_sqm
         ELSE NULL END              AS m2_basi_satis
FROM fact.sales_transaction s
JOIN dim.date                   d    ON d.date_id    = s.date_id
JOIN dim.store                  st   ON st.store_sk  = s.store_sk
JOIN dim.product                p    ON p.product_sk = s.product_sk
JOIN dim.product_category       pc   ON pc.category_sk = p.category_sk
JOIN dim.customer               c    ON c.customer_sk  = s.customer_sk
JOIN dim.campaign               camp ON camp.campaign_sk = s.campaign_sk
JOIN dict.d_transaction_type    tt   ON tt.transaction_type_id = s.transaction_type_id
JOIN dict.d_store_format        sf   ON sf.format_code = st.format_code
LEFT JOIN dict.d_payment_method pm   ON pm.payment_method_id = s.payment_method_id
LEFT JOIN dim.region            r    ON r.region_sk = st.region_sk
WHERE st.is_current = TRUE
  AND p.is_current  = TRUE;

-- ─────────────────────────────────────────
-- v_store_kpi_monthly  — Monthly KPI rollup
-- ─────────────────────────────────────────
CREATE OR REPLACE VIEW mart.v_store_kpi_monthly AS
SELECT
    d.year_number                           AS yil,
    d.fiscal_year                           AS mali_yil,
    d.month_number,
    d.month_name_tr                         AS ay,
    d.fiscal_month                          AS mali_ay,
    st.store_id,
    st.store_code,
    st.store_name,
    sf.format_name                          AS format,
    r.city                                  AS sehir,
    r.nuts2_name                            AS bolge,
    st.cluster,
    -- Sales
    SUM(k.gross_sales)                      AS brut_satis,
    SUM(k.discount_amount)                  AS toplam_indirim,
    SUM(k.net_sales)                        AS net_satis,
    SUM(k.cogs)                             AS satis_maliyeti,
    SUM(k.gross_margin)                     AS brut_kar,
    CASE WHEN SUM(k.net_sales) > 0
         THEN SUM(k.gross_margin) / SUM(k.net_sales) * 100
         ELSE 0 END                         AS brut_kar_marji_pct,
    SUM(k.return_amount)                    AS iade_tutari,
    -- Volume
    SUM(k.units_sold)                       AS satilan_adet,
    SUM(k.transaction_count)               AS islem_sayisi,
    SUM(k.unique_customers)                 AS tekil_musteri,
    -- Averages (semi-additive — AVG of daily values)
    AVG(k.avg_basket_size)                  AS ort_sepet_buyuklugu,
    AVG(k.avg_basket_items)                 AS ort_sepet_urun,
    AVG(k.gross_margin_pct)                 AS ort_kar_marji,
    AVG(k.return_rate_pct)                  AS ort_iade_orani,
    -- Targets
    SUM(k.sales_target)                     AS satis_hedefi,
    CASE WHEN SUM(k.sales_target) > 0
         THEN SUM(k.net_sales) / SUM(k.sales_target) * 100
         ELSE NULL END                      AS hedefe_ulasma_pct,
    -- M² verimlilik
    CASE WHEN MAX(st.store_sqm) > 0
         THEN SUM(k.net_sales) / MAX(st.store_sqm)
         ELSE NULL END                      AS m2_basi_satis,
    COUNT(DISTINCT k.date_id)               AS calisilan_gun_sayisi
FROM fact.daily_store_kpi k
JOIN dim.date           d    ON d.date_id   = k.date_id
JOIN dim.store          st   ON st.store_sk = k.store_sk
JOIN dict.d_store_format sf  ON sf.format_code = st.format_code
LEFT JOIN dim.region    r    ON r.region_sk = st.region_sk
WHERE st.is_current = TRUE
GROUP BY
    d.year_number, d.fiscal_year, d.month_number, d.month_name_tr, d.fiscal_month,
    st.store_id, st.store_code, st.store_name, sf.format_name,
    r.city, r.nuts2_name, st.cluster;

-- ─────────────────────────────────────────
-- v_inventory_alert  — Real-time stockout / overstock alerts
-- ─────────────────────────────────────────
CREATE OR REPLACE VIEW mart.v_inventory_alert AS
SELECT
    d.full_date,
    st.store_id,
    st.store_code,
    st.store_name,
    r.city,
    p.sku_code,
    p.product_name,
    p.brand,
    pc.department_name,
    pc.category_name,
    i.stock_qty                             AS mevcut_stok,
    i.min_stock_level                       AS min_stok,
    i.max_stock_level                       AS max_stok,
    i.days_of_stock                         AS stok_gunu,
    i.stock_value_cost                      AS stok_degeri_maliyet,
    i.stock_value_retail                    AS stok_degeri_satis,
    i.is_stockout                           AS stok_bitti_mi,
    i.is_overstock                          AS fazla_stok_mu,
    i.is_slow_mover                         AS yavash_satici_mi,
    CASE
        WHEN i.is_stockout  THEN '🔴 Stok Tükendi'
        WHEN i.days_of_stock < 3  THEN '🟠 Kritik Stok'
        WHEN i.days_of_stock < 7  THEN '🟡 Düşük Stok'
        WHEN i.is_overstock THEN '🔵 Fazla Stok'
        WHEN i.is_slow_mover THEN '⚫ Yavaş Hareket'
        ELSE '🟢 Normal'
    END                                     AS stok_durumu
FROM fact.inventory_snapshot i
JOIN dim.date           d   ON d.date_id   = i.date_id
JOIN dim.store          st  ON st.store_sk = i.store_sk
JOIN dim.product        p   ON p.product_sk = i.product_sk
JOIN dim.product_category pc ON pc.category_sk = p.category_sk
LEFT JOIN dim.region    r   ON r.region_sk = st.region_sk
WHERE st.is_current = TRUE
  AND p.is_current  = TRUE
  AND (i.is_stockout = TRUE OR i.is_overstock = TRUE OR i.is_slow_mover = TRUE
       OR i.days_of_stock < 7);

-- ─────────────────────────────────────────
-- v_category_performance  — Category P&L view
-- ─────────────────────────────────────────
CREATE OR REPLACE VIEW mart.v_category_performance AS
SELECT
    d.year_number,
    d.fiscal_year,
    d.month_number,
    d.month_name_tr                         AS ay,
    d.quarter_name                          AS ceyrek,
    pc.department_code,
    pc.department_name                      AS departman,
    pc.category_code,
    pc.category_name                        AS kategori,
    pc.subcategory_name                     AS alt_kategori,
    pc.is_food,
    pc.is_private_label,
    sf.format_name                          AS magaza_formati,
    r.nuts2_name                            AS bolge,
    COUNT(DISTINCT s.transaction_id)        AS islem_sayisi,
    COUNT(DISTINCT s.product_sk)            AS urun_cesidi,
    SUM(s.quantity)                         AS satilan_adet,
    SUM(s.net_sales_amount)                 AS net_satis,
    SUM(s.cogs_amount)                      AS maliyet,
    SUM(s.gross_margin_amount)              AS brut_kar,
    SUM(s.discount_amount)                  AS indirim,
    CASE WHEN SUM(s.net_sales_amount) > 0
         THEN SUM(s.gross_margin_amount) / SUM(s.net_sales_amount) * 100
         ELSE 0 END                         AS kar_marji_pct,
    CASE WHEN SUM(s.net_sales_amount) > 0
         THEN SUM(s.discount_amount) / SUM(s.net_sales_amount) * 100
         ELSE 0 END                         AS indirim_orani_pct
FROM fact.sales_transaction s
JOIN dim.date           d   ON d.date_id     = s.date_id
JOIN dim.product        p   ON p.product_sk  = s.product_sk
JOIN dim.product_category pc ON pc.category_sk = p.category_sk
JOIN dim.store          st  ON st.store_sk   = s.store_sk
JOIN dict.d_store_format sf ON sf.format_code = st.format_code
JOIN dict.d_transaction_type tt ON tt.transaction_type_id = s.transaction_type_id
LEFT JOIN dim.region    r   ON r.region_sk = st.region_sk
WHERE tt.affects_revenue = TRUE
  AND st.is_current = TRUE
  AND p.is_current  = TRUE
GROUP BY
    d.year_number, d.fiscal_year, d.month_number, d.month_name_tr, d.quarter_name,
    pc.department_code, pc.department_name, pc.category_code,
    pc.category_name, pc.subcategory_name, pc.is_food, pc.is_private_label,
    sf.format_name, r.nuts2_name;

-- ─────────────────────────────────────────
-- v_customer_rfm  — RFM Segmentation view
-- (Recency / Frequency / Monetary)
-- ─────────────────────────────────────────
CREATE OR REPLACE VIEW mart.v_customer_rfm AS
WITH rfm_base AS (
    SELECT
        s.customer_sk,
        MAX(d.full_date)                            AS son_alisveris_tarihi,
        COUNT(DISTINCT s.transaction_id)            AS alisveris_sayisi,
        SUM(s.net_sales_amount)                     AS toplam_harcama,
        CURRENT_DATE - MAX(d.full_date)             AS recency_gun,
        c.customer_segment,
        c.loyalty_tier,
        c.age_band,
        c.gender,
        c.city
    FROM fact.sales_transaction s
    JOIN dim.date d     ON d.date_id    = s.date_id
    JOIN dim.customer c ON c.customer_sk = s.customer_sk
    WHERE s.customer_sk <> -1
      AND s.transaction_type_id = 1
    GROUP BY s.customer_sk, c.customer_segment, c.loyalty_tier, c.age_band, c.gender, c.city
),
rfm_scored AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency_gun DESC)       AS r_score,
        NTILE(5) OVER (ORDER BY alisveris_sayisi ASC)   AS f_score,
        NTILE(5) OVER (ORDER BY toplam_harcama ASC)     AS m_score
    FROM rfm_base
    WHERE customer_sk <> -1
)
SELECT
    customer_sk,
    son_alisveris_tarihi,
    alisveris_sayisi,
    ROUND(toplam_harcama, 2)            AS toplam_harcama,
    recency_gun,
    r_score, f_score, m_score,
    (r_score + f_score + m_score)       AS rfm_toplam,
    CASE
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Şampiyon'
        WHEN r_score >= 3 AND f_score >= 3                   THEN 'Sadık Müşteri'
        WHEN r_score >= 4 AND f_score <= 2                   THEN 'Yeni Müşteri'
        WHEN r_score >= 3 AND f_score <= 2 AND m_score >= 3  THEN 'Potansiyel Sadık'
        WHEN r_score <= 2 AND f_score >= 3                   THEN 'Risk Altında'
        WHEN r_score <= 2 AND f_score >= 4                   THEN 'Kaybedilecek'
        WHEN r_score = 1 AND f_score = 1                     THEN 'Kayıp Müşteri'
        ELSE 'İzlemede'
    END                                 AS rfm_segmenti,
    customer_segment,
    loyalty_tier,
    age_band,
    gender,
    city
FROM rfm_scored;

-- ─────────────────────────────────────────
-- v_etl_monitor  — ETL health dashboard
-- ─────────────────────────────────────────
CREATE OR REPLACE VIEW mart.v_etl_monitor AS
SELECT
    batch_id,
    batch_name,
    target_table,
    load_strategy,
    source_system,
    status,
    start_time,
    end_time,
    EXTRACT(EPOCH FROM (COALESCE(end_time, NOW()) - start_time))::INT AS sure_saniye,
    rows_extracted,
    rows_inserted,
    rows_updated,
    rows_rejected,
    watermark_from,
    watermark_to,
    error_message,
    CASE status
        WHEN 'SUCCESS' THEN '✅'
        WHEN 'RUNNING' THEN '⏳'
        WHEN 'FAILED'  THEN '❌'
        ELSE '⚠️'
    END AS durum_icon
FROM staging.etl_batch_log
ORDER BY batch_id DESC;
