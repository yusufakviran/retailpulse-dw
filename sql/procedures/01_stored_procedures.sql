-- ============================================================
-- RetailPulse Data Warehouse
-- STORED PROCEDURES
-- 1. sp_load_dim_date          → Full load (one-time)
-- 2. sp_scd2_store             → SCD Type 2 merge for dim.store
-- 3. sp_scd2_product           → SCD Type 2 merge for dim.product
-- 4. sp_incremental_sales      → Incremental load for fact.sales_transaction
-- 5. sp_truncate_daily_kpi     → Truncate-Insert for fact.daily_store_kpi
-- 6. sp_truncate_inventory     → Truncate-Insert for fact.inventory_snapshot
-- 7. sp_run_etl_pipeline       → Master orchestration procedure
-- ============================================================

-- ─────────────────────────────────────────
-- 1. Load dim.date (Full - run once, covers 2020-2030)
-- ─────────────────────────────────────────
CREATE OR REPLACE PROCEDURE staging.sp_load_dim_date(
    p_start_date DATE DEFAULT '2020-01-01',
    p_end_date   DATE DEFAULT '2030-12-31'
)
LANGUAGE plpgsql AS $$
DECLARE
    v_date          DATE;
    v_fiscal_year   SMALLINT;
    v_fiscal_month  SMALLINT;
    v_batch_id      BIGINT;
    v_rows          BIGINT := 0;
BEGIN
    -- Log batch start
    INSERT INTO staging.etl_batch_log (batch_name, target_table, load_strategy, source_system, watermark_from, watermark_to)
    VALUES ('load_dim_date', 'dim.date', 'FULL_LOAD', 'SYSTEM', p_start_date::TIMESTAMP, p_end_date::TIMESTAMP)
    RETURNING batch_id INTO v_batch_id;

    v_date := p_start_date;

    WHILE v_date <= p_end_date LOOP
        -- Fiscal year: Starts February 1 (common in Turkish retail)
        IF EXTRACT(MONTH FROM v_date) >= 2 THEN
            v_fiscal_year  := EXTRACT(YEAR FROM v_date);
            v_fiscal_month := EXTRACT(MONTH FROM v_date) - 1;
        ELSE
            v_fiscal_year  := EXTRACT(YEAR FROM v_date) - 1;
            v_fiscal_month := EXTRACT(MONTH FROM v_date) + 11;
        END IF;

        INSERT INTO dim.date (
            date_id, full_date, day_of_week, day_name, day_name_tr,
            day_of_month, day_of_year, week_of_year, week_start_date,
            month_number, month_name, month_name_tr, month_short,
            quarter_number, quarter_name, year_number,
            fiscal_week, fiscal_month, fiscal_quarter, fiscal_year,
            is_weekend, season, day_type_id
        )
        VALUES (
            TO_CHAR(v_date,'YYYYMMDD')::INT,
            v_date,
            EXTRACT(ISODOW FROM v_date)::SMALLINT,
            TO_CHAR(v_date,'Day'),
            CASE EXTRACT(ISODOW FROM v_date)
                WHEN 1 THEN 'Pazartesi' WHEN 2 THEN 'Salı'   WHEN 3 THEN 'Çarşamba'
                WHEN 4 THEN 'Perşembe'  WHEN 5 THEN 'Cuma'   WHEN 6 THEN 'Cumartesi'
                WHEN 7 THEN 'Pazar' END,
            EXTRACT(DAY FROM v_date)::SMALLINT,
            EXTRACT(DOY FROM v_date)::SMALLINT,
            EXTRACT(WEEK FROM v_date)::SMALLINT,
            date_trunc('week', v_date)::DATE,
            EXTRACT(MONTH FROM v_date)::SMALLINT,
            TO_CHAR(v_date, 'Month'),
            CASE EXTRACT(MONTH FROM v_date)
                WHEN 1  THEN 'Ocak'      WHEN 2  THEN 'Şubat'    WHEN 3  THEN 'Mart'
                WHEN 4  THEN 'Nisan'     WHEN 5  THEN 'Mayıs'    WHEN 6  THEN 'Haziran'
                WHEN 7  THEN 'Temmuz'    WHEN 8  THEN 'Ağustos'  WHEN 9  THEN 'Eylül'
                WHEN 10 THEN 'Ekim'      WHEN 11 THEN 'Kasım'    WHEN 12 THEN 'Aralık' END,
            TO_CHAR(v_date, 'Mon'),
            EXTRACT(QUARTER FROM v_date)::SMALLINT,
            'Q' || EXTRACT(QUARTER FROM v_date)::TEXT,
            EXTRACT(YEAR FROM v_date)::SMALLINT,
            -- Fiscal week (approx)
            CEIL(v_fiscal_month * 4.33)::SMALLINT,
            v_fiscal_month,
            CEIL(v_fiscal_month / 3.0)::SMALLINT,
            v_fiscal_year,
            -- Weekend flag
            EXTRACT(ISODOW FROM v_date) IN (6,7),
            -- Season (Northern hemisphere)
            CASE
                WHEN EXTRACT(MONTH FROM v_date) IN (3,4,5)   THEN 'İlkbahar'
                WHEN EXTRACT(MONTH FROM v_date) IN (6,7,8)   THEN 'Yaz'
                WHEN EXTRACT(MONTH FROM v_date) IN (9,10,11) THEN 'Sonbahar'
                ELSE 'Kış' END,
            CASE EXTRACT(ISODOW FROM v_date) WHEN 7 THEN 3 WHEN 6 THEN 2 ELSE 1 END
        )
        ON CONFLICT (date_id) DO NOTHING;

        v_date := v_date + INTERVAL '1 day';
        v_rows := v_rows + 1;
    END LOOP;

    -- Update batch log
    UPDATE staging.etl_batch_log
    SET status = 'SUCCESS', end_time = NOW(), rows_inserted = v_rows
    WHERE batch_id = v_batch_id;

    RAISE NOTICE 'dim.date loaded: % rows', v_rows;
EXCEPTION WHEN OTHERS THEN
    UPDATE staging.etl_batch_log
    SET status = 'FAILED', end_time = NOW(), error_message = SQLERRM
    WHERE batch_id = v_batch_id;
    RAISE;
END;
$$;

-- ─────────────────────────────────────────
-- 2. SCD Type 2 — dim.store
-- Compares hash of tracked attributes; creates new version if changed
-- ─────────────────────────────────────────
CREATE OR REPLACE PROCEDURE staging.sp_scd2_store(
    p_effective_date DATE DEFAULT CURRENT_DATE
)
LANGUAGE plpgsql AS $$
DECLARE
    v_batch_id      BIGINT;
    v_inserted      BIGINT := 0;
    v_updated       BIGINT := 0;
    v_rec           RECORD;
    v_new_hash      VARCHAR(64);
BEGIN
    INSERT INTO staging.etl_batch_log (batch_name, target_table, load_strategy, source_system)
    VALUES ('scd2_store', 'dim.store', 'SCD2', 'ERP')
    RETURNING batch_id INTO v_batch_id;

    -- Iterate over staging records
    FOR v_rec IN
        SELECT * FROM staging.stg_store
    LOOP
        -- Compute hash of SCD2-tracked attributes
        v_new_hash := MD5(CONCAT_WS('|',
            v_rec.store_code,
            v_rec.store_name,
            v_rec.format_code,
            v_rec.store_sqm::TEXT,
            v_rec.selling_sqm::TEXT,
            v_rec.region_id::TEXT,
            v_rec.cluster,
            v_rec.headcount::TEXT,
            v_rec.is_franchise::TEXT
        ));

        -- Check if current record exists and hash differs
        IF EXISTS (
            SELECT 1 FROM dim.store
            WHERE store_id = v_rec.store_id
              AND is_current = TRUE
              AND scd_hash <> v_new_hash
        ) THEN
            -- Expire the old record
            UPDATE dim.store
            SET scd_end_date = p_effective_date - INTERVAL '1 day',
                is_current   = FALSE,
                updated_at   = NOW()
            WHERE store_id   = v_rec.store_id
              AND is_current = TRUE;

            v_updated := v_updated + 1;

            -- Insert new version
            INSERT INTO dim.store (
                store_id, store_code, store_name, store_short_name,
                format_code, store_sqm, selling_sqm, checkout_count,
                region_sk, address, mall_name, is_mall_store,
                opening_date, renovation_date, closing_date,
                is_franchise, cluster, headcount,
                scd_start_date, scd_end_date, is_current, scd_version, scd_hash
            )
            SELECT
                v_rec.store_id, v_rec.store_code, v_rec.store_name, v_rec.store_short_name,
                v_rec.format_code, v_rec.store_sqm, v_rec.selling_sqm, v_rec.checkout_count,
                r.region_sk, v_rec.address, v_rec.mall_name, v_rec.is_mall_store,
                v_rec.opening_date, v_rec.renovation_date, v_rec.closing_date,
                v_rec.is_franchise, v_rec.cluster, v_rec.headcount,
                p_effective_date, NULL, TRUE,
                COALESCE((SELECT MAX(scd_version) FROM dim.store WHERE store_id = v_rec.store_id), 0) + 1,
                v_new_hash
            FROM dim.region r
            WHERE r.region_id = v_rec.region_id;

            v_inserted := v_inserted + 1;

        ELSIF NOT EXISTS (
            SELECT 1 FROM dim.store WHERE store_id = v_rec.store_id
        ) THEN
            -- New store - first insert
            INSERT INTO dim.store (
                store_id, store_code, store_name, store_short_name,
                format_code, store_sqm, selling_sqm, checkout_count,
                region_sk, address, mall_name, is_mall_store,
                opening_date, renovation_date, closing_date,
                is_franchise, cluster, headcount,
                scd_start_date, scd_end_date, is_current, scd_version, scd_hash
            )
            SELECT
                v_rec.store_id, v_rec.store_code, v_rec.store_name, v_rec.store_short_name,
                v_rec.format_code, v_rec.store_sqm, v_rec.selling_sqm, v_rec.checkout_count,
                r.region_sk, v_rec.address, v_rec.mall_name, v_rec.is_mall_store,
                v_rec.opening_date, v_rec.renovation_date, v_rec.closing_date,
                v_rec.is_franchise, v_rec.cluster, v_rec.headcount,
                p_effective_date, NULL, TRUE, 1, v_new_hash
            FROM dim.region r
            WHERE r.region_id = v_rec.region_id;

            v_inserted := v_inserted + 1;
        END IF;
        -- If hash matches → no change, skip
    END LOOP;

    UPDATE staging.etl_batch_log
    SET status = 'SUCCESS', end_time = NOW(),
        rows_inserted = v_inserted, rows_updated = v_updated
    WHERE batch_id = v_batch_id;

    RAISE NOTICE 'SCD2 store complete: % inserted, % expired', v_inserted, v_updated;
EXCEPTION WHEN OTHERS THEN
    UPDATE staging.etl_batch_log
    SET status = 'FAILED', end_time = NOW(), error_message = SQLERRM
    WHERE batch_id = v_batch_id;
    RAISE;
END;
$$;

-- ─────────────────────────────────────────
-- 3. SCD Type 2 — dim.product  (same pattern)
-- ─────────────────────────────────────────
CREATE OR REPLACE PROCEDURE staging.sp_scd2_product(
    p_effective_date DATE DEFAULT CURRENT_DATE
)
LANGUAGE plpgsql AS $$
DECLARE
    v_batch_id  BIGINT;
    v_inserted  BIGINT := 0;
    v_updated   BIGINT := 0;
    v_rec       RECORD;
    v_new_hash  VARCHAR(64);
BEGIN
    INSERT INTO staging.etl_batch_log (batch_name, target_table, load_strategy, source_system)
    VALUES ('scd2_product', 'dim.product', 'SCD2', 'ERP')
    RETURNING batch_id INTO v_batch_id;

    FOR v_rec IN SELECT * FROM staging.stg_product LOOP

        v_new_hash := MD5(CONCAT_WS('|',
            v_rec.product_name, v_rec.brand, v_rec.supplier_code,
            v_rec.list_price::TEXT, v_rec.cost_price::TEXT,
            v_rec.status_code, v_rec.category_id::TEXT
        ));

        IF EXISTS (
            SELECT 1 FROM dim.product
            WHERE product_id = v_rec.product_id AND is_current = TRUE AND scd_hash <> v_new_hash
        ) THEN
            UPDATE dim.product
            SET scd_end_date = p_effective_date - INTERVAL '1 day',
                is_current   = FALSE, updated_at = NOW()
            WHERE product_id = v_rec.product_id AND is_current = TRUE;

            v_updated := v_updated + 1;

            INSERT INTO dim.product (
                product_id, sku_code, product_name, product_short_name,
                brand, supplier_name, supplier_code, category_sk,
                unit_of_measure, net_weight_gr, package_size,
                list_price, cost_price, status_code,
                is_private_label, is_perishable,
                scd_start_date, scd_end_date, is_current, scd_version, scd_hash
            )
            SELECT
                v_rec.product_id, v_rec.sku_code, v_rec.product_name, v_rec.product_short_name,
                v_rec.brand, v_rec.supplier_name, v_rec.supplier_code,
                pc.category_sk,
                v_rec.unit_of_measure, v_rec.net_weight_gr, v_rec.package_size,
                v_rec.list_price, v_rec.cost_price, v_rec.status_code,
                v_rec.is_private_label, v_rec.is_perishable,
                p_effective_date, NULL, TRUE,
                COALESCE((SELECT MAX(scd_version) FROM dim.product WHERE product_id = v_rec.product_id), 0) + 1,
                v_new_hash
            FROM dim.product_category pc
            WHERE pc.category_id = v_rec.category_id;

            v_inserted := v_inserted + 1;

        ELSIF NOT EXISTS (SELECT 1 FROM dim.product WHERE product_id = v_rec.product_id) THEN

            INSERT INTO dim.product (
                product_id, sku_code, product_name, product_short_name,
                brand, supplier_name, supplier_code, category_sk,
                unit_of_measure, net_weight_gr, package_size,
                list_price, cost_price, status_code,
                is_private_label, is_perishable,
                scd_start_date, scd_end_date, is_current, scd_version, scd_hash
            )
            SELECT
                v_rec.product_id, v_rec.sku_code, v_rec.product_name, v_rec.product_short_name,
                v_rec.brand, v_rec.supplier_name, v_rec.supplier_code,
                pc.category_sk,
                v_rec.unit_of_measure, v_rec.net_weight_gr, v_rec.package_size,
                v_rec.list_price, v_rec.cost_price, v_rec.status_code,
                v_rec.is_private_label, v_rec.is_perishable,
                p_effective_date, NULL, TRUE, 1, v_new_hash
            FROM dim.product_category pc
            WHERE pc.category_id = v_rec.category_id;

            v_inserted := v_inserted + 1;
        END IF;
    END LOOP;

    UPDATE staging.etl_batch_log
    SET status = 'SUCCESS', end_time = NOW(),
        rows_inserted = v_inserted, rows_updated = v_updated
    WHERE batch_id = v_batch_id;

EXCEPTION WHEN OTHERS THEN
    UPDATE staging.etl_batch_log
    SET status = 'FAILED', end_time = NOW(), error_message = SQLERRM
    WHERE batch_id = v_batch_id;
    RAISE;
END;
$$;

-- ─────────────────────────────────────────
-- 4. Incremental load — fact.sales_transaction
-- Uses watermark (etl_batch_log) to detect new records
-- ─────────────────────────────────────────
CREATE OR REPLACE PROCEDURE staging.sp_incremental_sales(
    p_watermark_from    TIMESTAMP DEFAULT NULL,
    p_watermark_to      TIMESTAMP DEFAULT NOW()
)
LANGUAGE plpgsql AS $$
DECLARE
    v_batch_id          BIGINT;
    v_watermark_from    TIMESTAMP;
    v_rows_inserted     BIGINT;
    v_rows_rejected     BIGINT;
BEGIN
    -- Auto-detect watermark if not provided
    IF p_watermark_from IS NULL THEN
        SELECT COALESCE(MAX(watermark_to), '2020-01-01'::TIMESTAMP)
        INTO v_watermark_from
        FROM staging.etl_batch_log
        WHERE target_table = 'fact.sales_transaction'
          AND status = 'SUCCESS';
    ELSE
        v_watermark_from := p_watermark_from;
    END IF;

    INSERT INTO staging.etl_batch_log (
        batch_name, target_table, load_strategy, source_system, watermark_from, watermark_to
    )
    VALUES ('incremental_sales', 'fact.sales_transaction', 'INCREMENTAL', 'POS', v_watermark_from, p_watermark_to)
    RETURNING batch_id INTO v_batch_id;

    RAISE NOTICE 'Loading sales: % → %', v_watermark_from, p_watermark_to;

    -- Insert new transactions, joining to current dimension records
    INSERT INTO fact.sales_transaction (
        transaction_id, line_number,
        date_id, store_sk, product_sk, customer_sk,
        employee_sk, campaign_sk, payment_method_id, transaction_type_id,
        quantity, unit_list_price, unit_selling_price, unit_cost,
        discount_amount, vat_amount,
        transaction_datetime, etl_batch_id, source_system
    )
    SELECT
        s.transaction_id,
        s.line_number,
        TO_CHAR(s.transaction_datetime, 'YYYYMMDD')::INT,
        st.store_sk,
        p.product_sk,
        COALESCE(c.customer_sk, -1),
        e.employee_sk,
        COALESCE(camp.campaign_sk, -1),
        s.payment_method_id,
        s.transaction_type_id,
        s.quantity,
        s.unit_list_price,
        s.unit_selling_price,
        s.unit_cost,
        s.discount_amount,
        s.vat_amount,
        s.transaction_datetime,
        v_batch_id,
        'POS'
    FROM staging.stg_sales_transaction s
    -- Dimension lookups — point-in-time correct (SCD2)
    JOIN dim.store st
        ON st.store_id = s.store_id
        AND s.transaction_datetime::DATE BETWEEN st.scd_start_date AND COALESCE(st.scd_end_date, '9999-12-31')
    JOIN dim.product p
        ON p.product_id = s.product_id
        AND s.transaction_datetime::DATE BETWEEN p.scd_start_date AND COALESCE(p.scd_end_date, '9999-12-31')
    LEFT JOIN dim.customer c
        ON c.customer_id = s.customer_id AND c.is_current = TRUE
    LEFT JOIN dim.employee e
        ON e.employee_id = s.employee_id AND e.is_current = TRUE
    LEFT JOIN dim.campaign camp
        ON camp.campaign_id = s.campaign_id
    WHERE s.transaction_datetime > v_watermark_from
      AND s.transaction_datetime <= p_watermark_to
    ON CONFLICT (transaction_id, line_number, source_system) DO NOTHING;

    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

    -- Count rejects (source rows without dimension match)
    SELECT COUNT(*) INTO v_rows_rejected
    FROM staging.stg_sales_transaction s
    LEFT JOIN dim.store st
        ON st.store_id = s.store_id
        AND s.transaction_datetime::DATE BETWEEN st.scd_start_date AND COALESCE(st.scd_end_date, '9999-12-31')
    LEFT JOIN dim.product p
        ON p.product_id = s.product_id
        AND s.transaction_datetime::DATE BETWEEN p.scd_start_date AND COALESCE(p.scd_end_date, '9999-12-31')
    WHERE s.transaction_datetime > v_watermark_from
      AND s.transaction_datetime <= p_watermark_to
      AND (st.store_sk IS NULL OR p.product_sk IS NULL);

    UPDATE staging.etl_batch_log
    SET status = 'SUCCESS', end_time = NOW(),
        rows_inserted = v_rows_inserted, rows_rejected = v_rows_rejected
    WHERE batch_id = v_batch_id;

    RAISE NOTICE 'Sales incremental: % inserted, % rejected', v_rows_inserted, v_rows_rejected;

EXCEPTION WHEN OTHERS THEN
    UPDATE staging.etl_batch_log
    SET status = 'FAILED', end_time = NOW(), error_message = SQLERRM
    WHERE batch_id = v_batch_id;
    RAISE;
END;
$$;

-- ─────────────────────────────────────────
-- 5. Truncate-Insert — fact.daily_store_kpi
-- Recalculates from fact.sales_transaction for given date range
-- ─────────────────────────────────────────
CREATE OR REPLACE PROCEDURE staging.sp_truncate_daily_kpi(
    p_date_from DATE DEFAULT CURRENT_DATE - 7,
    p_date_to   DATE DEFAULT CURRENT_DATE
)
LANGUAGE plpgsql AS $$
DECLARE
    v_batch_id  BIGINT;
    v_rows      BIGINT;
    v_from_id   INT := TO_CHAR(p_date_from,'YYYYMMDD')::INT;
    v_to_id     INT := TO_CHAR(p_date_to,'YYYYMMDD')::INT;
BEGIN
    INSERT INTO staging.etl_batch_log (
        batch_name, target_table, load_strategy, source_system,
        watermark_from, watermark_to
    )
    VALUES ('truncate_daily_kpi', 'fact.daily_store_kpi', 'TRUNCATE_INSERT', 'DW',
            p_date_from::TIMESTAMP, p_date_to::TIMESTAMP)
    RETURNING batch_id INTO v_batch_id;

    -- Delete the window being refreshed
    DELETE FROM fact.daily_store_kpi
    WHERE date_id BETWEEN v_from_id AND v_to_id;

    -- Re-aggregate from fact.sales_transaction
    INSERT INTO fact.daily_store_kpi (
        date_id, store_sk,
        gross_sales, discount_amount, net_sales, cogs, gross_margin,
        vat_collected, return_amount,
        units_sold, units_returned, transaction_count,
        return_transaction_count, unique_customers, loyalty_customers,
        avg_basket_size, avg_basket_items, gross_margin_pct,
        return_rate_pct, loyalty_rate_pct,
        etl_batch_id
    )
    SELECT
        s.date_id,
        s.store_sk,
        -- Sales aggregates
        SUM(CASE WHEN tt.direction = '-' THEN s.gross_sales_amount ELSE 0 END),
        SUM(CASE WHEN tt.direction = '-' THEN s.discount_amount    ELSE 0 END),
        SUM(CASE WHEN tt.direction = '-' THEN s.net_sales_amount   ELSE 0 END),
        SUM(CASE WHEN tt.direction = '-' THEN s.cogs_amount        ELSE 0 END),
        SUM(CASE WHEN tt.direction = '-' THEN s.gross_margin_amount ELSE 0 END),
        SUM(COALESCE(s.vat_amount, 0)),
        SUM(CASE WHEN s.transaction_type_id = 2 THEN ABS(s.net_sales_amount) ELSE 0 END),
        -- Volume
        SUM(CASE WHEN s.transaction_type_id = 1 THEN s.quantity ELSE 0 END)::INT,
        SUM(CASE WHEN s.transaction_type_id = 2 THEN s.quantity ELSE 0 END)::INT,
        COUNT(DISTINCT CASE WHEN s.transaction_type_id = 1 THEN s.transaction_id END),
        COUNT(DISTINCT CASE WHEN s.transaction_type_id = 2 THEN s.transaction_id END),
        COUNT(DISTINCT CASE WHEN s.customer_sk <> -1 THEN s.customer_sk END),
        COUNT(DISTINCT CASE WHEN c.loyalty_enrolled = TRUE THEN s.customer_sk END),
        -- Semi-additive (calculated)
        CASE WHEN COUNT(DISTINCT CASE WHEN s.transaction_type_id=1 THEN s.transaction_id END) > 0
             THEN SUM(CASE WHEN tt.direction='-' THEN s.net_sales_amount ELSE 0 END) /
                  COUNT(DISTINCT CASE WHEN s.transaction_type_id=1 THEN s.transaction_id END)
             ELSE 0 END,
        CASE WHEN COUNT(DISTINCT CASE WHEN s.transaction_type_id=1 THEN s.transaction_id END) > 0
             THEN SUM(CASE WHEN s.transaction_type_id=1 THEN s.quantity ELSE 0 END) /
                  COUNT(DISTINCT CASE WHEN s.transaction_type_id=1 THEN s.transaction_id END)
             ELSE 0 END,
        CASE WHEN SUM(CASE WHEN tt.direction='-' THEN s.net_sales_amount ELSE 0 END) > 0
             THEN SUM(CASE WHEN tt.direction='-' THEN s.gross_margin_amount ELSE 0 END) /
                  SUM(CASE WHEN tt.direction='-' THEN s.net_sales_amount ELSE 0 END) * 100
             ELSE 0 END,
        CASE WHEN SUM(CASE WHEN tt.direction='-' THEN s.net_sales_amount ELSE 0 END) > 0
             THEN SUM(CASE WHEN s.transaction_type_id=2 THEN ABS(s.net_sales_amount) ELSE 0 END) /
                  SUM(CASE WHEN tt.direction='-' THEN s.net_sales_amount ELSE 0 END) * 100
             ELSE 0 END,
        NULL,  -- loyalty_rate_pct (needs visitor data)
        v_batch_id
    FROM fact.sales_transaction s
    JOIN dict.d_transaction_type tt ON tt.transaction_type_id = s.transaction_type_id
    LEFT JOIN dim.customer c ON c.customer_sk = s.customer_sk
    WHERE s.date_id BETWEEN v_from_id AND v_to_id
    GROUP BY s.date_id, s.store_sk;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    UPDATE staging.etl_batch_log
    SET status = 'SUCCESS', end_time = NOW(), rows_inserted = v_rows
    WHERE batch_id = v_batch_id;

    RAISE NOTICE 'Daily KPI refresh: % rows', v_rows;
EXCEPTION WHEN OTHERS THEN
    UPDATE staging.etl_batch_log
    SET status = 'FAILED', end_time = NOW(), error_message = SQLERRM
    WHERE batch_id = v_batch_id;
    RAISE;
END;
$$;

-- ─────────────────────────────────────────
-- 6. Master pipeline orchestrator
-- ─────────────────────────────────────────
CREATE OR REPLACE PROCEDURE staging.sp_run_etl_pipeline(
    p_run_date DATE DEFAULT CURRENT_DATE
)
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE '=== RetailPulse ETL Pipeline START: % ===', p_run_date;

    -- Step 1: SCD2 dimensions
    CALL staging.sp_scd2_store(p_run_date);
    CALL staging.sp_scd2_product(p_run_date);

    -- Step 2: Incremental fact load (yesterday's transactions)
    CALL staging.sp_incremental_sales(
        p_watermark_from := (p_run_date - 1)::TIMESTAMP,
        p_watermark_to   := p_run_date::TIMESTAMP
    );

    -- Step 3: Refresh daily KPI aggregate (last 3 days for safety)
    CALL staging.sp_truncate_daily_kpi(
        p_date_from := p_run_date - 3,
        p_date_to   := p_run_date - 1
    );

    RAISE NOTICE '=== RetailPulse ETL Pipeline COMPLETE ===';
END;
$$;
