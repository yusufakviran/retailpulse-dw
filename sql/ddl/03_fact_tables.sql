-- ============================================================
-- RetailPulse Data Warehouse
-- Layer 2: FACT TABLES (Kimball - Additive & Semi-Additive)
-- ============================================================

-- ─────────────────────────────────────────
-- fact.sales_transaction  (Transactional Grain - Line Item)
-- Grain: One row per product per transaction
-- Load strategy: INCREMENTAL (append by date partition)
-- ─────────────────────────────────────────
CREATE TABLE fact.sales_transaction (
    -- Surrogate PK
    sales_tx_sk             BIGSERIAL       NOT NULL,
    -- Degenerate Dimensions
    transaction_id          BIGINT          NOT NULL,   -- POS receipt number
    line_number             SMALLINT        NOT NULL,
    -- Foreign Keys (Dimension)
    date_id                 INT             NOT NULL    REFERENCES dim.date(date_id),
    store_sk                INT             NOT NULL    REFERENCES dim.store(store_sk),
    product_sk              INT             NOT NULL    REFERENCES dim.product(product_sk),
    customer_sk             INT             NOT NULL    DEFAULT -1 REFERENCES dim.customer(customer_sk),
    employee_sk             INT             REFERENCES dim.employee(employee_sk),
    campaign_sk             INT             NOT NULL    DEFAULT -1 REFERENCES dim.campaign(campaign_sk),
    payment_method_id       SMALLINT        REFERENCES dict.d_payment_method(payment_method_id),
    transaction_type_id     SMALLINT        NOT NULL    REFERENCES dict.d_transaction_type(transaction_type_id),
    -- Additive Measures
    quantity                DECIMAL(10,3)   NOT NULL,
    unit_list_price         DECIMAL(12,2)   NOT NULL,
    unit_selling_price      DECIMAL(12,2)   NOT NULL,
    unit_cost               DECIMAL(12,2)   NOT NULL,
    gross_sales_amount      DECIMAL(14,2)   NOT NULL    GENERATED ALWAYS AS (quantity * unit_list_price) STORED,
    discount_amount         DECIMAL(14,2)   NOT NULL    DEFAULT 0.00,
    net_sales_amount        DECIMAL(14,2)   NOT NULL    GENERATED ALWAYS AS (quantity * unit_selling_price - discount_amount) STORED,
    cogs_amount             DECIMAL(14,2)   NOT NULL    GENERATED ALWAYS AS (quantity * unit_cost) STORED,
    gross_margin_amount     DECIMAL(14,2)   GENERATED ALWAYS AS ((quantity * unit_selling_price) - (quantity * unit_cost)) STORED,
    vat_amount              DECIMAL(14,2),
    -- Non-Additive / Semi-Additive
    gross_margin_pct        DECIMAL(7,4),               -- Recalculate in query; stored for convenience
    -- Timestamps
    transaction_datetime    TIMESTAMP       NOT NULL,
    -- ETL Audit
    etl_batch_id            BIGINT,
    etl_loaded_at           TIMESTAMP       NOT NULL DEFAULT NOW(),
    source_system           VARCHAR(30)     NOT NULL DEFAULT 'POS',
    CONSTRAINT pk_fact_sales_tx PRIMARY KEY (sales_tx_sk),
    CONSTRAINT uq_fact_sales_tx UNIQUE (transaction_id, line_number, source_system)
) PARTITION BY RANGE (date_id);

-- Monthly partitions (2023-01 to 2025-12)
CREATE TABLE fact.sales_transaction_202301 PARTITION OF fact.sales_transaction FOR VALUES FROM (20230101) TO (20230201);
CREATE TABLE fact.sales_transaction_202302 PARTITION OF fact.sales_transaction FOR VALUES FROM (20230201) TO (20230301);
CREATE TABLE fact.sales_transaction_202303 PARTITION OF fact.sales_transaction FOR VALUES FROM (20230301) TO (20230401);
CREATE TABLE fact.sales_transaction_202304 PARTITION OF fact.sales_transaction FOR VALUES FROM (20230401) TO (20230501);
CREATE TABLE fact.sales_transaction_202305 PARTITION OF fact.sales_transaction FOR VALUES FROM (20230501) TO (20230601);
CREATE TABLE fact.sales_transaction_202306 PARTITION OF fact.sales_transaction FOR VALUES FROM (20230601) TO (20230701);
CREATE TABLE fact.sales_transaction_202307 PARTITION OF fact.sales_transaction FOR VALUES FROM (20230701) TO (20230801);
CREATE TABLE fact.sales_transaction_202308 PARTITION OF fact.sales_transaction FOR VALUES FROM (20230801) TO (20230901);
CREATE TABLE fact.sales_transaction_202309 PARTITION OF fact.sales_transaction FOR VALUES FROM (20230901) TO (20231001);
CREATE TABLE fact.sales_transaction_202310 PARTITION OF fact.sales_transaction FOR VALUES FROM (20231001) TO (20231101);
CREATE TABLE fact.sales_transaction_202311 PARTITION OF fact.sales_transaction FOR VALUES FROM (20231101) TO (20231201);
CREATE TABLE fact.sales_transaction_202312 PARTITION OF fact.sales_transaction FOR VALUES FROM (20231201) TO (20240101);
CREATE TABLE fact.sales_transaction_202401 PARTITION OF fact.sales_transaction FOR VALUES FROM (20240101) TO (20240201);
CREATE TABLE fact.sales_transaction_202402 PARTITION OF fact.sales_transaction FOR VALUES FROM (20240201) TO (20240301);
CREATE TABLE fact.sales_transaction_202403 PARTITION OF fact.sales_transaction FOR VALUES FROM (20240301) TO (20240401);
CREATE TABLE fact.sales_transaction_202404 PARTITION OF fact.sales_transaction FOR VALUES FROM (20240401) TO (20240501);
CREATE TABLE fact.sales_transaction_202405 PARTITION OF fact.sales_transaction FOR VALUES FROM (20240501) TO (20240601);
CREATE TABLE fact.sales_transaction_202406 PARTITION OF fact.sales_transaction FOR VALUES FROM (20240601) TO (20240701);
CREATE TABLE fact.sales_transaction_202407 PARTITION OF fact.sales_transaction FOR VALUES FROM (20240701) TO (20240801);
CREATE TABLE fact.sales_transaction_202408 PARTITION OF fact.sales_transaction FOR VALUES FROM (20240801) TO (20240901);
CREATE TABLE fact.sales_transaction_202409 PARTITION OF fact.sales_transaction FOR VALUES FROM (20240901) TO (20241001);
CREATE TABLE fact.sales_transaction_202410 PARTITION OF fact.sales_transaction FOR VALUES FROM (20241001) TO (20241101);
CREATE TABLE fact.sales_transaction_202411 PARTITION OF fact.sales_transaction FOR VALUES FROM (20241101) TO (20241201);
CREATE TABLE fact.sales_transaction_202412 PARTITION OF fact.sales_transaction FOR VALUES FROM (20241201) TO (20250101);
CREATE TABLE fact.sales_transaction_202501 PARTITION OF fact.sales_transaction FOR VALUES FROM (20250101) TO (20250201);
CREATE TABLE fact.sales_transaction_202502 PARTITION OF fact.sales_transaction FOR VALUES FROM (20250201) TO (20250301);
CREATE TABLE fact.sales_transaction_202503 PARTITION OF fact.sales_transaction FOR VALUES FROM (20250301) TO (20250401);
CREATE TABLE fact.sales_transaction_202504 PARTITION OF fact.sales_transaction FOR VALUES FROM (20250401) TO (20250501);
CREATE TABLE fact.sales_transaction_202505 PARTITION OF fact.sales_transaction FOR VALUES FROM (20250501) TO (20250601);
CREATE TABLE fact.sales_transaction_202506 PARTITION OF fact.sales_transaction FOR VALUES FROM (20250601) TO (20250701);
CREATE TABLE fact.sales_transaction_202507 PARTITION OF fact.sales_transaction FOR VALUES FROM (20250701) TO (20250801);
CREATE TABLE fact.sales_transaction_202508 PARTITION OF fact.sales_transaction FOR VALUES FROM (20250801) TO (20250901);
CREATE TABLE fact.sales_transaction_202509 PARTITION OF fact.sales_transaction FOR VALUES FROM (20250901) TO (20251001);
CREATE TABLE fact.sales_transaction_202510 PARTITION OF fact.sales_transaction FOR VALUES FROM (20251001) TO (20251101);
CREATE TABLE fact.sales_transaction_202511 PARTITION OF fact.sales_transaction FOR VALUES FROM (20251101) TO (20251201);
CREATE TABLE fact.sales_transaction_202512 PARTITION OF fact.sales_transaction FOR VALUES FROM (20251201) TO (20260101);

-- Indexes on partitioned fact table
CREATE INDEX idx_fact_sales_date        ON fact.sales_transaction(date_id);
CREATE INDEX idx_fact_sales_store       ON fact.sales_transaction(store_sk);
CREATE INDEX idx_fact_sales_product     ON fact.sales_transaction(product_sk);
CREATE INDEX idx_fact_sales_customer    ON fact.sales_transaction(customer_sk);
CREATE INDEX idx_fact_sales_campaign    ON fact.sales_transaction(campaign_sk);
CREATE INDEX idx_fact_sales_tx_id       ON fact.sales_transaction(transaction_id);
CREATE INDEX idx_fact_sales_loaded      ON fact.sales_transaction(etl_loaded_at);

-- ─────────────────────────────────────────
-- fact.daily_store_kpi  (Periodic Snapshot - TRUNCATE & INSERT daily)
-- Grain: One row per store per day
-- Load strategy: TRUNCATE current month + INSERT
-- ─────────────────────────────────────────
CREATE TABLE fact.daily_store_kpi (
    kpi_sk                  BIGSERIAL       NOT NULL,
    -- Keys
    date_id                 INT             NOT NULL    REFERENCES dim.date(date_id),
    store_sk                INT             NOT NULL    REFERENCES dim.store(store_sk),
    -- Sales KPIs (Additive)
    gross_sales             DECIMAL(16,2)   NOT NULL    DEFAULT 0,
    discount_amount         DECIMAL(16,2)   NOT NULL    DEFAULT 0,
    net_sales               DECIMAL(16,2)   NOT NULL    DEFAULT 0,
    cogs                    DECIMAL(16,2)   NOT NULL    DEFAULT 0,
    gross_margin            DECIMAL(16,2)   NOT NULL    DEFAULT 0,
    vat_collected           DECIMAL(16,2)   NOT NULL    DEFAULT 0,
    return_amount           DECIMAL(16,2)   NOT NULL    DEFAULT 0,
    -- Volume KPIs (Additive)
    units_sold              INT             NOT NULL    DEFAULT 0,
    units_returned          INT             NOT NULL    DEFAULT 0,
    transaction_count       INT             NOT NULL    DEFAULT 0,
    return_transaction_count INT            NOT NULL    DEFAULT 0,
    unique_customers        INT             NOT NULL    DEFAULT 0,
    loyalty_customers       INT             NOT NULL    DEFAULT 0,
    -- Semi-Additive (average / ratio — do NOT SUM across stores)
    avg_basket_size         DECIMAL(12,2),
    avg_basket_items        DECIMAL(8,2),
    gross_margin_pct        DECIMAL(7,4),
    return_rate_pct         DECIMAL(7,4),
    loyalty_rate_pct        DECIMAL(7,4),
    -- Inventory KPIs (Snapshot - Semi-Additive)
    sku_count_active        INT,
    sku_count_stockout      INT,
    stockout_rate_pct       DECIMAL(7,4),
    total_inventory_value   DECIMAL(16,2),
    -- Targets (from planning)
    sales_target            DECIMAL(16,2),
    transaction_target      INT,
    -- Target achievement
    sales_vs_target_pct     DECIMAL(7,4)    GENERATED ALWAYS AS (
                                CASE WHEN sales_target > 0
                                THEN net_sales / sales_target * 100 ELSE NULL END
                            ) STORED,
    -- ETL Audit
    etl_batch_id            BIGINT,
    etl_loaded_at           TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_fact_daily_store_kpi PRIMARY KEY (kpi_sk),
    CONSTRAINT uq_fact_daily_store_kpi UNIQUE (date_id, store_sk)
);

CREATE INDEX idx_fact_dsk_date      ON fact.daily_store_kpi(date_id);
CREATE INDEX idx_fact_dsk_store     ON fact.daily_store_kpi(store_sk);
CREATE INDEX idx_fact_dsk_date_store ON fact.daily_store_kpi(date_id, store_sk);
CREATE INDEX idx_fact_dsk_loaded    ON fact.daily_store_kpi(etl_loaded_at);

-- ─────────────────────────────────────────
-- fact.inventory_snapshot  (Daily Snapshot - TRUNCATE current + INSERT)
-- Grain: One row per product per store per day
-- ─────────────────────────────────────────
CREATE TABLE fact.inventory_snapshot (
    inv_sk                  BIGSERIAL       NOT NULL,
    -- Keys
    date_id                 INT             NOT NULL    REFERENCES dim.date(date_id),
    store_sk                INT             NOT NULL    REFERENCES dim.store(store_sk),
    product_sk              INT             NOT NULL    REFERENCES dim.product(product_sk),
    -- Snapshot Measures (Semi-Additive — sum across products OK, NOT across dates)
    stock_qty               DECIMAL(12,3)   NOT NULL    DEFAULT 0,
    stock_value_cost        DECIMAL(14,2)   NOT NULL    DEFAULT 0,
    stock_value_retail      DECIMAL(14,2)   NOT NULL    DEFAULT 0,
    min_stock_level         DECIMAL(12,3),
    max_stock_level         DECIMAL(12,3),
    reorder_point           DECIMAL(12,3),
    days_of_stock           DECIMAL(8,2),               -- stock_qty / avg_daily_sales
    -- Flags
    is_stockout             BOOLEAN         NOT NULL    DEFAULT FALSE,
    is_overstock            BOOLEAN         NOT NULL    DEFAULT FALSE,
    is_slow_mover           BOOLEAN         NOT NULL    DEFAULT FALSE,
    -- ETL Audit
    etl_batch_id            BIGINT,
    etl_loaded_at           TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_fact_inv_snapshot PRIMARY KEY (inv_sk),
    CONSTRAINT uq_fact_inv_snapshot UNIQUE (date_id, store_sk, product_sk)
);

CREATE INDEX idx_fact_inv_date          ON fact.inventory_snapshot(date_id);
CREATE INDEX idx_fact_inv_store         ON fact.inventory_snapshot(store_sk);
CREATE INDEX idx_fact_inv_product       ON fact.inventory_snapshot(product_sk);
CREATE INDEX idx_fact_inv_stockout      ON fact.inventory_snapshot(date_id) WHERE is_stockout = TRUE;
CREATE INDEX idx_fact_inv_overstock     ON fact.inventory_snapshot(date_id) WHERE is_overstock = TRUE;

-- ─────────────────────────────────────────
-- fact.campaign_performance  (Accumulating Snapshot)
-- Grain: One row per campaign per store
-- ─────────────────────────────────────────
CREATE TABLE fact.campaign_performance (
    camp_perf_sk            BIGSERIAL       NOT NULL,
    -- Keys
    campaign_sk             INT             NOT NULL    REFERENCES dim.campaign(campaign_sk),
    store_sk                INT             NOT NULL    REFERENCES dim.store(store_sk),
    start_date_id           INT             REFERENCES dim.date(date_id),
    end_date_id             INT             REFERENCES dim.date(date_id),
    -- Measures
    participating_sku_count INT             NOT NULL    DEFAULT 0,
    total_sales_amount      DECIMAL(16,2)   NOT NULL    DEFAULT 0,
    baseline_sales_amount   DECIMAL(16,2),              -- pre-campaign avg
    incremental_sales       DECIMAL(16,2)   GENERATED ALWAYS AS (total_sales_amount - COALESCE(baseline_sales_amount,0)) STORED,
    total_discount_given    DECIMAL(16,2)   NOT NULL    DEFAULT 0,
    total_units_sold        INT             NOT NULL    DEFAULT 0,
    unique_customers        INT             NOT NULL    DEFAULT 0,
    new_customers           INT             NOT NULL    DEFAULT 0,
    campaign_roi_pct        DECIMAL(8,4),
    -- ETL
    etl_loaded_at           TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_fact_camp_perf PRIMARY KEY (camp_perf_sk),
    CONSTRAINT uq_fact_camp_perf UNIQUE (campaign_sk, store_sk)
);

CREATE INDEX idx_fact_camp_perf_campaign ON fact.campaign_performance(campaign_sk);
CREATE INDEX idx_fact_camp_perf_store    ON fact.campaign_performance(store_sk);

-- ─────────────────────────────────────────
-- staging.etl_batch_log  (ETL Control Table)
-- ─────────────────────────────────────────
CREATE TABLE staging.etl_batch_log (
    batch_id                BIGSERIAL       NOT NULL,
    batch_name              VARCHAR(100)    NOT NULL,
    target_table            VARCHAR(100)    NOT NULL,
    load_strategy           VARCHAR(20)     NOT NULL CHECK (load_strategy IN ('INCREMENTAL','TRUNCATE_INSERT','FULL_LOAD','SCD2')),
    source_system           VARCHAR(50),
    status                  VARCHAR(20)     NOT NULL DEFAULT 'RUNNING' CHECK (status IN ('RUNNING','SUCCESS','FAILED','PARTIAL')),
    start_time              TIMESTAMP       NOT NULL DEFAULT NOW(),
    end_time                TIMESTAMP,
    rows_extracted          BIGINT          DEFAULT 0,
    rows_inserted           BIGINT          DEFAULT 0,
    rows_updated            BIGINT          DEFAULT 0,
    rows_rejected           BIGINT          DEFAULT 0,
    watermark_from          TIMESTAMP,
    watermark_to            TIMESTAMP,
    error_message           TEXT,
    CONSTRAINT pk_etl_batch_log PRIMARY KEY (batch_id)
);

CREATE INDEX idx_etl_batch_target   ON staging.etl_batch_log(target_table, start_time DESC);
CREATE INDEX idx_etl_batch_status   ON staging.etl_batch_log(status);
