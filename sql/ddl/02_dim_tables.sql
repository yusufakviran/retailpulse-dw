-- ============================================================
-- RetailPulse Data Warehouse
-- Layer 1: DIMENSION TABLES (Kimball Star Schema)
-- SCD Type 2 on: dim.store, dim.product, dim.employee
-- SCD Type 1 on: dim.date, dim.region (static / slowly changing)
-- ============================================================

-- ─────────────────────────────────────────
-- dim.date  (Static - fully pre-populated)
-- No surrogate key needed; date_id = YYYYMMDD integer
-- ─────────────────────────────────────────
CREATE TABLE dim.date (
    date_id             INT             NOT NULL,   -- YYYYMMDD
    full_date           DATE            NOT NULL,
    day_of_week         SMALLINT        NOT NULL,   -- 1=Mon … 7=Sun
    day_name            VARCHAR(15)     NOT NULL,
    day_name_tr         VARCHAR(15)     NOT NULL,
    day_of_month        SMALLINT        NOT NULL,
    day_of_year         SMALLINT        NOT NULL,
    week_of_year        SMALLINT        NOT NULL,
    week_start_date     DATE            NOT NULL,
    month_number        SMALLINT        NOT NULL,
    month_name          VARCHAR(15)     NOT NULL,
    month_name_tr       VARCHAR(15)     NOT NULL,
    month_short         CHAR(3)         NOT NULL,
    quarter_number      SMALLINT        NOT NULL,
    quarter_name        CHAR(2)         NOT NULL,
    year_number         SMALLINT        NOT NULL,
    fiscal_week         SMALLINT        NOT NULL,
    fiscal_month        SMALLINT        NOT NULL,
    fiscal_quarter      SMALLINT        NOT NULL,
    fiscal_year         SMALLINT        NOT NULL,
    is_weekend          BOOLEAN         NOT NULL,
    is_public_holiday   BOOLEAN         NOT NULL DEFAULT FALSE,
    is_religious_day    BOOLEAN         NOT NULL DEFAULT FALSE,
    holiday_name        VARCHAR(100),
    day_type_id         SMALLINT        REFERENCES dict.d_day_type(day_type_id),
    season              VARCHAR(10),                -- İlkbahar, Yaz, Sonbahar, Kış
    CONSTRAINT pk_dim_date PRIMARY KEY (date_id)
);

CREATE INDEX idx_dim_date_full_date    ON dim.date(full_date);
CREATE INDEX idx_dim_date_year_month   ON dim.date(year_number, month_number);
CREATE INDEX idx_dim_date_fiscal_year  ON dim.date(fiscal_year, fiscal_month);

-- ─────────────────────────────────────────
-- dim.region (SCD Type 1 - overwrite)
-- ─────────────────────────────────────────
CREATE TABLE dim.region (
    region_sk           SERIAL          NOT NULL,
    region_id           INT             NOT NULL,   -- business key
    region_code         VARCHAR(10)     NOT NULL,
    region_name         VARCHAR(60)     NOT NULL,
    -- Hierarchy
    city                VARCHAR(60)     NOT NULL,
    city_code           VARCHAR(10)     NOT NULL,
    district            VARCHAR(60),
    nuts2_code          VARCHAR(5),                 -- TR statistical region
    nuts2_name          VARCHAR(60),
    -- Geo
    latitude            DECIMAL(10,7),
    longitude           DECIMAL(10,7),
    timezone            VARCHAR(40)     NOT NULL DEFAULT 'Europe/Istanbul',
    -- Meta
    population_segment  VARCHAR(20),                -- Metropolitan, Urban, Rural
    created_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_dim_region PRIMARY KEY (region_sk),
    CONSTRAINT uq_dim_region_id UNIQUE (region_id)
);

CREATE INDEX idx_dim_region_city ON dim.region(city);

-- ─────────────────────────────────────────
-- dim.store  (SCD Type 2)
-- Surrogate key: store_sk (auto-increment)
-- Business key:  store_id
-- ─────────────────────────────────────────
CREATE TABLE dim.store (
    -- SCD2 Keys
    store_sk            SERIAL          NOT NULL,
    store_id            INT             NOT NULL,   -- business key (natural key)
    -- Attributes
    store_code          VARCHAR(20)     NOT NULL,
    store_name          VARCHAR(100)    NOT NULL,
    store_short_name    VARCHAR(40),
    -- Format & Size
    format_code         VARCHAR(10)     REFERENCES dict.d_store_format(format_code),
    store_sqm           INT,
    selling_sqm         INT,
    checkout_count      SMALLINT,
    -- Location
    region_sk           INT             REFERENCES dim.region(region_sk),
    address             VARCHAR(255),
    mall_name           VARCHAR(100),
    is_mall_store       BOOLEAN         NOT NULL DEFAULT FALSE,
    -- Operations
    opening_date        DATE,
    renovation_date     DATE,
    closing_date        DATE,
    is_franchise        BOOLEAN         NOT NULL DEFAULT FALSE,
    cluster             VARCHAR(20),                -- A/B/C store performance tier
    headcount           SMALLINT,
    -- SCD2 Columns
    scd_start_date      DATE            NOT NULL,
    scd_end_date        DATE,                       -- NULL = current record
    is_current          BOOLEAN         NOT NULL DEFAULT TRUE,
    scd_version         SMALLINT        NOT NULL DEFAULT 1,
    scd_hash            VARCHAR(64),                -- MD5 of tracked attributes
    -- Audit
    created_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_dim_store PRIMARY KEY (store_sk)
);

CREATE INDEX idx_dim_store_id          ON dim.store(store_id);
CREATE INDEX idx_dim_store_current     ON dim.store(store_id, is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dim_store_format      ON dim.store(format_code);
CREATE INDEX idx_dim_store_region      ON dim.store(region_sk);
CREATE INDEX idx_dim_store_scd_dates   ON dim.store(scd_start_date, scd_end_date);

-- ─────────────────────────────────────────
-- dim.product_category (SCD Type 1, hierarchy)
-- ─────────────────────────────────────────
CREATE TABLE dim.product_category (
    category_sk         SERIAL          NOT NULL,
    category_id         INT             NOT NULL,
    -- 4-Level hierarchy
    department_code     VARCHAR(10)     NOT NULL,
    department_name     VARCHAR(60)     NOT NULL,
    category_code       VARCHAR(15)     NOT NULL,
    category_name       VARCHAR(80)     NOT NULL,
    subcategory_code    VARCHAR(20)     NOT NULL,
    subcategory_name    VARCHAR(100)    NOT NULL,
    segment_code        VARCHAR(25),
    segment_name        VARCHAR(100),
    -- Attributes
    is_food             BOOLEAN         NOT NULL DEFAULT FALSE,
    is_private_label    BOOLEAN         NOT NULL DEFAULT FALSE,
    vat_rate            DECIMAL(5,2)    NOT NULL DEFAULT 10.00,
    shelf_life_days     INT,
    -- Audit
    created_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_dim_product_category PRIMARY KEY (category_sk),
    CONSTRAINT uq_dim_product_category_id UNIQUE (category_id)
);

CREATE INDEX idx_dim_product_cat_dept ON dim.product_category(department_code);

-- ─────────────────────────────────────────
-- dim.product  (SCD Type 2)
-- Business key: sku_code (barcode)
-- ─────────────────────────────────────────
CREATE TABLE dim.product (
    -- SCD2 Keys
    product_sk          SERIAL          NOT NULL,
    product_id          INT             NOT NULL,   -- internal product ID
    sku_code            VARCHAR(30)     NOT NULL,   -- barcode / EAN
    -- Attributes
    product_name        VARCHAR(255)    NOT NULL,
    product_short_name  VARCHAR(100),
    brand               VARCHAR(80),
    supplier_name       VARCHAR(100),
    supplier_code       VARCHAR(20),
    -- Category
    category_sk         INT             REFERENCES dim.product_category(category_sk),
    -- Measures
    unit_of_measure     VARCHAR(10)     NOT NULL DEFAULT 'ADET',  -- ADET, KG, LT
    net_weight_gr       DECIMAL(10,3),
    package_size        VARCHAR(30),
    -- Pricing (at time of SCD snapshot)
    list_price          DECIMAL(12,2),
    cost_price          DECIMAL(12,2),
    -- Status
    status_code         VARCHAR(10)     REFERENCES dict.d_product_status(status_code),
    is_private_label    BOOLEAN         NOT NULL DEFAULT FALSE,
    is_perishable       BOOLEAN         NOT NULL DEFAULT FALSE,
    -- SCD2 Columns
    scd_start_date      DATE            NOT NULL,
    scd_end_date        DATE,
    is_current          BOOLEAN         NOT NULL DEFAULT TRUE,
    scd_version         SMALLINT        NOT NULL DEFAULT 1,
    scd_hash            VARCHAR(64),
    -- Audit
    created_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_dim_product PRIMARY KEY (product_sk)
);

CREATE INDEX idx_dim_product_id        ON dim.product(product_id);
CREATE INDEX idx_dim_product_sku       ON dim.product(sku_code);
CREATE INDEX idx_dim_product_current   ON dim.product(product_id, is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dim_product_category  ON dim.product(category_sk);
CREATE INDEX idx_dim_product_brand     ON dim.product(brand);
CREATE INDEX idx_dim_product_scd       ON dim.product(scd_start_date, scd_end_date);

-- ─────────────────────────────────────────
-- dim.customer  (SCD Type 2)
-- ─────────────────────────────────────────
CREATE TABLE dim.customer (
    customer_sk         SERIAL          NOT NULL,
    customer_id         BIGINT          NOT NULL,   -- CRM / loyalty ID
    -- Attributes
    customer_segment    VARCHAR(30),                -- Gold, Silver, Bronze, Occasional
    age_band            VARCHAR(15),                -- 18-24, 25-34, …
    gender              CHAR(1)         CHECK (gender IN ('M','F','U')),
    city                VARCHAR(60),
    acquisition_channel VARCHAR(40),
    loyalty_enrolled    BOOLEAN         NOT NULL DEFAULT FALSE,
    loyalty_tier        VARCHAR(20),
    -- Lifetime metrics (SCD2 snapshot)
    clv_segment         VARCHAR(20),                -- High / Medium / Low value
    total_visits_ytd    INT,
    total_spend_ytd     DECIMAL(14,2),
    -- SCD2
    scd_start_date      DATE            NOT NULL,
    scd_end_date        DATE,
    is_current          BOOLEAN         NOT NULL DEFAULT TRUE,
    scd_version         SMALLINT        NOT NULL DEFAULT 1,
    scd_hash            VARCHAR(64),
    created_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk)
);

-- Dimension for anonymous customers
INSERT INTO dim.customer (customer_sk, customer_id, customer_segment, gender, scd_start_date, is_current)
VALUES (-1, -1, 'Anonim', 'U', '2000-01-01', TRUE);

CREATE INDEX idx_dim_customer_id       ON dim.customer(customer_id);
CREATE INDEX idx_dim_customer_current  ON dim.customer(customer_id, is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dim_customer_segment  ON dim.customer(customer_segment);

-- ─────────────────────────────────────────
-- dim.employee  (SCD Type 2)
-- ─────────────────────────────────────────
CREATE TABLE dim.employee (
    employee_sk         SERIAL          NOT NULL,
    employee_id         INT             NOT NULL,
    -- Attributes
    employee_code       VARCHAR(20)     NOT NULL,
    full_name           VARCHAR(100)    NOT NULL,
    job_title           VARCHAR(80),
    department          VARCHAR(60),
    employment_type     VARCHAR(20),                -- Tam Zamanlı, Yarı Zamanlı, Part-Time
    -- Org hierarchy
    store_id            INT,                        -- home store
    manager_employee_id INT,
    -- SCD2
    scd_start_date      DATE            NOT NULL,
    scd_end_date        DATE,
    is_current          BOOLEAN         NOT NULL DEFAULT TRUE,
    scd_version         SMALLINT        NOT NULL DEFAULT 1,
    scd_hash            VARCHAR(64),
    created_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_dim_employee PRIMARY KEY (employee_sk)
);

CREATE INDEX idx_dim_employee_id       ON dim.employee(employee_id);
CREATE INDEX idx_dim_employee_current  ON dim.employee(employee_id, is_current) WHERE is_current = TRUE;

-- ─────────────────────────────────────────
-- dim.campaign
-- ─────────────────────────────────────────
CREATE TABLE dim.campaign (
    campaign_sk         SERIAL          NOT NULL,
    campaign_id         INT             NOT NULL,
    campaign_code       VARCHAR(30)     NOT NULL,
    campaign_name       VARCHAR(150)    NOT NULL,
    campaign_type       VARCHAR(40),                -- İndirim, BOGO, Puan, Çapraz Satış
    channel             VARCHAR(30),                -- Tüm Kanal, Mağaza, Online
    discount_type       VARCHAR(20),                -- Oran, Tutar, Ücretsiz Ürün
    discount_value      DECIMAL(10,2),
    start_date          DATE            NOT NULL,
    end_date            DATE            NOT NULL,
    budget_try          DECIMAL(14,2),
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_dim_campaign PRIMARY KEY (campaign_sk),
    CONSTRAINT uq_dim_campaign_id UNIQUE (campaign_id)
);

CREATE INDEX idx_dim_campaign_dates ON dim.campaign(start_date, end_date);

-- ─────────────────────────────────────────
-- Conformed "unknown" dimension members
-- (Kimball best practice - avoid NULL FKs in fact)
-- ─────────────────────────────────────────
INSERT INTO dim.campaign (campaign_sk, campaign_id, campaign_code, campaign_name, start_date, end_date, is_active)
VALUES (-1, -1, 'NO_CAMPAIGN', 'Kampanyasız', '2000-01-01', '2099-12-31', TRUE);
