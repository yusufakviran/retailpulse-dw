-- ============================================================
-- RetailPulse Data Warehouse
-- Layer 0: DICTIONARY TABLES (Static Reference / Lookup)
-- Methodology: Kimball Dimensional Modeling
-- Author: RetailPulse DW Team
-- ============================================================

CREATE SCHEMA IF NOT EXISTS dict;
CREATE SCHEMA IF NOT EXISTS dim;
CREATE SCHEMA IF NOT EXISTS fact;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;

-- ─────────────────────────────────────────
-- dict.d_currency
-- ─────────────────────────────────────────
CREATE TABLE dict.d_currency (
    currency_code       CHAR(3)         NOT NULL,
    currency_name       VARCHAR(50)     NOT NULL,
    currency_symbol     VARCHAR(5)      NOT NULL,
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_d_currency PRIMARY KEY (currency_code)
);

INSERT INTO dict.d_currency VALUES
('TRY', 'Türk Lirası',      '₺', TRUE),
('USD', 'US Dollar',        '$', TRUE),
('EUR', 'Euro',             '€', TRUE);

-- ─────────────────────────────────────────
-- dict.d_day_type
-- ─────────────────────────────────────────
CREATE TABLE dict.d_day_type (
    day_type_id         SMALLINT        NOT NULL,
    day_type_name       VARCHAR(30)     NOT NULL,
    is_working_day      BOOLEAN         NOT NULL,
    CONSTRAINT pk_d_day_type PRIMARY KEY (day_type_id)
);

INSERT INTO dict.d_day_type VALUES
(1, 'Hafta İçi',            TRUE),
(2, 'Cumartesi',            TRUE),
(3, 'Pazar',                FALSE),
(4, 'Resmi Tatil',          FALSE),
(5, 'Dini Bayram',          FALSE),
(6, 'Yarım Gün',            TRUE);

-- ─────────────────────────────────────────
-- dict.d_store_format
-- ─────────────────────────────────────────
CREATE TABLE dict.d_store_format (
    format_code         VARCHAR(10)     NOT NULL,
    format_name         VARCHAR(50)     NOT NULL,
    avg_sqm_min         INT,
    avg_sqm_max         INT,
    description         VARCHAR(200),
    CONSTRAINT pk_d_store_format PRIMARY KEY (format_code)
);

INSERT INTO dict.d_store_format VALUES
('HYPER',   'Hipermarket',          5000, 15000, 'Büyük alışveriş merkezi mağazası'),
('SUPER',   'Süpermarket',          1000, 4999,  'Orta ölçekli market'),
('EXPRESS', 'Express / Mahalle',    100,  999,   'Küçük mahalle mağazası'),
('ONLINE',  'Online Kanal',         NULL, NULL,  'E-ticaret kanalı'),
('OUTLET',  'Outlet Mağaza',        500,  3000,  'İndirimli outlet formatı');

-- ─────────────────────────────────────────
-- dict.d_product_status
-- ─────────────────────────────────────────
CREATE TABLE dict.d_product_status (
    status_code         VARCHAR(10)     NOT NULL,
    status_name         VARCHAR(40)     NOT NULL,
    is_sellable         BOOLEAN         NOT NULL,
    CONSTRAINT pk_d_product_status PRIMARY KEY (status_code)
);

INSERT INTO dict.d_product_status VALUES
('ACTIVE',      'Aktif',                TRUE),
('PASSIVE',     'Pasif',                FALSE),
('SEASONAL',    'Sezonluk',             TRUE),
('CLEARANCE',   'Tasfiye',              TRUE),
('DISCONTINUED','Üretimi Durdu',        FALSE),
('NEW',         'Yeni Ürün',            TRUE);

-- ─────────────────────────────────────────
-- dict.d_transaction_type
-- ─────────────────────────────────────────
CREATE TABLE dict.d_transaction_type (
    transaction_type_id SMALLINT        NOT NULL,
    type_name           VARCHAR(50)     NOT NULL,
    affects_inventory   BOOLEAN         NOT NULL,
    affects_revenue     BOOLEAN         NOT NULL,
    direction           CHAR(1)         NOT NULL CHECK (direction IN ('+','-')),
    CONSTRAINT pk_d_transaction_type PRIMARY KEY (transaction_type_id)
);

INSERT INTO dict.d_transaction_type VALUES
(1,  'Normal Satış',         TRUE,  TRUE,  '-'),
(2,  'İade',                 TRUE,  TRUE,  '+'),
(3,  'İptal',                FALSE, TRUE,  '+'),
(4,  'Stok Girişi',          TRUE,  FALSE, '+'),
(5,  'Stok Çıkışı (Fire)',   TRUE,  FALSE, '-'),
(6,  'Transfer Çıkış',       TRUE,  FALSE, '-'),
(7,  'Transfer Giriş',       TRUE,  FALSE, '+'),
(8,  'Kampanya İndirimi',    FALSE, TRUE,  '-'),
(9,  'Sayım Düzeltme',       TRUE,  FALSE, '+');

-- ─────────────────────────────────────────
-- dict.d_payment_method
-- ─────────────────────────────────────────
CREATE TABLE dict.d_payment_method (
    payment_method_id   SMALLINT        NOT NULL,
    method_name         VARCHAR(40)     NOT NULL,
    is_digital          BOOLEAN         NOT NULL,
    CONSTRAINT pk_d_payment_method PRIMARY KEY (payment_method_id)
);

INSERT INTO dict.d_payment_method VALUES
(1, 'Nakit',                FALSE),
(2, 'Kredi Kartı',          FALSE),
(3, 'Banka Kartı',          FALSE),
(4, 'Temassız',             TRUE),
(5, 'Mobil Ödeme',          TRUE),
(6, 'Hediye Çeki',          FALSE),
(7, 'Kurumsal Fatura',      FALSE),
(8, 'Online / Kapıda',      TRUE);

-- ─────────────────────────────────────────
-- dict.d_kpi_definition
-- ─────────────────────────────────────────
CREATE TABLE dict.d_kpi_definition (
    kpi_code            VARCHAR(30)     NOT NULL,
    kpi_name            VARCHAR(100)    NOT NULL,
    kpi_category        VARCHAR(50)     NOT NULL,
    unit                VARCHAR(20)     NOT NULL,
    higher_is_better    BOOLEAN         NOT NULL,
    description         TEXT,
    formula             TEXT,
    CONSTRAINT pk_d_kpi_definition PRIMARY KEY (kpi_code)
);

INSERT INTO dict.d_kpi_definition VALUES
('NET_SALES',       'Net Satış Tutarı',         'Finansal',     'TRY',      TRUE,  'İadeler düşülmüş net satış',           'gross_sales - returns'),
('GROSS_MARGIN',    'Brüt Kâr Marjı',           'Finansal',     '%',        TRUE,  '(Satış - COGS) / Satış',               '(net_sales - cogs) / net_sales * 100'),
('UNITS_SOLD',      'Satılan Adet',             'Satış',        'Adet',     TRUE,  'Toplam satılan ürün adedi',            'SUM(quantity)'),
('BASKET_SIZE',     'Sepet Büyüklüğü',          'Müşteri',      'TRY',      TRUE,  'Ortalama işlem tutarı',                'net_sales / transaction_count'),
('BASKET_ITEMS',    'Sepetteki Ürün Adedi',     'Müşteri',      'Adet',     TRUE,  'İşlem başına ortalama ürün',           'units_sold / transaction_count'),
('CONVERSION_RATE', 'Dönüşüm Oranı',            'Müşteri',      '%',        TRUE,  'Alışveriş yapan / toplam giriş',       'buyers / visitors * 100'),
('STOCKOUT_RATE',   'Stok Tükenme Oranı',       'Stok',         '%',        FALSE, 'Stokta olmayan SKU oranı',             'stockout_sku / total_sku * 100'),
('INVENTORY_TURN',  'Stok Devir Hızı',          'Stok',         'x/yıl',    TRUE,  'COGS / Ortalama Stok Değeri',          'cogs / avg_inventory_value'),
('SHRINKAGE_RATE',  'Fire ve Kayıp Oranı',      'Stok',         '%',        FALSE, 'Kayıp stok / toplam stok',             'shrinkage / total_inventory * 100'),
('SALES_PER_SQM',   'M² Başına Satış',          'Verimlilik',   'TRY/m²',   TRUE,  'Net satış / mağaza m²',                'net_sales / store_sqm'),
('STAFF_SALES',     'Çalışan Başına Satış',     'İK',           'TRY',      TRUE,  'Net satış / çalışan sayısı',           'net_sales / headcount'),
('NPS',             'Net Tavsiye Skoru',        'Müşteri',      'Puan',     TRUE,  'Müşteri memnuniyeti endeksi',          'promoters% - detractors%');
