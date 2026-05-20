\c target

-- ── Dimension Tables ──────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_date (
    date_id         INT          PRIMARY KEY,  -- YYYYMMDD
    full_date       DATE         NOT NULL UNIQUE,
    year            SMALLINT     NOT NULL,
    quarter         SMALLINT     NOT NULL,
    month_num       SMALLINT     NOT NULL,
    month_name      VARCHAR(9)   NOT NULL,
    week_of_year    SMALLINT     NOT NULL,
    day_of_week     SMALLINT     NOT NULL,  -- 0=Mon, 6=Sun
    day_name        VARCHAR(9)   NOT NULL,
    is_weekend      BOOLEAN      NOT NULL,
    is_business_day BOOLEAN      NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id     INT          PRIMARY KEY,
    full_name       VARCHAR(128) NOT NULL,
    email           VARCHAR(128) NOT NULL,
    signup_date     DATE         NOT NULL,
    tier            VARCHAR(16)  NOT NULL DEFAULT 'bronze',
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id      SERIAL        PRIMARY KEY,
    name            VARCHAR(64)   NOT NULL UNIQUE,
    category        VARCHAR(32)   NOT NULL,
    brand           VARCHAR(64)   NOT NULL,
    sku             VARCHAR(32)   NOT NULL UNIQUE,
    cost_price      NUMERIC(10,2) NOT NULL,
    list_price      NUMERIC(10,2) NOT NULL,
    is_active       BOOLEAN       NOT NULL DEFAULT TRUE,
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_geography (
    geo_id          SERIAL        PRIMARY KEY,
    country         VARCHAR(64)   NOT NULL,
    country_code    CHAR(2)       NOT NULL UNIQUE,
    region          VARCHAR(32)   NOT NULL,
    sub_region      VARCHAR(64)   NOT NULL,
    latitude        NUMERIC(9,6),
    longitude       NUMERIC(9,6)
);

CREATE TABLE IF NOT EXISTS dim_channel (
    channel_id      SERIAL       PRIMARY KEY,
    channel         VARCHAR(16)  NOT NULL UNIQUE,
    channel_type    VARCHAR(32)  NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_payment (
    payment_id      SERIAL       PRIMARY KEY,
    method          VARCHAR(16)  NOT NULL UNIQUE,
    is_digital      BOOLEAN      NOT NULL DEFAULT TRUE
);

-- ── Fact Table ────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id         BIGSERIAL     PRIMARY KEY,
    order_id        UUID          NOT NULL UNIQUE,
    order_ts        TIMESTAMPTZ   NOT NULL,
    date_id         INT           NOT NULL REFERENCES dim_date(date_id),
    customer_id     INT           NOT NULL REFERENCES dim_customer(customer_id),
    product_id      INT           NOT NULL REFERENCES dim_product(product_id),
    geo_id          INT           NOT NULL REFERENCES dim_geography(geo_id),
    channel_id      INT           NOT NULL REFERENCES dim_channel(channel_id),
    payment_id      INT           NOT NULL REFERENCES dim_payment(payment_id),
    qty             INT           NOT NULL CHECK (qty > 0),
    unit_price      NUMERIC(10,2) NOT NULL,
    cost_price      NUMERIC(10,2) NOT NULL,
    discount_pct    NUMERIC(5,2)  NOT NULL DEFAULT 0,
    total           NUMERIC(12,2) NOT NULL,
    gross_margin    NUMERIC(12,2) NOT NULL,
    is_returned     BOOLEAN       NOT NULL DEFAULT FALSE,
    source_file     TEXT,
    ingested_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- ── Indexes ───────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_fact_order_ts    ON fact_sales(order_ts);
CREATE INDEX IF NOT EXISTS idx_fact_date        ON fact_sales(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_customer    ON fact_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_product     ON fact_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_geo         ON fact_sales(geo_id);
CREATE INDEX IF NOT EXISTS idx_fact_channel     ON fact_sales(channel_id);
CREATE INDEX IF NOT EXISTS idx_fact_payment     ON fact_sales(payment_id);
CREATE INDEX IF NOT EXISTS idx_geo_country_code ON dim_geography(country_code);
CREATE INDEX IF NOT EXISTS idx_product_category ON dim_product(category);
CREATE INDEX IF NOT EXISTS idx_customer_tier    ON dim_customer(tier);

-- ── Seed dim_geography with country centroids (for Metabase maps) ─────────────

INSERT INTO dim_geography (country, country_code, region, sub_region, latitude, longitude) VALUES
    ('United States',  'US', 'Americas', 'Northern America',    37.090240,  -95.712891),
    ('Canada',         'CA', 'Americas', 'Northern America',    56.130366, -106.346771),
    ('Brazil',         'BR', 'Americas', 'South America',      -14.235004,  -51.925280),
    ('Mexico',         'MX', 'Americas', 'Central America',     23.634501, -102.552784),
    ('Germany',        'DE', 'Europe',   'Western Europe',      51.165691,   10.451526),
    ('United Kingdom', 'GB', 'Europe',   'Northern Europe',     55.378051,   -3.435973),
    ('France',         'FR', 'Europe',   'Western Europe',      46.227638,    2.213749),
    ('Netherlands',    'NL', 'Europe',   'Western Europe',      52.132633,    5.291266),
    ('Spain',          'ES', 'Europe',   'Southern Europe',     40.463667,   -3.749220),
    ('India',          'IN', 'Asia',     'Southern Asia',       20.593684,   78.962880),
    ('Singapore',      'SG', 'Asia',     'South-Eastern Asia',   1.352083,  103.819836),
    ('UAE',            'AE', 'Asia',     'Western Asia',        23.424076,   53.847818),
    ('Japan',          'JP', 'Asia',     'Eastern Asia',        36.204824,  138.252924),
    ('Nigeria',        'NG', 'Africa',   'Western Africa',       9.081999,    8.675277),
    ('Ghana',          'GH', 'Africa',   'Western Africa',       7.946527,   -1.023194),
    ('Kenya',          'KE', 'Africa',   'Eastern Africa',      -0.023559,   37.906193),
    ('South Africa',   'ZA', 'Africa',   'Southern Africa',    -30.559482,   22.937506),
    ('Egypt',          'EG', 'Africa',   'Northern Africa',     26.820553,   30.802498),
    ('Rwanda',         'RW', 'Africa',   'Eastern Africa',      -1.940278,   29.873888),
    ('Senegal',        'SN', 'Africa',   'Western Africa',      14.497401,  -14.452362),
    ('Tanzania',       'TZ', 'Africa',   'Eastern Africa',      -6.369028,   34.888822),
    ('Australia',      'AU', 'Oceania',  'Australia and NZ',   -25.274398,  133.775136)
ON CONFLICT (country_code) DO NOTHING;
