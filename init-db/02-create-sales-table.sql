\c target

CREATE TABLE IF NOT EXISTS sales (
    id              BIGSERIAL PRIMARY KEY,
    order_id        UUID         NOT NULL UNIQUE,
    order_ts        TIMESTAMPTZ  NOT NULL,
    customer_id     INT          NOT NULL,
    product         VARCHAR(64)  NOT NULL,
    region          VARCHAR(32)  NOT NULL,
    qty             INT          NOT NULL CHECK (qty > 0),
    unit_price      NUMERIC(10,2) NOT NULL CHECK (unit_price > 0),
    total           NUMERIC(12,2) NOT NULL,
    source_file     TEXT,
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sales_order_ts   ON sales(order_ts);
CREATE INDEX IF NOT EXISTS idx_sales_customer   ON sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_sales_region     ON sales(region);
CREATE INDEX IF NOT EXISTS idx_sales_product    ON sales(product);
