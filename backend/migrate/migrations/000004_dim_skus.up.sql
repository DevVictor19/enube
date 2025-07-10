CREATE TABLE dim_skus (
    sku_sk SERIAL PRIMARY KEY,
    sku_id VARCHAR UNIQUE,
    sku_name VARCHAR UNIQUE
);
