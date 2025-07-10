CREATE TABLE dim_products (
    product_sk SERIAL PRIMARY KEY,
    product_id VARCHAR UNIQUE,
    product_name VARCHAR UNIQUE
);
