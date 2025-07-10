CREATE TABLE dim_customers (
    customer_sk SERIAL PRIMARY KEY,
    customer_id VARCHAR UNIQUE,
    customer_name VARCHAR UNIQUE,
    customer_domain_name VARCHAR,
    customer_country VARCHAR,
    tier_2_mpn_id INTEGER UNIQUE
);

