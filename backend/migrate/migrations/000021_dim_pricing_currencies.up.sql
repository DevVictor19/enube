CREATE TABLE dim_pricing_currencies (
    pricing_currency_sk SERIAL PRIMARY KEY,
    currency VARCHAR UNIQUE
);