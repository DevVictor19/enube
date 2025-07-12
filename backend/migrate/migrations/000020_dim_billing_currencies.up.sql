CREATE TABLE dim_billing_currencies (
    billing_currency_sk SERIAL PRIMARY KEY,
    currency VARCHAR UNIQUE
);