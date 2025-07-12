CREATE TABLE dim_usage_dates (
    usage_date_sk SERIAL PRIMARY KEY,
    usage_date DATE UNIQUE
);