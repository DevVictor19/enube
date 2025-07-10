CREATE TABLE dim_meters (
    meter_sk SERIAL PRIMARY KEY,
    meter_id VARCHAR UNIQUE,
    name VARCHAR,
    category VARCHAR,
    type VARCHAR,
    subcategory VARCHAR,
    region VARCHAR,
    unit VARCHAR
);
