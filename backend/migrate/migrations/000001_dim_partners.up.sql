CREATE TABLE dim_partners (
    partner_sk SERIAL PRIMARY KEY,
    partner_id VARCHAR UNIQUE,
    partner_name VARCHAR,
    mpn_id INTEGER UNIQUE,
    invoice_number VARCHAR UNIQUE
);
