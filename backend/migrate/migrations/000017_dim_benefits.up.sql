CREATE TABLE dim_benefits (
    benefit_sk SERIAL PRIMARY KEY,
    benefit_id VARCHAR UNIQUE,
    type VARCHAR
);
