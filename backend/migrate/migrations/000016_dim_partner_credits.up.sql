CREATE TABLE dim_partner_credits (
    partner_credit_sk SERIAL PRIMARY KEY,
    type VARCHAR UNIQUE,
    percentage DECIMAL,
    partner_earned_percentage DECIMAL
);
