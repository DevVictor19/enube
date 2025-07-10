CREATE TABLE dim_months_charge_dates (
    months_charge_date_sk SERIAL PRIMARY KEY,
    charge_start_date DATE,
    charge_end_date DATE
);
