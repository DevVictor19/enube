CREATE TABLE dim_charge_types (
    charge_type_sk SERIAL PRIMARY KEY,
    type VARCHAR UNIQUE
);
