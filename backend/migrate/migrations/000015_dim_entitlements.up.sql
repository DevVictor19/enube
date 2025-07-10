CREATE TABLE dim_entitlements (
    entitlement_sk SERIAL PRIMARY KEY,
    description VARCHAR UNIQUE
);
