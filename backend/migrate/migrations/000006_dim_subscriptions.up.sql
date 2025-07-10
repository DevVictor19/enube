CREATE TABLE dim_subscriptions (
    subscription_sk SERIAL PRIMARY KEY,
    subscription_id VARCHAR UNIQUE,
    description VARCHAR
);
