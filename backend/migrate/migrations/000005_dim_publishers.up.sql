CREATE TABLE dim_publishers (
    publisher_sk SERIAL PRIMARY KEY,
    publisher_id VARCHAR UNIQUE,
    publisher_name VARCHAR UNIQUE
);
