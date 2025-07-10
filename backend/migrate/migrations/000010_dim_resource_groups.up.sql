CREATE TABLE dim_resource_groups (
    resource_group_sk SERIAL PRIMARY KEY,
    name VARCHAR UNIQUE
);
