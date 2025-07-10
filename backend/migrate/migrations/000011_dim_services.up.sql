CREATE TABLE dim_services (
    resource_service_sk SERIAL PRIMARY KEY,
    service VARCHAR UNIQUE
);
