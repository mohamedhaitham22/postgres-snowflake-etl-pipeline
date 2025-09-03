-- Create warehouse on snowflake
CREATE DATABASE shipping_dw;

-- Dimension: Customers
CREATE OR REPLACE TABLE dim_customers (
    customer_key INT AUTOINCREMENT PRIMARY KEY,
    customer_id INT,  -- original ID from Postgres
    name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(200)
);

-- Dimension: Ships
CREATE OR REPLACE TABLE dim_ships (
    ship_key INT AUTOINCREMENT PRIMARY KEY,
    ship_id INT,
    name VARCHAR(100),
    capacity INT
);

-- Dimension: Ports
CREATE OR REPLACE TABLE dim_ports (
    port_key INT AUTOINCREMENT PRIMARY KEY,
    port_id INT,
    name VARCHAR(100),
    country VARCHAR(50)
);

-- Fact table: Shipments
CREATE OR REPLACE TABLE fact_shipments (
    shipment_key INT AUTOINCREMENT PRIMARY KEY,
    shipment_id INT,
    customer_key INT REFERENCES dim_customers(customer_key),
    ship_key INT REFERENCES dim_ships(ship_key),
    origin_port_key INT REFERENCES dim_ports(port_key),
    destination_port_key INT REFERENCES dim_ports(port_key),
    shipment_date DATE,
    delivery_date DATE,
    status VARCHAR(50),
    total_weight DECIMAL(10,2),
    total_cost DECIMAL(10,2)
);
