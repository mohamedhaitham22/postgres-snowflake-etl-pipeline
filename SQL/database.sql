-- Create shipping database on postgresql
CREATE DATABASE shipping_company;

-- Connect to it, then create schema
\c shipping_company;

-- Customers table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(200)
);

-- Ships table
CREATE TABLE ships (
    ship_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    capacity INT
);

-- Ports table
CREATE TABLE ports (
    port_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    country VARCHAR(50)
);

-- Shipments table
CREATE TABLE shipments (
    shipment_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    ship_id INT REFERENCES ships(ship_id),
    origin_port INT REFERENCES ports(port_id),
    destination_port INT REFERENCES ports(port_id),
    shipment_date DATE,
    delivery_date DATE,
    status VARCHAR(50)
);

-- Shipment items table
CREATE TABLE shipment_items (
    item_id SERIAL PRIMARY KEY,
    shipment_id INT REFERENCES shipments(shipment_id),
    description VARCHAR(200),
    weight DECIMAL(10,2),
    cost DECIMAL(10,2)
);
