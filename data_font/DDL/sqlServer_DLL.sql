-- 1. Create Database
CREATE DATABASE ERP_Ecommerce;
GO

USE ERP_Ecommerce;
GO

-- 2. Customers Table
CREATE TABLE customers (
    id_customer INT IDENTITY(1,1) PRIMARY KEY,
    full_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    cpf VARCHAR(14) NOT NULL UNIQUE,
    address VARCHAR(200) NOT NULL,
    city VARCHAR(50) NOT NULL,
    state VARCHAR(2) NOT NULL,
    zip_code VARCHAR(10) NOT NULL,
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE()
);

-- 3. Categories and Products Table
CREATE TABLE categories (
    id_category INT IDENTITY(1,1) PRIMARY KEY,
    category_name VARCHAR(50) NOT NULL,
    description VARCHAR(255)
);

CREATE TABLE products (
    id_product INT IDENTITY(1,1) PRIMARY KEY,
    sku VARCHAR(30) NOT NULL UNIQUE,
    product_name VARCHAR(100) NOT NULL,
    id_category INT NOT NULL FOREIGN KEY REFERENCES categories(id_category),
    sale_price DECIMAL(10,2) NOT NULL,
    unit_cost DECIMAL(10,2) NOT NULL,
    current_stock INT NOT NULL DEFAULT 0,
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE()
);

-- 4. Orders Table (Header)
CREATE TABLE orders (
    id_order INT IDENTITY(1,1) PRIMARY KEY,
    id_customer INT NOT NULL FOREIGN KEY REFERENCES customers(id_customer),
    order_date DATETIME2 NOT NULL DEFAULT GETDATE(),
    total_value DECIMAL(10,2) NOT NULL,
    shipping_value DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    current_status VARCHAR(30) NOT NULL DEFAULT 'CREATED', -- CREATED, APPROVED, PICKING, SHIPPED, DELIVERED, CANCELLED
    updated_at DATETIME2 DEFAULT GETDATE()
);

-- 5. Order Items Table
CREATE TABLE order_items (
    id_item INT IDENTITY(1,1) PRIMARY KEY,
    id_order INT NOT NULL FOREIGN KEY REFERENCES orders(id_order),
    id_product INT NOT NULL FOREIGN KEY REFERENCES products(id_product),
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    subtotal AS (quantity * unit_price) -- Calculated column
);

-- 6. Payments Table
CREATE TABLE payments (
    id_payment INT IDENTITY(1,1) PRIMARY KEY,
    id_order INT NOT NULL FOREIGN KEY REFERENCES orders(id_order),
    payment_method VARCHAR(30) NOT NULL, -- CREDIT_CARD, PIX, BANK_SLIP
    payment_status VARCHAR(30) NOT NULL, -- PENDING, APPROVED, REJECTED
    amount DECIMAL(10,2) NOT NULL,
    payment_date DATETIME2
);
GO