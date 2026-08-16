-- PostgreSQL Initialization Script for E-commerce Analytics

-- =============================================================================
-- Tesouro Direto Tables (Legacy)
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.dadostesouroipca (
    id SERIAL PRIMARY KEY,
    CompraManha DECIMAL(10,2),
    VendaManha DECIMAL(10,2),
    PUCompraManha DECIMAL(10,2),
    PUVendaManha DECIMAL(10,2),
    PUBaseManha DECIMAL(10,2),
    Data_Vencimento BIGINT,
    Data_Base BIGINT,
    Tipo VARCHAR(50),
    dt_update BIGINT
);

CREATE TABLE IF NOT EXISTS public.dadostesouropre (
    id SERIAL PRIMARY KEY,
    CompraManha DECIMAL(10,2),
    VendaManha DECIMAL(10,2),
    PUCompraManha DECIMAL(10,2),
    PUVendaManha DECIMAL(10,2),
    PUBaseManha DECIMAL(10,2),
    Data_Vencimento BIGINT,
    Data_Base BIGINT,
    Tipo VARCHAR(50),
    dt_update BIGINT
);

-- Insert sample data for IPCA table
INSERT INTO public.dadostesouroipca (CompraManha, VendaManha, PUCompraManha, PUVendaManha, PUBaseManha, Data_Vencimento, Data_Base, Tipo, dt_update) VALUES
(12.73, 12.79, 631.4, 630.11, 629.81, 1420070400000, 1298851200000, 'IPCA', 1734381830665),
(13.25, 13.30, 640.2, 639.8, 638.5, 1420156800000, 1298937600000, 'IPCA', 1734381830666)
ON CONFLICT DO NOTHING;

-- Insert sample data for PRE table
INSERT INTO public.dadostesouropre (CompraManha, VendaManha, PUCompraManha, PUVendaManha, PUBaseManha, Data_Vencimento, Data_Base, Tipo, dt_update) VALUES
(12.73, 12.79, 631.4, 630.11, 629.81, 1420070400000, 1298851200000, 'PRE-FIXADOS', 1734381830665),
(13.25, 13.30, 640.2, 639.8, 638.5, 1420156800000, 1298937600000, 'PRE-FIXADOS', 1734381830666)
ON CONFLICT DO NOTHING;

-- =============================================================================
-- E-commerce Bronze Layer Tables
-- =============================================================================

-- Category dimension table
CREATE TABLE IF NOT EXISTS public.brz_category (
    category_code VARCHAR(50) PRIMARY KEY,
    category_name VARCHAR(255) NOT NULL,
    dt_update BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT * 1000
);

-- Brands dimension table
CREATE TABLE IF NOT EXISTS public.brz_brands (
    brand_code VARCHAR(50) PRIMARY KEY,
    brand_name VARCHAR(255) NOT NULL,
    category_code VARCHAR(50),
    dt_update BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT * 1000,
    FOREIGN KEY (category_code) REFERENCES public.brz_category(category_code)
);

-- Customers dimension table
CREATE TABLE IF NOT EXISTS public.brz_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    phone VARCHAR(50),
    country_code VARCHAR(10),
    country VARCHAR(100),
    state VARCHAR(100),
    city VARCHAR(100),
    dt_update BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT * 1000
);

-- Calendar dimension table
CREATE TABLE IF NOT EXISTS public.brz_calendar (
    date_code INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    day INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    dt_update BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT * 1000
);

-- Products dimension table
CREATE TABLE IF NOT EXISTS public.brz_products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    brand_code VARCHAR(50),
    category_code VARCHAR(50),
    price DECIMAL(10, 2),
    description TEXT,
    dt_update BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT * 1000,
    FOREIGN KEY (brand_code) REFERENCES public.brz_brands(brand_code),
    FOREIGN KEY (category_code) REFERENCES public.brz_category(category_code)
);

-- Order items fact table
CREATE TABLE IF NOT EXISTS public.brz_order_items (
    order_item_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    date_code INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    total_price DECIMAL(10, 2),
    dt_update BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT * 1000,
    FOREIGN KEY (customer_id) REFERENCES public.brz_customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES public.brz_products(product_id),
    FOREIGN KEY (date_code) REFERENCES public.brz_calendar(date_code)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_brz_brands_category ON public.brz_brands(category_code);
CREATE INDEX IF NOT EXISTS idx_brz_products_brand ON public.brz_products(brand_code);
CREATE INDEX IF NOT EXISTS idx_brz_products_category ON public.brz_products(category_code);
CREATE INDEX IF NOT EXISTS idx_brz_order_items_customer ON public.brz_order_items(customer_id);
CREATE INDEX IF NOT EXISTS idx_brz_order_items_product ON public.brz_order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_brz_order_items_date ON public.brz_order_items(date_code);
CREATE INDEX IF NOT EXISTS idx_brz_order_items_order ON public.brz_order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_brz_customers_country ON public.brz_customers(country);
CREATE INDEX IF NOT EXISTS idx_brz_calendar_date ON public.brz_calendar(full_date);

-- Create indexes on dt_update for Kafka Connect timestamp mode
CREATE INDEX IF NOT EXISTS idx_brz_category_dt_update ON public.brz_category(dt_update);
CREATE INDEX IF NOT EXISTS idx_brz_brands_dt_update ON public.brz_brands(dt_update);
CREATE INDEX IF NOT EXISTS idx_brz_customers_dt_update ON public.brz_customers(dt_update);
CREATE INDEX IF NOT EXISTS idx_brz_calendar_dt_update ON public.brz_calendar(dt_update);
CREATE INDEX IF NOT EXISTS idx_brz_products_dt_update ON public.brz_products(dt_update);
CREATE INDEX IF NOT EXISTS idx_brz_order_items_dt_update ON public.brz_order_items(dt_update);

-- Grant permissions (optional, adjust as needed)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;