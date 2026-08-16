-- Script to test the database

-- Create tables for Tesouro Direto data
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
(13.25, 13.30, 640.2, 639.8, 638.5, 1420156800000, 1298937600000, 'IPCA', 1734381830666);

-- Insert sample data for PRE table
INSERT INTO public.dadostesouropre (CompraManha, VendaManha, PUCompraManha, PUVendaManha, PUBaseManha, Data_Vencimento, Data_Base, Tipo, dt_update) VALUES
(12.73, 12.79, 631.4, 630.11, 629.81, 1420070400000, 1298851200000, 'PRE-FIXADOS', 1734381830665),
(13.25, 13.30, 640.2, 639.8, 638.5, 1420156800000, 1298937600000, 'PRE-FIXADOS', 1734381830666);