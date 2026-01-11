-- Modelagem Dimensional no BigQuery

-- Objetivo:
-- * Criar tabelas fato e dimensão
-- * Usar dados da camada ecommerce_trusted
-- * Otimizar para análise e dashboard
CREATE SCHEMA IF NOT EXISTS ecommerce_analytics;

-- Dimensão Produtos
CREATE OR REPLACE TABLE ecommerce_analytics.dim_products AS 
SELECT
    product_id,
    title AS product_name,
    category,
    price
FROM ecommerce_trusted.products;

-- Dimensão Usuários
CREATE OR REPLACE TABLE ecommerce_analytics.dim_users AS
SELECT
    user_id,
    full_name,
    email,
    username
FROM ecommerce_trusted.users;

-- Dimensão Datas
CREATE OR REPLACE TABLE ecommerce_analytics.dim_date AS -- Cria ou substitui a tabela dim_date
SELECT -- Seleciona os campos que irão compor a dimensão de datas
    DISTINCT -- Garante que cada data apareça apenas uma vez
    order_date AS date, -- Data do pedido, usada como chave principal da dimensão
    EXTRACT(YEAR FROM order_date) AS year, -- Extrai o ano da data do pedido
    EXTRACT(MONTH FROM order_date) AS month, -- Extrai o mês da data do pedido
    EXTRACT(DAY FROM order_date) AS day, -- Extrai o dia do mês da data do pedido
    EXTRACT(WEEK FROM order_date) AS week, -- Extrai o número da semana do ano da data do pedido
    FORMAT_DATE('%A', order_date) AS day_name -- Retorna o nome do dia da semana (ex: Monday, Tuesday)
FROM ecommerce_trusted.orders_items; -- Fonte dos dados: tabela de itens de pedidos
