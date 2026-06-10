-- EDA

-- 1. Hitung total baris tiap tabel
SELECT COUNT(*) AS total_rows
FROM analytics.category_translation;
-- 71 rows

SELECT COUNT(*) AS total_rows
FROM analytics.closed_deals;
-- 842 rows

SELECT COUNT(*) AS total_rows
FROM analytics.customers;
-- 99,441 rows

SELECT COUNT(*) AS total_rows
FROM analytics.geolocation;
-- 1,000,163 rows

SELECT COUNT(*) AS total_rows
FROM analytics.mql;
-- 8,000 rows

SELECT COUNT(*) AS total_rows
FROM analytics.order_items;
-- 112,650 rows

SELECT COUNT(*) AS total_rows
FROM analytics.order_payments;
-- 103,886 rows

SELECT COUNT(*) AS total_rows
FROM analytics.order_reviews;
-- 99,224 rows

SELECT COUNT(*) AS total_rows
FROM analytics.orders;
-- 99,441 rows

SELECT COUNT(*) AS total_rows
FROM analytics.products;
-- 32,951 rows

SELECT COUNT(*) AS total_rows
FROM analytics.sellers;
-- 3,095 rows

-- 2 Hitung missing value tiap tabel
SELECT COUNT(*) AS total_columns
