-- =============================================================================
-- sample_etl.sql  –  T-TDD Generator SQL Lineage Test File
-- Covers: CREATE VIEW, INSERT INTO SELECT, CTEs, JOINs, UNION, expressions
-- =============================================================================


-- ── 1. Simple CREATE VIEW with JOIN ─────────────────────────────────────────
CREATE OR REPLACE VIEW DWH.VW_CUSTOMER_ORDERS AS
SELECT
    c.CUSTOMER_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.EMAIL,
    o.ORDER_ID,
    o.ORDER_DATE,
    o.ORDER_STATUS,
    o.TOTAL_AMOUNT,
    p.PRODUCT_NAME,
    p.CATEGORY
FROM STG.CUSTOMERS        c
JOIN STG.ORDERS           o ON o.CUSTOMER_ID  = c.CUSTOMER_ID
JOIN STG.ORDER_ITEMS      oi ON oi.ORDER_ID   = o.ORDER_ID
JOIN STG.PRODUCTS         p  ON p.PRODUCT_ID  = oi.PRODUCT_ID
WHERE o.ORDER_STATUS <> 'CANCELLED';


-- ── 2. INSERT with CTE (Common Table Expression) ────────────────────────────
INSERT INTO DWH.FACT_SALES (
    SALE_ID,
    CUSTOMER_KEY,
    PRODUCT_KEY,
    DATE_KEY,
    QUANTITY,
    UNIT_PRICE,
    DISCOUNT_AMOUNT,
    NET_AMOUNT,
    LOAD_DATE
)
WITH cte_orders AS (
    SELECT
        o.ORDER_ID,
        o.CUSTOMER_ID,
        o.ORDER_DATE,
        oi.PRODUCT_ID,
        oi.QUANTITY,
        oi.UNIT_PRICE,
        oi.DISCOUNT
    FROM STG.ORDERS     o
    JOIN STG.ORDER_ITEMS oi ON oi.ORDER_ID = o.ORDER_ID
    WHERE o.ORDER_DATE >= '2024-01-01'
),
cte_dim_keys AS (
    SELECT
        co.ORDER_ID,
        dc.CUSTOMER_KEY,
        dp.PRODUCT_KEY,
        dd.DATE_KEY,
        co.QUANTITY,
        co.UNIT_PRICE,
        co.DISCOUNT
    FROM cte_orders        co
    JOIN DWH.DIM_CUSTOMER  dc ON dc.CUSTOMER_ID  = co.CUSTOMER_ID
    JOIN DWH.DIM_PRODUCT   dp ON dp.PRODUCT_ID   = co.PRODUCT_ID
    JOIN DWH.DIM_DATE      dd ON dd.CALENDAR_DATE = co.ORDER_DATE
)
SELECT
    dk.ORDER_ID                                     AS SALE_ID,
    dk.CUSTOMER_KEY,
    dk.PRODUCT_KEY,
    dk.DATE_KEY,
    dk.QUANTITY,
    dk.UNIT_PRICE,
    dk.DISCOUNT                                     AS DISCOUNT_AMOUNT,
    (dk.QUANTITY * dk.UNIT_PRICE) - dk.DISCOUNT     AS NET_AMOUNT,
    CURRENT_DATE()                                  AS LOAD_DATE
FROM cte_dim_keys dk;


-- ── 3. INSERT with expressions and hardcoded values ─────────────────────────
INSERT INTO DWH.DIM_CUSTOMER (
    CUSTOMER_KEY,
    CUSTOMER_ID,
    FULL_NAME,
    EMAIL,
    PHONE,
    COUNTRY,
    CUSTOMER_SEGMENT,
    IS_ACTIVE,
    CREATED_DATE,
    ETL_BATCH_ID
)
SELECT
    c.CUSTOMER_ID                                       AS CUSTOMER_KEY,
    c.CUSTOMER_ID,
    CONCAT(c.FIRST_NAME, ' ', c.LAST_NAME)             AS FULL_NAME,
    LOWER(c.EMAIL)                                      AS EMAIL,
    c.PHONE,
    COALESCE(c.COUNTRY, 'UNKNOWN')                     AS COUNTRY,
    CASE
        WHEN c.TOTAL_SPEND > 10000 THEN 'PLATINUM'
        WHEN c.TOTAL_SPEND > 1000  THEN 'GOLD'
        ELSE 'STANDARD'
    END                                                AS CUSTOMER_SEGMENT,
    1                                                  AS IS_ACTIVE,
    CURRENT_TIMESTAMP()                                AS CREATED_DATE,
    'BATCH_2024'                                       AS ETL_BATCH_ID
FROM STG.CUSTOMERS c
WHERE c.IS_DELETED = 0;


-- ── 4. UNION — combining two source tables ───────────────────────────────────
INSERT INTO DWH.FACT_WEB_EVENTS (
    EVENT_ID,
    USER_ID,
    EVENT_TYPE,
    PAGE_URL,
    EVENT_TIMESTAMP,
    SOURCE_SYSTEM
)
SELECT
    we.EVENT_ID,
    we.USER_ID,
    we.EVENT_TYPE,
    we.PAGE_URL,
    we.EVENT_TIMESTAMP,
    'WEB' AS SOURCE_SYSTEM
FROM STG.WEB_EVENTS we

UNION ALL

SELECT
    me.EVENT_ID,
    me.USER_ID,
    me.EVENT_TYPE,
    me.PAGE_NAME  AS PAGE_URL,
    me.EVENT_TIMESTAMP,
    'MOBILE' AS SOURCE_SYSTEM
FROM STG.MOBILE_EVENTS me;
