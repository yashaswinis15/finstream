WITH source AS (
    SELECT * FROM {{ source('finstream', 'transactions') }}
),

cleaned AS (
    SELECT
        transaction_id,
        account_id,
        txn_type,
        ROUND(amount::NUMERIC, 2) AS amount,
        COALESCE(merchant, 'Unknown') AS merchant,
        LOWER(COALESCE(category, 'uncategorized')) AS category,
        LOWER(status) AS status,
        ROUND(risk_score::NUMERIC, 3) AS risk_score,
        COALESCE(device_type, 'unknown') AS device_type,
        COALESCE(location_city, 'Unknown') AS location_city,
        COALESCE(location_country, 'USA') AS location_country,

        is_international,

        -- 🔥 STANDARDIZED TIMESTAMP
        created_at AS txn_timestamp,

        -- Derived fields (used everywhere downstream)
        DATE(created_at) AS txn_date,
        EXTRACT(HOUR FROM created_at)::INT AS txn_hour,
        EXTRACT(DOW FROM created_at)::INT AS day_of_week

    FROM source
    WHERE
        transaction_id IS NOT NULL
        AND account_id IS NOT NULL
        AND amount > 0
        AND amount < 1000000
)

SELECT
    transaction_id,
    account_id,
    txn_type,
    amount,
    merchant,
    category,
    status,
    risk_score,
    device_type,
    location_city,
    location_country,
    is_international,
    txn_timestamp,
    txn_date,
    txn_hour,
    day_of_week
FROM cleaned