WITH transactions AS (
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
        txn_hour
    FROM {{ ref('stg_transactions') }}
),

risk_classified AS (
    SELECT
        *,
        CASE
            WHEN risk_score >= 0.90 THEN 'critical'
            WHEN risk_score >= 0.70 THEN 'high'
            WHEN risk_score >= 0.40 THEN 'medium'
            ELSE 'low'
        END AS risk_tier,

        amount >= 5000 AS is_large_transaction,

        txn_hour < 6 OR txn_hour >= 23 AS is_after_hours
    FROM transactions
),

fraud_assessed AS (
    SELECT
        *,
        CASE
            WHEN status = 'flagged' THEN TRUE
            WHEN risk_score >= 0.90 THEN TRUE
            WHEN is_international AND amount > 3000 THEN TRUE
            WHEN is_large_transaction AND is_after_hours THEN TRUE
            WHEN risk_score >= 0.70 AND is_international THEN TRUE
            ELSE FALSE
        END AS is_fraud_suspect
    FROM risk_classified
)

SELECT * FROM fraud_assessed