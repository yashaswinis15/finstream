WITH transactions AS (
    SELECT
        account_id,
        amount,
        risk_score,
        status,
        is_international,
        txn_hour,
        txn_timestamp
    FROM {{ ref('stg_transactions') }}
),

account_stats AS (
    SELECT
        account_id,
        COUNT(*) AS total_transactions,
        ROUND(AVG(risk_score)::NUMERIC, 3) AS avg_risk_score,
        ROUND(MAX(risk_score)::NUMERIC, 3) AS max_risk_score,
        ROUND(SUM(amount)::NUMERIC, 2) AS total_volume,
        ROUND(AVG(amount)::NUMERIC, 2) AS avg_txn_amount,

        COUNT(*) FILTER (WHERE status = 'flagged') AS flagged_count,
        COUNT(*) FILTER (WHERE status = 'failed') AS failed_count,
        COUNT(*) FILTER (WHERE is_international) AS international_count,
        COUNT(*) FILTER (WHERE risk_score >= 0.90) AS critical_risk_count,
        COUNT(*) FILTER (WHERE risk_score >= 0.70 AND risk_score < 0.90) AS high_risk_count,

        COUNT(*) FILTER (WHERE txn_hour < 6 OR txn_hour >= 23) AS after_hours_count,

        MAX(txn_timestamp) AS last_transaction_at,
        MIN(txn_timestamp) AS first_transaction_at
    FROM transactions
    GROUP BY account_id
),

risk_classified AS (
    SELECT
        *,
        CASE
            WHEN avg_risk_score >= 0.70 THEN 'high_risk'
            WHEN avg_risk_score >= 0.40 THEN 'medium_risk'
            ELSE 'low_risk'
        END AS account_risk_tier,

        ROUND(flagged_count::NUMERIC / NULLIF(total_transactions, 0) * 100, 2)
            AS fraud_velocity_pct,

        ROUND(international_count::NUMERIC / NULLIF(total_transactions, 0) * 100, 2)
            AS international_pct
    FROM account_stats
)

SELECT
    r.*,
    a.full_name,
    a.email,
    a.account_type,
    a.status AS account_status,
    a.balance AS current_balance
FROM risk_classified r
JOIN {{ ref('stg_accounts') }} a
    ON r.account_id = a.account_id