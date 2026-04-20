WITH transactions AS (
    SELECT
        account_id,
        txn_type,
        category,
        amount,
        status,
        risk_score,
        is_international,
        txn_date
    FROM {{ ref('stg_transactions') }}
),

accounts AS (
    SELECT
        account_id,
        account_type,
        status AS account_status
    FROM {{ ref('stg_accounts') }}
),

joined AS (
    SELECT
        t.*,
        a.account_type,
        a.account_status
    FROM transactions t
    LEFT JOIN accounts a
        ON t.account_id = a.account_id
),

daily_summary AS (
    SELECT
        txn_date,
        txn_type,
        category,
        account_type,

        COUNT(*) AS total_transactions,
        SUM(amount) AS total_volume,
        AVG(amount) AS avg_transaction_amount,
        MAX(amount) AS max_transaction_amount,

        COUNT(*) FILTER (WHERE status = 'completed') AS completed_count,
        COUNT(*) FILTER (WHERE status = 'failed') AS failed_count,
        COUNT(*) FILTER (WHERE status = 'flagged') AS flagged_count,
        COUNT(*) FILTER (WHERE is_international) AS international_count,

        ROUND(
            COUNT(*) FILTER (WHERE status = 'failed')::NUMERIC
            / NULLIF(COUNT(*), 0) * 100, 2
        ) AS failure_rate_pct,

        ROUND(
            COUNT(*) FILTER (WHERE status = 'flagged')::NUMERIC
            / NULLIF(COUNT(*), 0) * 100, 2
        ) AS fraud_rate_pct,

        ROUND(AVG(risk_score)::NUMERIC, 3) AS avg_risk_score,

        COUNT(*) FILTER (WHERE risk_score >= 0.90) AS critical_risk_count,
        COUNT(*) FILTER (WHERE risk_score >= 0.70 AND risk_score < 0.90) AS high_risk_count

    FROM joined
    GROUP BY txn_date, txn_type, category, account_type
)

SELECT * FROM daily_summary