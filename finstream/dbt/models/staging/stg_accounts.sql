WITH source AS (
    SELECT * FROM {{ source('finstream', 'accounts') }}
)

SELECT
    account_id,
    user_id,
    full_name,
    email,
    account_type,
    ROUND(balance::NUMERIC, 2) AS balance,
    ROUND(COALESCE(credit_limit, 0)::NUMERIC, 2) AS credit_limit,
    currency,
    status,
    country,
    created_at,
    updated_at

FROM source
WHERE account_id IS NOT NULL