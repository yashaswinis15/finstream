-- ============================================================
-- FinStream — Fintech Database Schema (Improved)
-- ============================================================

-- ----------------------------
-- ACCOUNTS
-- ----------------------------
CREATE TABLE accounts (
    account_id      BIGSERIAL PRIMARY KEY,
    user_id         INTEGER NOT NULL,
    full_name       VARCHAR(100) NOT NULL,
    email           VARCHAR(100) UNIQUE NOT NULL,
    account_type    VARCHAR(20) NOT NULL CHECK (account_type IN ('checking','savings','credit','investment')),
    balance         NUMERIC(15,2) NOT NULL DEFAULT 0.00,
    credit_limit    NUMERIC(15,2),
    currency        VARCHAR(3) DEFAULT 'USD',
    status          VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active','frozen','closed','pending')),
    country         VARCHAR(50) DEFAULT 'USA',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ----------------------------
-- TRANSACTIONS
-- ----------------------------
CREATE TABLE transactions (
    transaction_id  BIGSERIAL PRIMARY KEY,
    account_id      INTEGER NOT NULL REFERENCES accounts(account_id),
    txn_type        VARCHAR(30) NOT NULL CHECK (txn_type IN ('deposit','withdrawal','payment','transfer','refund','fee')),
    amount          NUMERIC(15,2) NOT NULL CHECK (amount > 0),
    merchant        VARCHAR(100),
    merchant_category VARCHAR(50),
    category        VARCHAR(50),
    status          VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending','completed','failed','flagged','reversed')),
    risk_score      NUMERIC(4,3) NOT NULL DEFAULT 0.000 CHECK (risk_score >= 0 AND risk_score <= 1),
    ip_address      VARCHAR(45),
    device_type     VARCHAR(30),
    location_city   VARCHAR(100),
    location_country VARCHAR(50) DEFAULT 'USA',
    is_international BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ----------------------------
-- PAYMENTS
-- ----------------------------
CREATE TABLE payments (
    payment_id      BIGSERIAL PRIMARY KEY,
    from_account    INTEGER NOT NULL REFERENCES accounts(account_id),
    to_account      INTEGER NOT NULL REFERENCES accounts(account_id),
    amount          NUMERIC(15,2) NOT NULL CHECK (amount > 0),
    payment_method  VARCHAR(30) CHECK (payment_method IN ('ACH','wire','card','peer','crypto')),
    status          VARCHAR(20) DEFAULT 'initiated' CHECK (status IN ('initiated','processing','completed','failed','reversed')),
    fee             NUMERIC(10,2) DEFAULT 0.00,
    reference_id    VARCHAR(50) UNIQUE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    completed_at    TIMESTAMPTZ
);

-- ----------------------------
-- FRAUD FLAGS
-- ----------------------------
CREATE TABLE fraud_flags (
    flag_id         BIGSERIAL PRIMARY KEY,
    transaction_id  INTEGER REFERENCES transactions(transaction_id),
    account_id      INTEGER REFERENCES accounts(account_id),
    flag_reason     VARCHAR(200) NOT NULL,
    risk_score      NUMERIC(4,3),
    flagged_by      VARCHAR(50) DEFAULT 'system',
    reviewed        BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ----------------------------
-- INDEXES
-- ----------------------------
CREATE INDEX idx_transactions_account_id ON transactions(account_id);
CREATE INDEX idx_transactions_created_at ON transactions(created_at);
CREATE INDEX idx_transactions_risk_score ON transactions(risk_score);
CREATE INDEX idx_transactions_status ON transactions(status);

CREATE INDEX idx_payments_from_account ON payments(from_account);
CREATE INDEX idx_payments_to_account ON payments(to_account);
CREATE INDEX idx_payments_created_at ON payments(created_at);

CREATE INDEX idx_fraud_flags_account ON fraud_flags(account_id);
CREATE INDEX idx_fraud_flags_transaction ON fraud_flags(transaction_id);

-- ----------------------------
-- TRIGGER: auto-update updated_at
-- ----------------------------
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_accounts_updated
BEFORE UPDATE ON accounts
FOR EACH ROW
EXECUTE FUNCTION update_timestamp();

-- ----------------------------
-- CDC SETUP
-- ----------------------------

-- NOTE: In production, do NOT hardcode credentials.
-- Use environment variables or secret managers.

CREATE USER debezium_user WITH REPLICATION LOGIN;

GRANT CONNECT ON DATABASE fintechdb TO debezium_user;
GRANT USAGE ON SCHEMA public TO debezium_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium_user;

CREATE PUBLICATION finstream_pub FOR TABLE
    accounts,
    transactions,
    payments,
    fraud_flags;

-- ----------------------------
-- SEED DATA
-- ----------------------------
INSERT INTO accounts (user_id, full_name, email, account_type, balance, credit_limit, status, country)
SELECT
    gs,
    'User_' || gs,
    'user' || gs || '@finstream.io',
    (ARRAY['checking','savings','credit','investment'])[floor(random()*4+1)],
    round((random() * 50000)::numeric, 2),
    CASE WHEN random() > 0.6 THEN round((random() * 20000 + 5000)::numeric, 2) ELSE NULL END,
    CASE WHEN random() > 0.05 THEN 'active' ELSE 'frozen' END,
    'USA'
FROM generate_series(1, 200) AS gs;

-- ----------------------------
-- VERIFY
-- ----------------------------
SELECT 'Schema initialized: ' || count(*) || ' accounts seeded' AS status FROM accounts;