import psycopg2
import requests
import time
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

required_vars = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
missing = [var for var in required_vars if not os.getenv(var)]
if missing:
    raise ValueError(f"Missing environment variables: {missing}")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", 30))
RISK_THRESHOLD = float(os.getenv("RISK_THRESHOLD", 0.85))
LARGE_TXN_THRESHOLD = float(os.getenv("LARGE_TXN_THRESHOLD", 5000))

# Track last processed timestamp (basic state)
last_seen_time = datetime.utcnow()


def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )


def fetch_alerts(since_time):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    t.transaction_id,
                    t.account_id,
                    a.full_name,
                    t.txn_type,
                    t.amount,
                    t.merchant,
                    t.status,
                    t.risk_score,
                    t.is_international,
                    t.location_city,
                    t.location_country,
                    t.device_type,
                    t.created_at
                FROM transactions t
                JOIN accounts a ON t.account_id = a.account_id
                WHERE
                    t.created_at > %s
                    AND (
                        t.risk_score >= %s
                        OR (t.amount >= %s AND t.risk_score >= 0.60)
                        OR t.status = 'flagged'
                    )
                ORDER BY t.created_at ASC
                LIMIT 50
            """, (since_time, RISK_THRESHOLD, LARGE_TXN_THRESHOLD))

            return cur.fetchall()


def send_slack_alert(alert):
    if not SLACK_WEBHOOK_URL:
        print(f"[ALERT] TXN#{alert[0]} | risk={alert[7]:.3f}")
        return

    payload = format_slack_message(alert)

    for attempt in range(3):  # retry logic
        try:
            resp = requests.post(
                SLACK_WEBHOOK_URL,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=5
            )
            if resp.status_code == 200:
                return
        except requests.exceptions.RequestException:
            pass

        time.sleep(1)

    print("[ERROR] Failed to send Slack alert after retries")


def check_data_quality():
    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT COUNT(*) FROM transactions
                WHERE created_at > NOW() - INTERVAL '60 seconds'
            """)
            recent_count = cur.fetchone()[0]

            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE risk_score IS NULL)::FLOAT
                    / NULLIF(COUNT(*), 0) * 100
                FROM transactions
                WHERE created_at > NOW() - INTERVAL '5 minutes'
            """)
            null_rate = cur.fetchone()[0] or 0

    if recent_count == 0:
        print("[DQ ALERT] No transactions in last 60 seconds")

    if null_rate > 10:
        print(f"[DQ ALERT] risk_score null rate = {null_rate:.1f}%")


print("=" * 50)
print("FinStream Alert Engine Started")
print("=" * 50)

while True:
    print(f"\n[{datetime.utcnow().strftime('%H:%M:%S')}] Checking alerts...")

    try:
        alerts = fetch_alerts(last_seen_time)

        if alerts:
            print(f"Found {len(alerts)} alert(s)")

            for alert in alerts:
                send_slack_alert(alert)

            # update last seen time safely
            last_seen_time = max(a[12] for a in alerts)

        else:
            print("No new alerts")

        check_data_quality()

    except Exception as e:
        print(f"[ERROR] {e}")

    time.sleep(POLL_INTERVAL_SECONDS)