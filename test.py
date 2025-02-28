import requests
import snowflake.connector
import os, sys, logging
from dotenv import load_dotenv


load_dotenv()
from cryptography.hazmat.primitives import serialization

logging.basicConfig(level=logging.WARN)


def connect_snow():
    private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n)"
    p_key = serialization.load_pem_private_key(
        bytes(private_key, 'utf-8'),
        password=None
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pkb,
        role="INGEST",
        database="INGEST",
        schema="INGEST",
        warehouse="INGEST",
        session_parameters={'QUERY_TAG': 'py-snowpipe'}, 
    )

# GitHub CSV (RAW) URL
GITHUB_CSV_URL = "https://github.com/pallavi123-star/SnowflakeTASK/blob/main/category.csv"

# Download CSV
csv_path = "/tmp/data.csv"
with open(csv_path, "wb") as f:
    f.write(requests.get(GITHUB_CSV_URL).content)

# Upload CSV to Snowflake and trigger Snowpipe
conn = snowflake.connector.connect(user=SNOWFLAKE_USER, password=SNOWFLAKE_PASSWORD, account=SNOWFLAKE_ACCOUNT)
conn.cursor().execute(f"PUT 'file://{csv_path}' @{LIST_Category}")
conn.cursor().execute(f"ALTER PIPE {LIFT_CATEGORY_PIPE} REFRESH")
conn.close()

print("✅ CSV Loaded into Snowflake via Snowpipe!")
