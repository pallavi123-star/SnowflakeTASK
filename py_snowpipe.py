import os
import sys

import logging
import json
import uuid
import snowflake.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile

from dotenv import load_dotenv
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile
from cryptography.hazmat.primitives import serialization

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.DEBUG)  # Change logging level to DEBUG for detailed logs

def connect_snow():
    """Connect to Snowflake using a private key-based authentication."""
    print("\n[INFO] Connecting to Snowflake...")

    # Construct private key
    private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n)"
    print(f"[DEBUG] Private key loaded.")

    # Load private key in proper format
    p_key = serialization.load_pem_private_key(
        bytes(private_key, 'utf-8'),
        password=None
    )
   
    # Convert private key to bytes
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    print("[INFO] Successfully parsed private key.")
   
    # Establish connection to Snowflake
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pkb,
        role="INGEST",
        database="INGEST",
        schema="INGEST",
        warehouse="INGEST",
        session_parameters={'QUERY_TAG': 'py-snowpipe'},
    )
   
    print("[INFO] Connected to Snowflake.")
    return conn


def save_to_snowflake(snow, batch, temp_dir, ingest_manager):
    """Convert batch data to Parquet, stage it in Snowflake, and trigger Snowpipe ingestion."""
    print("\n[INFO] Inserting batch into Snowflake...")
    print(f"[DEBUG] Batch size: {len(batch)}")

    # Convert batch into a Pandas DataFrame
    pandas_df = pd.DataFrame(batch, columns=[
        "TXID", "RFID", "RESORT", "PURCHASE_TIME", "EXPIRATION_TIME",
        "DAYS", "NAME", "ADDRESS", "PHONE", "EMAIL", "EMERGENCY_CONTACT"
    ])
   
    print(f"[DEBUG] DataFrame created with shape: {pandas_df.shape}")

    # Convert DataFrame to Apache Arrow Table
    arrow_table = pa.Table.from_pandas(pandas_df)
    print(f"[DEBUG] Arrow Table created.")

    # Generate a unique file name
    file_name = f"{str(uuid.uuid1())}.parquet"
    out_path = f"{temp_dir.name}/{file_name}"  # Save file in temporary directory

    print(f"[INFO] Writing Parquet file: {out_path}")

    # Write Parquet file
    pq.write_table(arrow_table, out_path, use_dictionary=False, compression='SNAPPY')
    print(f"[INFO] Parquet file saved at {out_path}")

    # Upload the Parquet file to Snowflake table stage
    put_command = f"PUT 'file://{out_path}' @%LIFT_TICKETS_PY_SNOWPIPE"
    print(f"[INFO] Executing: {put_command}")

    snow.cursor().execute(put_command)
    print("[INFO] File successfully staged in Snowflake.")

    # Remove temporary file after staging
    os.unlink(out_path)
    print("[INFO] Temporary file deleted.")

    # Trigger Snowpipe ingestion
    print(f"[INFO] Triggering Snowpipe ingestion for file: {file_name}")
    resp = ingest_manager.ingest_files([StagedFile(file_name, None)])
    print(f"[INFO] Snowpipe Response Code: {resp['responseCode']}")


if __name__ == "__main__":
    print("[INFO] Starting Snowflake data ingestion process...")

    # Read arguments
    args = sys.argv[1:]
    batch_size = int(args[0])  # First argument should be batch size
    print(f"[INFO] Batch size set to: {batch_size}")

    # Connect to Snowflake
    snow = connect_snow()

    # Create temporary directory
    temp_dir = tempfile.TemporaryDirectory()
    print(f"[INFO] Temporary directory created at: {temp_dir.name}")

    # Load private key for Snowpipe
    private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n)"
    host = os.getenv("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com"
   
    # Initialize Snowpipe Ingest Manager
    ingest_manager = SimpleIngestManager(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        host=host,
        user=os.getenv("SNOWFLAKE_USER"),
        pipe='INGEST.INGEST.LIFT_TICKETS_PIPE',
        private_key=private_key
    )
    print("[INFO] Snowpipe Ingest Manager initialized.")

    # Read messages from standard input
    batch = []
    for message in sys.stdin:
        if message != '\n':
            record = json.loads(message)
            batch.append((
                record['txid'], record['rfid'], record["resort"], record["purchase_time"],
                record["expiration_time"], record['days'], record['name'], record['address'],
                record['phone'], record['email'], record['emergency_contact']
            ))
           
            if len(batch) == batch_size:
                print("[INFO] Batch size reached. Processing batch...")
                save_to_snowflake(snow, batch, temp_dir, ingest_manager)
                batch = []
        else:
            break

    # Process any remaining records in batch
    if len(batch) > 0:
        print("[INFO] Processing remaining records...")
        save_to_snowflake(snow, batch, temp_dir, ingest_manager)

    # Cleanup temporary directory
    temp_dir.cleanup()
    print("[INFO] Temporary directory cleaned up.")

    # Close Snowflake connection
    snow.close()
    print("[INFO] Snowflake connection closed.")

    print("[INFO] Ingestion process complete!")
