import streamlit as st
import pandas as pd
from google.cloud import bigquery, storage
from io import StringIO, BytesIO
import datetime
import os
import traceback
import logging
from flask import Flask, request, jsonify

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.getenv("PROJECT_ID", "call-data-461809")
DATASET_ID = os.getenv("DATASET_ID", "venkat_data")
BUCKET_NAME = os.getenv("BUCKET_NAME", "json_bucket4938")
STAGING_TABLE = os.getenv("STAGING_TABLE", "product_reveiws")
FACT_TABLE = os.getenv("FACT_TABLE", "fact_reviews")
MAX_ROWS = int(os.getenv("MAX_ROWS", "10000"))

# Initialize clients
try:
    bq = bigquery.Client(project=PROJECT_ID)
    gcs = storage.Client()
except Exception as e:
    logger.error(f"GCP client init failed: {str(e)}")
    raise

# Debug Flask app
app = Flask(__name__)

@app.route('/_health')
def health_check():
    return jsonify({"status": "healthy", "timestamp": datetime.datetime.utcnow().isoformat()})

@app.route('/_debug/upload', methods=['POST'])
def debug_upload():
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file uploaded"}), 400
        
        file = request.files['file']
        content = file.read()
        
        # Validate file
        try:
            pd.read_csv(BytesIO(content)).head(1)
            return jsonify({
                "filename": file.filename,
                "size_bytes": len(content),
                "first_line": content.decode('utf-8').split('\n')[0]
            })
        except Exception as e:
            return jsonify({"error": f"Invalid CSV: {str(e)}"}), 400
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def process_file(uploaded_file):
    try:
        logger.info(f"Processing file: {uploaded_file.name}")
        
        # 1. Read and validate CSV
        df = pd.read_csv(uploaded_file, encoding='utf-8', on_bad_lines='warn')
        
        # Validate required columns
        required_cols = {'Date and Time', 'Rating', 'Location ID'}
        if not required_cols.issubset(df.columns):
            missing = required_cols - set(df.columns)
            raise ValueError(f"Missing required columns: {missing}")

        # 2. Upload to GCS
        blob_name = f"uploads/{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}_{uploaded_file.name}"
        blob = gcs.bucket(BUCKET_NAME).blob(blob_name)
        
        # Convert to JSONL
        jsonl = df.to_json(orient='records', lines=True)
        blob.upload_from_string(jsonl, content_type='application/jsonl')
        logger.info(f"Uploaded to GCS: gs://{BUCKET_NAME}/{blob_name}")
        
        # 3. Load to BigQuery staging
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_APPEND",
            autodetect=True
        )
        
        load_job = bq.load_table_from_uri(
            f"gs://{BUCKET_NAME}/{blob_name}",
            f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}",
            job_config=job_config
        )
        load_job.result()
        logger.info(f"Loaded {load_job.output_rows} rows to staging")
        
        # 4. Update fact table
        merge_query = f"""
        MERGE `{PROJECT_ID}.{DATASET_ID}.{FACT_TABLE}` T
        USING (
          SELECT
            PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', `Date and Time`) as review_timestamp,
            `Location ID` as location_id,
            GENERATE_UUID() AS review_id,
            `Location ID` AS location_id,
            EXTRACT(DATE FROM PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', `Date and Time`)) AS review_date,
            Rating AS rating,
            LENGTH(COALESCE(Review, '')) AS review_length,
            LENGTH(COALESCE(Reply, '')) AS reply_length,
            CASE WHEN Review IS NULL THEN 0 ELSE 1 END AS has_review,
            CASE WHEN Reply IS NULL THEN 0 ELSE 1 END AS has_reply,
            Review AS review_text,
            Reply AS reply_text,
            Name AS customer_name,
            CURRENT_TIMESTAMP() AS etl_load_time
          FROM `{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}`
          WHERE `Date and Time` IS NOT NULL
        ) S
        ON T.review_timestamp = S.review_timestamp
        AND T.location_id = S.location_id
        WHEN NOT MATCHED THEN
          INSERT (review_id, location_id, review_date, rating, review_length, 
                  reply_length, has_review, has_reply, review_text, 
                  reply_text, customer_name, review_timestamp, etl_load_time)
          VALUES (review_id, location_id, review_date, rating, review_length,
                  reply_length, has_review, has_reply, review_text,
                  reply_text, customer_name, review_timestamp, etl_load_time)
        """
        
        query_job = bq.query(merge_query)
        query_job.result()
        logger.info(f"Merged {query_job.num_dml_affected_rows} rows to fact table")
        
        return True
        
    except Exception as e:
        logger.error(f"Processing failed: {traceback.format_exc()}")
        st.error(f"❌ Processing failed: {str(e)}")
        st.code(traceback.format_exc())
        return False
def main():
    st.set_page_config(layout="wide", page_title="Data Upload Portal")
    st.title("📥 Data Upload Portal")
    
    # Debug panel
    with st.expander("🛠️ Debug Console", expanded=False):
        if st.button("Check Service Health"):
            try:
                health = requests.get(f"{os.getenv('K_SERVICE_URL', '')}/_health").json()
                st.json(health)
            except Exception as e:
                st.error(f"Health check failed: {str(e)}")
    
    uploaded_file = st.file_uploader("Choose CSV", type=["csv"])
    
    if uploaded_file:
        st.info(f"File received: {uploaded_file.name} ({uploaded_file.size} bytes)")
        
        if st.button("Process Data", type="primary"):
            try:
                with st.spinner("Processing..."):
                    if process_file(uploaded_file):
                        st.success("File processed successfully!")
            except Exception as e:
                st.error(f"Error: {str(e)}")
                st.code(traceback.format_exc())

if __name__ == "__main__":
    # Run both Flask (debug) and Streamlit
    import threading
    import requests
    
    flask_thread = threading.Thread(
        target=app.run,
        kwargs={'host': '0.0.0.0', 'port': 8081, 'debug': False}
    )
    flask_thread.daemon = True
    flask_thread.start()
    
    main()
