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
        
        # Debug: Save raw file
        raw_content = uploaded_file.getvalue()
        with open("/tmp/last_upload.csv", "wb") as f:
            f.write(raw_content)
            
        # Read with error handling
        try:
            df = pd.read_csv(
                BytesIO(raw_content),
                encoding='utf-8',
                on_bad_lines='warn',
                dtype={'Rating': 'Int64'}
            )
        except Exception as e:
            logger.error(f"CSV read failed: {traceback.format_exc()}")
            raise ValueError(f"Invalid CSV format: {str(e)}")
        
        # Validate
        required_cols = {'Date and Time', 'Rating', 'Location ID'}
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns: {missing}")
        
        # Processing continues...
        logger.info(f"Successfully read {len(df)} rows")
        return True
        
    except Exception as e:
        logger.error(f"Processing failed: {traceback.format_exc()}")
        raise

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
