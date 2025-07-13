import streamlit as st
import pandas as pd
from google.cloud import bigquery, storage
from io import StringIO
import datetime
import os
import traceback

# Configuration with environment variable fallbacks
PROJECT_ID = os.getenv("PROJECT_ID", "call-data-461809")
DATASET_ID = os.getenv("DATASET_ID", "venkat_data")
BUCKET_NAME = os.getenv("BUCKET_NAME", "your-bucket-name")
STAGING_TABLE = os.getenv("STAGING_TABLE", "product_reveiws")
FACT_TABLE = os.getenv("FACT_TABLE", "fact_reviews")
MAX_ROWS = int(os.getenv("MAX_ROWS", "10000"))  # Safety limit

# Initialize clients with error handling
try:
    bq = bigquery.Client(project=PROJECT_ID)
    gcs = storage.Client()
except Exception as e:
    st.error(f"❌ Failed to initialize GCP clients: {str(e)}")
    st.stop()

def validate_gcs_bucket():
    """Check if bucket exists and is accessible"""
    try:
        gcs.get_bucket(BUCKET_NAME)
        return True
    except Exception as e:
        st.error(f"GCS bucket error: {str(e)}")
        return False

def process_file(uploaded_file):
    with st.status("Processing...", expanded=True) as status:
        try:
            # 1. Validate bucket first
            if not validate_gcs_bucket():
                return
            
            # 2. Read CSV with size limits
            st.write("🔍 Reading file")
            df = pd.read_csv(uploaded_file)
            
            # Clean data and enforce size limits
            df = df.where(pd.notnull(df), None)
            if len(df) > MAX_ROWS:
                st.warning(f"Large file - processing first {MAX_ROWS} rows")
                df = df.head(MAX_ROWS)
            
            # 3. Validate columns
            required_cols = {'Date and Time', 'Rating', 'Location ID'}
            if not required_cols.issubset(df.columns):
                missing = required_cols - set(df.columns)
                raise ValueError(f"Missing required columns: {missing}")
            
            # 4. Upload to GCS as JSONL
            st.write("📤 Uploading to Cloud Storage")
            jsonl = df.to_json(orient='records', lines=True)
            timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            blob_name = f"uploads/{timestamp}_{uploaded_file.name.replace('.csv','.jsonl')}"
            
            blob = gcs.bucket(BUCKET_NAME).blob(blob_name)
            blob.upload_from_string(jsonl, content_type='application/jsonl')
            
            # 5. Load to BigQuery
            st.write("💾 Loading to staging table")
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition="WRITE_APPEND",
                autodetect=True,
                max_bad_records=5  # Allow some bad records
            )
            
            job = bq.load_table_from_uri(
                f"gs://{BUCKET_NAME}/{blob_name}",
                f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}",
                job_config=job_config
            )
            job.result()
            st.success(f"✅ Added {job.output_rows} rows to staging")
            
            # 6. Update fact table incrementally
            st.write("⚡ Updating fact table")
            update_query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.{FACT_TABLE}`
            SELECT
              GENERATE_UUID() AS review_id,
              `Location ID` AS location_id,
              EXTRACT(DATE FROM `Date and Time`) AS review_date,
              Rating AS rating,
              LENGTH(Review) AS review_length,
              LENGTH(Reply) AS reply_length,
              CASE WHEN Review IS NULL THEN 0 ELSE 1 END AS has_review,
              CASE WHEN Reply IS NULL THEN 0 ELSE 1 END AS has_reply,
              Review AS review_text,
              Reply AS reply_text,
              Name AS customer_name,
              `Date and Time` AS review_timestamp,
              CURRENT_TIMESTAMP() AS etl_load_time
            FROM `{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}`
            WHERE `Date and Time` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
            AND NOT EXISTS (
              SELECT 1 FROM `{PROJECT_ID}.{DATASET_ID}.{FACT_TABLE}` f
              WHERE f.review_timestamp = `Date and Time`
              AND f.customer_name = Name
            )
            """
            
            query_job = bq.query(update_query)
            query_job.result()
            st.success(f"🆙 Fact table updated ({query_job.num_dml_affected_rows} new rows)")
            
        except pd.errors.EmptyDataError:
            st.error("Empty CSV file - please check your file")
        except pd.errors.ParserError:
            st.error("Invalid CSV format - please check column formatting")
        except Exception as e:
            st.error(f"❌ Processing failed: {str(e)}")
            st.code(traceback.format_exc())  # Detailed error for debugging
        finally:
            status.update(label="Process complete", state="complete")

# Streamlit UI
def main():
    st.set_page_config(
        layout="wide",
        page_title="Data Pipeline Portal",
        page_icon="📊"
    )
    
    st.title("📥 Data Upload Portal")
    
    with st.expander("📋 Expected CSV Format & Sample", expanded=True):
        st.markdown("""
        **Required columns:**
        - `Date and Time` (format: `YYYY-MM-DD HH:MM:SS`)
        - `Rating` (1-5)
        - `Location ID`
        
        **Optional columns:**
        - Review, Reply, Region, Name, State, City, Locality, Location Name
        """)
        
        st.download_button(
            "Download Sample CSV",
            data="""Date and Time,Review,Reply,Rating,Region,Name,Location ID,State,City,Locality,Location Name
2024-05-01 12:00:00,"Good product","Thanks",5,South,Test User,LOC-001,Karnataka,Bangalore,HSR,Duroflex""",
            file_name="sample_reviews.csv",
            mime="text/csv"
        )
    
    uploaded_file = st.file_uploader(
        "Choose CSV file", 
        type=["csv"],
        help="Max 10,000 rows will be processed"
    )
    
    if uploaded_file:
        if st.button("Process Data", type="primary"):
            process_file(uploaded_file)
            
            # Show previews in expandable sections
            with st.expander("📈 Latest Staging Data", expanded=False):
                st.dataframe(bq.query(f"""
                    SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}`
                    ORDER BY `Date and Time` DESC 
                    LIMIT 5
                """).to_dataframe())
            
            with st.expander("📊 Latest Fact Data", expanded=False):
                st.dataframe(bq.query(f"""
                    SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{FACT_TABLE}`
                    ORDER BY review_timestamp DESC
                    LIMIT 5
                """).to_dataframe())

if __name__ == '__main__':
    main()