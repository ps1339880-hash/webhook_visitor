from fastapi import FastAPI, Request, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from google.cloud import bigquery
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv
import json
import os
import secrets
import logging

# Load env vars
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI()
security = HTTPBasic()

BASIC_USER = os.getenv("BASIC_AUTH_USER")
BASIC_PASS = os.getenv("BASIC_AUTH_PASS")

# BigQuery configuration
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET_ID")
EVERY_VISIT_TABLE = f"{PROJECT_ID}.{DATASET_ID}.every_visit"
ANNUAL_VISIT_TABLE = f"{PROJECT_ID}.{DATASET_ID}.annual_visit"

# Questionnaire mapping
QUESTIONNAIRE_TABLE_MAP = {
    "every_visit_questionnaire_id": EVERY_VISIT_TABLE,
    "annual_visit_questionnaire_id": ANNUAL_VISIT_TABLE,
}

def get_bq_client():
    """Initialize BigQuery client"""
    return bigquery.Client(project=PROJECT_ID)

def verify_basic_auth(credentials: HTTPBasicCredentials = Depends(security)):
    """Verify HTTP Basic Authentication"""
    logger.info("=" * 50)
    logger.info("AUTH ATTEMPT")
    logger.info(f"credentials received: '{credentials}'")
    logger.info(f"Username received: '{credentials.username}'")
    logger.info(f"Expected username: '{BASIC_USER}'")
    logger.info(f"Password length: received={len(credentials.password)}, expected={len(BASIC_PASS) if BASIC_PASS else 0}")
    
    if not BASIC_USER or not BASIC_PASS:
        logger.error("CRITICAL: Auth credentials not set in environment!")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    username_match = secrets.compare_digest(credentials.username, BASIC_USER)
    password_match = secrets.compare_digest(credentials.password, BASIC_PASS)
    
    logger.info(f"Username match: {username_match}, Password match: {password_match}")
    
    if not (username_match and password_match):
        logger.error("AUTH FAILED")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            headers={"WWW-Authenticate": "Basic"},
        )
    
    logger.info("AUTH SUCCESS")
    logger.info("=" * 50)
    return credentials

@app.post("/webhook/visitor")
async def visitor_webhook(
    request: Request,
    credentials: HTTPBasicCredentials = Depends(verify_basic_auth)
):
    """Handle visitor webhook"""
    logger.info("=" * 80)
    logger.info("WEBHOOK REQUEST")
    logger.info("=" * 80)
    
    # Log request type and headers
    logger.info(f"Method: {request.method}")
    logger.info(f"Content-Type: {request.headers.get('content-type', 'Not set')}")
    logger.info(f"Client: {request.client.host if request.client else 'Unknown'}")
    
    # Read raw body
    body_bytes = await request.body()
    logger.info(f"Raw body length: {len(body_bytes)} bytes")
    logger.info(f"Raw body: {body_bytes.decode('utf-8', errors='replace')[:1000]}")
    
    # Parse form data
    try:
        form = await request.form()
        data = dict(form)
        logger.info(f"Form data ({len(data)} fields):")
        logger.info(json.dumps(data, indent=2, default=str))
    except Exception as e:
        logger.error(f"Form parsing error: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid form data: {str(e)}")

    # Store raw payload
    raw_payload = json.dumps(data)
    received_at = datetime.utcnow().isoformat()

    # Extract basic fields
    responder_name = data.get("name")
    location = data.get("location_name")
    signed_in = data.get("signed_in")

    # Group questionnaire submissions
    submissions = defaultdict(dict)
    for key, value in data.items():
        if key.startswith("questionnaireSubmissions"):
            try:
                idx = key.split("[")[1].split("]")[0]
                field = key.split("[")[2].split("]")[0]
                submissions[idx][field] = value
            except IndexError:
                pass
    
    logger.info(f"Found {len(submissions)} submissions")

    # Process submissions
    table_rows = defaultdict(list)

    for idx, submission in submissions.items():
        questionnaire_id = submission.get("questionnaire_id")
        table_id = QUESTIONNAIRE_TABLE_MAP.get(questionnaire_id)

        if not table_id:
            logger.warning(f"Unknown questionnaire_id: {questionnaire_id}")
            continue

        base_common = {
            "responder_name": submission.get("guest_name") or responder_name,
            "submitted": submission.get("created") or signed_in,
            "location": location,
            "questionnaire_name": submission.get("questionnaire_name"),
            "reason_for_visit": None,
            "young_person": None,
            "raw_payload": raw_payload,
            "received_at": received_at
        }

        if table_id == EVERY_VISIT_TABLE:
            row = {**base_common, "purpose_of_visit": None}
        elif table_id == ANNUAL_VISIT_TABLE:
            row = {**base_common, "age": None}
        else:
            continue

        table_rows[table_id].append(row)

    if not table_rows:
        logger.warning("No rows to insert")
        return {"status": "ok", "rows_inserted": 0}

    # Insert into BigQuery
    bq = get_bq_client()
    total_inserted = 0
    errors_all = []
    
    for table_id, rows in table_rows.items():
        logger.info(f"Inserting {len(rows)} rows into {table_id}")
        
        try:
            errors = bq.insert_rows_json(table_id, rows)
            
            if errors:
                logger.error(f"BigQuery errors: {errors}")
                errors_all.append({table_id: errors})
            else:
                total_inserted += len(rows)
                
        except Exception as e:
            logger.error(f"Insert exception: {e}")
            errors_all.append({table_id: str(e)})

    if errors_all:
        raise HTTPException(status_code=500, detail={"errors": errors_all})

    logger.info(f"SUCCESS - Inserted {total_inserted} rows")
    logger.info("=" * 80)
    
    return {
        "status": "ok",
        "rows_inserted": total_inserted,
        "tables_updated": list(table_rows.keys())
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy", "auth_configured": bool(BASIC_USER and BASIC_PASS)}

@app.get("/")
async def root():
    return {"message": "Visitor Webhook API", "status": "running"}