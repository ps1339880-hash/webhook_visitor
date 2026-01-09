from fastapi import FastAPI, Request, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from google.cloud import bigquery
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv
import json
import os
import secrets

# Load environment variables from .env file
load_dotenv()

app = FastAPI()
security = HTTPBasic()

BASIC_USER = os.getenv("BASIC_AUTH_USER")
BASIC_PASS = os.getenv("BASIC_AUTH_PASS")

def get_bq_client():
    """Get BigQuery client with proper credentials"""
    return bigquery.Client()

# Map questionnaire IDs to BigQuery tables
QUESTIONNAIRE_TABLE_MAP = {
    "8208": os.getenv("TABLE_ANNUAL_VISIT", "project.dataset.annual_visit"),
    "8895": os.getenv("TABLE_REASON_FOR_VISIT", "project.dataset.reason_for_visit")
}

# Default fallback table
DEFAULT_TABLE = os.getenv("DEFAULT_TABLE", "project.dataset.visitor_responses")

def verify_basic_auth(credentials: HTTPBasicCredentials = Depends(security)):
    if not (
        secrets.compare_digest(credentials.username, BASIC_USER)
        and secrets.compare_digest(credentials.password, BASIC_PASS)
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            headers={"WWW-Authenticate": "Basic"},
        )

@app.post("/webhook/visitor")
async def visitor_webhook(
    request: Request,
    _: None = Depends(verify_basic_auth)
):
    form = await request.form()
    data = dict(form)

    # Save full raw payload
    raw_payload = json.dumps(data)

    # Base fields
    responder_name = data.get("name")
    location = data.get("location_name")
    signed_in = data.get("signed_in")
    received_at = datetime.utcnow().isoformat()

    # Group questionnaire submissions
    submissions = defaultdict(dict)
    for key, value in data.items():
        if key.startswith("questionnaireSubmissions"):
            idx = key.split("[")[1].split("]")[0]
            field = key.split("[")[2].split("]")[0]
            submissions[idx][field] = value

    # Group rows by questionnaire ID to insert into correct tables
    table_rows = defaultdict(list)

    for submission in submissions.values():
        questionnaire_id = submission.get("questionnaire_id")
        
        # Determine target table based on questionnaire ID
        table_id = QUESTIONNAIRE_TABLE_MAP.get(questionnaire_id, DEFAULT_TABLE)
        
        row = {
            "responder_name": submission.get("guest_name") or responder_name,
            "submitted": submission.get("created") or signed_in,
            "location": location,
            "questionnaire_name": submission.get("questionnaire_name"),
            "questionnaire_id": questionnaire_id,
            "reason_for_visit": None,      # fill later if answers exist
            "young_person": None,          # fill later if answers exist
            "purpose_of_visit": None,      # fill later if answers exist
            "raw_payload": raw_payload,
            "received_at": received_at
        }
        
        table_rows[table_id].append(row)

    # Insert rows into respective tables
    total_inserted = 0
    all_errors = []

    bq = get_bq_client()  # Initialize BigQuery client when needed
    
    for table_id, rows in table_rows.items():
        errors = bq.insert_rows_json(table_id, rows)
        if errors:
            all_errors.append({"table": table_id, "errors": errors})
        else:
            total_inserted += len(rows)

    if all_errors:
        raise HTTPException(status_code=500, detail=all_errors)

    return {
        "status": "ok",
        "rows_inserted": total_inserted,
        "tables_updated": list(table_rows.keys())
    }