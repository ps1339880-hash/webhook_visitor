from fastapi import FastAPI, Request, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from google.cloud import bigquery
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv
import json
import os
import secrets

# Load env vars
load_dotenv()

app = FastAPI()
security = HTTPBasic()

BASIC_USER = os.getenv("BASIC_AUTH_USER")
BASIC_PASS = os.getenv("BASIC_AUTH_PASS")

# BigQuery tables
EVERY_VISIT_TABLE = "city-of-swan-youth-centres.every_visit_data.every_visit"
ANNUAL_VISIT_TABLE = "city-of-swan-youth-centres.annual_visit_data.annual_visit"

QUESTIONNAIRE_TABLE_MAP = {
    "8895": EVERY_VISIT_TABLE,   # Every Visit
    "8208": ANNUAL_VISIT_TABLE   # Annual Visit
}

def get_bq_client():
    return bigquery.Client()

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

    raw_payload = json.dumps(data)
    received_at = datetime.utcnow().isoformat()

    responder_name = data.get("name")
    location = data.get("location_name")
    signed_in = data.get("signed_in")

    # Group questionnaire submissions
    submissions = defaultdict(dict)
    for key, value in data.items():
        if key.startswith("questionnaireSubmissions"):
            idx = key.split("[")[1].split("]")[0]
            field = key.split("[")[2].split("]")[0]
            submissions[idx][field] = value

    table_rows = defaultdict(list)

    for submission in submissions.values():
        questionnaire_id = submission.get("questionnaire_id")
        table_id = QUESTIONNAIRE_TABLE_MAP.get(questionnaire_id)

        if not table_id:
            continue  # unknown questionnaire → ignore safely

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

        # 🔹 Build row PER TABLE SCHEMA
        if table_id == EVERY_VISIT_TABLE:
            row = {
                **base_common,
                "purpose_of_visit": None
            }

        elif table_id == ANNUAL_VISIT_TABLE:
            row = {
                **base_common,
                "age": None
            }

        table_rows[table_id].append(row)

    if not table_rows:
        return {"status": "ok", "rows_inserted": 0}

    bq = get_bq_client()
    total_inserted = 0
    errors_all = []
    
    for table_id, rows in table_rows.items():
        errors = bq.insert_rows_json(table_id, rows)
        if errors:
            errors_all.append({table_id: errors})
        else:
            total_inserted += len(rows)

    if errors_all:
        raise HTTPException(status_code=500, detail=errors_all)

    return {
        "status": "ok",
        "rows_inserted": total_inserted,
        "tables_updated": list(table_rows.keys())
    }
