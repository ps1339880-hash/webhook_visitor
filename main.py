from fastapi import FastAPI, Request, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from google.cloud import bigquery
from collections import defaultdict
from datetime import datetime, timezone
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
EVERY_VISIT_TABLE = "city-of-swan-youth-centres.every_visit_data.every_visit_v2"
ANNUAL_VISIT_TABLE = "city-of-swan-youth-centres.annual_visit_data.annual_visit_v2"

# Questionnaire mapping
QUESTIONNAIRE_TABLE_MAP = {
    "8208": EVERY_VISIT_TABLE,   # Every Visit
    "8895": ANNUAL_VISIT_TABLE   # Annual Visit
}

# Answer question ID mappings for every_visit (8208)
EVERY_VISIT_QUESTION_MAP = {
    "49028": "reason_for_visit",
    "49029": "young_person",
    "49030": "purpose_of_visit",
}

# Answer question ID mappings for annual_visit (8895)
ANNUAL_VISIT_QUESTION_MAP = {
    "54373": "type_of_visitor",
    "54374": "age",
    "54379": "gender",
    "54380": "school",
    "54381": "suburb",
    "54382": "ethnicity_culture",
    "54384": "emergency_contact",
}

def get_bq_client():
    return bigquery.Client()

def verify_basic_auth(credentials: HTTPBasicCredentials = Depends(security)):
    """Verify HTTP Basic Authentication"""
    logger.info("=" * 50)
    logger.info("AUTH ATTEMPT")
    logger.info(f"Username received: '{credentials.username}'")
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


def parse_form_to_nested(data: dict) -> dict:
    """Parse flat form-encoded keys with bracket notation into a nested dict.

    Handles keys like:
        contractorName -> top-level
        submissions[0][questionnaireId] -> nested
        submissions[0][answers][0][questionId] -> deeply nested
    """
    import re
    result = {}

    for key, value in data.items():
        # Split key into parts: submissions[0][answers][1][questionId]
        # -> ['submissions', '0', 'answers', '1', 'questionId']
        parts = re.split(r'\[|\]\[|\]', key)
        parts = [p for p in parts if p != '']

        current = result
        for i, part in enumerate(parts[:-1]):
            next_part = parts[i + 1]
            # If next part is a digit, we need a list-like dict; use dict with int keys
            if part not in current:
                current[part] = {}
            current = current[part]

        current[parts[-1]] = value

    return result


def dict_to_list(d: dict) -> list:
    """Convert a dict with numeric string keys to a list of dicts."""
    if not isinstance(d, dict):
        return d
    # Check if all keys are numeric
    if all(k.isdigit() for k in d.keys()):
        sorted_keys = sorted(d.keys(), key=int)
        return [d[k] for k in sorted_keys]
    return d


def extract_submissions(nested: dict) -> list:
    """Extract submissions from the parsed nested form data."""
    raw_subs = nested.get("submissions", {})
    if not isinstance(raw_subs, dict):
        return []

    submissions = []
    for idx in sorted(raw_subs.keys(), key=lambda x: int(x) if x.isdigit() else x):
        sub = raw_subs[idx]
        if not isinstance(sub, dict):
            continue

        submission = {
            "questionnaireId": sub.get("questionnaireId", ""),
            "questionnaireName": sub.get("questionnaireName", ""),
            "answers": [],
        }

        raw_answers = sub.get("answers", {})
        if isinstance(raw_answers, dict):
            for aidx in sorted(raw_answers.keys(), key=lambda x: int(x) if x.isdigit() else x):
                ans = raw_answers[aidx]
                if isinstance(ans, dict):
                    submission["answers"].append({
                        "questionId": ans.get("questionId", ""),
                        "question": ans.get("question", ""),
                        "answer": ans.get("answer", ""),
                    })

        submissions.append(submission)

    return submissions


def extract_answers(answers: list, question_map: dict) -> dict:
    """Extract answer values from the answers list based on question ID mapping."""
    result = {field: None for field in question_map.values()}
    for answer in answers:
        question_id = str(answer.get("questionId", ""))
        if question_id in question_map:
            value = answer.get("answer")
            if value == "":
                value = None
            result[question_map[question_id]] = value
    return result


@app.post("/webhook/visitor")
async def visitor_webhook(
    request: Request,
    credentials: HTTPBasicCredentials = Depends(verify_basic_auth)
):
    """Handle visitor webhook - accepts form-encoded payload"""
    logger.info("=" * 80)
    logger.info("WEBHOOK REQUEST")
    logger.info("=" * 80)

    # Log request info
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
    received_at = datetime.now(timezone.utc).isoformat()

    # Parse flat form keys into nested structure
    nested = parse_form_to_nested(data)
    logger.info(f"Decoded payload: {json.dumps(nested, indent=2, default=str)}")

    # Extract top-level fields
    contractor_name = nested.get("contractorName")
    organisation = nested.get("organisation")
    visitor_mobile = nested.get("visitorMobile") or None
    location = nested.get("location")
    sign_in = nested.get("signIn")

    # Extract submissions
    submissions = extract_submissions(nested)
    logger.info(f"Found {len(submissions)} submissions")

    # Process each submission
    table_rows = defaultdict(list)

    for submission in submissions:
        questionnaire_id = str(submission.get("questionnaireId", ""))
        table_id = QUESTIONNAIRE_TABLE_MAP.get(questionnaire_id)

        if not table_id:
            logger.warning(f"Unknown questionnaireId: {questionnaire_id}")
            continue

        questionnaire_name = submission.get("questionnaireName")
        answers = submission.get("answers", [])

        if table_id == EVERY_VISIT_TABLE:
            extracted = extract_answers(answers, EVERY_VISIT_QUESTION_MAP)
            row = {
                "responder_name": contractor_name,
                "submitted": sign_in,
                "location": location,
                "questionnaire_id": questionnaire_id,
                "questionnaire_name": questionnaire_name,
                "reason_for_visit": extracted.get("reason_for_visit"),
                "young_person": extracted.get("young_person"),
                "purpose_of_visit": extracted.get("purpose_of_visit"),
                "organisation": organisation,
                "visitor_mobile": visitor_mobile,
                "received_at": received_at,
                "raw_payload": raw_payload,
            }

        elif table_id == ANNUAL_VISIT_TABLE:
            extracted = extract_answers(answers, ANNUAL_VISIT_QUESTION_MAP)
            type_of_visitor = extracted.get("type_of_visitor")
            age_raw = extracted.get("age")
            age = None
            if age_raw is not None:
                try:
                    age = int(age_raw)
                except (ValueError, TypeError):
                    logger.warning(f"Could not cast age to int: {age_raw}")

            row = {
                "responder_name": contractor_name,
                "submitted": sign_in,
                "location": location,
                "questionnaire_id": questionnaire_id,
                "questionnaire_name": questionnaire_name,
                "type_of_visitor": type_of_visitor,
                "young_person": type_of_visitor,
                "age": age,
                "gender": extracted.get("gender"),
                "school": extracted.get("school"),
                "suburb": extracted.get("suburb"),
                "ethnicity_culture": extracted.get("ethnicity_culture"),
                "emergency_contact": extracted.get("emergency_contact"),
                "organisation": organisation,
                "visitor_mobile": visitor_mobile,
                "received_at": received_at,
                "raw_payload": raw_payload,
            }
        else:
            continue

        logger.info(f"Built row for table {table_id}: {json.dumps(row, default=str)}")
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
                logger.error(f"BigQuery errors for {table_id}: {errors}")
                errors_all.append({table_id: errors})
            else:
                total_inserted += len(rows)
                logger.info(f"Inserted {len(rows)} rows into {table_id}")

        except Exception as e:
            logger.error(f"Insert exception for {table_id}: {e}")
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
