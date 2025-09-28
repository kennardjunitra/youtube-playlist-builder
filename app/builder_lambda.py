# app/builder_lambda.py
import os, json, logging, boto3
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
import isodate

log = logging.getLogger()
log.setLevel(os.getenv("LOG_LEVEL", "INFO"))

s3 = boto3.client("s3")
secrets = boto3.client("secretsmanager")
cloudwatch = boto3.client("cloudwatch")

# ==== ENV ====
YOUTUBE_SECRET_NAME = os.environ["SECRET_NAME_COMBINED"]
CONFIG_S3_BUCKET    = os.environ["CONFIG_S3_BUCKET"]
CONFIG_S3_KEY       = os.environ["CONFIG_S3_KEY"]            # e.g., channels.json
REPORTS_BUCKET      = os.getenv("REPORTS_BUCKET")            # optional
APP_TZ              = os.getenv("APP_TZ", "Asia/Singapore")

# ==== HANDLER ====
def handler(event, context):
    """
    Supports:
      - SQS event ({Records: [{body: "...json..."}]})
      - Direct invocation (event is the task dict)
    """
    # support SQS or direct invoke
    task = json.loads(event["Records"][0]["body"]) if "Records" in event else event

    competition_id = task.get("competition_id")
    earliest_date  = task.get("earliest_date")  # ISO date or datetime
    test_mode      = bool(task.get("test_mode", False))
    region_code    = (task.get("region") or os.getenv("YOUTUBE_REGION_CODE") or "SG").upper()
    max_pages      = int(os.getenv("YOUTUBE_MAX_PAGES", "4"))
    cutoff_iso     = _compute_cutoff_iso(earliest_date)

    if not competition_id:
        raise ValueError("Missing 'competition_id' in event")
    if not earliest_date:
        raise ValueError("Missing 'earliest_date' in event")

    yt = youtube_client_from_secret()  # pulls creds from Secrets Manager

    if test_mode:
        try:
            probe = yt.channels().list(part="snippet", mine=True).execute()
            channel_title = (probe.get("items") or [{}])[0].get("snippet", {}).get("title", "Unknown Channel")
            _write_report_line(REPORTS_BUCKET, {
                "taskId": task.get("taskId", "manual"),
                "ok": True, "mode": "test", "channel": channel_title
            })
            return {"ok": True, "message": "YouTube API reachable", "channel": channel_title}
        except Exception as e:
            log.exception("YouTube API probe failed")
            _write_report_line(REPORTS_BUCKET, {
                "taskId": task.get("taskId", "manual"),
                "ok": False, "mode": "test", "error": str(e)
            })
            raise    

    # 1) Load channels.json
    channel_map = _load_channels_config()
    cfg = channel_map.get(competition_id)
    if not cfg:
        raise ValueError(f"Unknown competition_id '{competition_id}'")

    pl = task["playlist"]  # expects {title, description?, privacy?, tags?}
    playlist_id = _create_or_get_playlist(
        yt,
        title=pl["title"],
        description=pl.get("description", "Auto-generated playlist"),
        privacy=pl.get("privacy", "unlisted"),
        tags=pl.get("tags", []),
    )

    log.info("Ensured playlist '%s' -> %s", pl["title"], playlist_id)
    return {"ok": True, "playlist_id": playlist_id}  # return ignored for SQS

# ==== HELPERS ====
def youtube_client_from_secret(secret_name=YOUTUBE_SECRET_NAME):
    data = json.loads(secrets.get_secret_value(SecretId=secret_name)["SecretString"])
    creds = Credentials(
        token=None,  # don't pass an access token; library will fetch one
        refresh_token=data["REFRESH_TOKEN"],
        token_uri=data.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id=data["CLIENT_ID"],
        client_secret=data["CLIENT_SECRET"],
        scopes=["https://www.googleapis.com/auth/youtube"]
    )
    return build("youtube", "v3", credentials=creds, cache_discovery=False)

def _load_channels_config():
    """Load channels configuration from S3"""
    try:
        response = s3.get_object(Bucket=CONFIG_S3_BUCKET, Key=CONFIG_S3_KEY)
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        log.error(f"Failed to load channels config from S3: {e}")
        raise

def _create_or_get_playlist(yt, title, description="", privacy="unlisted", tags=None):
    """Create a new playlist or return existing one"""
    try:
        # Create new playlist
        playlist_body = {
            "snippet": {
                "title": title,
                "description": description,
            },
            "status": {"privacyStatus": privacy}
        }
        
        if tags:
            playlist_body["snippet"]["tags"] = tags
            
        playlist = yt.playlists().insert(
            part="snippet,status",
            body=playlist_body
        ).execute()
        
        return playlist["id"]
        
    except HttpError as e:
        log.error(f"Failed to create playlist: {e}")
        raise

def _compute_cutoff_iso(earliest_date):
    """Compute cutoff ISO datetime string"""
    try:
        # Parse input date
        if isinstance(earliest_date, str):
            if earliest_date.endswith('Z'):
                input_dt = datetime.fromisoformat(earliest_date.replace('Z', ''))
            else:
                input_dt = datetime.fromisoformat(earliest_date + 'T00:00:00')
        else:
            input_dt = earliest_date
            
        # Calculate days difference from today
        now_sg = datetime.now(ZoneInfo(APP_TZ))
        today_sg = now_sg.date()
        days_diff = (today_sg - input_dt.date()).days
        
        # Calculate cutoff datetime in UTC
        cutoff_datetime_utc = datetime.now(timezone.utc) - timedelta(days=days_diff) - timedelta(hours=24)
        return cutoff_datetime_utc.isoformat(timespec="seconds").replace("+00:00", "Z")
        
    except Exception as e:
        log.error(f"Failed to compute cutoff ISO: {e}")
        raise

def _write_report_line(bucket, record: dict):
    if not bucket:
        return
    # one-line NDJSON per object -> “append” friendly & atomic
    now = _now_tz()
    day_path = now.strftime("%Y/%m/%d")
    task_id = (record.get("taskId") or "manual").replace("/", "_")
    key = f"reports/logs/{day_path}/{int(now.timestamp())}-{task_id}.ndjson"
    body = (json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/x-ndjson")

def _now_tz():
    return datetime.now(ZoneInfo(APP_TZ))

def _now_epoch():
    return int(_now_tz().timestamp())
