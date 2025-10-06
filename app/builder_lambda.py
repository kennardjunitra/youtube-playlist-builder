# app/builder_lambda.py
import os, json, logging, boto3, re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
import isodate
from pathlib import Path

log = logging.getLogger()
log.setLevel(os.getenv("LOG_LEVEL", "INFO"))

s3 = boto3.client("s3")
secrets = boto3.client("secretsmanager")
cloudwatch = boto3.client("cloudwatch")

# ==== ENV ====
YOUTUBE_SECRET_NAME = os.environ["SECRET_NAME_COMBINED"]
# S3-related environment variables (no longer needed - using local channels.json)
#CONFIG_S3_BUCKET    = os.environ["CONFIG_S3_BUCKET"]
#CONFIG_S3_KEY       = os.environ["CONFIG_S3_KEY"]            # e.g., channels.json
#REPORTS_BUCKET      = os.getenv("REPORTS_BUCKET")            # optional
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
    # normalize top-level task keys by trimming stray ASCII/Unicode spaces from keys (common in manual tests)
    if isinstance(task, dict):
        def _trim_key(k: str):
            if not isinstance(k, str):
                return k
            # remove leading/trailing whitespace including NBSP/figure/thin spaces and zero-width separators
            return re.sub(r'^[\s\u00A0\u2007\u202F\u200B\u200C\u200D]+|[\s\u00A0\u2007\u202F\u200B\u200C\u200D]+$', '', k)
        task = { _trim_key(k): v for k, v in task.items() }

    competition_id = task.get("competition_id")
    earliest_date  = task.get("earliest_date")  # ISO date or datetime
    # coerce test_mode from various forms (bool, string, numbers)
    _tm = task.get("test_mode", False)
    if isinstance(_tm, str):
        test_mode = _tm.strip().lower() in ("true", "1", "yes", "y")
    else:
        test_mode = bool(_tm)
    region_code    = (task.get("region") or os.getenv("YOUTUBE_REGION_CODE") or "SG").upper()
    max_pages      = int(os.getenv("YOUTUBE_MAX_PAGES", "4"))

    # Log invocation context and parsed inputs
    source = "SQS" if "Records" in event else "Direct"
    try:
        event_keys = list(event.keys()) if isinstance(event, dict) else type(event).__name__
    except Exception:
        event_keys = "<unavailable>"
    log.info("Invocation source=%s, event_keys=%s", source, event_keys)
    if source == "SQS":
        try:
            log.info("SQS records count=%d", len(event.get("Records", [])))
        except Exception:
            pass
    if isinstance(task, dict):
        try:
            log.info("Normalized task keys=%s", list(task.keys()))
        except Exception:
            pass
    if isinstance(task, dict):
        try:
            log.info("Task key reprs=%s", [repr(k) for k in task.keys()])
        except Exception:
            pass
    log.info(
        "Parsed task: competition_id=%r, earliest_date=%r, test_mode=%r, region=%r, max_pages=%r",
        competition_id, earliest_date, test_mode, region_code, max_pages,
    )

    if not competition_id:
        raise ValueError("Missing 'competition_id' in event")
    if not earliest_date:
        raise ValueError("Missing 'earliest_date' in event")

    cutoff_iso     = _compute_cutoff_iso(earliest_date)

    yt = youtube_client_from_secret()  # pulls creds from Secrets Manager

    if test_mode:
        try:
            probe = yt.channels().list(part="snippet", mine=True).execute()
            channel_title = (probe.get("items") or [{}])[0].get("snippet", {}).get("title", "Unknown Channel")
            #_write_report_line(REPORTS_BUCKET, {
            #    "taskId": task.get("taskId", "manual"),
            #    "ok": True, "mode": "test", "channel": channel_title
            #})
            return {"ok": True, "message": "YouTube API reachable", "channel": channel_title}
        except Exception as e:
            log.exception("YouTube API probe failed")
            #_write_report_line(REPORTS_BUCKET, {
            #    "taskId": task.get("taskId", "manual"),
            #    "ok": False, "mode": "test", "error": str(e)
            #})
            raise    

    # 1) Load channels.json
    channel_map = _load_channels_config()
    cfg = channel_map.get(competition_id)
    if not cfg:
        raise ValueError(f"Unknown competition_id '{competition_id}'")

    # Debug: Log task structure to understand what keys are available
    log.info("Task keys: %s", list(task.keys()) if isinstance(task, dict) else "Not a dict")
    
    # Generate playlist based on competition
    competition_name = competition_id.replace("_", " ").title()
    playlist_title = f"{competition_name} Highlights"
    playlist_id = _create_playlist(
        yt,
        title=playlist_title,
        description=f"Auto-generated {competition_name} highlights playlist",
        privacy="unlisted",
        tags=[competition_name.lower(), "highlights"],
    )

    log.info("Ensured playlist '%s' -> %s", playlist_title, playlist_id)
    
    # 2) Search for videos based on channel configuration
    video_ids = _search_videos(yt, cfg, cutoff_iso, region_code, max_pages)
    
    if not video_ids:
        log.warning("No matching videos found for competition '%s'", competition_id)
        return {"ok": True, "playlist_id": playlist_id, "videos_added": 0, "message": "No matching videos found"}
    
    # 3) Add videos to playlist
    videos_added = _add_videos_to_playlist(yt, playlist_id, video_ids)
    
    log.info("Added %d videos to playlist '%s'", videos_added, playlist_title)
    return {"ok": True, "playlist_id": playlist_id, "videos_added": videos_added}

# ==== HELPERS ====
def youtube_client_from_secret(secret_name=YOUTUBE_SECRET_NAME):
    data = json.loads(secrets.get_secret_value(SecretId=secret_name)["SecretString"])
    creds = Credentials(
        token=None,  # don't pass an access token; library will fetch one
        refresh_token=data["REFRESH_TOKEN"],
        token_uri=data.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id=data["CLIENT_ID"],
        client_secret=data["CLIENT_SECRET_KEY"],
        scopes=["https://www.googleapis.com/auth/youtube"]
    )
    return build("youtube", "v3", credentials=creds, cache_discovery=False)

def _load_channels_config():
    """Load channels configuration from local channels.json file"""
    try:
        # Get the path to the channels.json file relative to this script
        script_dir = Path(__file__).parent
        channels_file = script_dir / "channels.json"
        
        with open(channels_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        log.error(f"Failed to load channels config from local file: {e}")
        raise

def _create_playlist(yt, title, description="", privacy="unlisted", tags=None):
    """Create a new playlist"""
    try:
        log.info("Creating new playlist '%s'", title)
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
        
        log.info("Created new playlist '%s' with ID: %s", title, playlist["id"])
        return playlist["id"]
        
    except HttpError as e:
        log.error(f"Failed to create playlist: {e}")
        raise

def _search_videos(yt, cfg, cutoff_iso, region_code, max_pages):
    """Search for videos based on channel configuration"""
    channel_ids = cfg.get("channel_ids", [])
    search_filter = cfg.get("search_filter", "")
    min_duration_minutes = cfg.get("min_duration_minutes", 3)
    search_keywords = cfg.get("search_keywords", [])
    
    min_duration_seconds = min_duration_minutes * 60
    video_ids = []
    seen = set()
    
    # Build search queries
    queries = [search_filter] if search_filter else []
    if search_keywords:
        queries.extend(search_keywords)
    
    if not queries:
        log.warning("No search queries available for video search")
        return video_ids
    
    log.info("Searching for videos with queries: %s", queries)
    log.info("Channel IDs: %s, Min duration: %d minutes", channel_ids, min_duration_minutes)
    
    for channel_id in channel_ids:
        for q in queries:
            log.info("🔎 Searching channel=%s query=%r", channel_id, q)
            page_token = None
            pages = 0

            while True:
                try:
                    resp = yt.search().list(
                        part="id,snippet",
                        channelId=channel_id,
                        q=q,
                        type="video",
                        order="date",
                        maxResults=50,
                        publishedAfter=cutoff_iso,
                        regionCode=region_code,
                        relevanceLanguage="en",
                        pageToken=page_token,
                    ).execute()
                except HttpError as e:
                    log.error("YouTube search failed for channel %s, query %s: %s", channel_id, q, e)
                    break

                items = resp.get("items", [])
                cand_ids = [it["id"].get("videoId") for it in items if it.get("id") and it["id"].get("videoId")]

                if cand_ids:
                    try:
                        details = yt.videos().list(
                            part="contentDetails",
                            id=",".join(cand_ids),
                        ).execute()
                    except HttpError as e:
                        log.error("videos.list failed for details: %s", e)
                        details = {"items": []}

                    for v in details.get("items", []):
                        vid = v["id"]
                        dur_s = isodate.parse_duration(v["contentDetails"]["duration"]).total_seconds()
                        if dur_s > min_duration_seconds and vid not in seen:
                            seen.add(vid)
                            video_ids.append(vid)
                            log.debug("Added video %s (duration: %.1f seconds)", vid, dur_s)

                page_token = resp.get("nextPageToken")
                pages += 1
                if not page_token or pages >= max_pages:
                    break

    log.info("Found %d unique videos matching criteria", len(video_ids))
    return video_ids

def _add_videos_to_playlist(yt, playlist_id, video_ids):
    """Add videos to the specified playlist"""
    videos_added = 0
    
    for video_id in video_ids:
        try:
            yt.playlistItems().insert(
                part="snippet",
                body={
                    "snippet": {
                        "playlistId": playlist_id,
                        "resourceId": {
                            "kind": "youtube#video",
                            "videoId": video_id
                        }
                    }
                }
            ).execute()
            videos_added += 1
            log.debug("Added video %s to playlist %s", video_id, playlist_id)
        except HttpError as e:
            log.error("Failed to add video %s to playlist %s: %s", video_id, playlist_id, e)
    
    log.info("Successfully added %d videos to playlist %s", videos_added, playlist_id)
    return videos_added

def _compute_cutoff_iso(earliest_date):
    """Compute cutoff ISO datetime string"""
    try:
        log.debug("_compute_cutoff_iso input earliest_date=%r type=%s", earliest_date, type(earliest_date).__name__)
        # Normalize to a date object regardless of input type
        if earliest_date is None:
            raise ValueError("earliest_date is required")

        input_date = None

        # String input: try datetime first, then date-only
        if isinstance(earliest_date, str):
            s = earliest_date.strip()
            try:
                # Handle 'Z' (UTC) by converting to +00:00 for fromisoformat
                if s.endswith('Z'):
                    dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
                else:
                    # Allows both date-only and full ISO datetime strings
                    dt = datetime.fromisoformat(s) if 'T' in s else datetime.fromisoformat(s + 'T00:00:00')
                input_date = dt.date()
            except Exception:
                # Fallback: parse as date-only using isodate
                input_date = isodate.parse_date(s)

        # Datetime input
        elif isinstance(earliest_date, datetime):
            input_date = earliest_date.date()

        # date-like input (datetime.date)
        elif hasattr(earliest_date, "year") and hasattr(earliest_date, "month") and hasattr(earliest_date, "day"):
            input_date = earliest_date

        else:
            raise TypeError(f"Unsupported type for earliest_date: {type(earliest_date)}")

        # Calculate days difference from today
        now_sg = datetime.now(ZoneInfo(APP_TZ))
        today_sg = now_sg.date()
        days_diff = (today_sg - input_date).days
        
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
