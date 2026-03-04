import base64
import hashlib
import requests
import psycopg2
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

# =========================
# НАСТРОЙКИ В КОДЕ
# =========================
BASE = "https://terminalsua.creatio.com"

# Creatio credentials
CREATIO_LOGIN = "ayushchenko"
CREATIO_PASSWORD = "asdf1234%"

# Supabase Postgres credentials
SUPABASE_HOST = "aws-1-eu-west-1.pooler.supabase.com"
SUPABASE_PORT = 6543
SUPABASE_DBNAME = "postgres"
SUPABASE_USER = "calluser.ewhypwoqhsjplhcsiujb"
SUPABASE_PASSWORD = "daQOn2W3jB"

# Pagination size for Creatio SelectQuery
ROW_COUNT = 500


def make_session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False  # do not use env proxies

    r = s.post(
        f"{BASE}/ServiceModel/AuthService.svc/Login",
        json={"UserName": CREATIO_LOGIN, "UserPassword": CREATIO_PASSWORD},
        timeout=30
    )
    r.raise_for_status()

    # Ensure session cookies (BPMSESSIONID/BPMCSRF) are set
    s.get(f"{BASE}/0/Nui/ViewModule.aspx", timeout=30)
    return s


def db_connect():
    return psycopg2.connect(
        host=SUPABASE_HOST,
        port=SUPABASE_PORT,
        dbname=SUPABASE_DBNAME,
        user=SUPABASE_USER,
        password=SUPABASE_PASSWORD,
        sslmode="require",
    )


def normalize_value(v: Any) -> str:
    """Нормализация значений Creatio: и строк, и lookup-объектов {value, displayValue}."""
    if isinstance(v, dict):
        return str(v.get("displayValue") or v.get("value") or "")
    return str(v or "")


def creatio_select_calls_by_date(
    session: requests.Session,
    day: date,
    row_count: int = ROW_COUNT
) -> List[Dict[str, Any]]:
    url = f"{BASE}/0/DataService/json/SyncReply/SelectQuery"

    headers = {
        "accept": "*/*",
        "content-type": "application/json",
        "x-requested-with": "XMLHttpRequest",
    }
    bpmcsrf = session.cookies.get("BPMCSRF")
    if bpmcsrf:
        headers["bpmcsrf"] = bpmcsrf

    start = datetime(day.year, day.month, day.day, 0, 0, 0).strftime("%Y-%m-%dT00:00:00.000")
    value_str = f"\"{start}\""

    payload = {
        "rootSchemaName": "Call",
        "operationType": 0,
        "includeProcessExecutionData": True,
        "filters": {
            "items": {
                "filter_createdon_day": {
                    "filterType": 1,
                    "comparisonType": 3,
                    "isEnabled": True,
                    "trimDateTimeParameterToDate": True,
                    "leftExpression": {"expressionType": 0, "columnPath": "CreatedOn"},
                    "rightExpression": {"expressionType": 2, "parameter": {"dataValueType": 7, "value": value_str}},
                }
            },
            "logicalOperation": 0,
            "isEnabled": True,
            "filterType": 6,
        },
        "columns": {
            "items": {
                # базовые
                "Id": {"expression": {"expressionType": 0, "columnPath": "Id"}},
                "CreatedBy": {"expression": {"expressionType": 0, "columnPath": "CreatedBy"}},
                "CreatedOn": {"expression": {"expressionType": 0, "columnPath": "CreatedOn"}},
                "Direction": {"expression": {"expressionType": 0, "columnPath": "Direction"}},
                "CallerId": {"expression": {"expressionType": 0, "columnPath": "CallerId"}},
                "CalledId": {"expression": {"expressionType": 0, "columnPath": "CalledId"}},
                "Duration": {"expression": {"expressionType": 0, "columnPath": "Duration"}},

                # новые колонки — ТОЧНО как в payload реестра
                "VvtOperatorScore": {"expression": {"expressionType": 0, "columnPath": "VvtOperatorScore"}},
                "CaseCategory": {"expression": {"expressionType": 0, "columnPath": "Case.Category"}},
                "CaseSubcategory": {"expression": {"expressionType": 0, "columnPath": "Case.IbCaseSubCategory"}},
                "CaseOperationNo": {"expression": {"expressionType": 0, "columnPath": "Case.IbOperationNo"}},
                "CaseIbService": {"expression": {"expressionType": 0, "columnPath": "Case.IbService"}},
                "VvtQueueTitle": {"expression": {"expressionType": 0, "columnPath": "VvtQueueTitle"}},
            }
        },
        "rowCount": row_count,
        "rowsOffset": 0,
        "isPageable": True,
        "allColumns": False,
        "useLocalization": True,
    }

    rows: List[Dict[str, Any]] = []
    offset = 0

    while True:
        payload["rowsOffset"] = offset
        r = session.post(url, json=payload, headers=headers, timeout=60)
        r.raise_for_status()
        data = r.json()
        chunk = data.get("rows") or []
        rows.extend(chunk)

        if len(chunk) < row_count:
            break
        offset += row_count

    return rows


# ======= ВРЕМЕННОЙ ИНТЕРВАЛ (CreatedOn приходит по Киеву, таймзоны не используем) =======

def last_60min_window_naive() -> Tuple[datetime, datetime]:
    """
    Окно: последние 60 минут от момента запуска (naive datetime).
    Под запуск на :59 — без дыр.
    """
    now = datetime.now().replace(microsecond=0)
    end = now.replace(second=0)  # стабилизация до минуты
    start = end - timedelta(hours=1)
    return start, end


def parse_createdon_naive(created_on_iso: str) -> datetime:
    """
    Парсим ISO и приводим к naive datetime.
    Если вдруг придёт timezone — просто отбросим tzinfo.
    """
    s = (created_on_iso or "").strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt


def in_window_naive(created_on_iso: str, start_dt: datetime, end_dt: datetime) -> bool:
    dt = parse_createdon_naive(created_on_iso)
    return start_dt <= dt <= end_dt


def fetch_calls_for_window(session: requests.Session, start_dt: datetime, end_dt: datetime) -> List[Dict[str, Any]]:
    """
    Если окно пересекает полночь — делаем 2 запроса (за оба дня) и объединяем.
    """
    days = [start_dt.date()]
    if end_dt.date() != start_dt.date():
        days.append(end_dt.date())

    all_rows: List[Dict[str, Any]] = []
    for d in days:
        all_rows.extend(creatio_select_calls_by_date(session, d))
    return all_rows


# ======= АУДИО =======

def download_audio_bytes(session: requests.Session, call_id: str) -> Optional[bytes]:
    url = f"{BASE}/0/rest/VvtTranscriptionWebService/DownloadAudio"
    headers = {"content-type": "application/json", "accept": "application/json"}

    bpmcsrf = session.cookies.get("BPMCSRF")
    if bpmcsrf:
        headers["bpmcsrf"] = bpmcsrf

    r = session.post(url, json={"callId": call_id}, headers=headers, timeout=60)
    if r.status_code != 200:
        return None

    data = r.json()
    res = data.get("DownloadAudioResult") or {}
    if not res.get("Success"):
        return None

    b64_audio = res.get("Audio")
    if not b64_audio:
        return None

    return base64.b64decode(b64_audio)


def upsert_call_and_audio(conn, row: Dict[str, Any], audio_bytes: Optional[bytes]):
    cur = conn.cursor()

    call_id = row["Id"]
    created_on = row.get("CreatedOn")
    created_by = normalize_value(row.get("CreatedBy"))
    direction = str(row.get("Direction"))
    caller = str(row.get("CallerId"))
    called = str(row.get("CalledId"))
    duration = int(row.get("Duration") or 0)

    # новые поля
    operator_score = int(row.get("VvtOperatorScore") or 0)
    case_category = normalize_value(row.get("CaseCategory"))
    case_subcategory = normalize_value(row.get("CaseSubcategory"))
    case_operation_code = normalize_value(row.get("CaseOperationNo"))
    case_display = normalize_value(row.get("CaseIbService"))  # ✅ IbService
    queue_display = normalize_value(row.get("VvtQueueTitle"))

    has_audio = audio_bytes is not None
    audio_size = len(audio_bytes) if audio_bytes else None
    audio_sha256 = hashlib.sha256(audio_bytes).hexdigest() if audio_bytes else None
    audio_mime = "audio/mpeg" if audio_bytes else None

    cur.execute(
        """
        INSERT INTO calls (
          id, created_on, created_by, direction, from_number, to_number, duration_seconds,
          operator_score, case_category, case_subcategory, case_operation_code, case_display, queue_display,
          has_audio, audio_mime, audio_size_bytes, audio_sha256,
          updated_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
        ON CONFLICT (id) DO UPDATE SET
          created_on = EXCLUDED.created_on,
          created_by = EXCLUDED.created_by,
          direction = EXCLUDED.direction,
          from_number = EXCLUDED.from_number,
          to_number = EXCLUDED.to_number,
          duration_seconds = EXCLUDED.duration_seconds,
          operator_score = EXCLUDED.operator_score,
          case_category = EXCLUDED.case_category,
          case_subcategory = EXCLUDED.case_subcategory,
          case_operation_code = EXCLUDED.case_operation_code,
          case_display = EXCLUDED.case_display,
          queue_display = EXCLUDED.queue_display,
          has_audio = EXCLUDED.has_audio,
          audio_mime = EXCLUDED.audio_mime,
          audio_size_bytes = EXCLUDED.audio_size_bytes,
          audio_sha256 = EXCLUDED.audio_sha256,
          updated_at = now();
        """,
        (
            call_id, created_on, created_by, direction, caller, called, duration,
            operator_score, case_category, case_subcategory, case_operation_code, case_display, queue_display,
            has_audio, audio_mime, audio_size, audio_sha256
        )
    )

    if audio_bytes:
        cur.execute(
            """
            INSERT INTO call_audio (call_id, audio_bytes)
            VALUES (%s, %s)
            ON CONFLICT (call_id) DO UPDATE SET
              audio_bytes = EXCLUDED.audio_bytes;
            """,
            (call_id, psycopg2.Binary(audio_bytes))
        )

    conn.commit()
    cur.close()


def main():
    session = make_session()
    conn = db_connect()

    # окно: последние 60 минут
    start_dt, end_dt = last_60min_window_naive()

    # если окно пересекает полночь — запросим 2 дня
    rows = fetch_calls_for_window(session, start_dt, end_dt)
    print("Calls fetched:", len(rows))

    picked: List[Dict[str, Any]] = []
    for r in rows:
        co = r.get("CreatedOn")
        if not co:
            continue
        try:
            if in_window_naive(co, start_dt, end_dt):
                picked.append(r)
        except Exception:
            continue

    print(f"Calls in window {start_dt} .. {end_dt}:", len(picked))

    for r in picked:
        call_id = r["Id"]
        audio = download_audio_bytes(session, call_id)
        upsert_call_and_audio(conn, r, audio)
        print("Saved:", call_id, "audio:", "yes" if audio else "no")

    conn.close()
    print("✅ Done")


if __name__ == "__main__":
    main()
