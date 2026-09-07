from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any


def query_relevant_grb_notices(
    *,
    config: dict[str, Any],
    task_id: str,
    tstart: float | int | str | None = None,
    tstop: float | int | str | None = None,
    margin_seconds: float | None = None,
    max_rows: int | None = None,
) -> dict[str, Any]:
    """
    Query the local GCN inbound notice database for long/short GRB notices.

    The function is intentionally fail-open for science pipeline tasks: missing
    PyMySQL, a stopped database, or a schema mismatch are logged and returned as
    structured metadata instead of failing the Airflow task.
    """
    if os.getenv("GCN_QUERY_ENABLED", "true").strip().lower() in {"0", "false", "no", "off"}:
        result = {
            "status": "disabled",
            "task_id": task_id,
            "count": 0,
            "notices": [],
        }
        print(f"[GCN Query:{task_id}] GCN DB query disabled by GCN_QUERY_ENABLED.")
        return result

    margin = float(
        margin_seconds
        if margin_seconds is not None
        else os.getenv("GCN_QUERY_WINDOW_MARGIN_SECONDS", "60")
    )
    limit = int(max_rows if max_rows is not None else os.getenv("GCN_QUERY_MAX_ROWS", "20"))
    window = _notice_query_window(config, tstart=tstart, tstop=tstop, margin_seconds=margin)
    if window is None:
        result = {
            "status": "skipped",
            "task_id": task_id,
            "reason": "no usable trigger time or science time window",
            "count": 0,
            "notices": [],
        }
        print(f"[GCN Query:{task_id}] No usable time reference for the GCN DB query.")
        return result

    query_start, query_stop, science_start, science_stop = window
    try:
        import pymysql
        import pymysql.cursors
    except ImportError as exc:
        result = {
            "status": "unavailable",
            "task_id": task_id,
            "reason": "PyMySQL is not installed in this runtime",
            "error": str(exc),
            "count": 0,
            "notices": [],
            "query_window": _window_payload(query_start, query_stop, science_start, science_stop),
        }
        print(f"[GCN Query:{task_id}] WARNING: PyMySQL is not available; skipping GCN DB query.")
        return result

    sql = """
        SELECT
          id,
          notice_uuid,
          received_at,
          topic,
          mission,
          instrument,
          alert_type,
          alert_tense,
          event_name,
          event_ids,
          COALESCE(trigger_time, isotime, alert_datetime, received_at) AS event_time,
          trigger_time,
          alert_datetime,
          isotime,
          ra_deg,
          dec_deg,
          ra_dec_error_json,
          healpix_url,
          classification_json,
          packet_type,
          packet_type_name,
          trig_id,
          sequence_num,
          parse_status,
          validation_status
        FROM gcn_inbound_notices
        WHERE COALESCE(trigger_time, isotime, alert_datetime, received_at) BETWEEN %s AND %s
          AND (
            LOWER(COALESCE(topic, '')) LIKE '%%grb%%'
            OR LOWER(COALESCE(event_name, '')) LIKE '%%grb%%'
            OR LOWER(COALESCE(packet_type_name, '')) LIKE '%%grb%%'
            OR LOWER(COALESCE(raw_payload, '')) LIKE '%%grb%%'
          )
          AND (
            LOWER(COALESCE(CAST(classification_json AS CHAR), '')) LIKE '%%short%%'
            OR LOWER(COALESCE(CAST(classification_json AS CHAR), '')) LIKE '%%long%%'
            OR (LOWER(COALESCE(raw_payload, '')) LIKE '%%short%%'
                AND LOWER(COALESCE(raw_payload, '')) LIKE '%%grb%%')
            OR (LOWER(COALESCE(raw_payload, '')) LIKE '%%long%%'
                AND LOWER(COALESCE(raw_payload, '')) LIKE '%%grb%%')
          )
        ORDER BY event_time ASC, received_at ASC, id ASC
        LIMIT %s
    """
    params = (_mysql_datetime(query_start), _mysql_datetime(query_stop), limit)

    try:
        db_password = os.getenv("GCN_DB_PASSWORD", "")
        if not db_password:
            raise RuntimeError("GCN_DB_PASSWORD is required")
        conn = pymysql.connect(
            host=os.getenv("GCN_DB_HOST", "gcn-mysql"),
            port=int(os.getenv("GCN_DB_PORT", "3306")),
            database=os.getenv("GCN_DB_NAME", "gcn"),
            user=os.getenv("GCN_DB_USER", "gcn_user"),
            password=db_password,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
            connect_timeout=int(os.getenv("GCN_DB_CONNECT_TIMEOUT", "10")),
        )
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = list(cur.fetchall())
        finally:
            conn.close()
    except Exception as exc:
        result = {
            "status": "unavailable",
            "task_id": task_id,
            "reason": "GCN database query failed",
            "error": f"{exc.__class__.__name__}: {exc}",
            "count": 0,
            "notices": [],
            "query_window": _window_payload(query_start, query_stop, science_start, science_stop),
        }
        print(f"[GCN Query:{task_id}] WARNING: GCN DB query failed: {result['error']}")
        return result

    notices = [_normalize_notice_row(row) for row in rows]
    result = {
        "status": "ok",
        "task_id": task_id,
        "count": len(notices),
        "notices": notices,
        "query_window": _window_payload(query_start, query_stop, science_start, science_stop),
    }
    _print_notice_summary(task_id, result)
    return result


def _notice_query_window(
    config: dict[str, Any],
    *,
    tstart: float | int | str | None,
    tstop: float | int | str | None,
    margin_seconds: float,
) -> tuple[datetime, datetime, datetime, datetime] | None:
    start_value = _float_or_none(tstart)
    stop_value = _float_or_none(tstop)
    trigger_dt = _parse_datetime(config.get("trigger_time"))
    science_start: datetime | None = None
    science_stop: datetime | None = None

    if _looks_like_unix_time(start_value) and _looks_like_unix_time(stop_value):
        science_start = datetime.fromtimestamp(float(start_value), tz=timezone.utc)
        science_stop = datetime.fromtimestamp(float(stop_value), tz=timezone.utc)
    elif trigger_dt is not None and start_value is not None and stop_value is not None:
        science_start = trigger_dt + timedelta(seconds=float(start_value))
        science_stop = trigger_dt + timedelta(seconds=float(stop_value))
    elif trigger_dt is not None:
        science_start = trigger_dt
        science_stop = trigger_dt

    if science_start is None or science_stop is None:
        return None
    if science_stop < science_start:
        science_start, science_stop = science_stop, science_start

    margin = timedelta(seconds=margin_seconds)
    return science_start - margin, science_stop + margin, science_start, science_stop


def _looks_like_unix_time(value: float | None) -> bool:
    if value is None:
        return False
    return 946684800.0 <= float(value) <= 4102444800.0


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _mysql_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(tzinfo=None).isoformat(
        sep=" ",
        timespec="microseconds",
    )


def _window_payload(
    query_start: datetime,
    query_stop: datetime,
    science_start: datetime,
    science_stop: datetime,
) -> dict[str, str]:
    return {
        "query_start_utc": _iso_z(query_start),
        "query_stop_utc": _iso_z(query_stop),
        "science_start_utc": _iso_z(science_start),
        "science_stop_utc": _iso_z(science_stop),
    }


def _iso_z(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _normalize_notice_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row.get("id"),
        "notice_uuid": row.get("notice_uuid"),
        "topic": row.get("topic"),
        "received_at": _value_to_json_safe(row.get("received_at")),
        "mission": row.get("mission"),
        "instrument": row.get("instrument"),
        "alert_type": row.get("alert_type"),
        "alert_tense": row.get("alert_tense"),
        "event_name": row.get("event_name"),
        "event_ids": _json_column(row.get("event_ids")),
        "event_time": _value_to_json_safe(row.get("event_time")),
        "trigger_time": _value_to_json_safe(row.get("trigger_time")),
        "alert_datetime": _value_to_json_safe(row.get("alert_datetime")),
        "isotime": _value_to_json_safe(row.get("isotime")),
        "ra_deg": row.get("ra_deg"),
        "dec_deg": row.get("dec_deg"),
        "ra_dec_error": _json_column(row.get("ra_dec_error_json")),
        "healpix_url": row.get("healpix_url"),
        "classification": _json_column(row.get("classification_json")),
        "packet_type": row.get("packet_type"),
        "packet_type_name": row.get("packet_type_name"),
        "trig_id": row.get("trig_id"),
        "sequence_num": row.get("sequence_num"),
        "parse_status": row.get("parse_status"),
        "validation_status": row.get("validation_status"),
    }


def _json_column(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list, int, float, bool)):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return _value_to_json_safe(value)


def _value_to_json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return _iso_z(value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value)
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    return value


def _float_or_none(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric <= -9000:
        return None
    return numeric


def _print_notice_summary(task_id: str, result: dict[str, Any]) -> None:
    notices = result.get("notices", [])
    window = result.get("query_window", {})
    if not notices:
        print(
            f"[GCN Query:{task_id}] No relevant long/short GRB entries found in the GCN DB "
            f"for {window.get('query_start_utc')} - {window.get('query_stop_utc')}."
        )
        return

    print(
        f"[GCN Query:{task_id}] Found {len(notices)} relevant long/short GRB notice(s) "
        f"in the GCN DB for {window.get('query_start_utc')} - {window.get('query_stop_utc')}."
    )
    for notice in notices:
        print(
            "[GCN Query:{task_id}] Notice id={id} topic={topic} event={event} "
            "time={time} mission={mission} instrument={instrument} "
            "classification={classification} ra={ra} dec={dec} healpix={healpix}".format(
                task_id=task_id,
                id=notice.get("id"),
                topic=notice.get("topic"),
                event=notice.get("event_name") or notice.get("event_ids"),
                time=notice.get("event_time") or notice.get("trigger_time") or notice.get("isotime"),
                mission=notice.get("mission"),
                instrument=notice.get("instrument"),
                classification=notice.get("classification"),
                ra=notice.get("ra_deg"),
                dec=notice.get("dec_deg"),
                healpix=notice.get("healpix_url"),
            )
        )
