from __future__ import annotations

import ast
import hashlib
import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4


DEFAULT_SCHEMA_URL = "https://gcn.nasa.gov/schema/main/gcn/notices/cosi/alert.schema.json"


def queue_cosi_alert(
    *,
    pipeline: str,
    instrument: str,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    trigger_time: str,
    topic: str | None = None,
    source_product_dir: str | None = None,
    source_config_path: str | None = None,
    products: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = build_cosi_initial_alert(
        pipeline=pipeline,
        instrument=instrument,
        dag_run_id=dag_run_id,
        trigger_time=trigger_time,
        source_product_dir=source_product_dir,
        source_config_path=source_config_path,
        products=products or {},
    )
    topic = topic or os.getenv("GCN_OUTBOUND_TOPIC_DEFAULT", "gcn.notices.cosi.test.alert")
    notice_path = _write_notice_json(
        payload,
        pipeline=pipeline,
        source_product_dir=source_product_dir,
        source_config_path=source_config_path,
    )
    canonical = _canonical_json(payload)
    payload_hash = _sha256(canonical)
    idempotency_key = f"{dag_id}:{dag_run_id}:{task_id}:{payload['alert_type']}:{payload['record_number']}"

    row = {
        "outbox_uuid": str(uuid4()),
        "status": "queued",
        "topic": topic,
        "topic_kind": "test",
        "payload_json": canonical,
        "payload_sha256": payload_hash,
        "schema_url": payload.get("$schema"),
        "schema_version": _schema_version(payload.get("$schema")),
        "validation_status": "not_checked",
        "validation_errors": None,
        "mission": payload.get("mission"),
        "instrument": payload.get("instrument"),
        "alert_type": payload.get("alert_type"),
        "alert_tense": payload.get("alert_tense"),
        "record_number": payload.get("record_number"),
        "event_name": payload.get("event_name"),
        "event_ids": json.dumps(payload.get("id")),
        "trigger_time": _mysql_datetime(payload.get("trigger_time")),
        "alert_datetime": _mysql_datetime(payload.get("alert_datetime")),
        "created_by_dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "task_id": task_id,
        "source_pipeline": pipeline,
        "source_product_dir": source_product_dir,
        "source_config_path": source_config_path,
        "priority": int(os.getenv("GCN_OUTBOUND_PRIORITY", "0")),
        "max_attempts": int(os.getenv("GCN_MAX_ATTEMPTS", "3")),
        "idempotency_key": idempotency_key,
    }
    notice_id = _insert_outbox_row(row)
    # TODO: send this notice to the real GCN network after the production topic,
    # credentials, and non-dry-run guard rails are finalized.
    return {
        "outbound_notice_id": notice_id,
        "topic": topic,
        "idempotency_key": idempotency_key,
        "notice_json_path": notice_path,
        "payload_sha256": payload_hash,
    }


def build_cosi_initial_alert(
    *,
    pipeline: str,
    instrument: str,
    dag_run_id: str,
    trigger_time: str,
    source_product_dir: str | None = None,
    source_config_path: str | None = None,
    products: dict[str, Any],
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    normalized_trigger_time = _iso_z(trigger_time) or now
    event_token = _sha256(f"{pipeline}:{dag_run_id}")[:9]
    event_id = os.getenv("GCN_OUTBOUND_EVENT_ID", event_token)
    event_name = os.getenv("GCN_OUTBOUND_EVENT_NAME", f"COSI {pipeline} test {event_token}")
    payload: dict[str, Any] = {
        "$schema": os.getenv("GCN_COSI_ALERT_SCHEMA_URL", DEFAULT_SCHEMA_URL),
        "alert_datetime": now,
        "alert_tense": os.getenv("GCN_OUTBOUND_ALERT_TENSE", "test"),
        "alert_type": "initial",
        "mission": "COSI",
        "instrument": instrument,
        "messenger": "EM",
        "record_number": int(os.getenv("GCN_OUTBOUND_RECORD_NUMBER", "1")),
        "trigger_time": normalized_trigger_time,
        "event_name": event_name,
        "id": [event_id],
        "classification": _classification(products),
    }
    _add_optional(payload, "data_archive_page", _data_archive_page(event_id))
    _add_probability(payload, "prob_short", _product_or_env_float(products, "prob_short", "GCN_OUTBOUND_PROB_SHORT"))

    if _is_bgo_pipeline(pipeline, instrument):
        _add_bgo_products(payload, products)
    elif _is_ged_pipeline(pipeline, instrument):
        _add_ged_products(payload, products, source_product_dir, source_config_path)
    else:
        _add_bgo_products(payload, products)
        _add_ged_products(payload, products, source_product_dir, source_config_path)
    return payload


# Backward-compatible public name for callers/tests that still import it.
build_cosi_test_alert = build_cosi_initial_alert


def _add_bgo_products(payload: dict[str, Any], products: dict[str, Any]) -> None:
    duration_result = _mapping_from(products.get("duration_result"))
    significance_result = _mapping_from(products.get("significance_result"))
    localization_result = _mapping_from(products.get("localization_result"))
    t90 = _float_or_none(duration_result.get("t90"))
    if t90 is not None and t90 >= 0:
        payload["t90"] = t90
        payload["rate_duration"] = t90
        _add_t90_error(payload, duration_result)
    sigmas = significance_result.get("sigmas")
    if isinstance(sigmas, list) and sigmas:
        numeric = [_float_or_none(item) for item in sigmas]
        numeric = [item for item in numeric if item is not None]
        if numeric:
            payload["rate_snr"] = max(numeric)
        payload["triggered_shields"] = _triggered_shields(sigmas)
    _add_localization(payload, localization_result)
    payload["trigger_type"] = "rate"
    payload.setdefault("rate_energy_range", _number_pair_env("GCN_BGO_RATE_ENERGY_RANGE", [80.0, 2000.0]))


def _add_ged_products(
    payload: dict[str, Any],
    products: dict[str, Any],
    source_product_dir: str | None,
    source_config_path: str | None,
) -> None:
    duration_result = _mapping_from(products.get("duration_result"))
    localization_result = _mapping_from(products.get("localization_result"))
    spectral_result = _mapping_from(products.get("spectral_result"))
    t90 = _float_or_none(duration_result.get("t90"))
    if t90 is not None and t90 >= 0:
        payload["t90"] = t90
        _add_t90_error(payload, duration_result)
    _add_localization(payload, localization_result)
    _add_ged_localization(payload, localization_result)
    ged_snr = _first_float(
        products.get("ged_snr"),
        localization_result.get("ged_snr"),
        localization_result.get("best_S_sigma"),
        localization_result.get("image_snr"),
    )
    if ged_snr is not None:
        payload["ged_snr"] = ged_snr
    if "healpix_url" not in payload:
        _add_optional(payload, "healpix_url", _first_text(localization_result.get("healpix_url"), localization_result.get("healpix_file")))
    if "healpix_url" not in payload:
        _add_optional(payload, "healpix_url", _local_healpix_path(source_product_dir, source_config_path))
    _add_spectral_products(payload, spectral_result)


def _write_notice_json(
    payload: dict[str, Any],
    *,
    pipeline: str,
    source_product_dir: str | None,
    source_config_path: str | None,
) -> str:
    notice_dir = _notice_directory(source_product_dir, source_config_path)
    notice_dir.mkdir(parents=True, exist_ok=True)
    record_number = payload.get("record_number", 1)
    filename = f"cosi_{pipeline.lower()}_alert_initial_record{record_number}.json"
    notice_path = notice_dir / filename
    notice_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    return str(notice_path)


def _notice_directory(source_product_dir: str | None, source_config_path: str | None) -> Path:
    for candidate in [source_product_dir, Path(source_config_path).parent if source_config_path else None]:
        if candidate:
            return Path(candidate).expanduser().resolve()
    return Path.cwd().resolve()


def _insert_outbox_row(row: dict[str, Any]) -> int:
    try:
        import pymysql
    except ImportError as exc:
        raise RuntimeError("PyMySQL is required to queue GCN outbox notices from Airflow") from exc

    columns = list(row.keys())
    placeholders = ", ".join(["%s"] * len(columns))
    update_columns = [
        "updated_at = CURRENT_TIMESTAMP(6)",
        "id = LAST_INSERT_ID(id)",
        "payload_json = VALUES(payload_json)",
        "payload_sha256 = VALUES(payload_sha256)",
        "status = VALUES(status)",
        "validation_status = VALUES(validation_status)",
        "last_error = NULL",
    ]
    sql = f"""
        INSERT INTO gcn_outbound_notices ({", ".join(columns)})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {", ".join(update_columns)}
    """
    conn = pymysql.connect(
        host=os.getenv("GCN_DB_HOST", "gcn-mysql"),
        port=int(os.getenv("GCN_DB_PORT", "3306")),
        database=os.getenv("GCN_DB_NAME", "gcn"),
        user=os.getenv("GCN_DB_USER", "gcn_user"),
        password=os.getenv("GCN_DB_PASSWORD", "gcn_password"),
        charset="utf8mb4",
        autocommit=False,
        connect_timeout=int(os.getenv("GCN_DB_CONNECT_TIMEOUT", "10")),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql, [row[column] for column in columns])
            notice_id = int(cur.lastrowid)
        conn.commit()
        return notice_id
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _iso_z(value: Any) -> str | None:
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value)
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _mysql_datetime(value: Any) -> str | None:
    normalized = _iso_z(value)
    if not normalized:
        return None
    text = normalized[:-1] + "+00:00"
    return datetime.fromisoformat(text).replace(tzinfo=None).isoformat(sep=" ", timespec="microseconds")


def _schema_version(schema_url: Any) -> str | None:
    if not schema_url:
        return None
    text = str(schema_url)
    marker = "/schema/"
    if marker not in text:
        return None
    return text.split(marker, 1)[1].split("/", 1)[0]


def _dict_or_empty(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _mapping_from(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value is None:
        return {}
    if isinstance(value, os.PathLike):
        value = str(value)
    if not isinstance(value, str):
        return {}

    text = value.strip()
    if not text or text.lower() in {"none", "null"}:
        return {}

    path = Path(text).expanduser()
    if path.is_file():
        return _mapping_from_text(path.read_text(encoding="utf-8"), source=str(path))
    return _mapping_from_text(text)


def _mapping_from_text(text: str, source: str | None = None) -> dict[str, Any]:
    for loader in (_loads_json, _loads_literal, _loads_yaml):
        loaded = loader(text)
        if isinstance(loaded, dict):
            return loaded
    if source:
        print(f"[gcn.outbox] Could not parse mapping from {source}")
    return {}


def _loads_json(text: str) -> Any:
    try:
        return json.loads(text)
    except Exception:
        return None


def _loads_literal(text: str) -> Any:
    try:
        return ast.literal_eval(text)
    except Exception:
        return None


def _loads_yaml(text: str) -> Any:
    try:
        import yaml
    except Exception:
        return None
    try:
        return yaml.safe_load(text)
    except Exception:
        return None


def _is_bgo_pipeline(pipeline: str, instrument: str) -> bool:
    text = f"{pipeline} {instrument}".lower()
    return "bgo" in text or "shield" in text


def _is_ged_pipeline(pipeline: str, instrument: str) -> bool:
    text = f"{pipeline} {instrument}".lower()
    return "ged" in text or "germanium" in text


def _classification(products: dict[str, Any]) -> dict[str, float]:
    classification_result = _mapping_from(products.get("classification_result"))
    classification = classification_result.get("classification") if "classification" in classification_result else classification_result
    if not isinstance(classification, dict):
        classification = _mapping_from(products.get("classification"))
    if not classification:
        classification = _mapping_from(os.getenv("GCN_OUTBOUND_CLASSIFICATION_JSON"))
    clean: dict[str, float] = {}
    if isinstance(classification, dict):
        for key, value in classification.items():
            probability = _float_or_none(value)
            if probability is not None:
                clean[str(key)] = probability
    return clean or {"Uncertain": 1.0}


def _data_archive_page(event_id: str) -> str | None:
    explicit = os.getenv("GCN_OUTBOUND_DATA_ARCHIVE_PAGE")
    if explicit:
        return explicit
    base_url = os.getenv("GCN_COSI_DATA_ARCHIVE_BASE_URL")
    if not base_url:
        return None
    return f"{base_url.rstrip('/')}/{event_id}/"


def _product_or_env_float(products: dict[str, Any], product_key: str, env_key: str) -> float | None:
    return _first_float(products.get(product_key), os.getenv(env_key))


def _add_probability(payload: dict[str, Any], key: str, value: Any) -> None:
    probability = _float_or_none(value)
    if probability is not None and 0.0 <= probability <= 1.0:
        payload[key] = probability


def _add_optional(payload: dict[str, Any], key: str, value: Any) -> None:
    if value is not None and value != "":
        payload[key] = value


def _add_t90_error(payload: dict[str, Any], duration_result: dict[str, Any]) -> None:
    low = _float_or_none(duration_result.get("t90_err_low"))
    high = _float_or_none(duration_result.get("t90_err_high"))
    if low is not None and high is not None and low >= 0 and high >= 0:
        payload["t90_error"] = [low, high]
        return
    err = _float_or_none(duration_result.get("t90_error"))
    if err is not None and err >= 0:
        payload["t90_error"] = err


def _triggered_shields(sigmas: list[Any]) -> list[bool]:
    threshold = _float_or_none(os.getenv("GCN_BGO_TRIGGERED_SHIELD_SIGMA_THRESHOLD")) or 0.0
    flags = []
    for item in sigmas[:6]:
        value = _float_or_none(item)
        flags.append(value is not None and value > threshold)
    while len(flags) < 6:
        flags.append(False)
    return flags


def _add_localization(payload: dict[str, Any], localization_result: dict[str, Any]) -> None:
    ra = _first_float(
        localization_result.get("ra"),
        localization_result.get("ra_deg"),
        localization_result.get("best_ra"),
        localization_result.get("best_ra_deg"),
    )
    dec = _first_float(
        localization_result.get("dec"),
        localization_result.get("dec_deg"),
        localization_result.get("best_dec"),
        localization_result.get("best_dec_deg"),
    )
    if ra is not None and dec is not None:
        payload["ra"] = ra
        payload["dec"] = dec
    error = _first_float(
        localization_result.get("ra_dec_error"),
        localization_result.get("eq_radius_deg"),
        localization_result.get("error_radius_deg"),
        localization_result.get("localization_error_deg"),
    )
    if error is not None and error >= 0:
        payload["ra_dec_error"] = error
    containment = _first_float(localization_result.get("containment_probability"), os.getenv("GCN_CONTAINMENT_PROBABILITY"))
    if containment is not None and 0.0 <= containment <= 1.0:
        payload["containment_probability"] = containment
    systematic = _bool_or_none(localization_result.get("systematic_included"))
    if systematic is not None:
        payload["systematic_included"] = systematic
    _add_optional(payload, "healpix_url", _first_text(localization_result.get("healpix_url"), localization_result.get("healpix_map_url")))


def _add_ged_localization(payload: dict[str, Any], localization_result: dict[str, Any]) -> None:
    if "ra" in payload and "dec" in payload:
        return
    l_deg = _first_float(localization_result.get("best_l_deg"), localization_result.get("l_deg"))
    b_deg = _first_float(localization_result.get("best_b_deg"), localization_result.get("b_deg"))
    if l_deg is None or b_deg is None:
        return
    ra, dec = _galactic_to_icrs(l_deg, b_deg)
    payload["ra"] = ra
    payload["dec"] = dec


def _add_spectral_products(payload: dict[str, Any], spectral_result: dict[str, Any]) -> None:
    for key in [
        "best_fit_model",
        "power_law",
        "cutoff_power_law",
        "band",
        "flux",
        "flux_error",
        "flux_unit",
        "flux_spectral_band",
        "flux_time_range",
        "fluence",
        "fluence_error",
        "fluence_spectral_band",
        "fluence_time_range",
        "polarization_candidate",
    ]:
        if key in spectral_result:
            payload[key] = spectral_result[key]


def _number_pair_env(key: str, default: list[float]) -> list[float]:
    value = os.getenv(key)
    if not value:
        return default
    parsed = _loads_json(value)
    if not isinstance(parsed, list):
        parsed = [part.strip() for part in value.split(",")]
    pair = [_float_or_none(item) for item in parsed[:2]]
    if len(pair) == 2 and pair[0] is not None and pair[1] is not None:
        return [pair[0], pair[1]]
    return default


def _local_healpix_path(source_product_dir: str | None, source_config_path: str | None) -> str | None:
    roots = []
    if source_product_dir:
        roots.append(Path(source_product_dir))
    if source_config_path:
        roots.append(Path(source_config_path).parent)
    for root in roots:
        try:
            for pattern in ("*.fits", "*.fit", "*.fits.gz"):
                matches = sorted(root.glob(pattern))
                if matches:
                    return str(matches[0])
        except Exception:
            continue
    return None


def _first_float(*values: Any) -> float | None:
    for value in values:
        parsed = _float_or_none(value)
        if parsed is not None:
            return parsed
    return None


def _first_text(*values: Any) -> str | None:
    for value in values:
        if value is not None and str(value).strip():
            return str(value)
    return None


def _bool_or_none(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off"}:
        return False
    return None


def _galactic_to_icrs(l_deg: float, b_deg: float) -> tuple[float, float]:
    l_rad = math.radians(l_deg)
    b_rad = math.radians(b_deg)
    gal = (
        math.cos(b_rad) * math.cos(l_rad),
        math.cos(b_rad) * math.sin(l_rad),
        math.sin(b_rad),
    )
    gal_to_eq = (
        (-0.0548755604, 0.4941094279, -0.8676661490),
        (-0.8734370902, -0.4448296300, -0.1980763734),
        (-0.4838350155, 0.7469822445, 0.4559837762),
    )
    x = sum(gal_to_eq[0][i] * gal[i] for i in range(3))
    y = sum(gal_to_eq[1][i] * gal[i] for i in range(3))
    z = sum(gal_to_eq[2][i] * gal[i] for i in range(3))
    ra = math.degrees(math.atan2(y, x)) % 360.0
    dec = math.degrees(math.asin(max(-1.0, min(1.0, z))))
    return ra, dec
