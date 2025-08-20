# iot_hourly_anomaly_batch.py
from datetime import datetime, timedelta
import os, io, json, gzip, logging
import pandas as pd
import numpy as np
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator

AWS_REGION = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
S3_RAW = os.environ.get("S3_RAW_BUCKET", "iot-sensor-anomaly-raw-data")
S3_PROCESSED = os.environ.get("S3_PROCESSED_BUCKET", "iot-sensor-anomaly-processed-data")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")  # optional

RAW_PREFIX_BASE = "iot/ingest"
ANOMALY_PREFIX_BASE = "iot/anomalies"
Z_THRESHOLD = float(os.environ.get("ANOMALY_THRESHOLD_Z", "3.5"))

default_args = {
    "owner": "iot-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="iot_hourly_anomaly_batch",
    default_args=default_args,
    schedule_interval="0 * * * *",
    catchup=True,
    tags=["iot", "batch", "anomaly"],
)

def _partition_for_hour(dt: datetime):
    ds = dt.strftime("%Y-%m-%d")
    hh = dt.strftime("%H")
    return f"{RAW_PREFIX_BASE}/dt={ds}/hour={hh}/", f"{ANOMALY_PREFIX_BASE}/dt={ds}/hour={hh}/"

def list_s3_objects(bucket, prefix):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith("/"):
                keys.append(obj["Key"])
    return keys

def _parse_concatenated_json(text: str):
    """
    Parse multiple JSON objects concatenated without delimiters in a single string.
    Returns a list of dicts.
    """
    dec = json.JSONDecoder()
    records = []
    idx = 0
    n = len(text)
    while idx < n:
        # skip whitespace
        while idx < n and text[idx].isspace():
            idx += 1
        if idx >= n:
            break
        try:
            obj, end = dec.raw_decode(text, idx)
            records.append(obj)
            idx = end
        except Exception:
            # If we get stuck, break to avoid infinite loop
            break
    return records

def _read_s3_object_to_df(s3, bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()

    # Decompress if gzip
    if key.endswith(".gz"):
        try:
            body = gzip.decompress(body)
        except Exception as e:
            logging.warning(f"Failed to decompress {key}: {e}")
            return None

    # Decode text once
    try:
        text = body.decode("utf-8", errors="replace")
    except Exception:
        text = None

    # 1) Tolerant JSON Lines (keep valid, skip bad)
    if text is not None:
        lines = [ln for ln in text.splitlines() if ln.strip()]
        if len(lines) > 1:
            recs = []
            bad = 0
            for ln in lines:
                try:
                    recs.append(json.loads(ln))
                except Exception:
                    bad += 1
            if recs:
                logging.info(f"Parsed {len(recs)} JSONL records from {key} (skipped {bad}).")
                return pd.DataFrame(recs)

    # 2) JSON array
    if text is not None:
        try:
            data = json.loads(text)
            if isinstance(data, list) and data and isinstance(data[0], dict):
                logging.info(f"Parsed JSON array with {len(data)} records from {key}.")
                return pd.DataFrame(data)
        except Exception:
            pass

    # 3) Concatenated JSON objects (single long line)
    if text is not None:
        recs = _parse_concatenated_json(text)
        if recs:
            logging.info(f"Parsed {len(recs)} concatenated JSON objects from {key}.")
            return pd.DataFrame(recs)

    # 4) CSV fallback
    try:
        df = pd.read_csv(io.BytesIO(body))
        logging.info(f"Parsed CSV with {len(df)} rows from {key}.")
        return df
    except Exception:
        logging.warning(f"Skipping unreadable object: s3://{bucket}/{key}")
        return None

def read_last_hour_to_df(execution_ts: str, **context):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    exec_dt = datetime.fromisoformat(execution_ts.replace("Z", "+00:00")).replace(minute=0, second=0, microsecond=0)
    raw_prefix, _ = _partition_for_hour(exec_dt)

    keys = list_s3_objects(S3_RAW, raw_prefix)
    if not keys:
        logging.info(f"No data found for s3://{S3_RAW}/{raw_prefix}")
        return None

    frames = []
    for key in keys:
        df = _read_s3_object_to_df(s3, S3_RAW, key)
        if df is not None and not df.empty:
            frames.append(df)

    if not frames:
        logging.info(f"No readable records in s3://{S3_RAW}/{raw_prefix}")
        return None

    df = pd.concat(frames, ignore_index=True)

    # Normalize schema
    for col in ["sensor_a", "sensor_b", "sensor_c"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)

    return df

def robust_zscore_anomalies(**context):
    ts = context["ts"]
    df = read_last_hour_to_df(ts, **context)
    if df is None or df.empty:
        logging.info("No data for this hour.")
        return "no_data"

    required = ["device_id", "sensor_a", "sensor_b", "sensor_c", "timestamp"]
    miss = [c for c in required if c not in df.columns]
    if miss:
        raise ValueError(f"Missing required columns: {miss}")

    # Data cleaning: drop rows with missing sensor values
    df = df.dropna(subset=["sensor_a", "sensor_b", "sensor_c"])
    if df.empty:
        logging.info("No numeric rows after NA drop.")
        return "no_rows"

    # Write curated data as Parquet to curated prefix
    exec_dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(minute=0, second=0, microsecond=0)
    date_str = exec_dt.strftime("%Y-%m-%d")
    hour_str = exec_dt.strftime("%H")
    curated_prefix = f"iot/curated/dt={date_str}/hour={hour_str}/"
    curated_path = f"s3://{S3_PROCESSED}/{curated_prefix}curated.parquet"
    try:
        df.to_parquet(curated_path, index=False)
        logging.info(f"Wrote curated data to {curated_path} with {len(df)} rows.")
    except Exception as e:
        logging.warning(f"Failed to write curated Parquet: {e}")

    def mad(x: np.ndarray) -> float:
        med = np.median(x)
        return float(np.median(np.abs(x - med)) or 1e-9)

    anomalies = []
    for device_id, g in df.groupby("device_id"):
        for col in ["sensor_a", "sensor_b", "sensor_c"]:
            series = g[col].values
            med = np.median(series)
            mad_val = mad(series)
            rz = 0.6745 * (series - med) / mad_val
            mask = np.abs(rz) >= Z_THRESHOLD
            if np.any(mask):
                flagged = g.loc[mask, ["timestamp", "device_id", "sensor_a", "sensor_b", "sensor_c"]].copy()
                flagged["metric"] = col
                flagged["median"] = med
                flagged["mad"] = mad_val
                flagged["robust_z"] = 0.6745 * (flagged[col] - med) / mad_val
                anomalies.append(flagged)

    if not anomalies:
        logging.info("No anomalies detected.")
        return "no_anomalies"

    out_df = pd.concat(anomalies, ignore_index=True).sort_values("timestamp")

    # Write anomalies as Parquet to anomalies prefix
    s3 = boto3.client("s3", region_name=AWS_REGION)
    _, out_prefix = _partition_for_hour(exec_dt)
    out_key = f"{out_prefix}anomalies.parquet"
    try:
        out_df.to_parquet(f"s3://{S3_PROCESSED}/{out_key}", index=False)
        logging.info(f"Wrote anomalies to s3://{S3_PROCESSED}/{out_key} with {len(out_df)} rows.")
    except Exception as e:
        logging.warning(f"Failed to write anomalies Parquet: {e}")
    output_uri = f"s3://{S3_PROCESSED}/{out_key}"

    if SNS_TOPIC_ARN:
        try:
            sns = boto3.client("sns", region_name=AWS_REGION)
            msg = {
                "hour": exec_dt.isoformat() + "Z",
                "count": int(len(out_df)),
                "max_abs_robust_z": float(out_df["robust_z"].abs().max()),
                "top_devices": out_df["device_id"].value_counts().head(3).to_dict(),
                "s3_uri": output_uri,
                "threshold": Z_THRESHOLD,
            }
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=f"IoT anomalies detected: {len(out_df)} rows",
                Message=json.dumps(msg, default=str),
            )
        except Exception as e:
            logging.warning(f"SNS publish failed: {e}")

    return output_uri

detect_task = PythonOperator(
    task_id="detect_hourly_anomalies",
    python_callable=robust_zscore_anomalies,
    provide_context=True,
    dag=dag,
)