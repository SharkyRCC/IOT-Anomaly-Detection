# iot_kinesis_producer_sim.py
from datetime import datetime
import os, json, time, random, logging
import numpy as np
import boto3
from botocore.exceptions import ClientError
from airflow import DAG
from airflow.operators.python import PythonOperator

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
KINESIS_STREAM = os.environ.get("KINESIS_STREAM_NAME", "iot-sensors-stream")

# Hard-coded runtime for DAG executions (30 seconds)
PRODUCER_RUN_SECONDS = 30

default_args = {
    "owner": "iot-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

dag = DAG(
    dag_id="iot_kinesis_producer_sim",
    default_args=default_args,
    schedule_interval=None,  # trigger manually
    catchup=False,
    tags=["iot", "kinesis", "sim"],
)

def generate_event(device_count=50):
    i = random.randint(0, 10_000_000)
    temp_base = 22 + 3 * np.sin(i * 0.01)
    humidity_base = 45 + 10 * np.sin(i * 0.008)
    pressure_base = 1013 + 5 * np.sin(i * 0.005)

    # ~2% anomalies
    if random.random() < 0.02:
        anomaly_type = random.randint(0, 2)
        if anomaly_type == 0:
            sensor_a = 42 + np.random.normal(0, 3)
            sensor_b = 45 + np.random.normal(0, 2)
            sensor_c = 1013 + np.random.normal(0, 1)
        elif anomaly_type == 1:
            sensor_a = 22 + np.random.normal(0, 0.5)
            sensor_b = 45 + np.random.normal(0, 2)
            sensor_c = 960 + np.random.normal(0, 5)
        else:
            sensor_a = 38 + np.random.normal(0, 2)
            sensor_b = 85 + np.random.normal(0, 4)
            sensor_c = 1045 + np.random.normal(0, 2)
    else:
        sensor_a = temp_base + np.random.normal(0, 0.5)
        sensor_b = humidity_base + np.random.normal(0, 2)
        sensor_c = pressure_base + np.random.normal(0, 1)

    return {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "device_id": f"sensor_{random.randint(0, 49):03d}",
        "sensor_a": float(sensor_a),
        "sensor_b": float(sensor_b),
        "sensor_c": float(sensor_c),
        "location": f"location_{random.randint(0, 4)}",
    }

def produce_events(rps=50, device_count=50, **context):
    """
    Produce events for exactly PRODUCER_RUN_SECONDS seconds.
    rps and device_count can be overridden via DAG Run config if desired,
    but duration is always fixed to 30 seconds.
    """
    # Optional: allow overriding rps/device_count via conf, but not duration
    if "dag_run" in context and context["dag_run"] and context["dag_run"].conf:
        rps = int(context["dag_run"].conf.get("rps", rps))
        device_count = int(context["dag_run"].conf.get("device_count", device_count))

    client = boto3.client("kinesis", region_name=AWS_REGION)
    logging.info(f"Producing to stream: {KINESIS_STREAM} in {AWS_REGION} for {PRODUCER_RUN_SECONDS}s at ~{rps} rps.")

    start = time.time()
    sent = 0
    while time.time() - start < PRODUCER_RUN_SECONDS:
        records = []
        for _ in range(rps):
            ev = generate_event(device_count=device_count)
            records.append({
                "Data": json.dumps(ev).encode("utf-8"),
                "PartitionKey": ev["device_id"],
            })
        # send in batches of up to 500
        for i in range(0, len(records), 500):
            try:
                resp = client.put_records(StreamName=KINESIS_STREAM, Records=records[i:i+500])
                failed = resp.get("FailedRecordCount", 0)
                sent += len(records[i:i+500]) - failed
                if failed:
                    logging.warning(f"{failed} records failed in batch; Kinesis may be throttling.")
            except ClientError as e:
                logging.error(f"Kinesis put_records error: {e}")
                time.sleep(1)
        time.sleep(1)

    logging.info(f"Produced approximately {sent} events in {PRODUCER_RUN_SECONDS} seconds.")

produce_task = PythonOperator(
    task_id="produce_events",
    python_callable=produce_events,
    dag=dag,
)