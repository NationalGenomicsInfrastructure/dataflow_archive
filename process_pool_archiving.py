import logging
import os
import socket
import subprocess
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta

import couchdb

# =========================
# Configuration
# =========================
COUCHDB_URL = "http://localhost:5984/"
DB_NAME = "pipeline_jobs"
MAX_WORKERS = min(4, os.cpu_count() or 1)
POLL_INTERVAL = 2
MAX_RETRIES = 3
STALE_TIMEOUT_SECONDS = 600
GPG_PASSPHRASE = os.environ.get("GPG_PASSPHRASE")

WORKER_ID = f"{socket.gethostname()}:{os.getpid()}"

# =========================
# Logging
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# =========================
# CouchDB Setup
# =========================
def get_db():
    server = couchdb.Server(COUCHDB_URL)
    if DB_NAME not in server:
        server.create(DB_NAME)
    db = server[DB_NAME]

    # Create Mango index for efficient pending job queries
    try:
        db.save(
            {
                "_id": "index_status_attempts",
                "index": {"fields": ["status", "attempts"]},
                "type": "json",
            }
        )
    except couchdb.http.ResourceConflict:
        pass  # Index already exists

    return db


# =========================
# Job Helpers
# =========================
def now_ts():
    return datetime.utcnow().isoformat()


def is_stale(job):
    if job.get("status") != "processing":
        return False
    updated = job.get("updated_at")
    if not updated:
        return True
    updated_dt = datetime.fromisoformat(updated)
    return datetime.utcnow() - updated_dt > timedelta(seconds=STALE_TIMEOUT_SECONDS)


def reset_stale_jobs(db):
    for row in db.view("_all_docs", include_docs=True):
        job = row.doc
        if is_stale(job):
            logging.warning(f"Resetting stale job: {job['_id']}")
            job["status"] = "pending"
            job["worker"] = None
            job["updated_at"] = now_ts()
            try:
                db.save(job)
            except couchdb.http.ResourceConflict:
                pass


def claim_job(db):
    # Use Mango query to fetch a pending job efficiently
    try:
        result = db.find(
            {
                "selector": {"status": "pending", "attempts": {"$lt": MAX_RETRIES}},
                "limit": 1,
            }
        )
    except Exception as e:
        logging.error(f"Failed to query CouchDB: {e}")
        return None

    docs = list(result)
    if not docs:
        return None

    job = docs[0]
    job["status"] = "processing"
    job["worker"] = WORKER_ID
    job["claimed_at"] = now_ts()
    job["updated_at"] = now_ts()

    try:
        db.save(job)  # Will fail on conflict
        return job
    except couchdb.http.ResourceConflict:
        return None  # Another worker claimed it


# =========================
# Processing Logic
# =========================
def process_dir(path):
    output_file = f"{path}.tar.gz.gpg"

    tar = subprocess.Popen(
        ["tar", "-czf", "-", path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    gpg_cmd = [
        "gpg",
        "--batch",
        "--yes",
        "--passphrase",
        GPG_PASSPHRASE or "",
        "--symmetric",
        "--cipher-algo",
        "AES256",
        "-o",
        output_file,
    ]

    gpg = subprocess.Popen(
        gpg_cmd,
        stdin=tar.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    tar.stdout.close()

    _, gpg_err = gpg.communicate()

    if tar.wait() != 0:
        raise RuntimeError("tar failed")

    if gpg.returncode != 0:
        raise RuntimeError(f"gpg failed: {gpg_err.decode()}")

    return output_file


# =========================
# Worker Task
# =========================
def worker_task(job):
    path = job["path"]
    try:
        output = process_dir(path)
        return (job["_id"], "done", output, None)
    except Exception as e:
        return (job["_id"], "failed", None, str(e))


# =========================
# Main Worker Loop
# =========================
def run_worker():
    db = get_db()

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}

        while True:
            reset_stale_jobs(db)

            # Fill queue
            while len(futures) < MAX_WORKERS:
                job = claim_job(db)
                if not job:
                    break

                future = executor.submit(worker_task, job)
                futures[future] = job

            # Process completed jobs
            for future in list(futures):
                if future.done():
                    job = futures.pop(future)
                    job_id, status, output, error = future.result()

                    doc = db[job_id]
                    doc["status"] = status
                    doc["output_file"] = output
                    doc["error"] = error
                    doc["updated_at"] = now_ts()

                    if status == "failed":
                        doc["attempts"] = doc.get("attempts", 0) + 1

                    try:
                        db.save(doc)
                        logging.info(f"Job {job_id} -> {status}")
                    except couchdb.http.ResourceConflict:
                        logging.warning(
                            f"Conflict saving job {job_id}, will retry later"
                        )

            if not futures:
                time.sleep(POLL_INTERVAL)


# =========================
# Entry Point
# =========================
if __name__ == "__main__":
    run_worker()
