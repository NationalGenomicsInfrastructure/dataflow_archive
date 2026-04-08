import asyncio
import logging
import socket
from datetime import datetime
from pathlib import Path

import aiohttp

BASE_DIR = Path("/data/sequencing_runs")
DONE_FILE = "RTAComplete.txt"
MAX_CONCURRENT_JOBS = 2
COUCHDB_URL = "http://localhost:5984/runs"

WORKER_ID = socket.gethostname()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("pipeline")

sem = asyncio.Semaphore(MAX_CONCURRENT_JOBS)

# ------------------------
# CouchDB helpers
# ------------------------


async def fetch_pending_runs(session):
    # simplistic: you'd normally use a view
    async with session.get(COUCHDB_URL) as resp:
        data = await resp.json()
        return data.get("rows", [])


async def claim_run(session, doc):
    updated = doc.copy()
    updated["status"] = "processing"
    updated["worker_id"] = WORKER_ID
    updated["updated_at"] = datetime.now(datetime.timezone.utc).isoformat()

    url = f"{COUCHDB_URL}/{doc['_id']}"

    async with session.put(url, json=updated) as resp:
        if resp.status == 409:
            return False  # lost the race
        elif resp.status in (200, 201, 202):
            return True
        else:
            text = await resp.text()
            raise RuntimeError(f"CouchDB error: {resp.status} {text}")


async def update_status(session, doc, status):
    doc["status"] = status
    doc["updated_at"] = datetime.now(datetime.timezone.utc).isoformat()

    await session.put(f"{COUCHDB_URL}/{doc['_id']}", json=doc)


# ------------------------
# Core pipeline
# ------------------------


async def run_pipeline(run_path: Path):
    output_file = run_path.with_suffix(".tar.gz.gpg")  # TODO: change the location

    tar_cmd = ["tar", "-czf", "-", "-C", str(run_path.parent), run_path.name]
    gpg_cmd = [
        "gpg",
        "--encrypt",
        "--recipient",
        "your-key-id",
        "--output",
        str(output_file),
    ]

    tar = await asyncio.create_subprocess_exec(
        *tar_cmd,
        stdout=asyncio.subprocess.PIPE,
    )

    gpg = await asyncio.create_subprocess_exec(
        *gpg_cmd,
        stdin=tar.stdout,
    )

    await gpg.wait()
    await tar.wait()

    if gpg.returncode != 0 or tar.returncode != 0:
        raise RuntimeError("Tar/GPG pipeline failed")

    return output_file


async def validate_gpg(file_path: Path):
    cmd = ["gpg", "--decrypt", str(file_path)]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )

    await proc.wait()

    if proc.returncode != 0:
        raise RuntimeError("GPG validation failed")


# ------------------------
# Worker
# ------------------------


async def process_run(session, doc):
    async with sem:
        run_path = Path(doc["path"])

        try:
            log.info(f"Processing {run_path}")

            output = await run_pipeline(run_path)
            await validate_gpg(output)

            await update_status(session, doc, "done")
            log.info(f"Completed {run_path}")

        except Exception as e:
            log.exception(f"Failed {run_path}: {e}")
            await update_status(session, doc, "failed")


# ------------------------
# Scanner
# ------------------------


async def scan_for_new_runs(session):
    for run_dir in BASE_DIR.iterdir():
        if not run_dir.is_dir():
            continue

        if not (run_dir / DONE_FILE).exists():
            continue

        doc = {
            "_id": run_dir.name,
            "path": str(run_dir),
            "status": "pending",
            "created_at": datetime.now(datetime.timezone.utc).isoformat(),
        }

        try:
            await session.put(f"{COUCHDB_URL}/{doc['_id']}", json=doc)
        except Exception:
            pass  # already exists


# ------------------------
# Main loop
# ------------------------


async def main():
    async with aiohttp.ClientSession() as session:
        while True:
            await scan_for_new_runs(session)

            # fetch pending jobs
            async with session.get(COUCHDB_URL) as resp:
                data = await resp.json()
                docs = [row["doc"] for row in data.get("rows", [])]

            tasks = []
            for doc in docs:
                if doc["status"] != "pending":
                    continue

                claimed = await claim_run(session, doc)
                if claimed:
                    tasks.append(process_run(session, doc))

            if tasks:
                await asyncio.gather(*tasks)

            await asyncio.sleep(30)


if __name__ == "__main__":
    asyncio.run(main())
