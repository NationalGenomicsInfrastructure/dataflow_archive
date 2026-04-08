import argparse
import asyncio
import logging
import os
import socket
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
import yaml

MAX_CONCURRENT_JOBS = 2

WORKER_ID = socket.gethostname()

CONFIG_DEFAULT_PATH = Path(
    os.environ.get(
        "ARCHIVE_CONFIG",
        os.path.join(os.path.expanduser("~"), ".df_archive/df_archive.yaml"),
    )
).expanduser()


def load_config(config_path: Path):
    with config_path.open() as file:
        config = yaml.safe_load(file)

    if not isinstance(config, dict):
        raise RuntimeError("Config file must contain a mapping")

    statusdb = config.get("statusdb")
    if not isinstance(statusdb, dict):
        raise RuntimeError("Missing 'statusdb' section in config")

    for key in ("username", "password", "url", "database"):
        if not statusdb.get(key):
            raise RuntimeError(f"Missing required statusdb config: {key}")

    sequencing_path = config.get("sequencing_path")
    if not sequencing_path:
        raise RuntimeError("Missing required config entry: sequencing_path")

    destination_path = config.get("destination_path")
    if not destination_path:
        raise RuntimeError("Missing required config entry: destination_path")

    ignore_list = config.get("ignore", [])
    if ignore_list is None:
        ignore_list = []
    if not isinstance(ignore_list, list):
        raise RuntimeError("Config entry 'ignore' must be a list")

    final_files = config.get("final_files", [])
    if final_files is None:
        final_files = []
    if not isinstance(final_files, list):
        raise RuntimeError("Config entry 'final_files' must be a list")

    return {
        "statusdb": statusdb,
        "sequencing_path": sequencing_path,
        "destination_path": destination_path,
        "ignore": ignore_list,
        "final_files": final_files,
    }


def build_couchdb_url(statusdb: dict) -> str:
    raw_url = statusdb["url"].strip()
    if not raw_url:
        raise RuntimeError("statusdb.url must not be empty")

    if not urlparse(raw_url).scheme:
        raw_url = f"http://{raw_url}"

    database = statusdb["database"].strip().lstrip("/")
    return f"{raw_url.rstrip('/')}/{database}"


logging.basicConfig(level=logging.INFO)
log = logging.getLogger("pipeline")

sem = asyncio.Semaphore(MAX_CONCURRENT_JOBS)

# ------------------------
# CouchDB helpers
# ------------------------


async def fetch_pending_runs(session, couchdb_url):
    # simplistic: you'd normally use a view
    async with session.get(couchdb_url) as resp:
        data = await resp.json()
        return data.get("rows", [])


async def claim_run(session, doc, couchdb_url):
    updated = doc.copy()
    updated["status"] = "processing"
    updated["worker_id"] = WORKER_ID
    updated["updated_at"] = datetime.now(datetime.timezone.utc).isoformat()

    url = f"{couchdb_url}/{doc['_id']}"

    async with session.put(url, json=updated) as resp:
        if resp.status == 409:
            return False  # lost the race
        elif resp.status in (200, 201, 202):
            return True
        else:
            text = await resp.text()
            raise RuntimeError(f"CouchDB error: {resp.status} {text}")


async def update_status(session, doc, status, couchdb_url):
    doc["status"] = status
    doc["updated_at"] = datetime.now(datetime.timezone.utc).isoformat()

    await session.put(f"{couchdb_url}/{doc['_id']}", json=doc)


# ------------------------
# Core pipeline
# ------------------------


async def run_pipeline(run_path: Path, destination_path: Path):
    output_file = destination_path / f"{run_path.name}.tar.gpg"

    tar_cmd = ["tar", "-cf", "-", "-C", str(run_path.parent), run_path.name]
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


async def process_run(session, doc, couchdb_url, destination_path: Path):
    async with sem:
        run_path = Path(doc["path"])

        try:
            log.info(f"Processing {run_path}")

            output = await run_pipeline(run_path, destination_path)
            await validate_gpg(output)

            await update_status(session, doc, "done", couchdb_url)
            log.info(f"Completed {run_path}")

        except Exception as e:
            log.exception(f"Failed {run_path}: {e}")
            await update_status(session, doc, "failed", couchdb_url)


# ------------------------
# Scanner
# ------------------------


async def scan_for_new_runs(
    session,
    couchdb_url,
    sequencing_path: Path,
    ignore_dirs: list[str],
    final_files: list[str],
):
    for sequencer_dir in sequencing_path.iterdir():
        if not sequencer_dir.is_dir():
            continue

        for run_dir in sequencer_dir.iterdir():
            if not run_dir.is_dir():
                continue
            if run_dir.name in ignore_dirs:
                continue

            # Check if any of the final files exist
            has_final_file = any((run_dir / f).exists() for f in final_files)
            if not has_final_file:
                continue

            doc = {
                "_id": run_dir.name,
                "path": str(run_dir),
                "status": "pending",
                "created_at": datetime.now(datetime.timezone.utc).isoformat(),
            }

            try:
                await session.put(f"{couchdb_url}/{doc['_id']}", json=doc)
            except Exception:
                pass  # already exists


# ------------------------
# Main loop
# ------------------------


async def main(config_path: Path):
    conf = load_config(config_path)
    statusdb_conf = conf["statusdb"]
    sequencing_path = Path(conf["sequencing_path"])
    destination_path = Path(conf["destination_path"])
    ignore_dirs = conf["ignore"]
    final_files = conf["final_files"]

    if not sequencing_path.is_dir():
        raise RuntimeError(
            f"sequencing_path does not exist or is not a directory: {sequencing_path}"
        )

    if not destination_path.is_dir():
        raise RuntimeError(
            f"destination_path does not exist or is not a directory: {destination_path}"
        )

    couchdb_url = build_couchdb_url(statusdb_conf)
    auth = aiohttp.BasicAuth(statusdb_conf["username"], statusdb_conf["password"])

    async with aiohttp.ClientSession(auth=auth) as session:
        while True:
            await scan_for_new_runs(
                session, couchdb_url, sequencing_path, ignore_dirs, final_files
            )

            # fetch pending jobs
            async with session.get(couchdb_url) as resp:
                data = await resp.json()
                docs = [row["doc"] for row in data.get("rows", [])]

            tasks = []
            for doc in docs:
                if doc["status"] != "pending":
                    continue

                claimed = await claim_run(session, doc, couchdb_url)
                if claimed:
                    tasks.append(
                        process_run(session, doc, couchdb_url, destination_path)
                    )

            if tasks:
                await asyncio.gather(*tasks)

            await asyncio.sleep(30)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Archive runs and update CouchDB status from a YAML config file"
    )
    parser.add_argument(
        "-c",
        "--config-file",
        default=str(CONFIG_DEFAULT_PATH),
        help="Path to YAML config file containing statusdb credentials and URL",
    )
    args = parser.parse_args()

    asyncio.run(main(Path(args.config_file)))
