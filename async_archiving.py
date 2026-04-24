import argparse
import asyncio
import logging
import os
import socket
from datetime import datetime, timezone
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

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("pipeline")


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

    tar_options = config.get("tar_options", [])
    if tar_options is None:
        tar_options = []
    if not isinstance(tar_options, list):
        raise RuntimeError("Config entry 'tar_options' must be a list")

    return {
        "statusdb": statusdb,
        "sequencing_path": sequencing_path,
        "destination_path": destination_path,
        "ignore": ignore_list,
        "tar_options": tar_options,
    }


# ------------------------
# CouchDB helpers
# ------------------------


def build_couchdb_url(statusdb: dict) -> str:
    raw_url = statusdb["url"].strip()
    if not raw_url:
        raise RuntimeError("statusdb.url must not be empty")

    if not urlparse(raw_url).scheme:
        raw_url = f"http://{raw_url}"

    database = statusdb["database"].strip().lstrip("/")
    return f"{raw_url.rstrip('/')}/{database}"


async def fetch_pending_runs(session, couchdb_url):
    """Fetch all runs with status 'pending' from CouchDB using the lookup design document view."""
    view_url = f"{couchdb_url}/_design/lookup/_view/runfolder_id?include_docs=true"

    async with session.get(view_url) as resp:
        data = await resp.json()
        rows = data.get("rows", [])
        # Extract and filter for documents with pending status
        pending_docs = [
            row["doc"] for row in rows if row.get("doc", {}).get("status") == "pending"
        ]
        return pending_docs


async def claim_run(session, doc, couchdb_url):
    """Attempt to claim a run for processing by updating its status to 'processing' in CouchDB. Returns True if successful, False if another worker claimed it first."""
    updated = doc.copy()
    updated["status"] = "processing"
    updated["worker_id"] = WORKER_ID
    updated["updated_at"] = datetime.now(timezone.utc).isoformat()

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
    """Update a run's status in CouchDB. Refetches the document to ensure we have the current _rev."""
    # Refetch the document to get the latest _rev
    async with session.get(f"{couchdb_url}/{doc['_id']}") as resp:
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(
                f"Failed to fetch document for status update: {resp.status} {text}"
            )
        current_doc = await resp.json()

    # Update the status in the fetched document
    current_doc["status"] = status
    current_doc["updated_at"] = datetime.now(timezone.utc).isoformat()

    async with session.put(f"{couchdb_url}/{doc['_id']}", json=current_doc) as resp:
        if resp.status not in (200, 201, 202):
            text = await resp.text()
            raise RuntimeError(
                f"Failed to update status to '{status}': {resp.status} {text}"
            )


# ------------------------
# Core pipeline
# ------------------------


async def run_pipeline(run_path: Path, destination_path: Path, tar_options: list[str]):
    """Run the tar + gpg pipeline for a given run directory and return the path to the output GPG file."""
    output_file = destination_path / f"{run_path.name}.tar.gpg"
    key_file = destination_path / f"{run_path.name}.key"
    key_file.parent.mkdir(parents=True, exist_ok=True)
    gen_key_cmd = ["gpg", "--gen-random", "1", "256"]
    proc = await asyncio.create_subprocess_exec(
        *gen_key_cmd,
        stdout=asyncio.subprocess.PIPE,
    )
    key_data, _ = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"GPG key generation failed with code {proc.returncode}")
    key_file.write_bytes(key_data)

    tar_cmd = (
        ["tar"] + tar_options + ["-cf", "-", "-C", str(run_path.parent), run_path.name]
    )
    gpg_cmd = [
        "gpg",
        "--symmetric",
        "--cipher-algo",
        "aes256",
        "--passphrase-file",
        str(key_file),
        "--batch",
        "--compress-algo",
        "none",
        "--output",
        str(output_file),
    ]
    # gpg --symmetric --cipher-algo aes256 --passphrase-file run_key_file --batch --compress-algo none -o {run.tar_encrypted} {run.tar}

    tar = await asyncio.create_subprocess_exec(
        *tar_cmd,
        stdout=asyncio.subprocess.PIPE,
    )

    gpg = await asyncio.create_subprocess_exec(
        *gpg_cmd,
        stdin=asyncio.subprocess.PIPE,
    )

    # Pipe tar's output to gpg's input
    try:
        while True:
            chunk = await tar.stdout.read(8192)
            if not chunk:
                break
            gpg.stdin.write(chunk)
            await gpg.stdin.drain()
    except Exception as e:
        log.error(f"Error piping data: {e}")
        gpg.stdin.close() if gpg.stdin else None
        await gpg.terminate()
        raise
    finally:
        gpg.stdin.close()
        await gpg.stdin.wait_closed()

    await tar.wait()
    await gpg.wait()

    if tar.returncode != 0 or gpg.returncode != 0:
        raise RuntimeError(
            f"Tar/GPG pipeline failed: tar={tar.returncode}, gpg={gpg.returncode}"
        )

    return output_file


async def validate_gpg(file_path: Path):
    """Validate the GPG file by attempting to decrypt it (without actually writing the output)."""
    key_file = file_path.parent / (file_path.name.rsplit(".", 2)[0] + ".key")
    cmd = [
        "gpg",
        "--decrypt",
        "--cipher-algo",
        "aes256",
        "--passphrase-file",
        str(key_file),
        "--batch",
        "--quiet",
        str(file_path),
    ]
    # gpg --decrypt --cipher-algo aes256 --passphrase-file {run.key} --batch {run.tar_encrypted}

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


async def process_run(
    session, doc, couchdb_url, destination_path: Path, tar_options: list[str]
):
    """Process a single run: tar, encrypt, validate, and update status in CouchDB."""
    async with asyncio.Semaphore(MAX_CONCURRENT_JOBS):
        log.info(f"Starting processing of run {doc['_id']} at {doc['path']}")
        run_path = Path(doc["path"])

        try:
            log.info(f"Processing {run_path}")

            output = await run_pipeline(run_path, destination_path, tar_options)
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
    final_file: str,
):
    """Scan the sequencing directory for new runs and add them to CouchDB if they are not already present."""
    log.info(f"Scanning for new runs in {sequencing_path}")
    for sequencer_dir in sequencing_path.iterdir():
        if not sequencer_dir.is_dir():
            log.info(f"Skipping non-directory {sequencer_dir}")
            continue

        for run_dir in sequencer_dir.iterdir():
            if not run_dir.is_dir():
                log.info(f"Skipping non-directory {run_dir}")
                continue
            if run_dir.name in ignore_dirs:
                log.info(f"Skipping ignored directory {run_dir}")
                continue

            # Check if the final file exists
            if not (run_dir / final_file).exists():
                log.info(
                    f"Skipping run directory {run_dir} without final file {final_file}"
                )
                continue

            doc = {
                "_id": run_dir.name,
                "path": str(run_dir),
                "status": "pending",
                "created_at": datetime.now(timezone.utc).isoformat(),
            }

            try:
                async with session.put(f"{couchdb_url}/{doc['_id']}", json=doc) as resp:
                    if resp.status in (200, 201, 202):
                        await resp.text()  # consume response to avoid warnings
                    else:
                        resp.raise_for_status()
                log.info(f"Added new run to CouchDB: {doc['_id']} at {doc['path']}")
            except Exception:
                log.info(f"Run {doc['_id']} already exists in CouchDB")
                pass  # already exists


# ------------------------
# Main loop
# ------------------------


async def main(config_path: Path):
    """Main loop: scan for new runs and process pending runs."""
    conf = load_config(config_path)
    statusdb_conf = conf["statusdb"]
    sequencing_path = Path(conf["sequencing_path"])
    destination_path = Path(conf["destination_path"])
    ignore_dirs = conf["ignore"]
    tar_options = conf["tar_options"]
    final_file = ".metadata_rsync_exitcode"

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
    log.info(f"Using CouchDB URL: {couchdb_url}")
    async with aiohttp.ClientSession(auth=auth) as session:
        while True:
            await scan_for_new_runs(
                session, couchdb_url, sequencing_path, ignore_dirs, final_file
            )

            # fetch pending jobs
            docs = await fetch_pending_runs(session, couchdb_url)

            log.info(f"Found {len(docs)} pending runs in CouchDB")

            tasks = []
            for doc in docs:
                log.info(f"Found pending run: {doc['_id']} at {doc['path']}")
                claimed = await claim_run(session, doc, couchdb_url)
                if claimed:
                    log.info(f"Claimed run {doc['_id']} for processing")
                    tasks.append(
                        process_run(
                            session, doc, couchdb_url, destination_path, tar_options
                        )
                    )

            if tasks:
                await asyncio.gather(*tasks)
                log.info(f"Completed processing batch of {len(tasks)} runs")
            else:
                log.info("No pending runs to process")

            log.info("Sleeping before next scan...")
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
    log.info(f"Starting archive worker with config: {args.config_file}")
    asyncio.run(main(Path(args.config_file)))
