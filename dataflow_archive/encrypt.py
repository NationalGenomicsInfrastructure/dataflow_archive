import argparse
import asyncio
import logging
import signal
import socket
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

from dataflow_archive.log import init_logger_file
from dataflow_archive.utils.utils import (
    CONFIG_DEFAULT_PATH,
    build_couchdb_url,
    load_config,
)

log = logging.getLogger(__name__)

MAX_CONCURRENT_JOBS = 2
MAX_RETRIES = 3

WORKER_ID = socket.gethostname()


# ------------------------
# CouchDB helpers
# ------------------------


async def fetch_pending_runs(session, couchdb_url):
    """Fetch all runs with status 'pending' from CouchDB using the lookup design document view."""
    view_url = f"{couchdb_url}/_design/lookup/_view/pending_runs?include_docs=true"  # view should emit rows for docs with status 'pending'

    try:
        async with session.get(view_url) as resp:
            if resp.status != 200:
                text = await resp.text()
                log.error(f"Failed to fetch pending runs: {resp.status} {text}")
                return []
            data = await resp.json()
            rows = data.get("rows", [])
            log.debug(f"Fetched {len(rows)} rows from CouchDB view")
            # Extract and filter for documents with pending status
            pending_docs = [row["doc"] for row in rows]
            return pending_docs
    except aiohttp.ClientError as e:
        log.error(f"Network error fetching pending runs: {e}")
        return []


async def claim_run(session, doc, couchdb_url):
    """Attempt to claim a run for processing by updating its status to 'processing' in CouchDB. Returns True if successful, False if another worker claimed it first."""
    updated = doc.copy()
    updated["status"] = "processing"
    updated["encryption_worker_id"] = WORKER_ID
    updated["updated_at"] = datetime.now(timezone.utc).isoformat()

    url = f"{couchdb_url}/{doc['_id']}"

    async with session.put(url, json=updated) as resp:
        if resp.status == 409:
            log.warning(
                f"Lost race to claim run {doc['_id']}, another worker claimed it first"
            )
            return False  # lost the race
        elif resp.status in (200, 201, 202):
            log.info(f"Claimed run {doc['_id']} on worker {WORKER_ID}")
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
        log.info(f"Run {doc['_id']}: status updated to '{status}'")


async def handle_failure(session, doc, couchdb_url):
    """Increment encryption_failure_count on the document. Reset status to 'pending' for retry,
    or set to 'failed' if MAX_RETRIES is exceeded."""
    # Refetch to get latest _rev and current failure count
    async with session.get(f"{couchdb_url}/{doc['_id']}") as resp:
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(
                f"Failed to fetch document for failure update: {resp.status} {text}"
            )
        current_doc = await resp.json()

    failure_count = current_doc.get("encryption_failure_count", 0) + 1
    current_doc["encryption_failure_count"] = failure_count
    current_doc["updated_at"] = datetime.now(timezone.utc).isoformat()

    if failure_count >= MAX_RETRIES:
        current_doc["status"] = "failed"
        log.warning(
            f"Run {doc['_id']} has failed {failure_count} times, marking as failed"
        )
    else:
        current_doc["status"] = "pending"
        log.info(
            f"Run {doc['_id']} failed (attempt {failure_count}/{MAX_RETRIES}), resetting to pending for retry"
        )

    async with session.put(f"{couchdb_url}/{doc['_id']}", json=current_doc) as resp:
        if resp.status not in (200, 201, 202):
            text = await resp.text()
            raise RuntimeError(
                f"Failed to update document after failure: {resp.status} {text}"
            )


async def reset_stale_processing_runs(session, couchdb_url):
    """On startup, reset any runs stuck in 'processing' by this worker back to 'pending'.
    This handles the case where the script was interrupted or crashed mid-run."""
    view_url = f"{couchdb_url}/_design/lookup/_view/runfolder_id?include_docs=true"
    try:
        async with session.get(view_url) as resp:
            if resp.status != 200:
                log.error(f"Could not check for stale runs on startup: {resp.status}")
                return
            data = await resp.json()
    except aiohttp.ClientError as e:
        log.error(f"Network error checking for stale runs on startup: {e}")
        return

    stale = [
        row["doc"]
        for row in data.get("rows", [])
        if row.get("doc", {}).get("status") == "processing"
        and row.get("doc", {}).get("encryption_worker_id") == WORKER_ID
    ]

    if not stale:
        log.info("No stale processing runs found on startup")
        return

    log.warning(
        f"Found {len(stale)} stale run(s) stuck in 'processing', resetting to 'pending'"
    )
    for doc in stale:
        try:
            await update_status(session, doc, "pending", couchdb_url)
            log.info(f"Reset stale run {doc['_id']} to 'pending'")
        except Exception as e:
            log.error(f"Failed to reset stale run {doc['_id']}: {e}")


# ------------------------
# Pipeline steps
# ------------------------


async def run_pipeline(
    run_path: Path, archive_staging_path: Path, tar_exclusions: list[str]
):
    """Run the tar + gpg pipeline for a given run directory and return the path to the output GPG file."""
    output_file = archive_staging_path / f"{run_path.name}.tar.gpg"
    key_file = archive_staging_path / f"{run_path.name}.key"
    key_file.parent.mkdir(parents=True, exist_ok=True)
    log.info(f"Generating encryption key for {run_path.name}")
    gen_key_cmd = ["gpg", "--gen-random", "1", "256"]
    proc = await asyncio.create_subprocess_exec(
        *gen_key_cmd,
        stdout=asyncio.subprocess.PIPE,
    )
    key_data, _ = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"GPG key generation failed with code {proc.returncode}")
    key_file.write_bytes(key_data)
    log.debug(f"Encryption key written to {key_file}")

    tar_cmd = ["tar"]
    for excl in tar_exclusions:
        tar_cmd.extend(["--exclude", excl])
    tar_cmd.extend(["-cf", "-", "-C", str(run_path.parent), run_path.name])
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
    log.info(f"Starting tar+gpg pipeline: {run_path} -> {output_file}")
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
        if gpg.returncode is None:
            gpg.terminate()
            await gpg.wait()
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

    log.info(f"Pipeline complete: {output_file} ({output_file.stat().st_size} bytes)")
    return output_file


async def validate_gpg(file_path: Path):
    """Validate the GPG file by attempting to decrypt it (without actually writing the output)."""
    log.info(f"Validating encrypted archive: {file_path.name}")
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

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )

    await proc.wait()

    if proc.returncode != 0:
        raise RuntimeError("GPG validation failed")

    log.info(f"Validation successful: {file_path.name}")


async def encrypt_and_archive_key(key_file: Path, gpg_receiver: str):
    """Encrypt the run key and archive it to ~/run_keys/."""
    keys_dir = Path.home() / "run_keys"
    keys_dir.mkdir(parents=True, exist_ok=True)

    encrypted_key_path = keys_dir / (key_file.name + ".gpg")
    log.info(f"Encrypting key {key_file.name} for recipient {gpg_receiver}")
    cmd = [
        "gpg",
        "--encrypt",
        "-r",
        gpg_receiver,
        "--batch",
        "--output",
        str(encrypted_key_path),
        str(key_file),
    ]

    proc = await asyncio.create_subprocess_exec(*cmd)
    await proc.wait()

    if proc.returncode != 0:
        raise RuntimeError(f"Failed to encrypt key: {proc.returncode}")

    log.info(f"Encrypted key generated in {encrypted_key_path}")
    # delete the unencrypted key file after encryption
    key_file.unlink()


# ------------------------
# Worker
# ------------------------


async def process_run(
    session,
    doc,
    couchdb_url,
    archive_staging_path: Path,
    tar_exclusions: list[str],
    gpg_receiver: str,
    semaphore: asyncio.Semaphore,
):
    """Process a single run: tar, encrypt, validate, encrypt key, and update status in CouchDB."""
    async with semaphore:
        log.info(f"Starting processing of run {doc['_id']} at {doc['path']}")
        run_path = Path(doc["path"])
        run_id = doc["_id"]

        # Track all generated files for cleanup on failure or cancellation
        output_file = None
        key_file = archive_staging_path / f"{run_path.name}.key"  # created by run_pipeline
        encrypted_key_file = None

        try:
            output_file = await run_pipeline(run_path, archive_staging_path, tar_exclusions)
            await validate_gpg(output_file)

            # Encrypt and archive the key
            encrypted_key_file = Path.home() / "run_keys" / f"{run_id}.key.gpg"
            await encrypt_and_archive_key(key_file, gpg_receiver)

            await update_status(session, doc, "encrypted", couchdb_url)
            log.info(f"Completed {run_path}")

        except asyncio.CancelledError:
            log.warning(f"Run {run_path} was cancelled, cleaning up partial files")
            for f in (output_file, key_file, encrypted_key_file):
                if f and f.exists():
                    f.unlink()
                    log.info(f"Cleaned up {f}")
            raise  # propagate; run stays 'processing' until next startup reset

        except Exception as e:
            log.exception(f"Failed {run_path}: {e}")

            # Clean up generated files on failure
            for f in (output_file, key_file, encrypted_key_file):
                if f and f.exists():
                    f.unlink()
                    log.info(f"Cleaned up {f}")

            await handle_failure(session, doc, couchdb_url)


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
            log.debug(f"Skipping non-directory {sequencer_dir}")
            continue

        for run_dir in sequencer_dir.iterdir():
            if not run_dir.is_dir():
                log.debug(f"Skipping non-directory {run_dir}")
                continue
            if run_dir.name in ignore_dirs:
                log.debug(f"Skipping ignored directory {run_dir}")
                continue

            # Check if the final file exists
            if not (run_dir / final_file).exists():
                log.debug(
                    f"Skipping {run_dir.name}: final file '{final_file}' not present"
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
                        log.info(
                            f"Added new run to CouchDB: {doc['_id']} at {doc['path']}"
                        )
                    elif resp.status == 409:
                        log.debug(
                            f"Run {doc['_id']} already exists in CouchDB, skipping"
                        )
                    else:
                        text = await resp.text()
                        log.error(
                            f"Failed to add run {doc['_id']} to CouchDB: {resp.status} {text}"
                        )
            except aiohttp.ClientError as e:
                log.error(f"Network error adding run {doc['_id']} to CouchDB: {e}")


# ------------------------
# Main loop
# ------------------------


async def main(conf: dict):
    """Main loop: scan for new runs and process pending runs."""
    shutdown_event = asyncio.Event()
    active_tasks: set[asyncio.Task] = set()

    loop = asyncio.get_running_loop()

    def force_cancel():
        log.warning(
            f"Received second signal, cancelling {len(active_tasks)} active task(s) immediately..."
        )
        for t in active_tasks:
            t.cancel()

    def graceful_shutdown(sig):
        log.warning(
            f"Received {sig.name}, shutting down after current work completes..."
            " (press Ctrl+C again to cancel immediately)"
        )
        shutdown_event.set()
        # Re-register SIGINT so a second Ctrl+C triggers force-cancel
        loop.add_signal_handler(signal.SIGINT, force_cancel)

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: graceful_shutdown(s))

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)
    statusdb_conf = conf["statusdb"]
    sequencing_path = Path(conf["sequencing_path"])
    archive_staging_path = Path(conf["archive_staging_path"])
    ignore_dirs = conf["ignore"]
    tar_exclusions = conf["tar_exclusions"]
    gpg_receiver = conf["gpg_receiver"]
    final_file = ".metadata_rsync_exitcode"

    if not sequencing_path.is_dir():
        raise RuntimeError(
            f"sequencing_path does not exist or is not a directory: {sequencing_path}"
        )

    if not archive_staging_path.is_dir():
        raise RuntimeError(
            f"archive_staging_path does not exist or is not a directory: {archive_staging_path}"
        )

    couchdb_url = build_couchdb_url(statusdb_conf)
    auth = aiohttp.BasicAuth(statusdb_conf["username"], statusdb_conf["password"])
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(auth=auth, timeout=timeout) as session:
        await reset_stale_processing_runs(session, couchdb_url)
        while not shutdown_event.is_set():
            try:
                await scan_for_new_runs(
                    session, couchdb_url, sequencing_path, ignore_dirs, final_file
                )
            except Exception as e:
                log.error(f"Error scanning for new runs: {e}")

            # fetch pending jobs
            docs = await fetch_pending_runs(session, couchdb_url)

            log.info(f"Found {len(docs)} pending runs in CouchDB")

            # Process runs, picking up new ones as slots become available
            docs_queue = list(docs)

            while docs_queue or active_tasks:
                if shutdown_event.is_set():
                    break

                # Fill available slots with new jobs
                while len(active_tasks) < MAX_CONCURRENT_JOBS and docs_queue:
                    doc = docs_queue.pop(0)
                    log.info(f"Found pending run: {doc['_id']} at {doc['path']}")
                    claimed = await claim_run(session, doc, couchdb_url)
                    if claimed:
                        task = asyncio.create_task(
                            process_run(
                                session,
                                doc,
                                couchdb_url,
                                archive_staging_path,
                                tar_exclusions,
                                gpg_receiver,
                                semaphore,
                            )
                        )
                        active_tasks.add(task)
                        task.add_done_callback(active_tasks.discard)

                if not active_tasks:
                    break

                # Wait for any task to complete so we can pick up the next one
                done, pending = await asyncio.wait(
                    active_tasks, return_when=asyncio.FIRST_COMPLETED
                )
                log.info(f"Completed {len(done)} task(s), {len(pending)} still running")

            if shutdown_event.is_set():
                break

            log.info("Sleeping before next scan...")
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass  # normal wake-up, continue the loop

    log.info("Shutdown complete")


def cli():
    """Entry point for the archive encryption script."""
    parser = argparse.ArgumentParser(
        description="Encrypt runs and update CouchDB status"
    )
    parser.add_argument(
        "-c",
        "--config-file",
        default=str(CONFIG_DEFAULT_PATH),
        help="Path to YAML config file containing statusdb credentials and URL",
    )
    args = parser.parse_args()

    # Load config and set up logging before starting main loop
    conf = load_config(Path(args.config_file))
    log_file = conf.get("log_file")
    if log_file:
        log_level = conf.get("log_level", "INFO")
        init_logger_file(log_file, log_level)

    log.info(f"Starting encryption worker with config: {args.config_file}")
    asyncio.run(main(conf))
