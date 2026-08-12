import argparse
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from time import sleep

import requests

from dataflow_archive.log import ROOT_LOG as log
from dataflow_archive.log import init_logger_file
from dataflow_archive.utils.utils import (
    CONFIG_DEFAULT_PATH,
    build_couchdb_url,
    load_config,
)


def collect_runs_to_archive(conf, couchdb_url, auth):
    """
    Collect runs to archive by:
    1. Fetching encrypted runs from statusdb view
    2. Looking for tar.gpg files in archive_staging_path
    3. Checking that the corresponding key files exist in ~/run_keys/
    4. Returning list of runs to archive
    """
    runs_to_archive = []
    archive_staging_path = Path(conf["archive_staging_path"])

    try:
        view_url = (
            f"{couchdb_url}/_design/lookup/_view/encrypted_runs?include_docs=true"
        )
        resp = requests.get(view_url, auth=auth, timeout=30)
        if resp.status_code != 200:
            log.error(f"Failed to fetch encrypted runs view: {resp.status_code}")
            return runs_to_archive

        rows = resp.json().get("rows", [])
        encrypted_runs = {}
        for row in rows:
            doc = row.get("doc", {})
            run_id = doc.get("_id")
            if run_id:
                encrypted_runs[run_id] = doc

        # Find all tar.gpg files in archive_staging_path
        for gpg_file in archive_staging_path.glob("*.tar.gpg"):
            run_id = gpg_file.name.replace(".tar.gpg", "")

            # Check that the corresponding key file exists in ~/run_keys/
            key_file = Path.home() / "run_keys" / f"{run_id}.key.gpg"
            if not key_file.exists():
                log.debug(f"Skipping {run_id}: key file not found at {key_file}")
                continue

            if not encrypted_runs.get(run_id):
                log.debug(f"Skipping {run_id}: not present in encrypted_runs view")
                continue

            log.info(f"Found run eligible for archiving: {run_id}")
            runs_to_archive.append(
                {"run_id": run_id, "gpg_file": gpg_file, "key_file": key_file}
            )

    except (requests.RequestException, ValueError) as e:
        log.error(f"Error collecting runs to archive: {e}")

    return runs_to_archive


def upload_to_pdc(run, gpg_file, key_file):
    """Upload the run's tar.gpg and key files to PDC using dsmc."""
    try:
        subprocess.run(["dsmc", "archive", str(gpg_file)], check=True)
    except subprocess.CalledProcessError as e:
        log.error(f"Error uploading archive file for run {run}: {e}")
        return False

    sleep(15)  # ensure file is fully written and closed before next upload

    try:
        subprocess.run(["dsmc", "archive", str(key_file)], check=True)
    except subprocess.CalledProcessError as e:
        log.error(f"Error uploading key file for run {run}: {e}")
        return False

    sleep(5)  # ensure file is fully written and closed before verification

    try:
        result = subprocess.run(
            ["dsmc", "query", "archive", str(gpg_file)],
            check=True,
            capture_output=True,
            text=True,
        )
        if "Archived" not in result.stdout:
            log.error(f"Archive file for run {run} not found in PDC after upload")
            return False
    except subprocess.CalledProcessError as e:
        log.error(f"Error querying archive file for run {run}: {e}")
        return False

    try:
        result = subprocess.run(
            ["dsmc", "query", "archive", str(key_file)],
            check=True,
            capture_output=True,
            text=True,
        )
        if "Archived" not in result.stdout:
            log.error(f"Key file for run {run} not found in PDC after upload")
            return False
    except subprocess.CalledProcessError as e:
        log.error(f"Error querying key file for run {run}: {e}")
        return False

    return True


def update_status(run, status, couchdb_url, auth, max_retries=3, extra_fields=None):
    """Update the status of a run in CouchDB.

    Retries up to max_retries times. On a 409 conflict the document is
    refetched before each retry so the latest _rev is always used.
    """
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(f"{couchdb_url}/{run}", auth=auth, timeout=30)
            if response.status_code != 200:
                log.error(
                    f"Failed to fetch document for run {run}: {response.status_code}"
                )
                continue

            doc = response.json()
            doc["status"] = status
            if extra_fields:
                doc.update(extra_fields)
            doc["updated_at"] = datetime.now(timezone.utc).isoformat()

            response = requests.put(
                f"{couchdb_url}/{run}", json=doc, auth=auth, timeout=30
            )
            if response.status_code in (200, 201, 202):
                log.debug(f"Updated status for run {run} to '{status}'")
                return True
            elif response.status_code == 409:
                log.warning(
                    f"Conflict updating status for run {run} "
                    f"(attempt {attempt}/{max_retries}), retrying..."
                )
                continue
            else:
                log.error(
                    f"Failed to update status for run {run}: {response.status_code}"
                )
                return False

        except requests.RequestException as e:
            log.error(f"Network error updating status for run {run}: {e}")
            if attempt < max_retries:
                sleep(2)
                continue
            return False

    log.error(
        f"Failed to update status for run {run} to '{status}' "
        f"after {max_retries} attempts"
    )
    return False


def delete_archived_files(run, gpg_file, key_file):
    """Delete local tar.gpg and key files for the run after successful upload to PDC"""
    try:
        if gpg_file.exists():
            gpg_file.unlink()
            log.info(f"Deleted local archive file: {gpg_file}")
        if key_file.exists():
            key_file.unlink()
            log.info(f"Deleted local key file: {key_file}")
    except Exception as e:
        log.error(f"Error deleting archived files for run {run}: {e}")
        return False
    return True


def main(conf):
    statusdb_conf = conf["statusdb"]
    couchdb_url = build_couchdb_url(statusdb_conf)
    auth = (statusdb_conf["username"], statusdb_conf["password"])

    runs_to_archive = collect_runs_to_archive(conf, couchdb_url, auth)
    for run in runs_to_archive:
        run_id = run["run_id"]
        gpg_file = run["gpg_file"]
        key_file = run["key_file"]

        # Phase 1: claim the run by marking it as 'archiving' before touching PDC.
        # This prevents a re-run from uploading the same run again if status update
        # failed after a previous successful upload.
        if not update_status(run_id, "archiving", couchdb_url, auth):
            log.warning(
                f"Could not set status to 'archiving' for run {run_id}, skipping"
            )
            continue

        # Phase 2: upload and then record the outcome.
        if upload_to_pdc(run_id, gpg_file, key_file):
            log.info(f"Successfully uploaded run {run_id} to PDC")
            pdc_archived = datetime.now(timezone.utc).isoformat()
            if update_status(
                run_id,
                "archived",
                couchdb_url,
                auth,
                extra_fields={"pdc_archived": pdc_archived},
            ):
                if not delete_archived_files(run_id, gpg_file, key_file):
                    log.warning(f"Failed to delete local files for run {run_id}")
            else:
                log.warning(
                    f"Could not set status to 'archived' for run {run_id}, skipping"
                )
                continue
        else:
            log.error(f"Failed to archive run {run_id}")
            update_status(run_id, "archiving_failed", couchdb_url, auth)


def cli():
    """Entry point for the archive uploading script."""
    parser = argparse.ArgumentParser(
        description="Archive runs and update CouchDB status"
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

    log.info(f"Starting archive uploading with config: {args.config_file}")
    main(conf)
