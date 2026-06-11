import argparse
from pathlib import Path

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
    2. Looking for tar.gpg files in destination_path
    3. Checking that the corresponding key files exist in ~/run_keys/
    4. Returning list of runs to archive
    """
    runs_to_archive = []
    destination_path = Path(conf["destination_path"])

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

        # Find all tar.gpg files in destination_path
        for gpg_file in destination_path.glob("*.tar.gpg"):
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
            runs_to_archive.append(run_id)

    except (requests.RequestException, ValueError) as e:
        log.error(f"Error collecting runs to archive: {e}")

    return runs_to_archive


def upload_to_pdc(run, conf):
    # Use dsmc to upload file and key files to PDC
    # Return True if upload successful, False otherwise
    ## dsmc archive gpg file (sleep 15)
    ## dsmc archive key file (sleep 5)
    ## Check that files are archived
    return True


def update_status(run, status, couchdb_url, auth):
    # Update statusdb document for run with new status
    # Use CouchDB _update handler or PUT to update document
    pass


def delete_archived_files(run, conf):
    """Delete local tar.gpg and key files for the run after successful upload to PDC"""
    destination_path = Path(conf["destination_path"])
    gpg_file = destination_path / f"{run}.tar.gpg"
    key_file = Path.home() / "run_keys" / f"{run}.key.gpg"

    try:
        if gpg_file.exists():
            gpg_file.unlink()
            log.info(f"Deleted local archive file: {gpg_file}")
        if key_file.exists():
            key_file.unlink()
            log.info(f"Deleted local key file: {key_file}")
    except Exception as e:
        log.error(f"Error deleting archived files for run {run}: {e}")


def main(conf):
    statusdb_conf = conf["statusdb"]
    couchdb_url = build_couchdb_url(statusdb_conf)
    auth = (statusdb_conf["username"], statusdb_conf["password"])

    runs_to_archive = collect_runs_to_archive(conf, couchdb_url, auth)
    for run in runs_to_archive:
        if upload_to_pdc(run, conf):
            log.info(f"Successfully archived run {run}")
            update_status(run, "archived", couchdb_url, auth)
            delete_archived_files(run, conf)
        else:
            log.error(f"Failed to archive run {run}")
            update_status(run, "archiving_failed", couchdb_url, auth)


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
