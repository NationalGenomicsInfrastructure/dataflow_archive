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
    1. Looking for tar.gpg files in destination_path
    2. Checking that the corresponding key files exist in ~/run_keys/
    3. Checking that they have "encrypted" status in statusdb
    4. Checking that they are not in PDC
    5. Returning list of runs to archive
    """
    runs_to_archive = []
    destination_path = Path(conf["destination_path"])

    try:
        # Find all tar.gpg files in destination_path
        for gpg_file in destination_path.glob("*.tar.gpg"):
            run_id = gpg_file.name.replace(".tar.gpg", "")

            # Check that the corresponding key file exists in ~/run_keys/
            key_file = Path.home() / "run_keys" / f"{run_id}.key.gpg"
            if not key_file.exists():
                log.debug(f"Skipping {run_id}: key file not found at {key_file}")
                continue

            # Check that the run has "encrypted" status in statusdb
            try:
                resp = requests.get(f"{couchdb_url}/{run_id}", auth=auth, timeout=30)

                if resp.status_code == 404:
                    log.debug(f"Skipping {run_id}: not found in statusdb")
                    continue

                if resp.status_code != 200:
                    log.warning(
                        f"Failed to fetch status for {run_id}: {resp.status_code}"
                    )
                    continue

                doc = resp.json()
                status = doc.get("status")

                # Check that it's encrypted (not archived, not failed, etc.)
                if status != "encrypted":
                    log.debug(
                        f"Skipping {run_id}: status is '{status}', not 'encrypted'"
                    )
                    continue

                # Check that it's not already in PDC (check if in_pdc field exists and is True)
                if doc.get("in_pdc", False):
                    log.debug(f"Skipping {run_id}: already marked as in PDC")
                    continue

                log.info(f"Found run eligible for archiving: {run_id}")
                runs_to_archive.append(run_id)

            except requests.RequestException as e:
                log.error(f"Error checking status for {run_id}: {e}")
                continue

    except Exception as e:
        log.error(f"Error collecting runs to archive: {e}")

    return runs_to_archive


def upload_to_pdc(run, conf):
    # Use dsmc to upload file and key files to PDC
    # Return True if upload successful, False otherwise
    ## dsmc archive gpg file (sleep 15)
    ## dsmc archive key file (sleep 5)
    ## Check that files are archived
    return True


def main(conf):
    statusdb_conf = conf["statusdb"]
    couchdb_url = build_couchdb_url(statusdb_conf)
    auth = (statusdb_conf["username"], statusdb_conf["password"])

    # Collect runs to archive
    runs_to_archive = collect_runs_to_archive(conf, couchdb_url, auth)
    # For each run:
    for run in runs_to_archive:
        if upload_to_pdc(run, conf):
            log.info(f"Successfully archived run {run}")
            # Update statusdb to "archived"
            # Remove local tar.gpg and key files if upload successful
        else:
            log.error(f"Failed to archive run {run}")
            # Update statusdb to "archive_failed"


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
