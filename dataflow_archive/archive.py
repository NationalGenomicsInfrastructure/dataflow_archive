import argparse
from pathlib import Path

from dataflow_archive.log import ROOT_LOG as log
from dataflow_archive.log import init_logger_file
from dataflow_archive.utils.utils import CONFIG_DEFAULT_PATH, load_config


def collect_runs_to_archive(conf):
    # Look for tar.gpg files in destination_path
    # Check that the corresponding key files exist in ~/run_keys/
    # Check that they have "encrypted" status in statusdb.
    # Check that they are not in PDC
    # Return list of runs to archive.
    return []


def upload_to_pdc(run):
    # Use dsmc to upload file and key files to PDC
    # Return True if upload successful, False otherwise
    ## dsmc archive gpg file (sleep 15)
    ## dsmc archive key file (sleep 5)
    ## Check that files are archived
    return True


def main(conf):
    print("Hello, World!")
    # Collect runs to archive
    runs_to_archive = collect_runs_to_archive(conf)
    # For each run:
    for run in runs_to_archive:
        if upload_to_pdc(run):
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
