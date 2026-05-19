import argparse
from pathlib import Path

from dataflow_archive.log import ROOT_LOG as log
from dataflow_archive.log import init_logger_file
from dataflow_archive.utils.utils import CONFIG_DEFAULT_PATH, load_config


def main():
    print("Hello, World!")


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
    main()