import logging
import os
import re
import time

from dataflow_archive.utils.filesystem import find_runs, get_run_dir

logger = logging.getLogger(__name__)

RUN_TYPES = {
    "Illumina": r"^\d{6,8}_[a-zA-Z\d\-]+_\d{2,}_[AB0][A-Z\d\-]+$",
    "ONT": r"^(\d{8})_(\d{4})_([0-9a-zA-Z]+)_([0-9a-zA-Z]+)_([0-9a-zA-Z]+)$",
    "Element": r"^\d{8}_AV\d{6}_[AB]\d{10}$",
    "Element_teton": r"^\d{8}_AV\d{6}_[AB]P\d*P[12]$",
}


class Run:
    def __init__(self, run_dir):
        self.run_dir = run_dir
        self.run_type = self._set_run_type()

    def _set_run_type(self):
        for run_type, run_pattern in RUN_TYPES.items():
            if re.match(run_pattern, os.path.basename(self.run_dir)):
                return run_type
        raise ValueError(
            f"Run directory {self.run_dir} does not match any known run type."
        )


def process_run(run_dir, config):
    run = Run(run_dir)
    logger.info(f"Processing run {run.run_dir} of type {run.run_type}")


def encrypt_runs(conf, given_run=None):
    start_time = time.time()
    if given_run:
        logger.info(f"Encrypting specific run: {given_run}")
        run_dir = get_run_dir(given_run)
        process_run(run_dir, conf)
        end_time = time.time()
    else:
        logger.info("Archiving all runs as per configuration")

        data_dirs = conf.get("data_dirs", {})
        for data_dir in data_dirs:
            for run_dir in find_runs(data_dir, conf.get("ignore_folders", [])):
                logger.info(f"Processing directory: {run_dir}")
                try:
                    process_run(run_dir, conf)
                except Exception as e:
                    logger.error(f"Error processing run {run_dir}: {e}")
                    continue  # Continue with the next run
        end_time = time.time()
    elapsed_time = end_time - start_time
    logger.info(f"Data transfer process completed in {elapsed_time:.2f} seconds.")


def upload_runs(conf, given_run=None):
    logger.info("Uploading runs to PDC as per configuration")
    # Placeholder for upload logic, similar structure to encrypt_runs
