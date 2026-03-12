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
    def __init__(self, run_dir, config):
        self.run_dir = run_dir
        self.run_id = os.path.basename(run_dir)
        self.run_type = self._set_run_type()
        self.sequencer_specific_settings = config.get(
            "sequencer_specific_settings", {}
        ).get(self.run_type, {})
        self.tar_exclude_patterns = self.sequencer_specific_settings.get(
            "tar_exclude_patterns", []
        )
        self.final_file = os.path.join(
            self.run_dir, self.sequencer_specific_settings.get("final_file", None)
        )

    def _set_run_type(self):
        for run_type, run_pattern in RUN_TYPES.items():
            if re.match(run_pattern, self.run_id):
                return run_type
        raise ValueError(f"Run {self.run_id} does not match any known run type.")


def encrypt_run(run_dir, config):
    run = Run(run_dir, config)
    logger.info(
        f"Processing run {run.run_id} of type {run.run_type}"
    )


def encrypt_runs(config, given_run=None):
    """Tar and encrypt run directories based on the provided configuration."""
    start_time = time.time()
    if given_run:
        logger.info(f"Encrypting specific run: {given_run}")
        run_dir = get_run_dir(given_run)
        encrypt_run(run_dir, config)
        end_time = time.time()
    else:
        logger.info("Archiving all runs as per configuration")
        sequencing_dirs = config.get("sequencing_dirs", {})
        for sequencing_dir in sequencing_dirs:
            for run_dir in find_runs(sequencing_dir, config.get("ignore_folders", [])):
                logger.info(f"Processing directory: {run_dir}")
                try:
                    encrypt_run(run_dir, config)
                except Exception as e:
                    logger.error(f"Error processing run {run_dir}: {e}")
                    continue  # Continue with the next run
        end_time = time.time()
    elapsed_time = end_time - start_time
    logger.info(f"Data transfer process completed in {elapsed_time:.2f} seconds.")


def upload_runs(config, given_run=None):
    """Upload encrypted runs to PDC. Placeholder for actual upload logic."""
    pass
