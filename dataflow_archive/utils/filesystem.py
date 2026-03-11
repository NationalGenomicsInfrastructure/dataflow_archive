import logging
import os
import subprocess

logger = logging.getLogger(__name__)


def get_run_dir(run):
    """Get absolute path of the run directory."""
    if os.path.isabs(run) and os.path.isdir(run):
        return run
    elif os.path.isdir(run):
        return os.path.abspath(run)
    else:
        raise ValueError(f"Provided run path is not a valid directory: {run}")


def find_runs(base_dir, ignore_folders=[]):
    """Find run directories in the given base directory, ignoring specified folders."""
    runs = []
    for entry in os.listdir(base_dir):
        entry_path = os.path.join(base_dir, entry)
        if os.path.isdir(entry_path) and entry not in ignore_folders:
            runs.append(entry_path)
    return runs


def rsync_is_running(src, dst):
    """Check if rsync is already running for given src and destination."""
    pattern = f"rsync.*{src}.*{dst}"
    try:
        subprocess.check_output(["pgrep", "-f", pattern])
        return True
    except subprocess.CalledProcessError:
        return False


def submit_background_process(command_str: str):
    """Submit a command string as a background process."""

    subprocess.Popen(command_str, stdout=subprocess.PIPE, shell=True)


def check_exit_status(file_path):
    """Check the exit status from a given file.
    Return True if exit code is 0, else False."""
    if os.path.exists(file_path):
        with open(file_path) as f:
            exit_code = f.read().strip()
            if exit_code == "0":
                return True
    return False
