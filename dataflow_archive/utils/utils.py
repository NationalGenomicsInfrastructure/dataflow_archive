import os
from pathlib import Path
from urllib.parse import urlparse

import yaml

CONFIG_DEFAULT_PATH = Path(
    os.environ.get(
        "ARCHIVE_CONFIG",
        os.path.join(os.path.expanduser("~"), "conf/df_archive.yaml"),
    )
).expanduser()

def load_config(config_path: Path):
    with config_path.open() as file:
        config = yaml.safe_load(file)

    if not isinstance(config, dict):
        raise RuntimeError("Config file must contain a mapping")

    log_file = config.get("log_file")
    if log_file and not isinstance(log_file, str):
        raise RuntimeError("Config entry 'log_file' must be a string")

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

    tar_exclusions = config.get("tar_exclusions", [])
    if tar_exclusions is None:
        tar_exclusions = []
    if not isinstance(tar_exclusions, list):
        raise RuntimeError("Config entry 'tar_exclusions' must be a list")

    gpg_receiver = config.get("gpg_receiver")
    if not gpg_receiver:
        raise RuntimeError("Missing required config entry: gpg_receiver")

    return {
        "log_file": log_file,
        "statusdb": statusdb,
        "sequencing_path": sequencing_path,
        "destination_path": destination_path,
        "ignore": ignore_list,
        "tar_exclusions": tar_exclusions,
        "gpg_receiver": gpg_receiver,
    }
    
def build_couchdb_url(statusdb: dict) -> str:
    raw_url = statusdb["url"].strip()
    if not raw_url:
        raise RuntimeError("statusdb.url must not be empty")

    if not urlparse(raw_url).scheme:
        raw_url = f"https://{raw_url}"

    database = statusdb["database"].strip().lstrip("/")
    return f"{raw_url.rstrip('/')}/{database}"