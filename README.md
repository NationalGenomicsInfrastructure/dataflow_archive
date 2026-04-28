# dataflow_archive

An async worker that scans sequencing run directories, encrypts them with GPG, and tracks progress via CouchDB.

## Overview

The worker runs in a continuous loop:

1. **Scan** — walks the sequencing directory tree looking for completed runs (indicated by the presence of a sentinel file)
2. **Register** — new runs are added to CouchDB with status `pending`
3. **Claim** — the worker atomically claims a pending run to prevent other workers from processing it simultaneously
4. **Archive** — the run directory is packed with `tar` and symmetrically encrypted with GPG using a randomly generated 256-bit key
5. **Validate** — the encrypted archive is test-decrypted to verify integrity
6. **Secure key** — the encryption key is asymmetrically encrypted to a configured GPG recipient and stored in `~/run_keys/`, then the plaintext key is deleted
7. **Update status** — the CouchDB document is updated to `encrypted`

On failure a run is reset to `pending` for retry. After 3 failed attempts it is marked `failed`.

## Requirements

- Python ≥ 3.14
- `gpg` available on `PATH`
- `tar` available on `PATH`
- A running CouchDB instance with a `_design/lookup/_view/runfolder_id` view
- The GPG recipient key imported into the worker's keyring

## Installation

```bash
pip install -e .
```

## Configuration

The worker reads a YAML config file. The default path is `~/.df_archive/df_archive.yaml`, overridable with the `ARCHIVE_CONFIG` environment variable or the `-c` flag.

```yaml
statusdb:
  username: myuser
  password: mypassword
  url: url
  database: archiving_status

sequencing_path: /data/sequencing    # top-level directory; subdirs are per-sequencer
destination_path: /data/archives     # where .tar.gpg and .key files are written

gpg_receiver: user       # GPG key ID or email for key encryption

ignore:                              # optional: run directory names to skip
  - nosync
  - transferring

tar_exclusions:                      # optional: patterns passed to tar --exclude
  - "Demultiplex*"
  - "demux_*"

log_file: /var/log/dataflow_archive.log   # optional: write logs to file
log_level: INFO                           # optional: DEBUG, INFO, WARN, ERROR (default: INFO)
```

### Directory layout expected under `sequencing_path`

```
sequencing_path/
  sequencer_A/
    run_001/
      .metadata_rsync_exitcode    ← sentinel file; run is picked up only when this exists
      ...
    run_002/
      ...
  sequencer_B/
    ...
```

## Usage

```bash
# Use the default config path
dataflow_archive

# Specify a config file explicitly
dataflow_archive -c /path/to/config.yaml
```

### Shutdown

| Input | Behaviour |
|-------|-----------|
| **Ctrl+C** (first) | Graceful — finishes any runs currently in progress, then exits |
| **Ctrl+C** (second) | Immediate — cancels in-progress tasks, cleans up partial files, then exits |
| **SIGTERM** | Same as first Ctrl+C |

## CouchDB document schema

Each run is stored as a document with `_id` set to the run directory name:

```json
{
  "_id": "run_001",
  "path": "/data/sequencing/sequencer_A/run_001",
  "status": "pending",
  "worker_id": "hostname",
  "failure_count": 0,
  "created_at": "2026-04-28T10:00:00+00:00",
  "updated_at": "2026-04-28T10:05:00+00:00"
}
```

| Status | Meaning |
|--------|---------|
| `pending` | Waiting to be processed (or reset after a recoverable failure) |
| `processing` | Currently being archived by a worker |
| `encrypted` | Successfully archived and validated |
| `failed` | Failed more than 3 times; requires manual intervention |

## Output files

| File | Location | Description |
|------|----------|-------------|
| `<run>.tar.gpg` | `destination_path/` | AES-256 symmetrically encrypted tar archive |
| `<run>.key` | `destination_path/` | Plaintext encryption key (deleted after key encryption step) |
| `<run_id>.key.gpg` | `~/run_keys/` | Encryption key, asymmetrically encrypted to `gpg_receiver` |

## Development

Install dev dependencies:

```bash
pip install -e ".[dev]"
```

Run linting:

```bash
ruff check .
```
