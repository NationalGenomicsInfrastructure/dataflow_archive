# dataflow_archive

Two scripts for archiving sequencing runs to long-term tape storage, tracking progress via CouchDB.

## Overview

### `dataflow_encrypt` — encryption worker

An async worker that runs in a continuous loop:

1. **Scan** — walks the sequencing directory tree looking for completed runs (indicated by the presence of a sentinel file)
2. **Register** — new runs are added to CouchDB with status `pending`
3. **Claim** — the worker atomically claims a pending run to prevent other workers from processing it simultaneously
4. **Archive** — the run directory is packed with `tar` and symmetrically encrypted with GPG using a randomly generated 256-bit key
5. **Validate** — the encrypted archive is test-decrypted to verify integrity
6. **Secure key** — the encryption key is asymmetrically encrypted to a configured GPG recipient and stored in `~/run_keys/`, then the plaintext key is deleted
7. **Update status** — the CouchDB document is updated to `encrypted`

On failure a run is reset to `pending` for retry. After 3 failed attempts it is marked `failed`.

### `dataflow_archive` — PDC upload script

A script that picks up runs with status `encrypted` and uploads them to PDC (tape storage):

1. **Collect** — fetches runs with status `encrypted` from CouchDB and checks that both the `.tar.gpg` and `.key.gpg` files exist locally
2. **Claim** — sets status to `archiving` before touching PDC to prevent duplicate uploads on re-runs
3. **Upload** — uploads the `.tar.gpg` and `.key.gpg` files to PDC using `dsmc`, then verifies both are present in PDC
4. **Update status** — sets status to `archived` in CouchDB
5. **Clean up** — deletes the local `.tar.gpg` and `.key.gpg` files

Runs that get stuck in `archiving` (e.g. due to a failed status update after a successful upload) are skipped on subsequent runs and require manual intervention.

## Requirements

- Python ≥ 3.14
- `gpg` available on `PATH`
- `tar` available on `PATH`
- `dsmc` available on `PATH` (for PDC uploads)
- A running CouchDB instance with the following views:
  - `_design/lookup/_view/pending_runs`
  - `_design/lookup/_view/encrypted_runs`
- The GPG recipient key imported into the worker's keyring

## Installation

```bash
pip install -e .
```

## Configuration

Both scripts read the same YAML config file. The default path is `~/conf/df_archive.yaml`, overridable with the `-c` flag.

```yaml
statusdb:
  username: myuser
  password: mypassword
  url: url
  database: archiving_status

sequencing_path: /data/sequencing    # top-level directory; subdirs are per-sequencer (encrypt only)
destination_path: /data/archives     # where .tar.gpg and .key files are written

gpg_receiver: user       # GPG key ID or email for key encryption (encrypt only)

ignore:                              # optional: run directory names to skip (encrypt only)
  - nosync
  - transferring

tar_exclusions:                      # optional: patterns passed to tar --exclude (encrypt only)
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

### Encryption worker

```bash
# Use the default config path
dataflow_encrypt

# Specify a config file explicitly
dataflow_encrypt -c /path/to/config.yaml
```

#### Shutdown

| Input | Behaviour |
|-------|-----------|
| **Ctrl+C** (first) | Graceful — finishes any runs currently in progress, then exits |
| **Ctrl+C** (second) | Immediate — cancels in-progress tasks, cleans up partial files, then exits |
| **SIGTERM** | Same as first Ctrl+C |

### PDC upload

```bash
# Use the default config path
dataflow_archive

# Specify a config file explicitly
dataflow_archive -c /path/to/config.yaml
```

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

| Status | Set by | Meaning |
|--------|--------|---------|
| `pending` | `dataflow_encrypt` | Waiting to be processed (or reset after a recoverable failure) |
| `processing` | `dataflow_encrypt` | Currently being encrypted by a worker |
| `encrypted` | `dataflow_encrypt` | Successfully encrypted and validated; ready for PDC upload |
| `failed` | `dataflow_encrypt` | Failed more than 3 times; requires manual intervention |
| `archiving` | `dataflow_archive` | PDC upload claimed; upload in progress or stuck (requires manual check if stale) |
| `archived` | `dataflow_archive` | Successfully uploaded to PDC; local files deleted |
| `archiving_failed` | `dataflow_archive` | Upload to PDC failed; local files NOT deleted |

## Output files

| File | Location | Description |
|------|----------|-------------|
| `<run>.tar.gpg` | `destination_path/` | AES-256 symmetrically encrypted tar archive |
| `<run>.key` | `destination_path/` | Plaintext encryption key (temporary; deleted after key encryption step) |
| `<run>.key.gpg` | `~/run_keys/` | Encryption key, asymmetrically encrypted to `gpg_receiver` |

## Development

Install dev dependencies:

```bash
pip install -e ".[dev]"
```

Run linting:

```bash
ruff check .
```
