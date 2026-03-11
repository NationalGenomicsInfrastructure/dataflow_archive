# Dataflow Archive

A Python application for automating the archiving of sequencing data.

## Overview

Dataflow Archive monitors sequencing run directories and orchestrates the encryption and archiving of sequencing data. It supports multiple sequencer types (Illumina, Oxford Nanopore and Element) and logs archiving completion in a CouchDB-based status database.

## Supported Sequencers

- **Illumina**: NextSeq, MiSeqi100, NovaSeqXPlus, MiSeq
- **Oxford Nanopore (ONT)**: PromethION, MinION
- **Element**: AVITI

## Installation

### Requirements

- Python 3.14+
- Dependencies listed in [pyproject.toml](pyproject.toml):
  - PyYAML
  - click
  - ibmcloudant
- [run-one](https://launchpad.net/ubuntu/+source/run-one)

### Setup suggestions

1. Clone the repository:

```bash
git clone <repository-url>
cd dataflow_archive
```

2. Install the package:

```bash
pip install -e .
```

Or with development dependencies:

```bash
pip install -e ".[dev]"
```

## Usage

### Command Line Interface

```bash
dataflow_archive [OPTIONS] COMMAND
```

#### Options

- `-c, --config-file PATH`: Path to configuration YAML file. Defaults to `~/.df_archive/df_archive.yaml`. Can also be set via `ARCHIVE_CONFIG` environment variable.
- `-r, --run RUN_ID`: Archive a specific run (e.g., `20250528_LH00217_0219_A22TT52LT4`).
- `--version`: Show version and exit.

#### Commands

- `encrypt`: Tar and encrypt run directories based on the provided configuration.
- `upload`: Upload encrypted runs to PDC.

#### Examples

```bash
# Encrypt all runs (uses configuration for sequencing directories)
dataflow_archive encrypt

# Encrypt a specific run
dataflow_archive --run 20250528_LH00217_0219_A22TT52LT4 encrypt

# Upload all encrypted runs
dataflow_archive upload

# Upload a specific run
dataflow_archive --run 20250528_LH00217_0219_A22TT52LT4 upload

# Use a custom config file
dataflow_archive --config-file /path/to/config.yaml encrypt
```

## Configuration

Create a YAML configuration file with the following structure:

```yaml
log:
  file: /path/to/dataflow_archive.log

statusdb:
  username: couchdb_user
  password: couchdb_password
  url: couchdb.host.com
  database: sequencing_runs

data_dirs:
  - /sequencing/MiSeqi100
  - /sequencing/NextSeq/Runs
  - /sequencing/PromethION
ignore_folders:
  - nosync
archive_dir: /sequencing/archiving
sequencer_specific_settings:
  Illumina:
    tar_exclude:
      - Demultiplex*
    final_file:
      - CopyComplete.txt
  ONT:
    tar_exclude:
      - Pod5*
    final_file:
      - final_summary.txt
  # ... additional sequencer configurations
```

## Assumptions

- Run directories are named according to sequencer-specific ID formats (defined in RUN_TYPES)
- Final completion is indicated by the presence of a sequencer-specific final file (e.g. `CopyComplete.txt` for Illumina)
- CouchDB is accessible and the database exists

### Status Files

The logic of the script relies on the following status files:

- `run.inal_file` - The final file written by each sequencing machine. Used to indicate when the sequencing has completed.

## Development

### Running Tests

```bash
pytest
```

With coverage:

```bash
pytest --cov --cov-branch
```

### Code Quality

Run linting and formatting checks:

```bash
ruff check .
ruff format --check .
```

### Project Structure

```
dataflow_archive/
├── cli.py                 # Command-line interface
├── dataflow_archive.py    # Main transfer orchestration
├── log/                   # Logging utilities
├── utils/                 # Utility modules (filesystem, statusdb)
└── tests/                 # Unit tests
```

### Adding a new sequencer

To add support for a new sequencer, add the following to dataflow_transfer:

1. Update the RUN_TYPES dictionary to cover the format of the new run folder name
4. Add entries for the sequencer in the config file (`data_dirs` and any nessesary changes/additions to `sequencer_specific_settings`)
