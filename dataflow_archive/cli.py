import logging
import os

import click
import yaml

from dataflow_archive import log
from dataflow_archive.dataflow_archive import encrypt_runs, upload_runs

logger = logging.getLogger(__name__)


def load_config(config_file_path):
    with open(config_file_path) as file:
        config = yaml.safe_load(file)
    return config


@click.group()
@click.version_option()
@click.option(
    "-c",
    "--config-file",
    default=os.path.join(os.environ["HOME"], ".df_archive/df_archive.yaml"),
    envvar="ARCHIVE_CONFIG",
    type=click.File("r"),
    help="Path to dataflow_archive configuration file. Defaults to ~/.df_archive/df_archive.yaml",
)
@click.option(
    "-r",
    "--run",
    required=False,
    type=str,
    default=None,
    help="Only archive a specific run, e.g., 20250528_LH00217_0219_A22TT52LT4.",
)
@click.pass_context
def cli(ctx, config_file, run):
    """
    Command line interface for dataflow_archive.
    """
    config = load_config(config_file.name)
    ctx.obj = {"config": config, "run": run}
    log_file = config.get("log", {}).get("file", None)
    if log_file:
        level = config.get("log").get("log_level", "INFO")
        log.init_logger_file(log_file, level)


@cli.command()
@click.pass_context
def encrypt(ctx):
    """Tar and encrypt run directories based on the provided configuration."""
    config = ctx.obj.get("config")
    run = ctx.obj.get("run")
    encrypt_runs(config, run)


@cli.command()
@click.pass_context
def upload(ctx):
    """Upload enctypted runs to PDC."""
    config = ctx.obj.get("config")
    run = ctx.obj.get("run")
    upload_runs(config, run)
