import argparse
import json
import logging
import logging.config
import os
import sentry_sdk

import ocdskingfisherprocess.cli.util
import ocdskingfisherprocess.config
import ocdskingfisherprocess.signals.signals
from ocdskingfisherprocess.database import DataBase


def main():

    config = ocdskingfisherprocess.config.Config()
    config.load_user_config()

    if config.sentry_dsn:
        sentry_sdk.init(config.sentry_dsn)

    database = DataBase(config)

    ocdskingfisherprocess.signals.signals.setup_signals(config, database)

    logging_config_file_full_path = os.path.expanduser('~/.config/ocdskingfisher-process/logging.json')
    if os.path.isfile(logging_config_file_full_path):
        with open(logging_config_file_full_path) as f:
            logging.config.dictConfig(json.load(f))

    logger = logging.getLogger('ocdskingfisher.cli')

    parser = argparse.ArgumentParser()
    parser.add_argument("--quiet", help="remove output",
                        action="store_true")

    subparsers = parser.add_subparsers(dest='subcommand')

    commands = ocdskingfisherprocess.cli.util.gather_cli_commands_instances(config=config, database=database)

    for command in commands.values():
        command.configure_subparser(subparsers.add_parser(command.command))

    args = parser.parse_args()

    if args.subcommand and args.subcommand in commands.keys():
        logger.info("Running CLI command " + args.subcommand + " " + repr(args))
        commands[args.subcommand].run_command(args)
    else:
        print("Please select a subcommand (try --help)")
