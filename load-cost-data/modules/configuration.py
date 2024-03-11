# import configparser
import tomllib
# import json
# import logging

# from typing import Dict
config = None
db_config = None


def read_config_file(file_path: str):
    with open(file_path, mode="rb") as file:
        local_config = tomllib.load(file)
    return local_config


def process_configuration(file_path: str):
    global config, db_config

    config = read_config_file(file_path)
    db_config = config["DB_CONFIG"]
    return

def print_configuration():
    global config, db_config

    print(f"\nConfiguration:")
    # print(f"\tdb_config:     {db_config}")
    print(f"\thost:     {db_config['host']}")
    print(f"\tport:     {db_config['port']}")
    print(f"\tdatabase: {db_config['database']}")
    print(f"\tuser:     {db_config['user']}")
    print(f"\tpassword: {db_config['password']}")
