"""
Stores the helper functions for the main function
like parsing, loading and verifying correctness.

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""

import json
import typing
import yaml

def load_config_file(file_path) -> typing.Dict:
    data = {}
    try:
        if any(file_path.endswith(ext) for ext in [".yaml", ".yml"]):
            with open(file_path, "r") as yaml_file:
                data = yaml.safe_load(yaml_file)
        elif file_path.endswith(".json"):
            with open(file_path, "r") as json_file:
                data = json.loads(json_file.read())
    except (yaml.YAMLError, json.JSONDecodeError) as ex:
        raise ValueError(f"Error parsing file {file_path}: {ex}")
    except FileNotFoundError as ex:
        raise FileNotFoundError(f"File not found: {ex.filename}")
    return data


def verify_config(config, required_params, filename):
    for conf_item in config:
        for param in required_params:
            if param not in conf_item:
                raise KeyError(
                    f"Key '{param}' not found in the {filename} configuration:"
                    f"{json.dumps(conf_item, indent=2)}")


"""Maps common time units to their strings and factor to convert to
    multiples of a second
"""
str_to_time_unit_map = {
    's': ('seconds', 1),
    'm': ('minutes', 60),
    'h': ('hour', 60*60),
    'd': ('day_time', 24*60*60)
}


def parse_freq_str_to_json(frequency_str) -> dict:
    """Converts the frequency string to a JSON which stores these values
    """
    str_to_check = frequency_str.strip()
    if str_to_check[-1].lower() in str_to_time_unit_map:
        freq_unit = str_to_time_unit_map[str_to_check[-1].lower()][0]
        try:
            freq_val = int(frequency_str[0:-1])
        except ValueError as ve:
            raise f"Invalid time frequency string {str_to_check} received. Exception: {ve}"
    else:
        raise f"Invalid time frequency string {str_to_check} received."
    return {freq_unit: freq_val}


def convert_freq_str_to_seconds(frequency_str) -> float:
    """Converts the frequency string to number of seconds
        """
    str_to_check = frequency_str.strip()
    if str_to_check[-1].lower() in str_to_time_unit_map:
        try:
            freq_val = int(frequency_str[0:-1])
            return freq_val * str_to_time_unit_map[str_to_check[-1].lower()][1]
        except ValueError as ve:
            raise f"Invalid time frequency string {str_to_check} received. Exception: {ve}"
    raise f"Invalid time frequency string {str_to_check} received."
