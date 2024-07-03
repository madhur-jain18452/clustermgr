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


"""
    Maps common time units to their strings used in Task Manager and factor to convert to
    multiples of a second
"""
STR_TO_TIME_UNIT_MAP = {
    's': ('seconds', 1),
    'm': ('minutes', 60),
    'h': ('hour', 60*60),
    'd': ('days', 24*60*60)
}


def parse_freq_str_to_json(frequency_str) -> dict:
    """Converts the frequency string to a JSON which stores these values
    
        Raises:
            ValueError | Exception
    """
    str_to_check = frequency_str.strip()
    # Special case to handle something that needs to run every day at a specific time:
    if str_to_check[0].startswith("@"):
        time_str = str_to_check[1:]
        # Only accept cases where the string is like 530 or 1730
        if len(time_str) not in [3, 4]:
            raise ValueError(f"The string {str_to_check} is invalid. Please provide 24-hour format string.")
        at_minutes = time_str[-2:]
        at_hour = time_str[:-2]
        if not (int(at_minutes) >= 0 and int(at_minutes) < 60):
            raise ValueError(f"The string {str_to_check} is invalid. Please check again.")
        if not (int(at_hour) >= 0 and int(at_hour) < 24):
            raise ValueError(f"The string {str_to_check} is invalid. Please check again.")
        return {"day_time": f"{at_hour}:{at_minutes}"}

    if str_to_check[-1].lower() in STR_TO_TIME_UNIT_MAP:
        freq_unit = STR_TO_TIME_UNIT_MAP[str_to_check[-1].lower()][0]
        try:
            freq_val = int(frequency_str[0:-1])
        except ValueError as ve:
            raise f"Invalid time frequency string {str_to_check} received. Exception: {ve}"
    else:
        raise ValueError(f"Invalid time frequency string {str_to_check} received.")
    return {freq_unit: freq_val}


def convert_freq_str_to_seconds(time_str) -> float:
    """Converts the time string to number of seconds
    E.g. 1d -> 1 day -> 86400 seconds
    """
    str_to_check = time_str.strip()
    if str_to_check[-1].lower() in STR_TO_TIME_UNIT_MAP:
        try:
            freq_val = int(time_str[0:-1])
            return freq_val * STR_TO_TIME_UNIT_MAP[str_to_check[-1].lower()][1]
        except ValueError as ve:
            raise Exception(f"Invalid time frequency string {str_to_check} received. Exception: {ve}")
    raise Exception(f"Invalid time frequency string {str_to_check} received.")

def time_until_next_run(target_hour, target_minute):
    from datetime import datetime, timedelta
    now = datetime.now()
    today_run_time = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
    if now > today_run_time:
        # If the current time is past today's run time, schedule it for tomorrow
        next_run_time = today_run_time + timedelta(days=1)
    else:
        # Otherwise, it's still today
        next_run_time = today_run_time
    
    # Calculate the time remaining until the next run
    time_remaining = next_run_time - now
    return time_remaining.total_seconds()
