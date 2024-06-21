import yaml
import json
import typing


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
