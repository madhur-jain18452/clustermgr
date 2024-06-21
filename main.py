"""File to run the main logic of the ClusterMgr
"""
from flask import Flask
import typing
import argparse

from http import HTTPStatus

from rest_api.cache_api import cache_blue_print
from rest_api.cluster_api import cluster_blue_print
from rest_api.user_api import user_blue_print

from cluster_manager.global_cluster_cache import GlobalClusterCache
from helper import load_config_file, verify_config
from scheduler.task import TaskManager

REQ_CLUSTER_PARAMS = ["name", "ip", "user", "password"]

# TODO Add quota information (?) -- maybe some flag to determine if we want to run it in the ClusterMgr mode
REQ_USER_PARAMS = ["name", "email", "prefix", "quota"]


flask_cluster_mgr_app = Flask("cluster_mgr_server")
flask_cluster_mgr_app.register_blueprint(cache_blue_print)
flask_cluster_mgr_app.register_blueprint(cluster_blue_print)
flask_cluster_mgr_app.register_blueprint(user_blue_print)


str_to_time_unit_map = {
    's': 'seconds',
    'm': 'minutes',
    'h': 'hour',
    'd': 'day'
}

def parse_frequency(frequency_str) -> dict:
    str_to_check = frequency_str.strip()
    if frequency_str[-1].lower() in str_to_time_unit_map:
        freq_unit = str_to_time_unit_map[str_to_check[-1].lower()]
        try:
            freq_val = int(frequency_str[0:-1])
        except ValueError as ve:
            raise (f"Invalid time frequency string {str_to_check} received. Exception: {ve}")
    else:
        raise (f"Invalid time frequency string {str_to_check} received.")
    return {freq_unit: freq_val}


def verify_and_parse_config(cluster_file, user_file) -> typing.Tuple[dict, dict]:
    cluster_config = {}
    user_config = {}

    print("Parsing Cluster list")
    try:
        cluster_config = load_config_file(cluster_file)
        if "clusters" not in cluster_config:
            raise KeyError(f"'clusters' key not found in the file {cluster_file}")
        verify_config(cluster_config["clusters"], REQ_CLUSTER_PARAMS, cluster_file)
    except FileNotFoundError as ex:
        raise FileNotFoundError(f"File not found: {ex.filename}")
    except ValueError as ex:
        raise ValueError(f"Error parsing {user_file}: {ex}")

    print("Parsing User list")
    try:
        user_config = load_config_file(user_file)
        if "users" not in user_config:
            raise KeyError(f"'users' key not found in the file {user_file}")
        verify_config(user_config["users"], REQ_USER_PARAMS, user_file)
    except Exception as ex:
        raise ex

    return cluster_config, user_config

@flask_cluster_mgr_app.route("/")
def hello_cluster_mgr():
    return "Hello from cluster mgr!", HTTPStatus.OK


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='clustermgr',
        description='The clustermgr'
    )
    parser.add_argument('cluster_config', help="File containing the cluster configuration. Can be JSON or YAML")
    parser.add_argument('user_config', help="File containing the user configuration. Can be JSON or YAML")
    parser.add_argument('--port', '-p', default=5000, type=int, help="Port to run the server on")
    parser.add_argument('--debug', '-d', action='store_true', help="Run the server in the debug mode")
    parser.add_argument('--run-cache-refresh', '-r', type=str, default='2m', help="Frequency to run the cache rebuild. Pass the value in the form 2m, 2h, 1d, etc.")
    args = parser.parse_args()
    # global gcc
    # if len(sys.argv) != 3:
    #     print("Usage: python3 main.py <cluster.yaml|JSON> <userlist.yaml|JSON>")
    #     sys.exit(1)

    cluster_config, user_config = verify_and_parse_config(args.cluster_config, args.user_config)
    print(f"Cluster and User configuration check successful. Tracking "
          f"{len(cluster_config["clusters"])} clusters and "
          f"{len(user_config["users"])} users")

    global_cache = GlobalClusterCache(cluster_config["clusters"], user_config["users"])

    # offending_items = global_cache.get_offending_items(print_summary=True,
    #                                                    get_users_over_util=True,
    #                                                    get_vm_resources_per_cluster=True,
    #                                                    include_vm_without_prefix=True)
    # if offending_items:
    #     user_offenses, vm_resources, vms_without_prefix = offending_items
    #     print(f"User offenses: {json.dumps(user_offenses, indent=4)}")
    #     print(f"VM Resources: {json.dumps(vm_resources, indent=4)}")
    #     print(f"VM without prefixes: {json.dumps(vms_without_prefix, indent=4)}")
    # else:
    #     print("No offending items received!")
    tm = TaskManager()
    print("Adding repeated tasks to the TaskManager")
    fre = parse_frequency(args.run_cache_refresh)
    print(fre)
    tm.add_repeated_task(global_cache, "rebuild_cache", fre,
                         task_name="Refreshing cluster cache")
    kwargs = {'print_summary': False}
    # tm.add_repeated_task(global_cache, "get_offending_items", {"minutes": 3},
    #                      task_name="Getting offending cluster information",
    #                      **kwargs)
    tm.start_task_runner()
    print("Task manager started!")

    # while True:
    #     time.sleep(1)
    #

    flask_cluster_mgr_app.run(host='0.0.0.0', port=args.port, debug=args.debug)
