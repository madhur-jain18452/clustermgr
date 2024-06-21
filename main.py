"""File to run the main logic of the ClusterMgr
"""
from flask import Flask, jsonify, request
import json
import sys
import time
import typing
import argparse

from functools import partial
from http import HTTPMethod, HTTPStatus

from cluster_manager.global_cluster_cache import GlobalClusterCache
from helper import load_config_file, verify_config
from scheduler.task import TaskManager

REQ_CLUSTER_PARAMS = ["name", "ip", "user", "password"]

SERVER_PORT = 5000

# TODO Add quota information (?) -- maybe some flag to determine if we want to run it in the ClusterMgr mode
REQ_USER_PARAMS = ["name", "email", "prefix", "quota"]

global_cache = None

flask_cluster_mgr_app = Flask("cluster_mgr_server")


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


@flask_cluster_mgr_app.route("/cache/offenses", methods=["GET"])
def get_all_offenses():
    resources_req = False
    resources_param = request.args.get('resources', 'false').lower()
    if resources_param in ['true', '1', 'yes', 'on']:
        resources_req = True
    elif resources_param in ['false', '0', 'no', 'off']:
        resources_req = False
    
    cluster_param = request.args.get('cluster', None)
    if cluster_param in ["None", "none", "NONE"]:
        cluster_param = None

    response = {}
    if cluster_param is not None and cluster_param not in global_cache.GLOBAL_CLUSTER_CACHE:
        return jsonify({"message": f"Cluster {cluster_param} not found in the cache"}), HTTPStatus.NOT_FOUND
    offending_items = global_cache.get_offending_items(
        get_vm_resources_per_cluster=resources_req,
        cluster_name=cluster_param,
        print_summary=True)
    if offending_items:
        user_offenses, vm_resources, vms_without_prefix = offending_items
        response["users"] = user_offenses
        response["vms"] = vms_without_prefix
        response["resources"] = vm_resources
        return jsonify(response), HTTPStatus.OK
    else:
        return jsonify({"message": "No offending items found. Clusters are in good shape."}), HTTPStatus.OK


@flask_cluster_mgr_app.route("/cache", methods=[HTTPMethod.GET])
def get_cache_summary():
    return jsonify(global_cache.summary(print_summary=False)), HTTPStatus.OK


@flask_cluster_mgr_app.route("/clusters", methods=[HTTPMethod.GET])
def list_clusters():
    return jsonify(global_cache.list_clusters()), HTTPStatus.OK


@flask_cluster_mgr_app.route("/clusters/<cluster_name>/vms", methods=[HTTPMethod.GET])
def get_cluster_info(cluster_name):
    arguments = request.args.to_dict()
    cluster_vm_info = global_cache.cluster_info(cluster_name=cluster_name, arguments=arguments)
    if cluster_vm_info:
        return cluster_vm_info, HTTPStatus.OK
    return jsonify({"error": f"Cluster with name {cluster_name} not found!"}), HTTPStatus.NOT_FOUND


@flask_cluster_mgr_app.route("/clusters/<cluster_name>/vms/power_state", methods=[HTTPMethod.POST])
def change_vm_power_state(cluster_name):
    arguments = json.loads(request.json)
    status, msg = global_cache.process_cluster_vm_power_change(cluster_name, arguments)
    return jsonify({"resp": msg}), status


@flask_cluster_mgr_app.route("/users", methods=[HTTPMethod.GET])
def list_users():
    return jsonify(global_cache.list_users()), HTTPStatus.OK


@flask_cluster_mgr_app.route("/users/<email>", methods=[HTTPMethod.POST,
                                                        HTTPMethod.PATCH])
def add_update_user(email):
    request_args = json.loads(request.get_json())
    # Parse the comma-sep prefix list and clean
    if request.method == HTTPMethod.POST:
        temp_list = request_args['prefix'].split(',')
        prefix_ls = []
        for each_pr in temp_list:
            prefix_ls.append(each_pr.strip())
        request_args['prefix'] = prefix_ls
    elif request.method == HTTPMethod.PATCH:
        if 'remove_prefixes' in request_args:
            temp_list = request_args['remove_prefixes'].split(',')
            prefix_ls = []
            for each_pr in temp_list:
                prefix_ls.append(each_pr.strip())
            request_args['remove_prefixes'] = prefix_ls
        if 'add_prefixes' in request_args:
            temp_list = request_args['add_prefixes'].split(',')
            prefix_ls = []
            for each_pr in temp_list:
                prefix_ls.append(each_pr.strip())
            request_args['add_prefixes'] = prefix_ls
    request_args['email'] = email
    is_patch=True if request.method == HTTPMethod.PATCH else False
    val = global_cache.add_update_user(request_args, is_patch=is_patch)
    addition_failed = False
    flush_failed = False
    file_name = None
    if val:
        if not val[2]:
            addition_failed = True
        else: #  If successfully added / updated the user in the cache
           to_flush = request_args.get('flush', False)
           if to_flush:
            file_name = request_args.get('file', 'all_users.json')
            users_list = global_cache.list_users()
            if file_name.endswith(".json"):
                try:
                    with open(file_name, 'w') as fileh:
                        fileh.write(json.dumps(users_list, indent=4))
                except Exception as ex:
                    print(ex)
                    flush_failed = True
        if not addition_failed:
            message = f"User {email} {'updated' if is_patch else 'added'} successfully in the cache."
            if not flush_failed:
                if val[0] or val[1]:
                    message += f"\tPrefixes that failed to add: '{', '.join(val[0])}."
                    f"\n\tPrefixes that failed to remove: {', '.join(val[1])}"
                return jsonify({'message': message}), HTTPStatus.ACCEPTED
            else:
                message += f"\nFailed to flush the list of users to {file_name}."
                return jsonify({'message': message}), HTTPStatus.INTERNAL_SERVER_ERROR
        else:
            message = f"User {email} COULD NOT be {'updated' if is_patch else 'added'} in the cache."
            return jsonify({'message': message}), HTTPStatus.INTERNAL_SERVER_ERROR
        

@flask_cluster_mgr_app.route("/users/<email>/vms", methods=[HTTPMethod.GET])
def list_user_vms(email):
    cname = request.args.get('cluster')
    return jsonify(global_cache.get_vms_for_user(email, cname)), HTTPStatus.OK


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='clustermgr',
        description='The clustermgr'
    )
    parser.add_argument('cluster_config', help="File containing the cluster configuration. Can be JSON or YAML")
    parser.add_argument('user_config', help="File containing the user configuration. Can be JSON or YAML")
    parser.add_argument('--port', '-p', default=5000, type=int, help="Port to run the server on")
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
    tm.add_repeated_task(global_cache, "rebuild_cache", {"minutes": 2}, 
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

    SERVER_PORT = args.port

    flask_cluster_mgr_app.run(host='0.0.0.0', port=args.port, debug=True)
