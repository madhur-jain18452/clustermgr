"""
Defines all the REST APIs for the cluster.

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""

import json

from flask import Blueprint, jsonify, request
from http import HTTPStatus, HTTPMethod

from custom_exceptions.exceptions import ActionAlreadyPerformedError
from cluster_manager.global_cluster_cache import GlobalClusterCache


cluster_blue_print = Blueprint('cluster', __name__)


@cluster_blue_print.route("/clusters", methods=[HTTPMethod.GET])
def list_clusters():
    global_cache = GlobalClusterCache()
    return jsonify(global_cache.get_clusters()), HTTPStatus.OK

@cluster_blue_print.route("/clusters/<cluster_name>", methods=[HTTPMethod.GET])
def get_cluster_info(cluster_name):
    global_cache = GlobalClusterCache()
    if cluster_name in global_cache.GLOBAL_CLUSTER_CACHE:
        return jsonify(global_cache.GLOBAL_CLUSTER_CACHE[cluster_name].summary(summary_verbosity=2)), HTTPStatus.OK
    return jsonify({'message': f'Cluster with name "{cluster_name}" not found in the cache'}), HTTPStatus.NOT_FOUND

@cluster_blue_print.route("/clusters", methods=[HTTPMethod.POST])
def add_cluster():
    cluster_info = json.loads(request.json)
    global_cache = GlobalClusterCache()
    try:
        global_cache.add_cluster(cluster_info)
    except ActionAlreadyPerformedError as aape:
        return jsonify({"message": str(aape)}), HTTPStatus.BAD_REQUEST
    return jsonify(global_cache.get_clusters()), HTTPStatus.OK

@cluster_blue_print.route("/clusters/<cluster_name>", methods=[HTTPMethod.DELETE])
def delete_cluster(cluster_name): 
    global_cache = GlobalClusterCache()
    try:
        global_cache.untrack_cluster(cluster_name)
    except Exception as e:
        return jsonify({"error": str(e)}), HTTPStatus.BAD_REQUEST
    return jsonify(global_cache.get_clusters()), HTTPStatus.OK


@cluster_blue_print.route("/clusters/<cluster_name>/vms", methods=[HTTPMethod.GET])
def get_cluster_vm_info(cluster_name):
    arguments = request.args.to_dict()
    global_cache = GlobalClusterCache()
    cluster_vm_info = global_cache.get_cluster_info(cluster_name=cluster_name, arguments=arguments)
    if cluster_vm_info:
        return cluster_vm_info, HTTPStatus.OK
    return jsonify({"error": f"Cluster with name {cluster_name} not found!"}), HTTPStatus.NOT_FOUND


@cluster_blue_print.route("/clusters/<cluster_name>/vms/power_state", methods=[HTTPMethod.POST])
def change_vm_power_state(cluster_name):
    arguments = json.loads(request.json)
    global_cache = GlobalClusterCache()
    status, msg = global_cache.perform_cluster_vm_power_change(cluster_name, arguments)
    return jsonify({"resp": msg}), status

@cluster_blue_print.route("/clusters/<cluster_name>/vms/nics/", methods=[HTTPMethod.DELETE])
def remove_vm_nic(cluster_name):
    arguments = json.loads(request.json)
    global_cache = GlobalClusterCache()
    status, msg = global_cache.perform_cluster_vm_nic_remove(cluster_name, arguments)
    return jsonify({"resp": msg}), status
