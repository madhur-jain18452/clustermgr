import json

from flask import Blueprint, jsonify, request
from http import HTTPStatus, HTTPMethod

from cluster_manager.global_cluster_cache import GlobalClusterCache


cluster_blue_print = Blueprint('cluster', __name__)

global_cache = GlobalClusterCache()


@cluster_blue_print.route("/clusters", methods=[HTTPMethod.GET])
def list_clusters():
    return jsonify(global_cache.get_clusters()), HTTPStatus.OK


@cluster_blue_print.route("/clusters/<cluster_name>/vms", methods=[HTTPMethod.GET])
def get_cluster_info(cluster_name):
    arguments = request.args.to_dict()
    cluster_vm_info = global_cache.get_cluster_info(cluster_name=cluster_name, arguments=arguments)
    if cluster_vm_info:
        return cluster_vm_info, HTTPStatus.OK
    return jsonify({"error": f"Cluster with name {cluster_name} not found!"}), HTTPStatus.NOT_FOUND


@cluster_blue_print.route("/clusters/<cluster_name>/vms/power_state", methods=[HTTPMethod.POST])
def change_vm_power_state(cluster_name):
    arguments = json.loads(request.json)
    status, msg = global_cache.perform_cluster_vm_power_change(cluster_name, arguments)
    return jsonify({"resp": msg}), status

