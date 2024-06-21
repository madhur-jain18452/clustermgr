from cluster_manager.global_cluster_cache import GlobalClusterCache
from flask import Blueprint, jsonify, request
from http import HTTPStatus, HTTPMethod

cache_blue_print = Blueprint('cache', __name__)

global_cache = GlobalClusterCache()


@cache_blue_print.route("/cache", methods=[HTTPMethod.GET])
def get_cache_summary():
    return jsonify(global_cache.summary(print_summary=False)), HTTPStatus.OK


@cache_blue_print.route("/cache/offenses", methods=["GET"])
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