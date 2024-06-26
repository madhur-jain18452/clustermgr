"""
Defines all the REST APIs for the cache.

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""

import json
import os

from flask import Blueprint, jsonify, request, render_template
from http import HTTPStatus, HTTPMethod

from cluster_manager.global_cluster_cache import GlobalClusterCache,\
    GLOBAL_MGR_LOGGER, DEFAULT_OFFENSE_RETAIN_VALUE

cache_blue_print = Blueprint('cache', __name__)


@cache_blue_print.route("/cache", methods=[HTTPMethod.GET])
def get_cache_summary():
    global_cache = GlobalClusterCache()
    return jsonify(global_cache.summary(print_summary=False)), HTTPStatus.OK


@cache_blue_print.route("/cache/offenses", methods=[HTTPMethod.GET])
def get_all_offenses():
    global_cache = GlobalClusterCache()
    if 'Accept' in request.headers and request.headers['Accept'] == 'application/json':
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
            return jsonify({"message": f"Cluster {cluster_param} not"
                            " found in the cache"}), HTTPStatus.NOT_FOUND
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
            return jsonify({"message": "No offending items found. "
                            "Clusters are in good shape."}), HTTPStatus.OK
    else:
        # FIXME -- To render webpage, I am considering all the parameters are requested.
        resources_req = True
        cluster_param = None
        response = {'users': {}, 'vms': {}, 'resources': {}, 'health_status': {}}
        offending_items = global_cache.get_offending_items(
            get_vm_resources_per_cluster=resources_req,
            cluster_name=cluster_param,
            print_summary=False)
        if offending_items:
            user_offenses, vm_resources, vms_without_prefix = offending_items
            response["users"] = user_offenses
            response["vms"] = vms_without_prefix
            response["resources"] = vm_resources
        for c_name in global_cache.GLOBAL_CLUSTER_CACHE:
            response['health_status'][c_name] = global_cache.GLOBAL_CLUSTER_CACHE[c_name].get_health_status()
        return render_template("offenses.html", offenses=response)



@cache_blue_print.route("/cache/refresh", methods=[HTTPMethod.PUT])
def force_cache_refresh():
    global_cache = GlobalClusterCache()
    GLOBAL_MGR_LOGGER.info("Forcing cache refresh.")
    global_cache.rebuild_cache()
    return jsonify({"message": "Cache refresh successful."}), HTTPStatus.OK

    

@cache_blue_print.route("/cache/offenses/retain", methods=[HTTPMethod.PUT])
def update_offesne_retain_count():
    """Update the count of offenses to retain in the cache."""
    arguments = json.loads(request.json)
    if 'retain_offense' in arguments:
        try:
            new_retain_val = int(arguments['retain_offense'])
            msg = f"Updated the Offense retention from"\
                  f" {os.environ.get('offense_cache_retain', DEFAULT_OFFENSE_RETAIN_VALUE)} "\
                  f"to {new_retain_val}"
            os.environ['offense_cache_retain'] = str(new_retain_val)
            GLOBAL_MGR_LOGGER.info(msg)
            return jsonify({'message': msg}), HTTPStatus.OK
        except ValueError:
            err_str = "Please provide integer value for count of timed "\
                      f"offenses to retain. '{arguments['retain_offense']}'"\
                      " is invalid."
            return jsonify({'message': err_str}), HTTPStatus.INTERNAL_SERVER_ERROR
