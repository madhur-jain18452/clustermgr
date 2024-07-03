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
    GLOBAL_MGR_LOGGER, DEFAULT_DEVIATION_RETAIN_VALUE

cache_blue_print = Blueprint('cache', __name__)


@cache_blue_print.route("/cache", methods=[HTTPMethod.GET])
def get_cache_summary():
    global_cache = GlobalClusterCache()
    return jsonify(global_cache.summary(print_summary=False)), HTTPStatus.OK


@cache_blue_print.route("/cache/deviations", methods=[HTTPMethod.GET])
def get_all_overutil():
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
        deviating_items = global_cache.get_deviating_items(
            get_vm_resources_per_cluster=resources_req,
            cluster_name=cluster_param,
            print_summary=True)
        if deviating_items:
            user_overutil, vm_resources, vms_without_prefix = deviating_items
            response["users"] = user_overutil
            response["vms"] = vms_without_prefix
            response["resources"] = vm_resources
            return jsonify(response), HTTPStatus.OK
        else:
            return jsonify({"message": "No deviating items found. "
                            "Clusters are in good shape."}), HTTPStatus.OK
    else:
        # FIXME -- To render webpage, I am considering all the parameters are requested.
        resources_req = True
        cluster_param = None
        response = {'users': {}, 'vms': {}, 'resources': {}, 'health_status': {}}
        deviating_items = global_cache.get_deviating_items(
            get_vm_resources_per_cluster=resources_req,
            cluster_name=cluster_param,
            print_summary=False)
        if deviating_items:
            user_overutil, vm_resources, vms_without_prefix = deviating_items
            response["vms"] = vms_without_prefix
            response["resources"] = vm_resources

            user_dict = {}
            for email, info in user_overutil.items():
                user_dict[email] = {}
                for cname, use_info in info['deviations'].items():
                    user_dict[email][cname] = {
                        "used_cores": use_info.get('cores', '-'),
                        "used_memory": use_info.get('memory', '-'),
                        "quota_cores": info.get('quotas', {}).get(cname, {}).get('cores', '-'),
                        "quota_memory": info.get('quotas', {}).get(cname, {}).get('memory', '-'),
                    }
            response["users"] = user_dict

        cluster_health = {}
        for c_name in global_cache.GLOBAL_CLUSTER_CACHE:
            cluster_health[c_name] = global_cache.GLOBAL_CLUSTER_CACHE[c_name].get_health_status()
        return render_template("deviations.html", deviations=response, chealth=cluster_health)



@cache_blue_print.route("/cache/refresh", methods=[HTTPMethod.PUT])
def force_cache_refresh():
    global_cache = GlobalClusterCache()
    GLOBAL_MGR_LOGGER.info("Forcing cache refresh.")
    global_cache.rebuild_cache()
    return jsonify({"message": "Cache refresh successful."}), HTTPStatus.OK

    

@cache_blue_print.route("/cache/overutil/retain", methods=[HTTPMethod.PUT])
def update_offesne_retain_count():
    """Update the count of overutil to retain in the cache."""
    arguments = json.loads(request.json)
    if 'retain_overutil' in arguments:
        try:
            new_retain_val = int(arguments['retain_overutil'])
            msg = f"Updated the overutil retention from"\
                  f" {os.environ.get('overutil_cache_retain', DEFAULT_DEVIATION_RETAIN_VALUE)} "\
                  f"to {new_retain_val}"
            os.environ['overutil_cache_retain'] = str(new_retain_val)
            GLOBAL_MGR_LOGGER.info(msg)
            return jsonify({'message': msg}), HTTPStatus.OK
        except ValueError:
            err_str = "Please provide integer value for count of timed "\
                      f"overutil to retain. '{arguments['retain_overutil']}'"\
                      " is invalid."
            return jsonify({'message': err_str}), HTTPStatus.INTERNAL_SERVER_ERROR
