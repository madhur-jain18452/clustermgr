"""
Defines all the REST APIs for the cluster.

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""

import json
import os

from flask import Blueprint, jsonify, request
from http import HTTPStatus, HTTPMethod

from scheduler.task import TaskManager
from cluster_manager.constants import CACHE_REBUILD_TASK_TAG, \
    GET_DEVIATIONS_TASK_TAG,\
    SEND_WARNING_TASK_TAG, TAKE_ACTION_TASK_TAG
from helper import parse_freq_str_to_json


tool_blue_print = Blueprint('tool', __name__)


@tool_blue_print.route("/tool", methods=[HTTPMethod.GET])
def list_tasks():
    task_mgr = TaskManager()
    all_jobs = task_mgr.schedule.get_jobs()
    job_list = []
    for j in all_jobs:
        job_list.append({
            "name": j.job_func.__qualname__,
            "interval": j.interval,
            "unit": j.unit
        })
    return jsonify(job_list), HTTPStatus.OK


@tool_blue_print.route("/tool/override_dnd", methods=[HTTPMethod.PUT])
def update_dnd_override():
    """Updates the Do Not Disturb override for the cluster when performing
        garbage collection
    """
    args = json.loads(request.json)
    override_str = args['new_override_str']
    if override_str.lower() not in ["true", "yes", "no", "false"]:
        return jsonify({'message': f'Invalid value for override "{override_str}"'}), \
            HTTPStatus.BAD_REQUEST
    try:
        os.environ.update({'override_dnd': override_str})
    except Exception as ex:
        return jsonify({'message': f'Override update failed. Error: {ex}'}), \
            HTTPStatus.INTERNAL_SERVER_ERROR
    return jsonify({'message': f'Override update successful. Set to "{override_str}"'}), \
            HTTPStatus.OK


@tool_blue_print.route("/tool/cache_refresh", methods=[HTTPMethod.PUT])
def update_cache_refresh():
    """
    This needs to be separate for each function as we have tag associated with
    each one and function name rechecks are not really a good way as these are
    a class methods
    """
    from cluster_manager.global_cluster_cache import GlobalClusterCache
    args = json.loads(request.json)
    new_freq_str = args['new_freq_str']
    freq_json = None
    try:
        freq_json = parse_freq_str_to_json(new_freq_str)
    except Exception as ex:
        return jsonify({'message': ex.__str__}), HTTPStatus.INTERNAL_SERVER_ERROR
    task_mgr = TaskManager()
    global_cache = GlobalClusterCache()
    task_mgr.delete_job(CACHE_REBUILD_TASK_TAG)
    task_mgr.add_repeated_task(global_cache, "rebuild_cache", freq_json,
                               CACHE_REBUILD_TASK_TAG,
                               task_name="Refreshing cluster cache")
    return (jsonify({'message': 'New Cache Refresh job add successful '
                     f'for frequency {json.dumps(freq_json)}'}),
            HTTPStatus.OK)

@tool_blue_print.route("/tool/deviation_refresh", methods=[HTTPMethod.PUT])
def update_deviation_refresh():
    """
    This needs to be separate for each function as we have tag associated with
    each one and function name rechecks are not really a good way as these are
    a class methods
    """
    from cluster_manager.global_cluster_cache import GlobalClusterCache
    args = json.loads(request.json)
    new_freq_str = args['new_freq_str']
    freq_json = None
    try:
        freq_json = parse_freq_str_to_json(new_freq_str)
    except Exception as ex:
        return jsonify({'message': ex.__str__}), HTTPStatus.INTERNAL_SERVER_ERROR
    task_mgr = TaskManager()
    global_cache = GlobalClusterCache()
    task_mgr.delete_job(GET_DEVIATIONS_TASK_TAG)
    kwargs = {'get_vm_resources_per_cluster': True, 'retain_diff': True}
    task_mgr.add_repeated_task(global_cache, "get_deviating_items", freq_json,
                               GET_DEVIATIONS_TASK_TAG,
                               task_name="Refreshing cluster cache", **kwargs)
    return (jsonify({'message': 'New Offense Refresh job add successful '
                     f'for frequency {json.dumps(freq_json)}'}),
            HTTPStatus.OK)

@tool_blue_print.route("/tool/mail_frequency", methods=[HTTPMethod.PUT])
def update_mail_frequency():
    """
    This needs to be separate for each function as we have tag associated with
    each one and function name rechecks are not really a good way as these are
    a class methods
    """
    from cluster_manager.cluster_monitor import ClusterMonitor
    args = json.loads(request.json)
    new_freq_str = args['new_freq_str']
    freq_json = None
    try:
        freq_json = parse_freq_str_to_json(new_freq_str)
    except Exception as ex:
        return jsonify({'message': ex.__str__}), HTTPStatus.INTERNAL_SERVER_ERROR
    task_mgr = TaskManager()
    cluster_monitor = ClusterMonitor()
    task_mgr.delete_job(SEND_WARNING_TASK_TAG)
    task_mgr.add_repeated_task(cluster_monitor, "send_warning_emails", freq_json,
                               SEND_WARNING_TASK_TAG,
                               task_name="Sending Warning mails")
    return (jsonify({'message': 'New Warning mail job updated successful '
                     f'for frequency {json.dumps(freq_json)}'}),
            HTTPStatus.OK)


@tool_blue_print.route("/tool/action_frequency", methods=[HTTPMethod.PUT])
def update_action_frequency():
    """
    This needs to be separate for each function as we have tag associated with
    each one and function name rechecks are not really a good way as these are
    a class methods
    """
    from cluster_manager.cluster_monitor import ClusterMonitor
    args = json.loads(request.json)
    new_freq_str = args['new_freq_str']
    freq_json = None
    try:
        freq_json = parse_freq_str_to_json(new_freq_str)
    except Exception as ex:
        return jsonify({'message': ex.__str__}), HTTPStatus.INTERNAL_SERVER_ERROR
    task_mgr = TaskManager()
    cluster_monitor = ClusterMonitor()
    task_mgr.delete_job(TAKE_ACTION_TASK_TAG)
    task_mgr.add_repeated_task(cluster_monitor, "take_action_deviations", freq_json,
                               TAKE_ACTION_TASK_TAG,
                            task_name="Taking action on deviations")
    return (jsonify({'message': 'New offense action job updated successful '
                     f'for frequency {json.dumps(freq_json)}'}),
            HTTPStatus.OK)

@tool_blue_print.route("/tool/dump_user_config", methods=[HTTPMethod.POST])
def dump_user_config():
    from cluster_manager.global_cluster_cache import GlobalClusterCache
    global_cache = GlobalClusterCache()
    args = json.loads(request.json)
    file_name = args.get('dump_file', 'user_config.json')
    ret_u = global_cache.dump_user_config(file_name)
    if ret_u:
        return (jsonify({'message': f'User config dumped to {file_name}'}),
                HTTPStatus.OK)
    else:
        return (jsonify({'message': f'User config dump failed'}),
                HTTPStatus.INTERNAL_SERVER_ERROR)

@tool_blue_print.route("/tool/dump_cluster_config", methods=[HTTPMethod.POST])
def dump_cluster_config():
    from cluster_manager.global_cluster_cache import GlobalClusterCache
    global_cache = GlobalClusterCache()
    args = json.loads(request.json)
    file_name = args.get('dump_file', 'cluster_config.json')
    ret_c = global_cache.dump_cluster_config(file_name)
    if ret_c:
        return (jsonify({'message': f'Cluster config dumped to {file_name}'}),
                HTTPStatus.OK)
    else:
        return (jsonify({'message': f'Cluster config dump failed'}),
                HTTPStatus.INTERNAL_SERVER_ERROR)

@tool_blue_print.route("/tool/eval_mode", methods=[HTTPMethod.PUT])
def update_eval_mode():
    args = json.loads(request.json)
    new_state = args['new_state']
    if new_state.lower() not in ["on", "off"]:
        return jsonify({'message': f'Invalid value for state "{new_state}"'}), \
            HTTPStatus.BAD_REQUEST
    from cluster_manager.cluster_monitor import cm_logger
    try:
        if new_state.lower() == 'on':
            os.environ.update({'eval_mode': 'True'})
            cm_logger.info("Eval mode set to 'ON' successfully")
        else:
            os.environ.update({'eval_mode': 'False'})
            cm_logger.info("Eval mode set to 'OFF' successfully")
    except Exception as ex:
        cm_logger.exception(f"Eval mode update to {new_state} failed. Exception: {ex}")
        return jsonify({'message': f'Eval mode update failed. Error: {ex}'}), \
            HTTPStatus.INTERNAL_SERVER_ERROR
    return jsonify({'message': f'Eval mode update successful. Set to "{new_state.upper()}"'}), HTTPStatus.OK
