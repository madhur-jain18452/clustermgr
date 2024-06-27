"""
Defines all the REST APIs for the cluster.

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""

import json

from flask import Blueprint, jsonify, request
from http import HTTPStatus, HTTPMethod

from scheduler.task import TaskManager
from cluster_manager.constants import CACHE_REBUILD_TASK_TAG, GET_OFFENSES_TASK_TAG,\
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
    return (jsonify({'message': 'New Cache Refresh job add succsful '
                     f'for frequency {json.dumps(freq_json)}'}),
            HTTPStatus.OK)

@tool_blue_print.route("/tool/offense_refresh", methods=[HTTPMethod.PUT])
def update_offense_refresh():
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
    task_mgr.delete_job(GET_OFFENSES_TASK_TAG)
    kwargs = {'get_vm_resources_per_cluster': True, 'retain_diff': True}
    task_mgr.add_repeated_task(global_cache, "get_offending_items", freq_json,
                               GET_OFFENSES_TASK_TAG,
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
    task_mgr.add_repeated_task(cluster_monitor, "take_action_offenses", freq_json,
                               TAKE_ACTION_TASK_TAG,
                               task_name="Taking action on offenses")
    return (jsonify({'message': 'New offense action job updated successful '
                     f'for frequency {json.dumps(freq_json)}'}),
            HTTPStatus.OK)