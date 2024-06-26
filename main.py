"""File to run the main logic of the ClusterMgr

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""
import argparse
import os
import time
import typing

from datetime import datetime
from flask import Flask
from http import HTTPStatus

from rest_api.cache_api import cache_blue_print
from rest_api.cluster_api import cluster_blue_print
from rest_api.user_api import user_blue_print

from cluster_manager.global_cluster_cache import GlobalClusterCache
from cluster_manager.cluster_monitor import ClusterMonitor
from helper import load_config_file, verify_config, parse_freq_str_to_json, \
    convert_freq_str_to_seconds
from scheduler.task import TaskManager


REQ_CLUSTER_PARAMS = ["name", "ip", "user", "password"]


REQ_USER_PARAMS = ["name", "email", "prefix", "quota"]


flask_cluster_mgr_app = Flask("cluster_mgr_server")
flask_cluster_mgr_app.register_blueprint(cache_blue_print)
flask_cluster_mgr_app.register_blueprint(cluster_blue_print)
flask_cluster_mgr_app.register_blueprint(user_blue_print)


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
    parser.add_argument('cluster_config', help="File containing the cluster "
                                               "configuration. Can be JSON or YAML")
    parser.add_argument('user_config', help="File containing the user "
                                            "configuration. Can be JSON or YAML")
    # parser.add_argument('--port', '-p', default=5000, type=int, help="Port to run the server on")
    parser.add_argument('--debug', '-d', action='store_true',
                        help="Run the server in the debug mode")
    parser.add_argument('--run-cache-refresh', '-r', type=str,default='2m',
                        help="Frequency to run the cache rebuild. Pass the value in "
                             "the form 2m, 2h, 1d, etc.")
    parser.add_argument('--mail-frequency', '-m', type=str, default='@1000',
                        help="Frequency to mail the offending Users. To run it "
                             "at a specific time per day, pass "
                             "'@[time_of_day_24hours_format]'. "
                             "Default: Once per day at 10:00 AM.")
    parser.add_argument('--action-frequency', '-a', type=str,
                        default='@1700', help="Frequency to take action on the "
                                              "offending Users. To run it at a specific "
                                              "time per day, pass "
                                              "'@[time_of_day_24hours_format]'. "
                                              "Default: Once per day at 05:00 PM.")
    parser.add_argument('--offense-refresh', '-o', type=str,
                        default='3m', help="Frequency to calculate and "
                                           "retain the offending users")
    parser.add_argument('--retain-offense', type=str, default='7d',
                        help="Time for which to store the offenses. Default is 7 days")
    parser.add_argument('--offense-checkback', '-c', type=str,
                        default='2d', help="How far back should the sustained "
                                           "offenses be checked. Default is 2 days "
                                           "-- Checks the offenses that have been "
                                           "there since two days")
    parser.add_argument('--eval', action='store_true', help="Run the tool "
                                                            "in Eval mode (does not power-off "
                                                            "or remove NIC from the VM")
    args = parser.parse_args()
    
    cluster_config, user_config = verify_and_parse_config(args.cluster_config, args.user_config)
    print(f"Cluster and User configuration check successful. Tracking "
          f"{len(cluster_config["clusters"])} clusters and "
          f"{len(user_config["users"])} users")

    global_cache = GlobalClusterCache(cluster_config["clusters"], user_config["users"])
    cluster_monitor = ClusterMonitor()
    tm = TaskManager()

    # print("Adding repeated tasks to the TaskManager")
    ref_freq = parse_freq_str_to_json(args.run_cache_refresh)
    tm.add_repeated_task(global_cache, "rebuild_cache", ref_freq,
                         task_name="Refreshing cluster cache")

    global_cache.get_offending_items(get_vm_resources_per_cluster=True,
                                     retain_diff=True)
    kwargs = {'get_vm_resources_per_cluster': True, 'retain_diff': True}
    tm.add_repeated_task(global_cache, "get_offending_items",
                         parse_freq_str_to_json(args.offense_refresh),
                         task_name="Populating offenses", **kwargs)
    # c_time = time.time()
    # e_time = c_time + 100
    # # while time.time() < e_time:
    # time.sleep(10)
    # #     print(f"current ts: {time.time()}, waiting till {e_time}")
    # global_cache.get_offending_items(get_vm_resources_per_cluster=True,
    #                                  retain_diff=True)

    # cluster_monitor.calculate_continued_offenses()
    if args.eval:
        os.environ.setdefault('eval_mode', 'True')
    else:
        os.environ.setdefault('eval_mode', 'False')

    if args.offense_checkback:
        number_of_secs = convert_freq_str_to_seconds(args.offense_checkback)
        print(f"Calculating the offenses sustained from last {number_of_secs} sec.")
        os.environ.setdefault('offense_checkback', str(number_of_secs))
    """
    make the first run of the action 2 times time after the mails are done.
    For e.g., if the mail is sent every 3 hours, first run of action will be 
    done after 6 hours, and then the frequency will take effect/
    """
    first_mailing = None
    if args.mail_frequency.startswith("@"):
        mail_frequency = parse_freq_str_to_json(args.mail_frequency)
        tm.add_repeated_task(cluster_monitor, "send_warning_emails",
                             mail_frequency, task_name="Send Warning Emails")
        print(f"Will send first warning mail at {mail_frequency['day_time']} and then every day.")
    else:
        mail_frequency = parse_freq_str_to_json(args.mail_frequency)
        tm.add_repeated_task(cluster_monitor, "send_warning_emails",
                             mail_frequency, task_name="Send Warning Emails")
        first_mailing = time.time() + convert_freq_str_to_seconds(args.mail_frequency)
        print(f"Will send the warning mails starting at {datetime.fromtimestamp(first_mailing)}"
              f" with frequency: {mail_frequency}")
    
    if args.action_frequency.startswith("@"):
        parsed_time = parse_freq_str_to_json(args.action_frequency)
        # TODO Still need to have it run the next day after sending mail
        tm.add_repeated_task(cluster_monitor, "take_action_offenses", parsed_time,
                         task_name="Take action on user offenses")
        print(f"Will take action on the continued offenses at every day at "
              f"{parsed_time['day_time']}")
    else:
        first_action_run = first_mailing + convert_freq_str_to_seconds(args.mail_frequency)
        os.environ.setdefault("first_action_run", str(first_action_run))
        action_frequency = parse_freq_str_to_json(args.action_frequency)
        print(f"Will take action on the continued offenses at "
              f"{datetime.fromtimestamp(first_action_run)} with"
              f" frequency: {action_frequency}")
        tm.add_repeated_task(cluster_monitor, "take_action_offenses",
                             action_frequency, task_name="Take action on user offenses")

    retain_in_sec = convert_freq_str_to_seconds(args.retain_offense)
    print(f"\nRetaining the offenses for {args.retain_offense}: {retain_in_sec} secs")
    off_refresh = convert_freq_str_to_seconds(args.offense_refresh)
    print(f"\nRefreshing the offenses every {args.offense_refresh}: {off_refresh} secs")
    import math
    length_to_retain = math.ceil(retain_in_sec / off_refresh) + 1
    # length_to_retain = 5
    print(f"setting the cache to retain {length_to_retain} timestamps")
    os.environ.setdefault('offense_cache_retain', str(length_to_retain))
    # while True:
    #     time.sleep(1)
    #
    tm.start_task_runner()
    print("Task manager started!")

    flask_cluster_mgr_app.run(host='0.0.0.0', port=5000, debug=args.debug)
