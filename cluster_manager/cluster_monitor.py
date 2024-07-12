"""
Global ClusterMonitor: Monitors the clusters, performs checks and takes actions

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""

import logging
import json
import threading
import typing
import os
import time

from datetime import date, datetime
from http import HTTPStatus

from caching.server_constants import is_dnd
from common.constants import ONE_DAY_IN_SECS, UserKeys, Resources as res
from cluster_manager.global_cluster_cache import GlobalClusterCache
from custom_exceptions.exceptions import SameTimestampError

cm_logger = logging.getLogger(__name__)
cm_logger.setLevel(logging.DEBUG)
handler = logging.FileHandler("cmgr_cluster_monitor.log", mode='w')
formatter = logging.Formatter("%(filename)s:%(lineno)d - %(asctime)s %(levelname)s - %(message)s")
handler.setFormatter(formatter)
cm_logger.addHandler(handler)


class ClusterMonitor:
    """Class that actually monitors the cache (subsequently, the entire cluster setup)
        This is a singleton class.
    """
    # Singleton
    _g_cluster_monitor_instance = None
    _g_c_monitor_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._g_cluster_monitor_instance:
            with cls._g_c_monitor_lock:
                if not cls._g_cluster_monitor_instance:
                    cls._g_cluster_monitor_instance = super(ClusterMonitor, cls).__new__(cls)
        return cls._g_cluster_monitor_instance

    def __init__(self):
        cm_logger.info("Initializing the ClusterMonitor")
        # self.mailer = smtplib.SMTP()
        pass

    def send_warning_emails(self):
        """Function that sends warning emails to the users who are over-utilizing the resources.
            This is added as a task in the scheduler.
        """
        cm_logger.info(f"Sending warning emails to the users who are over-utilizing the resources. timestamp: {time.time()}")
        pass

    def _calculate_continued_deviations(self, from_timestamp=None,
                                     new_timestamp=None,
                                     timediff=ONE_DAY_IN_SECS
                                     ) -> typing.Optional[typing.Tuple[dict, int, int]]:
        """Helper function that can calculate difference in upto
            two levels of JSON
            Args:
                from_timestamp (optional): Start timestamp. If not provided, it is calculated from new_timestamp
                new_timestamp (Optional): End timestamp. If not provided, current time is used
                timediff: Time difference between the two timestamps. Default is 1 day (86400 seconds).
            Returns:
                None: If the deviations could not be calculated
                else:
                continued_deviations (dict): deviations that are continued from the old to the new timestamp
                actual_old_ts (int): Actual old timestamp used for the calculation
                actual_new_ts (int): Actual new timestamp used for the calculation
        """
        global_cache = GlobalClusterCache()
        continued_deviations = {}
        if new_timestamp is None:
            new_timestamp = int(time.time())
        if from_timestamp is None:
            from_timestamp = new_timestamp - timediff
        cm_logger.info(f'Getting the deviations done between {from_timestamp}:'
                       f'{date.fromtimestamp(from_timestamp)} to {new_timestamp}:'
                       f'{date.fromtimestamp(new_timestamp)}')

        try:
            timed_deviations = global_cache.get_timed_deviations(
                start_ts=from_timestamp,
                end_ts=new_timestamp
            )
        except SameTimestampError as ste:
            cm_logger.warning(ste)
            return
        actual_old_ts, old_deviations, actual_new_ts, new_deviations = timed_deviations
        cm_logger.info(f'Actual timestamps for the deviations is {actual_old_ts}:'
                       f'{date.fromtimestamp(actual_old_ts)} to {actual_new_ts}:'
                       f'{date.fromtimestamp(actual_new_ts)}')

        # Create a generic process that calculates the difference
        for k, old_v in old_deviations.items():
            if k in new_deviations:
                new_v = new_deviations[k]
                # If the values are dict, we need the keys
                if type(old_v) is dict:
                    common_deviations = set(old_v.keys()).intersection(set(new_v.keys()))
                    # For the dict, insert the key into the tracker, and insert the latest values
                    continued_deviations[k] = {}
                    for co in common_deviations:
                        continued_deviations[k][co] = new_v[co]
                # if the values is a list
                elif type(old_v) is list:
                    # list of tuples. NOTE: First value (idx 0) is assumed to
                    # be the UUID (Names are also fine if they are immutable)
                    if old_v[0]:
                        if old_v[0] is tuple:
                            common_deviations = set([v[0] for v in old_v]).intersection(
                                set([v[0] for v in new_v]))
                            # Reconstruct the tuple
                            continued_deviations[k] = [v for v in new_v if v[0] in common_deviations]
                        else:
                            # List of values (typically strings) TODO Handle other cases
                            common_deviations = set(old_v).intersection(set(new_v))
                            continued_deviations[k] = common_deviations
            # TODO Send emails here
        return continued_deviations, actual_old_ts, actual_new_ts

    def take_action_deviations(self):
        """Function that takes action on the deviations calculated
        For each user, we try to bring the individual cluster utilization under\
            control marking the VMs and powering them off.
            First checks if any non-DND VMs can be turned off.
                If not sufficient, check if the overriding is allowed.
                    If overriding is allowed, checks and powers OFF the DND VMs.
        After all clusters are under control, the code checks if the global utilization under control.\
            If not, checks the VMs across all clusters and marks them for power off greedily.
        """

        def _user_json_to_list(u_json, cluster_name=None):
            """Utility function that converts the user JSON to a list of VMs
            Args:
                u_json: User JSON
                cluster_name (Optional): Cluster name to filter the VMs. \
                    If not provided, all VMs are returned.
            """
            if cluster_name:
                return [{
                    res.CORES: util_info[res.CORES],
                    res.MEMORY: util_info[res.MEMORY],
                    'uuid': _vm_uuid,
                    'parent_cluster': util_info['parent_cluster'],
                    'name': util_info['name']
                    } for _vm_uuid, util_info in u_json[UserKeys.VM_UTILIZED].items()
                    if util_info['parent_cluster'] == cluster_name
                ]
            return [{
                res.CORES: util_info[res.CORES],
                res.MEMORY: util_info[res.MEMORY],
                'uuid': _vm_uuid,
                'parent_cluster': util_info['parent_cluster'],
                'name': util_info['name']
                } for _vm_uuid, util_info in u_json[UserKeys.VM_UTILIZED].items()
            ]

        def _debug_resource_log(vm_info, u_email, gl_over_util_core,
                                gl_over_util_mem, cl_name=None,
                                cl_over_util_core=None, cl_over_util_mem=None):
            """Utility function that logs the VMs Marked for removal
            """
            if cl_name:
                cm_logger.debug(f"Mark for REMOVE: VM:{vm_info['name']} user:"
                                f"{u_email} cluster:{cname}. "
                                f"RES release:- Cores:{vm_info[res.CORES]}, "
                                f"Memory:{vm_info[res.MEMORY]}. "
                                f"Cluster Util now cores:{cl_over_util_core}, "
                                f" memory:{cl_over_util_mem}. "
                                f"Global OverUtil now cores:{gl_over_util_core}"
                                f", memory:{gl_over_util_mem}")
            else:
                cm_logger.debug(f"Mark for REMOVE VM:{vm_info['name']} user:"
                                f" {u_email} GLOBAL (cluster: {vm_info['parent_cluster']}) "
                                "RES release:- Cores:"
                                f"{vm_info[res.CORES]}, Memory:{vm_info[res.MEMORY]}."
                                f" Global OverUtil now: cores:"
                                f"{gl_over_util_core} memory:"
                                f"{gl_over_util_mem}")

        def _mark_vm_power_off_greedy(already_marked_set, _sorted_vm_list,
                               gl_core_util, gl_mem_util, cl_name=None, cl_core_util=None,
                               cl_mem_util=None, check_cores=True, check_mem=False,
                               skip_dnd=True):
            """Utility function that marks the VMs for power off
            Args:
                already_marked_set: Set of VMs that are already marked. The function will add to this set
                _sorted_vm_list: List of VMs sorted by the resource consumption
                gl_core_util: Global core utilization
                gl_mem_util: Global memory utilization
                cl_name (Optional): Cluster name, used when the VMs for a specific cluster are being marked
                cl_core_util (Optional): Cluster core utilization
                cl_mem_util (Optional): Cluster memory utilization
                check_cores: Boolean flag to check if the we are getting (only) cores under control
                check_mem: Boolean flag to check if we are getting (only) memory under control
                skip_dnd: Boolean flag to skip the DND VMs
            """
            for _vm in _sorted_vm_list:
                if is_dnd(_vm['name']) and skip_dnd:
                    continue
                if (_vm['uuid'], _vm['parent_cluster']) not in already_marked_set:
                    # Update the resource over_utils to check if sufficient
                    if cl_core_util is not None:
                        cl_core_util -= _vm[res.CORES]
                    gl_core_util -= _vm[res.CORES]

                    if cl_mem_util is not None:
                        cl_mem_util -= _vm[res.MEMORY]
                    gl_mem_util -= _vm[res.MEMORY]

                    already_marked_set.add((_vm['uuid'], _vm['parent_cluster']))
                    _debug_resource_log(_vm, user_email, gl_core_util,
                                        gl_mem_util,
                                        cl_name=cl_name,
                                        cl_over_util_core=cl_core_util,
                                        cl_over_util_mem=cl_mem_util)
                    # If either of the deviations are under control after
                    # powering off this VM, check only for the other resource
                    # We will either pass both the params or none of them
                    if cl_core_util is not None and cl_mem_util is not None:
                        if check_cores and check_mem:
                            if cl_core_util <= 0 or cl_mem_util <= 0:
                                return cl_core_util, cl_mem_util, gl_core_util, gl_mem_util
                        elif check_cores:
                            if cl_core_util <= 0:
                                return cl_core_util, cl_mem_util, gl_core_util, gl_mem_util
                        elif check_mem:
                            if cl_mem_util <= 0:
                                return cl_core_util, cl_mem_util, gl_core_util, gl_mem_util
                    else:
                        if check_cores and check_mem:
                            if gl_core_util <= 0 or gl_mem_util <= 0:
                                return cl_core_util, cl_mem_util, gl_core_util, gl_mem_util
                        elif check_cores:
                            if gl_core_util <= 0:
                                return cl_core_util, cl_mem_util, gl_core_util, gl_mem_util
                        elif check_mem:
                            if gl_mem_util <= 0:
                                return cl_core_util, cl_mem_util, gl_core_util, gl_mem_util

        global_cache = GlobalClusterCache()
        current_time = time.time()
        cm_logger.info(f"Taking action on the users who are over-utilizing the resources. timestamp: {time.time()}")
        # If the first_action_run is set, honor that, else this is a task that runs at X time per day
        first_action_run = os.environ.get("first_action_run")
        if first_action_run is None:
            pass
        else:
            if current_time < float(first_action_run):
                cm_logger.info(f"Triggered action at {datetime.fromtimestamp(current_time)}. "
                            f"First should not start before "
                            f"{datetime.fromtimestamp(float(first_action_run))}")
                return
            pass
        checkback_seconds = os.environ.get("deviations_checkback", str(ONE_DAY_IN_SECS))
        start_time = current_time - int(checkback_seconds)
        deviations = \
            self._calculate_continued_deviations(from_timestamp=start_time,
                                               new_timestamp=current_time)
        if deviations:
            continued_deviations, actual_old_ts, actual_new_ts = deviations
        else:
            cm_logger.error("No deviations could be calculated.")
            return
        # if actual_new_ts - actual_old_ts < ONE_DAY_IN_SECS:
        #     cm_logger.error(f"Diff between the new and old TS is less than the set value of {ONE_DAY_IN_SECS}")
        #     return
        vm_uuids_to_turn_off = []
        users_over_util = continued_deviations.get('users_over_util', {})
        cm_logger.debug(f"Users over-utilizing the resources: {json.dumps(users_over_util)}")
        for user_email, over_util_cluster_info in users_over_util.items():
            # Store the marked VMs as a tuple(UUID, cluster_name)
            user_vms_marked_power_off = set()
            # The structure is 
            # "email": {
            #     "deviations": {
            #         "global": {
            #             "cores": val,
            #             "memory": val
            #         },
            #         "cluster1": {
            #             "cores": val,
            #             "memory": val
            #         }
            #     },
            #     "quotas": {
            #         "global": {
            #             "cores": val,
            #             "memory": val
            #         }
            #     }
            # }
            # The 'deviations' is tracking the total utilization of the user in the cluster
            # with global_cache.global_user_cache_lock:
            cm_logger.debug(f"User: {user_email} Total utilization is "
                            f"{json.dumps(over_util_cluster_info['deviations'])}")
            user_json = global_cache.global_user_cache[user_email].to_json()

            gl_over_util_mem = 0
            gl_over_util_core = 0
            # Global DEVIATION may or may not be populated
            global_over_util_info = over_util_cluster_info['deviations'].get("global", {})
            if global_over_util_info:
                gl_over_util_core = global_over_util_info.get("cores", 0) - over_util_cluster_info['quotas'].get('global', {}).get('cores', 0)
                gl_over_util_mem = global_over_util_info.get("memory", 0) - over_util_cluster_info['quotas'].get('global', {}).get('memory', 0)
                cm_logger.debug(f"User: {user_email} Global Over-utilization is {gl_over_util_core} cores, {gl_over_util_mem} memory")
            for cname, resource_info in over_util_cluster_info['deviations'].items():
                if cname == "global":
                    # We want to take care of the individual clusters first,
                    # and then see if the user is still deviating from their global quota
                    continue
                # First check if any VMs without DND can be turned off
                user_vm_list_this_cluster = _user_json_to_list(user_json, cname)
                # cm_logger.info(f"VM list for the user {user_email}: "
                #                f"{json.dumps(user_vm_list_this_cluster)}")
                cl_over_util_core = resource_info.get('cores', 0) - over_util_cluster_info['quotas'].get(cname, {}).get('cores', 0)
                cl_over_util_mem = resource_info.get('memory', 0) - over_util_cluster_info['quotas'].get(cname, {}).get('memory', 0)
                # Calculate all the VMs that need to be turned OFF for this cluster util to get under quota
                # TODO Ignoring the DND VMs
                if cl_over_util_core > 0 and cl_over_util_mem > 0:
                    # First take the VMs which are consuming most of sum(cores, memory)
                    sorted_vm_list = sorted(user_vm_list_this_cluster,
                                            key=lambda x: x[res.CORES]+x[res.MEMORY],
                                            reverse=True)
                    cl_over_util_core, cl_over_util_mem, gl_over_util_core, gl_over_util_mem =\
                        _mark_vm_power_off_greedy(user_vms_marked_power_off,
                                                  sorted_vm_list,
                                                  gl_over_util_core, gl_over_util_mem,
                                                  cl_name=cname,
                                                  cl_core_util=cl_over_util_core,
                                                  cl_mem_util=cl_over_util_mem,
                                                  check_cores=True, check_mem=True)
                if cl_over_util_core > 0:
                    cores_sorted_list = sorted(user_vm_list_this_cluster,
                                               key=lambda x: x[res.CORES],
                                               reverse=True)
                    cl_over_util_core, cl_over_util_mem, gl_over_util_core, gl_over_util_mem = \
                        _mark_vm_power_off_greedy(
                            user_vms_marked_power_off, cores_sorted_list,
                            gl_over_util_core, gl_over_util_mem,
                            cl_name=cname, cl_core_util=cl_over_util_core,
                            cl_mem_util=cl_over_util_mem,
                            check_cores=True, check_mem=False)
                if cl_over_util_mem > 0:
                    memory_sorted_list = sorted(user_vm_list_this_cluster,
                                                key=lambda x: x[res.MEMORY],
                                                reverse=True)
                    # Check how many do we need to turn off (Greedy approach).
                    # If required, can change this algorithm with an IF conditional
                    cl_over_util_core, cl_over_util_mem, gl_over_util_core, gl_over_util_mem = \
                        _mark_vm_power_off_greedy(
                            user_vms_marked_power_off, memory_sorted_list,
                            gl_over_util_core, gl_over_util_mem,
                            cl_name=cname,
                            cl_core_util=cl_over_util_core,
                            cl_mem_util=cl_over_util_mem,
                            check_cores=False, check_mem=True)
                """We have went through all the Non-DND VMs in the cluster to power OFF for this user
                    If the user is still over-subscribed, override the DND mark if provided in the OS env else skip
                """
                if cl_over_util_mem > 0 or cl_over_util_core > 0:
                    if os.env.get("override_dnd", "False").lower() in ["true", 'yes']:
                        cm_logger.info(f"User {user_email} is still over-utilizing"
                                       f" the cluster {cname} by {cl_over_util_core}"
                                       f" cores and {cl_over_util_mem} memory."
                                       f" Considering the DND VMs to power OFF as override is set.")
                        sorted_list = sorted(user_vm_list_this_cluster,
                                                    key=lambda x: x[res.MEMORY]+x[res.CORES],
                                                    reverse=True)
                        cl_over_util_core, cl_over_util_mem, gl_over_util_core, gl_over_util_mem = \
                            _mark_vm_power_off_greedy(
                                user_vms_marked_power_off, sorted_list,
                                gl_over_util_core, gl_over_util_mem,
                                cl_name=cname,
                                cl_core_util=cl_over_util_core,
                                cl_mem_util=cl_over_util_mem,
                                check_cores=False, check_mem=True, skip_dnd=True)
                    else:
                        cm_logger.info(f"User {user_email} is still over-utilizing"
                                       f" the cluster {cname} by {cl_over_util_core}"
                                       f" cores and {cl_over_util_mem} memory."
                                       f" DND VMs are not being considered.")
            # All the clusters are getting under control for this user.
            # Check if the global consumption is under control or not.
            if gl_over_util_core > 0 or gl_over_util_mem > 0:
                cm_logger.info(f"User {user_email} is over-utilizing global quota by "
                               f"Cores:  {gl_over_util_core}, "
                               f"Memory: {gl_over_util_mem}")
                all_vm_of_user = _user_json_to_list(user_json)
                # cm_logger.info(f"All VMs of the user {user_email}: {json.dumps(all_vm_of_user)}")
                if gl_over_util_core > 0:
                    all_vm_list = sorted(all_vm_of_user, key=lambda x: x[res.CORES], reverse=True)
                    cl_over_util_core, cl_over_util_mem, gl_over_util_core, gl_over_util_mem = \
                        _mark_vm_power_off_greedy(
                            user_vms_marked_power_off, all_vm_list,
                            gl_over_util_core, gl_over_util_mem,
                            check_cores=True, check_mem=False)
                if gl_over_util_mem > 0:
                    all_vm_list = sorted(all_vm_of_user, key=lambda x: x[res.MEMORY], reverse=True)
                    cl_over_util_core, cl_over_util_mem, gl_over_util_core, gl_over_util_mem =\
                        _mark_vm_power_off_greedy(
                            user_vms_marked_power_off, all_vm_list,
                            gl_over_util_core, gl_over_util_mem,
                            check_cores=False, check_mem=True)
                if gl_over_util_core > 0 or gl_over_util_mem > 0:
                    cm_logger.error(f"User {user_email} is still over-utilizing the global quota"
                                    f" by Cores: {gl_over_util_core}, Memory: {gl_over_util_mem}")
                    if os.env.get("override_dnd", "False").lower() in ["true", 'yes']:
                        cm_logger.info("Considering the DND VMs to power OFF as override is set.")
                        all_vm_list = sorted(all_vm_of_user, key=lambda x: x[res.CORES]+x[res.CORES], reverse=True)
                        cl_over_util_core, cl_over_util_mem, gl_over_util_core, gl_over_util_mem =\
                            _mark_vm_power_off_greedy(
                                user_vms_marked_power_off, all_vm_list,
                                gl_over_util_core, gl_over_util_mem,
                                check_cores=False, check_mem=True, skip_dnd=True)
                    else:
                        cm_logger.warning(f"Not considering the DND VMs to power OFF as override is not set. User: {user_email}")
            list_of_vm_uuid_cnames = list(user_vms_marked_power_off)
            cm_logger.info(f"User {user_email}, VMs to shut down: "
                           f"{','.join([f'{vm_set[1]}:{vm_set[0]}'
                                        for vm_set in list_of_vm_uuid_cnames])}")
            vm_uuids_to_turn_off.extend(list_of_vm_uuid_cnames)
            # TODO Send emails here
        # We have all the VMs that need to be powered off in the cluster.
        # Perform actual power-off and NIC removal.
        # BACKLOG: Can do multi-threaded
        cm_logger.info("Processing the VMs whose owners are unknown")
        vm_without_prefix = continued_deviations.get('vm_without_prefix', [])
        # cm_logger.info(f"Although not processing them, this is the list of VMs to turn off: {json.dumps(vm_uuids_to_turn_off)}")
        # cm_logger.info(f"Although not processing them, this is the list of VMs without prefix: {json.dumps(vm_without_prefix)}")

        if os.environ.get('eval_mode', "False").lower() not in ['true', 'yes']:
            for vm_uuid, cname in vm_uuids_to_turn_off:
                po_status, po_msg = global_cache.perform_cluster_vm_power_change(
                    cluster_name=cname, vm_info={'uuid': vm_uuid}
                )
                if po_status == HTTPStatus.OK:
                    cm_logger.info(f"PowerOFF:SUCC Cluster {cname}, VM UUID: {vm_uuid}"
                                f". Attempting NIC Removal.")
                    nic_status, nic_msg = global_cache.perform_cluster_vm_nic_remove(
                        cluster_name=cname, vm_info={'uuid': vm_uuid}
                    )
                    if nic_status == HTTPStatus.OK:
                        cm_logger.info(f"PowerOFF:SUCC RemoveNIC:SUCC Cluster "
                                    f"{cname}, VM UUID: {vm_uuid}")
                    else:
                        cm_logger.warning(f"PowerOFF:SUCC RemoveNIC:FAIL Cluster "
                                        f"{cname}, VM UUID: {vm_uuid}. Error: "
                                        f"{nic_msg['message']}")
                else:
                    cm_logger.error(f"PowerOFF:FAIL RemoveNIC:-NA- Cluster "
                                    f"{cname}, VM UUID: {vm_uuid}. Error: "
                                    f"{po_msg['message']}")
                    continue

            #  Check the VMs whose owners are unknown
            #  This is stored as a {cluster1: [(uuid1, vm_name1), (uuid2, vm_name2)]}
            cm_logger.info("Processing the VMs whose owners are unknown")
            vm_without_prefix = continued_deviations.get('vm_without_prefix', [])
            for cname, vm_list in vm_without_prefix.items():
                for vm_uuid, vm_name in vm_list:
                    po_status, po_msg = global_cache.perform_cluster_vm_power_change(
                        cluster_name=cname, vm_info={'uuid': vm_uuid}
                    )
                    if po_status == HTTPStatus.OK:
                        cm_logger.info(f"PowerOFF:SUCC Cluster {cname}, VM UUID: {vm_uuid}"
                                    f". Attempting NIC Removal.")
                        nic_status, nic_msg = global_cache.perform_cluster_vm_nic_remove(
                            cluster_name=cname, vm_info={'uuid': vm_uuid}
                        )
                        if nic_status == HTTPStatus.OK:
                            cm_logger.info(f"PowerOFF:SUCC RemoveNIC:SUCC Cluster "
                                        f"{cname}, VM UUID: {vm_uuid}")
                        else:
                            cm_logger.warning(f"PowerOFF:SUCC RemoveNIC:FAIL Cluster "
                                            f"{cname}, VM UUID: {vm_uuid}. Error: "
                                            f"{nic_msg['message']}")
                    else:
                        cm_logger.error(f"PowerOFF:FAIL RemoveNIC:-NA- Cluster "
                                    f"{cname}, VM UUID: {vm_uuid}. Error: "
                                    f"{po_msg['message']}")
                        continue
