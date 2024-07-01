"""Class to maintain information about the users

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""

import json
import logging
import threading
import typing

from copy import deepcopy

from caching.server_constants import PowerState
from common.constants import Resources as RES, UserKeys as USRK
from custom_exceptions.exceptions import InconsistentCacheError
from tools.helper import BINARY_CONVERSION_FACTOR

USER_LOGGER_ = logging.getLogger(__name__)
USER_LOGGER_.setLevel(logging.DEBUG)
handler = logging.FileHandler("cmgr_user.log", mode='w')
formatter = logging.Formatter("%(filename)s:%(lineno)d - %(asctime)s %(levelname)s - %(message)s")
handler.setFormatter(formatter)
USER_LOGGER_.addHandler(handler)


def _extract_info_from_vm_config(vm_config):
    uuid = vm_config['uuid']
    vm_name = vm_config["name"]
    power_state = vm_config["power_state"]
    parent_cluster = vm_config["cluster"]
    total_resources_used = vm_config['total_resources_used']
    total_mem = total_resources_used["total_mem_used_mb"]
    total_cores = total_resources_used["total_cores_used"]
    return uuid, vm_name, power_state, parent_cluster, total_mem, total_cores


def _log_vm_update(email, vm_config, mem_diff, cores_diff, is_new_cluster=False,
                   is_new_vm=False, old_power_state=PowerState.OFF):
    (uuid, vm_name, current_power_state, parent_cluster, mem, cores) = (
        _extract_info_from_vm_config(vm_config))

    transition_str = "NONE"
    if (old_power_state == PowerState.OFF and
            current_power_state == PowerState.ON):
        transition_str = "OFF -> ON"
    elif (old_power_state == PowerState.ON and
            current_power_state == PowerState.OFF):
        transition_str = "ON -> OFF"
    elif old_power_state == PowerState.UNKNOWN:
        transition_str = "(NEW VM) -> {}".format(current_power_state.upper())

    resource_updt_log_fmt = ("Resource Util UPDATED: User {} - {} cluster {} -"
                             " {} VM {:<20}:{} - Transition: {} - Resources "
                             "diff: {}")
    USER_LOGGER_.info(resource_updt_log_fmt.format(
        email, "NEW" if is_new_cluster else "EXS", parent_cluster,
        "NEW" if is_new_vm else "EXS", vm_name[:20], uuid,
        transition_str, "cores={} memory={}".format(cores_diff, mem_diff)
    ))


class User:

    def __init__(self, config_json):
        self.name = config_json[USRK.NAME]
        self.email = config_json[USRK.EMAIL]

        self.prefixes = []
        for each_prefix in config_json[USRK.PREFIX]:
            self.prefixes.append(each_prefix.lower())

        # Resources quota per user
        self.global_cores_quota = 0
        self.global_mem_quota = 0
        self.quotas = {}
        for each_quota in config_json[USRK.QUOTA]:
            for cluster_name, quota_json in each_quota.items():
                if cluster_name == 'global':
                    # print(self.email)
                    self.global_cores_quota = quota_json[RES.CORES]
                    # This is in GB
                    self.global_mem_quota = quota_json[RES.MEMORY]*BINARY_CONVERSION_FACTOR
                else:
                    self.quotas[cluster_name] = {RES.CORES: quota_json.get(RES.CORES, -1),
                                                 RES.MEMORY: (quota_json.get(RES.MEMORY, -1)*BINARY_CONVERSION_FACTOR)}
        # Verify if the global quota is provided, If not, the total
        # allowed resources will be the limit
        if self.global_mem_quota == 0 or self.global_cores_quota == 0:
            USER_LOGGER_.warning(f"Global quota for the user {self.email}"
                                 f"not specified. Populating with the "
                                 f"current resources utilized.")
            total_mem = 0
            total_core = 0
            for _, quota_json in self.quotas.items():
                total_core += quota_json[RES.CORES] if quota_json[RES.CORES] > 0 else 0
                total_mem += quota_json[RES.MEMORY] if quota_json[RES.MEMORY] > 0 else 0
            self.global_mem_quota = self.global_mem_quota if self.global_mem_quota != 0 else total_mem
            self.global_cores_quota = self.global_cores_quota if self.global_cores_quota != 0 else total_core

        self.powered_on_vms = set()

        self.powered_off_vms = set()

        # Tracks resources consumed for each VM
        self.vm_resource_tracker = dict()
        # Tracks resources consumed per cluster
        self.cluster_resource_tracker = dict()
        # Tracks total resources consumed across clusters
        self.total_resource_tracker = {RES.CORES: 0, RES.MEMORY: 0}
        self.resources_lock = threading.RLock()

    def summary(self, summary_verbosity=0, print_summary=False)\
            -> typing.Optional[typing.Dict]:
        if print_summary:
            count = 0
            list_clusters = []
            for cluster_name, _ in self.cluster_resource_tracker.items():
                count += 1
                list_clusters.append(cluster_name)
            print(f"\tUser {self.email[:25]:<25} - total resources: "
                  f"{self.total_resource_tracker[RES.CORES]:<3} cores, "
                  f"{self.total_resource_tracker[RES.MEMORY]:.3f} GB "
                  f"memory, across {count} cluster{'s' if count > 1 else ''} - "
                  f"[{', '.join(list_clusters)}]")
            if summary_verbosity > 0:
                print("\t\tCluster: " + json.dumps(self.cluster_resource_tracker))
            if summary_verbosity > 1:
                print("\t\tVM: " + json.dumps(self.vm_resource_tracker))
        else:
            return self.to_json()
    
    def to_json(self) -> typing.Dict:
        json_obj = dict()
        json_obj[USRK.NAME] = self.name
        json_obj[USRK.EMAIL] = self.email
        json_obj[USRK.PREFIX] = self.prefixes
        json_obj[USRK.GLOBAL_QUOTA] = {RES.CORES: self.global_cores_quota, RES.MEMORY: self.global_mem_quota}
        json_obj[USRK.CLUSTER_QUOTA] = self.quotas
        json_obj[USRK.GLOBAL_UTILIZED] = self.total_resource_tracker
        json_obj[USRK.CLUSTER_UTILIZED] = self.cluster_resource_tracker
        json_obj[USRK.VM_UTILIZED] = self.vm_resource_tracker
        return deepcopy(json_obj)

    def _back_populate_vm_resources(self, vm_uuid, parent_cluster_name,
                                    mem_diff, cores_diff, power_state=PowerState.OFF):
        with self.resources_lock:
            # Any new VM is added before calling this function
            # If it is a new cluster
            if parent_cluster_name not in self.cluster_resource_tracker:
                self.cluster_resource_tracker[parent_cluster_name] = {
                    RES.CORES: 0,
                    RES.MEMORY: 0
                }
                USER_LOGGER_.debug(f"User: {self.email} - New cluster"
                                   f" {parent_cluster_name} being tracked.")

            USER_LOGGER_.debug(f"VM UUID {vm_uuid} {power_state} - {self.email} - VM {vm_uuid} resources: "
                               f"mem_diff: {mem_diff} cores_diff: {cores_diff}")
            # Update total resources per user
            # if (power_state == PowerState.ON) or (power_state == PowerState.OFF):
            # Update cores and memory for the User per VM
            self.vm_resource_tracker[vm_uuid][RES.CORES] += cores_diff
            self.vm_resource_tracker[vm_uuid][RES.MEMORY] += mem_diff
            # Update cores and memory for the parent cluster
            self.cluster_resource_tracker[parent_cluster_name][RES.MEMORY] += mem_diff
            self.cluster_resource_tracker[parent_cluster_name][RES.CORES] += cores_diff
            # Update global resource usage
            self.total_resource_tracker[RES.MEMORY] += mem_diff
            self.total_resource_tracker[RES.CORES] += cores_diff
            USER_LOGGER_.debug(f"Cluster - {self.email} - Cluster "
                               f"{parent_cluster_name} resources: "
                               f"{self.cluster_resource_tracker[parent_cluster_name]}")
            USER_LOGGER_.debug(f"Total - {self.email}: {self.total_resource_tracker} CLUSTER: {self.cluster_resource_tracker}")

    def update_vm_resources(self, vm_config):
        with self.resources_lock:
            try:
                uuid, name, new_power_state, parent_cluster, vm_mem_used, vm_core_used\
                    = _extract_info_from_vm_config(vm_config)

                # Calculate the difference between the old resources and the new
                # resources.
                # If +ve -> resources consumed
                # If -ve -> resources released
                if new_power_state == PowerState.OFF:
                    vm_old_resources = {RES.CORES: 0, RES.MEMORY: 0}
                else:
                    vm_old_resources = self.vm_resource_tracker.get(uuid, {RES.CORES: 0, RES.MEMORY: 0})

                cores_diff = vm_core_used - vm_old_resources[RES.CORES]
                mem_diff = vm_mem_used - vm_old_resources[RES.MEMORY]
                USER_LOGGER_.debug(f"UUID: {uuid}, cores changed: {cores_diff} mem_diff: {mem_diff}")
                # Existing VM
                if uuid in self.vm_resource_tracker:
                    # old state Powered ON
                    if uuid in self.powered_on_vms:
                        old_power_state = PowerState.ON
                        # No transition (ON -> ON)
                        if new_power_state == PowerState.ON:
                            # If the resources consumption changed
                            if mem_diff != 0 or cores_diff != 0:
                                self._back_populate_vm_resources(uuid, parent_cluster, mem_diff, cores_diff, new_power_state)
                                _log_vm_update(self.email, vm_config, mem_diff, cores_diff,
                                            old_power_state=old_power_state)
                            # else nothing
                            else:
                                USER_LOGGER_.debug(f"VM {uuid} has remained ON")
                                pass
                        
                        else:
                            # Transition from ON -> OFF (The VM has now stopped)
                            USER_LOGGER_.debug(f"VM {uuid} transitioned from ON -> OFF")
                            # Add to the stopped VM cache
                            self.vm_resource_tracker[uuid]['power_state'] = new_power_state
                            self.powered_off_vms.add(uuid)
                            # Update the consumption
                            self._back_populate_vm_resources(uuid, parent_cluster, mem_diff, cores_diff, new_power_state)
                            # Remove from running VM cache
                            self.powered_on_vms.remove(uuid)
                            _log_vm_update(self.email, vm_config, mem_diff, cores_diff,
                                        old_power_state=old_power_state)
                    # Old state Powered OFF
                    elif uuid in self.powered_off_vms:
                        old_power_state = PowerState.OFF
                        # OFF -> ON transition
                        if new_power_state == PowerState.ON:
                            self.vm_resource_tracker[uuid]['power_state'] = new_power_state
                            # Add to the running VM cache
                            self.powered_on_vms.add(uuid)
                            # Release resources
                            self._back_populate_vm_resources(uuid, parent_cluster, mem_diff, cores_diff, new_power_state)
                            # Remove from OFF Cache
                            self.powered_off_vms.remove(uuid)
                            _log_vm_update(self.email, vm_config, mem_diff, cores_diff,
                                        old_power_state=old_power_state)
                        # No transition (OFF -> OFF)
                        else:
                            USER_LOGGER_.debug(f"VM {uuid} has remained OFF")
                            pass
                # New VM
                else:
                    self.vm_resource_tracker[uuid] = {
                        RES.CORES: 0,
                        RES.MEMORY: 0,
                        "parent_cluster": parent_cluster,
                        "name": name,
                        "power_state": new_power_state
                    }
                    USER_LOGGER_.debug(f"{self.email} - New VM {uuid} being tracked.")

                    if new_power_state == PowerState.ON:
                        # The resources will be updated in _update_all_resources
                        self.powered_on_vms.add(uuid)
                        self._back_populate_vm_resources(uuid, parent_cluster, mem_diff, cores_diff)
                    else:
                        self.powered_off_vms.add(uuid)
                        # Check if the Parent cluster is already being tracked. Since the new VM is powered OFF, the resources will not be updated
                        self._back_populate_vm_resources(uuid, parent_cluster, 0, 0)
                    _log_vm_update(self.email, vm_config, mem_diff, cores_diff,
                                old_power_state=PowerState.UNKNOWN)

                self._check_consistency()
            except Exception as ex:
                USER_LOGGER_.exception(ex)
                raise ex

    def update_prefix(self, prefix, op) -> typing.Optional[typing.List]:
        """Adds or removes a prefix for the user.
        
        Only to be called by the cluster_cache to maintain consistency.
        
        Args:
            prefix (str): Prefix to be updated for this user
            op (str): Operation to perform (add or remove)
        Raises:
            Exception
        """
        # Since a user has only a few prefixes, O(n) for remove should be fine
        if op == "add":
            self.prefixes.append(prefix)
            USER_LOGGER_.info(f"Adding prefix {prefix} for the user "
                              f"{self.email}.")
        elif op == "remove":
            self.prefixes.remove(prefix)
            to_delete_uuid = []
            delete_vm_name = []
            for uuid, vm_info in self.vm_resource_tracker.items():
                if vm_info['name'].lower().startswith(prefix):
                    delete_vm_name.append(vm_info['name'])
                    to_delete_uuid.append((uuid, vm_info['parent_cluster']))
            for (uuid, parent_cluster) in to_delete_uuid:
                self.process_deleted_vm(uuid)
            USER_LOGGER_.info(f"Removed prefix {prefix} for the user "
                              f"{self.email}. Removed the following VMs: "
                              f"{','.join(delete_vm_name)}")
            return to_delete_uuid
        else:
            raise Exception(f"Unknown operation {op} requested.")

    def get_all_vms(self) -> set:
        """Returns a set of all the tracked VMs for this user
            (Running and stopped)

            Returns:
                A set of all the VMs (UUIDs) for the current user
        """
        return self.powered_on_vms | self.powered_off_vms

    def _calculate_return_consumed_resources(self) -> typing.Tuple[dict, float, float]:
        # Consolidate all the VMs
        vm_calculated_resources_per_cluster = {}
        for uuid, vm_info in self.vm_resource_tracker.items():
            cluster_name = vm_info['parent_cluster']
            if cluster_name not in vm_calculated_resources_per_cluster:
                vm_calculated_resources_per_cluster[cluster_name] = {RES.CORES: 0, RES.MEMORY: 0}
            if vm_info['power_state'] == PowerState.ON:
                vm_calculated_resources_per_cluster[cluster_name][RES.CORES] += vm_info[RES.CORES]
                vm_calculated_resources_per_cluster[cluster_name][RES.MEMORY] += vm_info[RES.MEMORY]

        # Consolidate all the Clusters
        cluster_name_list = []
        cluster_consolidated_cores = 0
        cluster_consolidated_mem = 0
        for cluster_name, resources in self.cluster_resource_tracker.items():
            USER_LOGGER_.debug(f"Cluster {cluster_name} - {resources}")
            cluster_name_list.append(cluster_name)
            cluster_consolidated_cores += resources[RES.CORES]
            cluster_consolidated_mem += resources[RES.MEMORY]
        USER_LOGGER_.debug(f"{self.name} cluster_consolidated_cores: {cluster_consolidated_cores} cluster_consolidated_mem: {cluster_consolidated_mem}")

        # Verify the resources in all VMs against their respective clusters
        for cluster_name, resources in vm_calculated_resources_per_cluster.items():
            cached_resources = self.cluster_resource_tracker.get(cluster_name, None)
            if cached_resources is None:
                raise InconsistentCacheError(f"UserCache: Cache for the cluster"
                                             f" {cluster_name} not found! User: {self.email}")
            if int(cached_resources[RES.CORES]) != self.cluster_resource_tracker[cluster_name][RES.CORES]:
                raise InconsistentCacheError(f"UserCache: Number of cores alloc for the user {self.email}"
                                             f" on the cluster {cluster_name} is "
                                             f"cached as {cached_resources[RES.CORES]} "
                                             f"which is different than the calculated "
                                             f"cores {self.cluster_resource_tracker[cluster_name][RES.CORES]}")
            if cached_resources[RES.MEMORY] != self.cluster_resource_tracker[cluster_name][RES.MEMORY]:
                raise InconsistentCacheError(f"UserCache: Memory allocated for user {self.email}"
                                             f" on the cluster {cluster_name} is "
                                             f"cached as {cached_resources[RES.MEMORY]} "
                                             f"which is different than the "
                                             f"calculated memory "
                                             f"{self.cluster_resource_tracker[cluster_name][RES.MEMORY]}")
        # Verify the resources in all clusters against total cache
        if cluster_consolidated_cores != self.total_resource_tracker[RES.CORES]:
            raise InconsistentCacheError(f"UserCache: Number of cores for the"
                                         f" total cache is "
                                         f"cached as {self.total_resource_tracker[RES.CORES]} "
                                         f"which is different than the calculated "
                                         f"cores {cluster_consolidated_cores}")
        if cluster_consolidated_mem != self.total_resource_tracker[RES.MEMORY]:
            raise InconsistentCacheError(f"UserCache: Number of memory for the"
                                         f" total cache is "
                                         f"cached as {self.total_resource_tracker[RES.MEMORY]} "
                                         f"which is different than the calculated "
                                         f"memory {cluster_consolidated_mem}")
        return vm_calculated_resources_per_cluster, cluster_consolidated_cores, cluster_consolidated_mem

    def _check_consistency(self):
        """Checks consistency of the cache before and after update operations
            Raises:
                InconsistentCacheError
        """
        # After each complete update, there should not be any intersection
        # in the UUIDs in running and powered OFF VMs
        intersection = self.powered_off_vms & self.powered_on_vms
        if intersection:
            raise InconsistentCacheError("The cache is not consistent")
        self._calculate_return_consumed_resources()
        USER_LOGGER_.info(f"User cache is consistent for the user {self.email}")

    def process_deleted_vm(self, deleted_vm_uuid):
        # Since the VM Cache were already populated, we can be sure that this
        # will exist.
        if deleted_vm_uuid in self.powered_on_vms:
            # We need to process the resources only if the VM was running
            old_resources = self.vm_resource_tracker[deleted_vm_uuid]
            mem_diff = (-1) * old_resources[RES.MEMORY]
            cores_diff = (-1) * old_resources[RES.CORES]
            parent_cluster_name = old_resources['parent_cluster']
            self._back_populate_vm_resources(deleted_vm_uuid, parent_cluster_name,
                                             mem_diff, cores_diff)
            self.powered_on_vms.remove(deleted_vm_uuid)
        elif deleted_vm_uuid in self.powered_off_vms:
            self.powered_off_vms.remove(deleted_vm_uuid)
        # Since the VM is deleted, it is safe to delete from the resource tracker
        if deleted_vm_uuid in self.vm_resource_tracker:
            del self.vm_resource_tracker[deleted_vm_uuid]
        self._check_consistency()

    def is_over_utilizing_quota(self) -> typing.Tuple[bool, dict]:
        resources = self._calculate_return_consumed_resources()
        per_cluster_resource_used = resources[0]
        total_cores_used = resources[1]
        total_memory_used = resources[2]
        is_offending = False
        offenses = {}
        # Check for individual clusters
        for cluster_name, res_used in per_cluster_resource_used.items():
            if cluster_name in self.quotas:
                for key in [RES.CORES, RES.MEMORY]:
                    if key in self.quotas[cluster_name]:
                        if (self.quotas[cluster_name][key] > 0 and
                            res_used[key] > self.quotas[cluster_name][key]):
                            is_offending = True
                            val = self.quotas[cluster_name][key]
                            diff = res_used[key] - val
                            USER_LOGGER_.warning(f"User {self.email} is over-utilizing "
                                                 f"allocated {key} on the cluster "
                                                 f"{cluster_name} by {diff} "
                                                 f"{"GB" if key == RES.MEMORY else ""}. "
                                                 f"Quota: {val if val > 0 else "Nil"}"
                                                 f", used: {res_used[key]}")
                            if cluster_name not in offenses:
                                offenses[cluster_name] = {}
                            offenses[cluster_name][key] = diff

        # Check for total quota
        if total_cores_used > self.global_cores_quota:
            is_offending = True
            diff_core = total_cores_used - self.global_cores_quota
            offenses["global"] = {RES.CORES: diff_core}
        if total_memory_used > self.global_mem_quota:
            is_offending = True
            if "global" not in offenses:
                offenses["global"] = {}
            offenses["global"][RES.MEMORY] = total_memory_used - self.global_mem_quota

        # Add quotas to reduce the API calls to the global cache
        # quotas = {"global": {}, "cluster": {}}
        # quotas["global"][RES.CORES] = self.global_cores_quota
        # quotas["global"][RES.MEMORY] = self.global_mem_quota
        # quotas["cluster"] = self.quotas
        # offenses["\"] = quotas

        return is_offending, offenses

    def update_name(self, new_name) -> bool:
        USER_LOGGER_.info("Updated the name of the user with email {self.email}"
                          " from {self.name} to {new_name}")
        self.name = new_name
        return True

    def update_resource_quota(self,
                              quota: typing.Dict
                            ) -> bool:
        """Function to update the resources quota for the user.
        Since we are updating the quota, consistency check is not required.

        NOTE: This is typically used in a PATCH operation, so anything that is received
        in the request over-writes the existing value.

        Args:
            quota (Dict); This is the dict of values to be updated for a\
                cluster or global in the form of JSONs:
                {
                    "global": {},
                    "cname1": {},
                    "cname2": {},
                }
        """
        for cluster_name, quota_json in quota.items():
            if cluster_name == 'global':
                if RES.CORES in quota_json:
                    USER_LOGGER_.info(f"Updated global cores quota for the user {self.email} from {self.global_cores_quota} to {quota_json[RES.CORES]}")
                    self.global_cores_quota = quota_json[RES.CORES]
                if RES.MEMORY in quota_json:
                    USER_LOGGER_.info(f"Updated global memory quota for the user {self.email} from {self.global_mem_quota} to {quota_json[RES.MEMORY]}")
                    self.global_mem_quota = quota_json[RES.MEMORY]
            else:
                if cluster_name not in self.quotas:
                    self.quotas[cluster_name] = {}
                for key in [RES.CORES, RES.MEMORY]:
                    if key in quota_json:
                        self.quotas[cluster_name][key] = quota_json[key]
        return True

    def untrack_cluster(self, cluster_name):
        """Function to untrack the cluster for the user.
        Performs:
            1. Lists all the VMs that need to be untracked
            2. Processes the deletion of the VMs (in the user context)
            3. Updates the quota for the user
            4. Updates the cluster resource tracker

        Args:
            cluster_name (str): Name of the cluster to be untracked
        """
        with self.resources_lock:
            # Delete the VMs and process their resources
            uuid_to_del = []
            for uuid, vm_info in self.vm_resource_tracker.items():
                if vm_info['parent_cluster'] == cluster_name:
                    uuid_to_del.append(uuid)
            if uuid_to_del:
                USER_LOGGER_.debug(f"Deleting VMs on {cluster_name} for user "
                                   f"{self.email}: {','.join(uuid_to_del)}")
            for uuid in uuid_to_del:
                self.process_deleted_vm(uuid)
            if cluster_name in self.quotas:
                memory = self.quotas[cluster_name].get(RES.MEMORY, -1)
                cores = self.quotas[cluster_name].get(RES.CORES, -1)
                if memory > 0:
                    self.global_mem_quota -= memory
                if cores > 0:
                    self.global_cores_quota -= cores
                USER_LOGGER_.info(f"Untracking the quota for cluster "
                                  f"'{cluster_name}' for the user {self.email}")
                del self.quotas[cluster_name]
            else:
                USER_LOGGER_.info(f"No quota existed for cluster "
                                  f"'{cluster_name}' for the user {self.email}")
            if cluster_name in self.cluster_resource_tracker:
                USER_LOGGER_.info(f"Untracking the cluster '{cluster_name}' "
                                  f"for the user {self.email}")
                del self.cluster_resource_tracker[cluster_name]
            return True
