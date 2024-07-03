"""Class Cluster
    Logic to fetch, list, categorize and cache VMs running on a cluster

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""

import json
import logging
import requests
import time
import typing
import urllib3

from concurrent import futures
from threading import Lock
from datetime import datetime
from http import HTTPMethod, HTTPStatus

from caching.NuVM import NuVM
from caching.server_constants import PRISM_PORT, PRISM_REST_FINAL_EP, \
    basic_auth_header, HTTPS, VM_ENDPOINT, generate_query_string, \
    CacheState, PowerState, HOSTS_EP, CLUSTER_EP, check_vm_name_to_skip
from tools.helper import bytes_to_mb
from custom_exceptions.exceptions import InconsistentCacheError
from common.constants import NuVMKeys, ClusterKeys, TaskStatus

urllib3.disable_warnings()

# Setting up the logging
CLUSTER_CACHE_LOGGER_ = logging.getLogger(__name__)
CLUSTER_CACHE_LOGGER_.setLevel(logging.DEBUG)
handler = logging.FileHandler("cmgr_cluster.log", mode='w')
formatter = logging.Formatter("%(filename)s:%(lineno)d - %(asctime)s %(levelname)s - %(message)s")
handler.setFormatter(formatter)
CLUSTER_CACHE_LOGGER_.addHandler(handler)


def update_managed_vm_memory_per_cvm(ndb_cvm_obj, prism_resp_json):
    ndb_cvm_obj.update_db_server_mem_consumed(prism_resp_json)


class Cluster:
    """Class encapsulating the logic for fetching, processing and caching the
        information of VMs running on a single cluster
    """

    def __init__(self, name, ip, username, password):
        self.name = name
        self._ip_address = ip
        self.available_memory = 0 #  Stored in Bytes
        self.available_cores = 0
        # Populated by querying the Prism API
        self.id = ""
        self.cluster_uuid = ""
        self.hosts = []

        self.cache_state = CacheState.PENDING
        self.prism_rest_ep = (HTTPS + self._ip_address + f":{str(PRISM_PORT)}" +
                              PRISM_REST_FINAL_EP)

        self._username = username
        self._password = password
        self._headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': basic_auth_header(self._username, self._password)
        }
        self._threadpool_exec = futures.ThreadPoolExecutor(max_workers=15)

        # Variables specific to the management framework
        self.cache_build_done_ts = None
        # This is the total manageable memory and cores UTILIZED/Allocated on the cluster
        self.utilized_resources = {ClusterKeys.CORES: 0, ClusterKeys.MEMORY: 0}
        
        # This is the absolute total memory and cores UTILIZED/Allocated on the cluster
        # Specifically includes the white-listed VMs
        # The memory is always stored in MBs
        self.abs_utilized_resources = {ClusterKeys.CORES: 0, ClusterKeys.MEMORY: 0}

        # Names of the VMs that do not follow naming conv and
        # the timestamp of checking
        self.vms_not_following_naming_conv = {}

        self.power_off_vms = {}
        self.powered_off_vm_count = 0

        self.vm_cache = {}  # Tracks the powered ON VMs
        self.vm_cache_lock = Lock()
        self.vm_count = 0
        self.vms_added_since_last_cache_build = set()

        # These are the template VMs which will not be added to the regular
        # caching logic. We will only store config instead of creating an object
        self.skipped_vms = dict()

        self.abs_available_memory = 0
        self.abs_available_cores = 0
        self.cluster_info_populated = False

        self._fetch_cluster_host_info()

    def is_cache_ready(self) -> bool:
        """Check if the cache is ready to be utilized
        Returns:
            bool: True if Cache is ready, else False
        """
        # FIXME -- locking?
        if self.cache_state == CacheState.READY:
            return True
        return False

    def _fetch_cluster_host_info(self):
        """Populates the Cluster's hosts, combined and individual host
            resources and UUIDs
        """
        # Get the Cluster UUID, as it is required to power off a VM
        try:
            status_code, resp_json = self._request_cluster(self.prism_rest_ep + CLUSTER_EP)
        except requests.exceptions.Timeout as ex:
            self.cluster_info_populated = False
            CLUSTER_CACHE_LOGGER_.exception(f"Timeout occurred: {ex}")
            return
        self.cluster_uuid = resp_json['cluster_uuid']
        self.id = resp_json['id']

        # Populate the hosts and their resources
        mem = 0
        cores = 0
        try:
            status_code, resp_json = self._request_cluster(self.prism_rest_ep + HOSTS_EP)
        except requests.exceptions.Timeout as ex:
            self.cluster_info_populated = False
            CLUSTER_CACHE_LOGGER_.exception(f"Fetching hosts information failed for the cluster {self.name} : {ex}")
            return

        for entity in resp_json['entities']:
            temp_mem = entity.get('memory_capacity_in_bytes', 0)
            if temp_mem is not None:
                mem += temp_mem
            else:
                CLUSTER_CACHE_LOGGER_.error(f"Memory capacity not found for host {entity['name']} UUID: {entity['uuid']} on cluster {self.name}")
            temp_cores = entity.get('num_cpu_cores', 0)
            temp_threads = entity.get('num_cpu_threads', 0)
            if temp_cores is not None and temp_threads is not None:
                cores += temp_threads * temp_cores
            else:
                CLUSTER_CACHE_LOGGER_.error(f"CPU not found for host {entity['name']} UUID: {entity['uuid']} on cluster {self.name}")
            pass
        # Stored in Bytes
        self.abs_available_memory = bytes_to_mb(mem)
        self.abs_available_cores = cores

        # If everything is successful, mark the information is populated
        self.cluster_info_populated = True
        pass

    def summary(self, summary_verbosity=0, print_summary=False
                ) -> typing.Optional[typing.Dict]:
        """Summaries the Cluster, Can print the summary or get a JSON which holds everything.
            Args:
                summary_verbosity (int): Level at which the summary should be presented
                print_summary (bool): Set True to flush the summary to the STDOUT
        """
        with self.vm_cache_lock:
            biggest_ts = 0
            for ts, _ in self.vms_not_following_naming_conv.items():
                if ts > biggest_ts:
                    biggest_ts = ts
            if print_summary:
                print(f"\nCluster '{self.name}' has total: {self.vm_count} Powered ON "
                      f"VMs, which are allocated {self.utilized_resources['cores']} "
                      f"cores and {self.utilized_resources['memory']}"
                      f"GB memory.")
                print(f"Cluster has total: {self.powered_off_vm_count} "
                      f"Powered OFF VMs")
                if summary_verbosity > 0:
                    print(f"{len(self.vms_not_following_naming_conv.get(biggest_ts, []))} VMs do not "
                          f"conform to the naming convention")
                if summary_verbosity > 1:
                    print("List of VMs not following name convention: " + ", "
                          .join(self.vms_not_following_naming_conv.get(
                                    biggest_ts, []))
                    )
                if summary_verbosity > 2:
                    for _, each_vm in self.vm_cache.items():
                        each_vm.summary(print_summary=print_summary)
            else:
                cluster_info = {
                    ClusterKeys.NAME: self.name,
                    "count_powered_off_vm": self.powered_off_vm_count,
                    "health_status": self.get_health_status()
                }
                if summary_verbosity > 1:
                    cluster_info["list_powered_off_vm"] = list(self.power_off_vms.keys())
                    cluster_info["list_powered_on_vm"] = list(self.vm_cache.keys())
                if summary_verbosity > 2:
                    cluster_info['vm_info'] = {}
                    for vm_uuid, each_vm in self.vm_cache.items():
                        cluster_info['vm_info'][vm_uuid] = each_vm.summary(print_summary=print_summary)
                return cluster_info

    def to_json(self):
        """Convert the Cluster object to a JSON
        """
        from copy import deepcopy
        return deepcopy({
            "name": self.name,
            "ip": self._ip_address,
            "username": self._username,
            "password": self._password
        })

    def add_vm_with_no_prefix(self, timestamp, vm_name, uuid):
        """Add a new VM whch does not have a recognizable prefix to the cache
        Args:
            timestamp (int): int(time) value signifying the timestamp at which\
                  the the VMs were processed.
            vm_name (str): The VM Name
            uuid (str): The UUID of this VM
        """
        if timestamp not in self.vms_not_following_naming_conv:
            self.vms_not_following_naming_conv[timestamp] = []
        # Since the mapping comes from cache, the VM is already part of the Cache
        vm_obj = self.vm_cache.get(uuid, None)
        if not vm_obj:
            vm_obj = self.power_off_vms.get(uuid, None)
        cores = vm_obj.cores_used_per_vcpu * vm_obj.num_vcpu if vm_obj.power_state == PowerState.ON else 0
        mem = vm_obj.memory if vm_obj.power_state == PowerState.ON else 0
        power_state = vm_obj.power_state
        self.vms_not_following_naming_conv[timestamp].append((uuid, vm_name, cores, mem, power_state))

    def get_vm_with_no_prefix(self,
                              sort_by_power_state=False,
                              sort_by_memory=False,
                              sort_by_cores=False) -> list:
        """Returns a new VM whch does not have a recognizable prefix to the cache
            By default sorts the list by the name of the VM.

            Args:
                sort_by_power_state (bool): Sort the list by the power state of the VM
                sort_by_memory (bool): Sort the list by the memory allocated to the VM
                sort_by_cores (bool): Sort the list by the number of cores allocated to the VM

            Returns
                list of tuple(uuid, name) of the VM whose name does not \
                conform to the naming convention
        """
        biggest_ts = 0
        for ts, _ in self.vms_not_following_naming_conv.items():
            if ts > biggest_ts:
                biggest_ts = ts
        vm_list = self.vms_not_following_naming_conv.get(biggest_ts, [])
        if sort_by_power_state:
            return sorted(vm_list, key=lambda x: x[4])
        return sorted(vm_list, key=lambda x: x[1].lower())

    def build_refresh_cache(self) -> None:
        """Build and/or refresh the VM cache for the cluster.
            Cache rebuild takes time -- this op is expected to work
             like a state machine. We will try to populate the cache
             asynchronously.
             The cache_state will signify when the cache is ready to be used.

             We are performing cache-rebuild one cluster per thread
                (no interference from other threads)
            The function _categorize_vms_on_cluster does not exit before all
                threads from threadpool are done executing
                (all VMs are contacted and categorized)
            == the cache state update is atomic

            FIXME: There can be race condition in READ -- if the READ checks \
            just before the update starts. However, if we keep the refresh \
            intervals very spread out and time the reads so that there is \
            minimal interference -- we can potentially do this lockless.

            Calls: _categorize_vms_on_cluster
        """
        if self.cache_state == CacheState.READY:
            CLUSTER_CACHE_LOGGER_.info(f"Cache refresh start for Cluster "
                                       f"{self.name} Start time: {datetime.now()}")
            self.cache_state = CacheState.REBUILDING
        elif self.cache_state == CacheState.PENDING:
            CLUSTER_CACHE_LOGGER_.info(f"Cache build start for Cluster "
                                       f"{self.name} Start time: {datetime.now()}")
            self.cache_state = CacheState.BUILDING

        self._fetch_and_cache_vm()

        self.cache_state = CacheState.READY
        self.cache_build_done_ts = time.time()

        CLUSTER_CACHE_LOGGER_.info(f"Cache build done for Cluster "
                                   f"{self.name} Finish time: {datetime.now()},"
                                   f" timestamp: {self.cache_build_done_ts}")

    def _verify_conn(self):
        """TODO Verify if the Cluster is reachable.
        If not, check with other IP addresses (if available)"""
        pass

    def _fetch_vms_on_cluster(self) -> list:
        """Helper function to fetch all the VMs created on the cluster
            We set the include_vm_nic_config param as True to get the
            IP address information of the VMs

        Returns:
            List of all VMs running on the cluster
        """
        res = None
        CLUSTER_CACHE_LOGGER_.info(f"Trying to fetch VMs "
                                   f"on the cluster {self.name}")

        try:
            extra_requests = {"include_vm_nic_config": True}
            status_code, resp_json = self._request_cluster(self.prism_rest_ep +
                                                           VM_ENDPOINT +
                                                           generate_query_string(extra_requests))
            CLUSTER_CACHE_LOGGER_.info(f"VMs on the cluster  "
                                       f"{self.name} fetched!")
            return resp_json['entities']
        except Exception as ex:
            CLUSTER_CACHE_LOGGER_.exception(f"Exception occurred: {ex}."
                                            f"\n\t\tResponse: {res.status_code}")
            raise ex

    def _check_update_resources(self, new_config, old_config=None, is_whitelisted=True):
        """Utility function to check and update the resources used by a whitelisted VM (Consolidating the checks)
            Args:
                new_config (dict): New VM config
                old_config (dict): Old VM config
                is_whitelisted (bool): If the VM is whitelisted, only update absolute_resources

            Returns:
                None
        """
        if is_whitelisted:
            # If the old config is None == new VM
            if old_config is None:
                CLUSTER_CACHE_LOGGER_.debug(f"NEW {new_config['name']}:{new_config['power_state']} ==> Resources: core->{new_config['num_cores_per_vcpu'] * new_config['num_vcpus']} memory->{new_config['memory_mb']}")
                CLUSTER_CACHE_LOGGER_.debug(f"NEW ==> Absolute Resources: {self.abs_utilized_resources}")
                if new_config['power_state'] == PowerState.ON:
                    self.abs_utilized_resources[ClusterKeys.CORES] += \
                        new_config['num_cores_per_vcpu'] * new_config['num_vcpus']
                    self.abs_utilized_resources[ClusterKeys.MEMORY] += new_config['memory_mb']
                CLUSTER_CACHE_LOGGER_.debug(f"NEW ==> Resources After-updating {new_config['name']}: {self.abs_utilized_resources}")
                return
            # If previously OFF
            if old_config['power_state'] == PowerState.OFF:
                # And transitioned to ON
                if new_config['power_state'] == PowerState.ON:
                    self.abs_utilized_resources[ClusterKeys.CORES] += \
                        new_config['num_cores_per_vcpu'] * new_config['num_vcpus']
                    self.abs_utilized_resources[ClusterKeys.MEMORY] += new_config['memory_mb']
                    return
                else:
                    pass
            else:
                # If previously ON
                # If transitioned to OFF, release the resources
                if new_config['power_state'] == PowerState.OFF:
                    self.abs_utilized_resources[ClusterKeys.CORES] -= \
                        old_config['num_cores_per_vcpu'] * old_config['num_vcpus']
                    self.abs_utilized_resources[ClusterKeys.MEMORY] -= old_config['memory_mb']
                    return
                else:
                    # still powered ON, check and update if the resources have changed
                    old_cpu = old_config['num_cores_per_vcpu'] * old_config['num_vcpus']
                    old_mem = old_config['memory_mb']
                    new_cpu = new_config['num_cores_per_vcpu'] * new_config['num_vcpus']
                    new_mem = new_config['memory_mb']
                    cpu_diff = old_cpu - new_cpu
                    mem_diff = old_mem - new_mem
                    self.abs_utilized_resources[ClusterKeys.CORES] += cpu_diff
                    self.abs_utilized_resources[ClusterKeys.MEMORY] += mem_diff
                    return
        else:
            # Not yet implemented, handled through the other code path
            pass

    def _fetch_and_cache_vm(self) -> None:
        """Parent function which performs following:
             1. Fetch all the VMs in the cluster
             2. Adds them to the cache

        Returns:
            None

        Raises:
            None
        TODO Check if the name of the VM has changed since last fetch
        """
        if self.cache_state not in [CacheState.REBUILDING, CacheState.BUILDING]:
            raise Exception(f"Cluster: {self.name} Cache should be in either "
                            f"REBUILDING or BUILDING state. Found: "
                            f"{CacheState.to_str(self.cache_state)}")

        if not self.cluster_info_populated:
            self._fetch_cluster_host_info()

        try:
            all_vm_config = self._fetch_vms_on_cluster()
        except Exception as ex:
            ex_str = (f"Exception occurred when fetching VMs on the cluster "
                      f"{self.name}. Setting the cache_state to "
                      f"{CacheState.to_str(CacheState.PENDING)}")
            CLUSTER_CACHE_LOGGER_.error(ex_str)
            self.cache_state = CacheState.PENDING
            return

        with self.vm_cache_lock:
            for vm_config in all_vm_config:
                uuid = vm_config["uuid"]
                name = vm_config["name"]
                if check_vm_name_to_skip(name):
                    CLUSTER_CACHE_LOGGER_.debug(f"Skipped caching whitelisted VM {name}, uuid {uuid}")
                    # It is possible that the name might have changed
                    if uuid in self.skipped_vms:
                        if self.skipped_vms[uuid]['name'] != name:
                            CLUSTER_CACHE_LOGGER_.info(f"(TEMPLATE VM Name "
                                                       f"change alert) TO_SKIP"
                                                       f" VM UUID {uuid} old_name: "
                                                       f"{self.skipped_vms[uuid]}"
                                                       f" -> new_name:{name}")
                    self._check_update_resources(vm_config, self.skipped_vms.get(uuid))
                    self.skipped_vms[uuid] = vm_config
                    continue
                # If the VM is running
                if vm_config["power_state"] == PowerState.ON:
                    # And was previously shut down, update the resources used
                    if uuid in self.power_off_vms:
                        vm_obj = self.power_off_vms[uuid]
                        vm_json = vm_obj.to_json()
                        owner_email = vm_json[NuVMKeys.OWNER_EMAIL]
                        vm_obj.process_power_on(vm_config["memory_mb"],
                                                vm_config['num_vcpus'],
                                                vm_config['num_cores_per_vcpu'],
                                                vm_config['host_uuid'])
                        new_cores_used = vm_config['num_vcpus'] * vm_config['num_cores_per_vcpu']
                        new_mem_used = vm_config['memory_mb']
                        # Add the resources
                        self.utilized_resources[ClusterKeys.CORES] += new_cores_used
                        self.utilized_resources[ClusterKeys.MEMORY] += new_mem_used
                        # Update the resources for absolute used resources
                        self.abs_utilized_resources[ClusterKeys.CORES] += new_cores_used
                        self.abs_utilized_resources[ClusterKeys.MEMORY] += new_mem_used
                        # As the VM turned ON, it now has a host_uuid
                        vm_obj.host_uuid = vm_config["host_uuid"]
                        # Add the VM to the cache tracking running VMs
                        self.vm_count += 1
                        self.vm_cache[uuid] = vm_obj
                        # Remove the object from powered down VMs cache
                        del self.power_off_vms[uuid]
                        self.powered_off_vm_count -= 1
                        CLUSTER_CACHE_LOGGER_.info(f"VM {vm_obj.name} owned"
                                                   f" by user {owner_email}"
                                                   f" was previously OFF, "
                                                   f"but is now ON. "
                                                   f"Consumed {new_cores_used} "
                                                   f"cores and {new_mem_used} "
                                                   f"memory.")
                        # Since the NIC config and the IP can change, just update it
                        vm_obj.nics = vm_config.get('vm_nics', [])
                        continue
                    # If it was already cached, no changes
                    if uuid in self.vm_cache:
                        if uuid in self.vms_added_since_last_cache_build:
                            self.vms_added_since_last_cache_build.remove(uuid)
                        vm_obj = self.vm_cache[uuid]
                        # Since the NIC config and the IP can change, just update it
                        vm_obj.nics = vm_config.get('vm_nics', [])
                        if (vm_obj.cores_used_per_vcpu != vm_config.get("num_cores_per_vcpu") or
                            vm_obj.num_vcpu != vm_config.get("num_vcpus")):
                            CLUSTER_CACHE_LOGGER_.info(f"VM {vm_obj.name}, cores_per_vcpu: "
                                                       f"{vm_obj.cores_used_per_vcpu} -> "
                                                       f"{vm_config.get("num_cores_per_vcpu")}; "
                                                       f"num_vcpu: {vm_obj.num_vcpu}"
                                                       f" -> {vm_config.get("num_vcpus")}")
                            core_diff = ((vm_config.get("num_cores_per_vcpu") *
                                         vm_config.get("num_vcpus")) -
                                         (vm_obj.cores_used_per_vcpu*vm_obj.num_vcpu))
                            vm_obj.cores_used_per_vcpu = vm_config.get("num_cores_per_vcpu")
                            vm_obj.num_vcpu = vm_config.get("num_vcpus")
                            self.utilized_resources[ClusterKeys.CORES] += core_diff
                            # Update the resources for absolute used resources
                            self.abs_utilized_resources[ClusterKeys.CORES] += core_diff
                        if vm_obj.memory != vm_config.get("memory_mb"):
                            CLUSTER_CACHE_LOGGER_.info(f"VM {vm_obj.name}, "
                                                       f"Memory: {vm_obj.memory}"
                                                       f" -> {vm_config.get("memory_mb")}")
                            mem_diff = vm_config.get("memory_mb") - vm_obj.memory
                            vm_obj.memory = vm_config.get("memory_mb")
                            self.utilized_resources[ClusterKeys.MEMORY] += mem_diff
                            self.abs_utilized_resources[ClusterKeys.MEMORY] += mem_diff
                        continue
                    # If the VM is a new VM altogether
                    else:
                        self.vm_count += 1
                        new_cores = vm_config["num_cores_per_vcpu"] * vm_config["num_vcpus"]
                        self.utilized_resources[ClusterKeys.CORES] += new_cores
                        self.abs_utilized_resources[ClusterKeys.CORES] += new_cores
                            
                        self.utilized_resources[ClusterKeys.MEMORY] += vm_config['memory_mb']
                        self.abs_utilized_resources[ClusterKeys.MEMORY] += vm_config['memory_mb']

                        self.vm_cache[uuid] = NuVM(vm_config, self.name)

                        CLUSTER_CACHE_LOGGER_.debug(f"Added VM {vm_config['name']}"
                                                    f" to the cache")
                        self.vms_added_since_last_cache_build.add(uuid)
                # If the VM is now turned OFF
                else:
                    # If it was previously running, update the resources
                    # used for the cluster and the resource used per user
                    if uuid not in self.power_off_vms:
                        if uuid in self.vm_cache:
                            # Get the VM from cache
                            vm_obj = self.vm_cache[uuid]
                            vm_json = vm_obj.to_json()
                            owner_email = vm_json[NuVMKeys.OWNER_EMAIL]
                            # Release the cluster resources
                            prev_res = vm_json['total_resources_used']
                            core_diff = prev_res[NuVMKeys.CORES_USED]
                            mem_diff = prev_res[NuVMKeys.MEMORY_USED]
                            # Release the resources
                            self.utilized_resources[ClusterKeys.CORES] -= core_diff
                            self.utilized_resources[ClusterKeys.MEMORY] -= mem_diff
                            # Update the resources for absolute used resources
                            self.abs_utilized_resources[ClusterKeys.CORES] -= core_diff
                            self.abs_utilized_resources[ClusterKeys.MEMORY] -= mem_diff
                            # Update the VM resources to 0 and
                            # Delete the host_uuid from the VM object
                            vm_obj.process_power_off()
                            # Add the VM to the stopped VM cache
                            self.power_off_vms[uuid] = vm_obj
                            self.powered_off_vm_count += 1
                            # Remove the object from Running VMs cache
                            del self.vm_cache[uuid]
                            self.vm_count -= 1
                            CLUSTER_CACHE_LOGGER_.info(f"VM {vm_obj.name} owned"
                                                       f" by user {owner_email}"
                                                       f" was previously ON, "
                                                       f"but is now OFF. "
                                                       f"Released {core_diff} "
                                                       f"cores and {mem_diff} "
                                                       f"memory.")
                            # Since the NIC config and the IP can change, just update it
                            vm_obj.nics = vm_config.get('vm_nics', [])
                            continue
                        # If it is a new VM altogether
                        else:
                            self.power_off_vms[uuid] = NuVM(vm_config, self.name)
                            self.powered_off_vm_count += 1
                    # The VM was already in the cache tracking powered down VMs
                    else:
                        # Since the NIC config and the IP can change, just update it
                        self.power_off_vms[uuid].nics = vm_config.get('vm_nics', [])
                        pass
            self._check_consistency()

    def process_deleted_vm(self, deleted_vm_uuid):
        """Process a deleted VM and release the tracked resources consumed
            by that VM
            Args:
                deleted_vm_uuid (str): UUID of the VM which is deleted
        """
        if self.cache_state in [CacheState.PENDING]:
            CLUSTER_CACHE_LOGGER_.warning(f"The cache for cluster {self.name}"
                                          f" is PENDING! Cannot process DELETED"
                                          f" VM UUID {deleted_vm_uuid}!!")
            return
        if deleted_vm_uuid in self.vm_cache:
            vm_obj = self.vm_cache[deleted_vm_uuid]
            prev_res = vm_obj.to_json()
            owner_email = prev_res[NuVMKeys.OWNER_EMAIL]
            core_diff = (-1) * prev_res[NuVMKeys.CORES_USED]
            mem_diff = (-1) * prev_res[NuVMKeys.MEMORY_USED]
            # Update the resources
            self.utilized_resources[ClusterKeys.CORES] += core_diff
            self.utilized_resources[ClusterKeys.MEMORY] += mem_diff
            self.abs_utilized_resources[ClusterKeys.CORES] += core_diff
            self.abs_utilized_resources[ClusterKeys.MEMORY] += mem_diff
            CLUSTER_CACHE_LOGGER_.info(f"Deleted Running VM UUID {deleted_vm_uuid} "
                                       f"Owner {owner_email} from the cache")
            # Remove the VM to the cache tracking running VMs
            self.vm_count -= 1
            del self.vm_cache[deleted_vm_uuid]
        elif deleted_vm_uuid in self.power_off_vms:
            vm_obj = self.power_off_vms[deleted_vm_uuid]
            CLUSTER_CACHE_LOGGER_.info(f"Deleted PowerOFF VM UUID {deleted_vm_uuid} "
                                       f"Owner {vm_obj.owner_email} from the "
                                       f"cache")
            del self.power_off_vms[deleted_vm_uuid]
            self.powered_off_vm_count -= 1
        # self._check_consistency()
        pass

    def _check_consistency(self):
        """Utility function that verifies that all the Cache items are consistent

            Raises:
                InconsistentCacheError
        """
        intersection = set(self.power_off_vms.keys()).intersection(set(self.vm_cache.keys()))
        if intersection:
            print(f"Intersection: {len(list(intersection))}")
            raise InconsistentCacheError("Following VMs are part of both "
                                         f"running and powered OFF VMs "
                                         f"{', '.join(intersection)}")
        cores_util = 0
        abs_cores_util = 0

        mem_util = 0
        abs_mem_util = 0

        CLUSTER_CACHE_LOGGER_.debug(f"The memory consumed is "
                                    f"{self.utilized_resources[ClusterKeys.MEMORY]} "
                                    f"and the cores consumed is "
                                    f"{self.utilized_resources[ClusterKeys.CORES]}")

        for _, vm_obj in self.vm_cache.items():
            res = vm_obj.to_json()["total_resources_used"]
            cores_util += res[NuVMKeys.CORES_USED]
            abs_cores_util += res[NuVMKeys.CORES_USED]
            mem_util += res[NuVMKeys.MEMORY_USED]
            abs_mem_util += res[NuVMKeys.MEMORY_USED]
        CLUSTER_CACHE_LOGGER_.debug(f"{mem_util} Absolute: {abs_mem_util}")

        for _, vm_config in self.skipped_vms.items():
            if vm_config['power_state'] == PowerState.ON:
                abs_cores_util += vm_config['num_cores_per_vcpu'] * vm_config['num_vcpus']
                abs_mem_util += vm_config['memory_mb']
        CLUSTER_CACHE_LOGGER_.debug(f"{mem_util} Absolute: {abs_mem_util}")

        if cores_util != self.utilized_resources[ClusterKeys.CORES]:
            raise InconsistentCacheError("The number of cores utilized by VMs"
                                         " and stored in the cache"
                                         "are different.")
        if abs_cores_util != self.abs_utilized_resources[ClusterKeys.CORES]:
            raise InconsistentCacheError("The number of cores utilized by VMs"
                                         " and stored in the cache"
                                         "are different.")
        if mem_util != self.utilized_resources[ClusterKeys.MEMORY]:
            raise InconsistentCacheError(f"The memory utilized by VMs"
                                         " and stored in the cache "
                                         "are different. Stored: "
                                         f"{self.utilized_resources[ClusterKeys.MEMORY]}"
                                         f" Utilized: {mem_util}")
        # CLUSTER_CACHE_LOGGER_.debug(f"Memory: abs_mem_util, type(abs_mem_util)")
        # CLUSTER_CACHE_LOGGER_.debug(self.abs_utilized_resources[ClusterKeys.MEMORY], type(self.abs_utilized_resources[ClusterKeys.MEMORY]))
        if abs_mem_util != int(self.abs_utilized_resources[ClusterKeys.MEMORY]):
            raise InconsistentCacheError(f"The memory utilized by VMs"
                                         " and stored in the cache "
                                         "are different. Stored: "
                                         f"{self.abs_utilized_resources[ClusterKeys.MEMORY]}"
                                         f" Utilized: {abs_mem_util}")
        CLUSTER_CACHE_LOGGER_.debug("Everything is consistent")

    def get_vm_using_resources_sorted(self, count=-1,
                                      sort_by_cores=False, 
                                      sort_by_mem=False) -> list:
        """Returns sorted list of the VMs (to_json) on the particular cluster
            The list is by default sorted by sum(cores, memory)

            Args:
                count (int): Number of top 'count' elements to return
                sort_by_cores (bool): Sort the list as per the number of cores\
                    allocated to the VM
                sort_by_mem (bool): Sort the list as per the memory allocated \
                    to the VM

            Returns:
                (list) List of to_json of VM which is sorted
        """
        vm_res_list = []
        with self.vm_cache_lock:
            for _, vm_obj in self.vm_cache.items():
                vm_res_list.append(vm_obj.to_json())
        if sort_by_cores:
            sorted_vm_list = sorted(vm_res_list, key=lambda x:
                                    x["total_resources_used"][NuVMKeys.CORES_USED],
                                    reverse=True)
        elif sort_by_mem:
            sorted_vm_list = sorted(vm_res_list, key=lambda x:
                                    x["total_resources_used"][NuVMKeys.MEMORY_USED],
                                    reverse=True)
        else:
            sorted_vm_list = sorted(vm_res_list, key=lambda x:
                                    (x["total_resources_used"][NuVMKeys.MEMORY_USED] +
                                     x["total_resources_used"][NuVMKeys.CORES_USED]),
                                     reverse=True)
        if count > 0:
            print(count)
            return sorted_vm_list[:count]
        return sorted_vm_list

    def get_vm_list(self) -> typing.Tuple[typing.List, typing.List, typing.List]:
        """Returns list of Running and Stopped VMs
            Returns:
                list, list: List of running VMs and stopped VMs
        """
        running_vm_list = []
        stopped_vm_list = []
        templated_vm_list = []
        with self.vm_cache_lock:
            for _, vm_obj in self.vm_cache.items():
                running_vm_list.append(vm_obj.to_json())
            for _, vm_obj in self.power_off_vms.items():
                stopped_vm_list.append(vm_obj.to_json())
            for _, vm_config_dict in self.skipped_vms.items():
                templated_vm_list.append(vm_config_dict)
        return running_vm_list, stopped_vm_list, templated_vm_list

    def _map_vm_name_to_obj(self, vm_name) -> typing.Optional[typing.Tuple[str, str, NuVM]]:
        """Utility function to Map the VM Name to its object

        Args:
            vm_name (str): Name of the VM to search in the cache

        Returns:
            None: If the VM_name not found in the cache
            str, str, NuVM object: UUID, Power state of the VM and VM object
        """
        with self.vm_cache_lock:
            for uuid, vm_obj in self.vm_cache.items():
                if vm_obj.name == vm_name:
                    return uuid, vm_obj.power_state, vm_obj
            for uuid, vm_obj in self.power_off_vms.items():
                if vm_obj.name == vm_name:
                    return uuid, vm_obj.power_state, vm_obj
        return None

    def power_down_vm(self, vm_name=None,
                      uuid=None) -> typing.Tuple[HTTPStatus, dict]:
        """Function which powers down a VM running on this cluster.
        Creates a Task on the Cluster and waits for the task to finish.

        Args:
            vm_name (str): Name of the VM to search in the cache
            uuid (str): UUID of the VM to search in the cache

        Returns:
            HTTPStatus Object, dict: Status of the request and the dict \
                containing the message
        """
        from http import HTTPStatus
        if not vm_name and not uuid:
            exc = {'message': "Please provide any one of VM name or UUID"}
            CLUSTER_CACHE_LOGGER_.exception(exc['message'])
            return HTTPStatus.BAD_REQUEST, exc
        vm_obj = None

        if uuid:
            with self.vm_cache_lock:
                vm_obj = self.vm_cache.get(uuid, None)
                if vm_obj is None:
                    vm_obj = self.power_off_vms.get(uuid, None)
                    if vm_obj:
                        msg = {'message': f"VM with UUID {uuid} "
                                          f"is already powered OFF"}
                        CLUSTER_CACHE_LOGGER_.info(msg['message'])
                        return HTTPStatus.OK, msg
                    else:
                        err = {'message': f"VM with UUID {uuid} does "
                               "not exist in the cluster {self.name}"}
                        CLUSTER_CACHE_LOGGER_.error(err['message'])
                        return HTTPStatus.NOT_FOUND, err
        else:
            info = self._map_vm_name_to_obj(vm_name)
            if info:
                if info[1] == PowerState.OFF:
                    msg = {'message': f"VM with name {vm_name} "
                                      f"is already powered OFF"}
                    CLUSTER_CACHE_LOGGER_.info(msg['message'])
                    return HTTPStatus.OK, msg
                else:
                    vm_obj = info[2]
                    uuid = info[0]
            else:
                err = {'message': f"VM with name {vm_name} does not "
                                  f"exist in the cluster {self.name}"}
                CLUSTER_CACHE_LOGGER_.error(err['message'])
                return HTTPStatus.NOT_FOUND, err
        # Now we have a populated vm_obj
        data = {
            "host_uuid": vm_obj.host_uuid,
            "transition": PowerState.OFF.upper(),
            "uuid": vm_obj.uuid
        }
        url = self.prism_rest_ep + VM_ENDPOINT + vm_obj.uuid + '/set_power_state'

        try:
            status_code, resp_json = self._request_cluster(url, data=data,
                                                           method=HTTPMethod.POST)
        except Exception as ex:
            err = {'message': f"Task to power OFF VM {vm_name} UUID {uuid} could not"
                          f" be created."}
            CLUSTER_CACHE_LOGGER_.error(err['message'])
            return HTTPStatus.SERVICE_UNAVAILABLE, err

        task_uuid = resp_json['task_uuid']
        task_info_str = f"POWERING OFF the VM {vm_name} UUID {uuid}"
        CLUSTER_CACHE_LOGGER_.info(f"Created the task {task_uuid} to {task_info_str}")
        status, msg = self._wait_for_task_completion(task_uuid, task_info_str)
        if status == HTTPStatus.OK:
            with self.vm_cache_lock:

                vm_json = vm_obj.to_json()
                prev_res = vm_json['total_resources_used']
                core_diff = prev_res[NuVMKeys.CORES_USED]
                mem_diff = prev_res[NuVMKeys.MEMORY_USED]

                CLUSTER_CACHE_LOGGER_.debug(f"Previous Resources was "
                                            f"{self.utilized_resources[ClusterKeys.CORES]} cores"
                                            f" and {self.utilized_resources[ClusterKeys.MEMORY]}"
                                            f" memory (Cluster: {self.name})")
                self.utilized_resources[ClusterKeys.CORES] -= core_diff
                self.utilized_resources[ClusterKeys.MEMORY] -= mem_diff
                CLUSTER_CACHE_LOGGER_.debug(f"Now Resources was "
                                            f"{self.utilized_resources[ClusterKeys.CORES]}"
                                            f" cores and {self.utilized_resources[ClusterKeys.MEMORY]}"
                                            f" memory")

                vm_obj.process_power_off()

                # Add the VM to the stopped VM cache
                self.power_off_vms[uuid] = vm_obj
                self.powered_off_vm_count += 1
                # Remove the object from Running VMs cache
                del self.vm_cache[uuid]
                self.vm_count -= 1

                msg = {
                    "message": f"Task {task_uuid} ({task_info_str}) succeeded.",
                    "vm_config": vm_obj.to_json()
                }
                CLUSTER_CACHE_LOGGER_.info(msg['message'])

                # Update the VM state in cache to process shutdown
                vm_obj.process_power_off()
                # self._check_consistency()
                return status, msg
        err = {'message': f"Task UUID {task_uuid} checking failed, timed out"}
        CLUSTER_CACHE_LOGGER_.error(err['message'])
        return HTTPStatus.EXPECTATION_FAILED, err

    def _request_cluster(self, url, data=None, params=None, json_val=None,
                         method=HTTPMethod.GET) -> typing.Tuple[HTTPStatus, typing.Dict]:
        """Utility function and a wrapper to contact the Cluster.
            Abstracts out the headers, information and so on.
            Defaults to performing GET request.
        
            Args:
                url: URL to contact the PRISM element on
                data (optional): Dict object containing the data for the \
                    request
                json_val (optional): Dict object containing the JSON to be sent\
                  in the body for the request

            Returns:
                Tuple(HTTPStatus, dict): Status returned from the request and \
                    the dict containing message


        """
        res = None
        if method == HTTPMethod.GET:
            try:
                res = requests.get(url=url, headers=self._headers, verify=False, timeout=5)
            except requests.exceptions.Timeout as te:
                err_str = f"GET request to {url} timed out"
                CLUSTER_CACHE_LOGGER_.error(err_str)
                raise te
            if res.status_code in [HTTPStatus.OK,
                                   HTTPStatus.ACCEPTED,
                                   HTTPStatus.CREATED,
                                   HTTPStatus.NO_CONTENT]:
                return res.status_code, res.json()
            else:
                err_str = (f"GET request failed with status code "
                           f"{res.status_code}, message: {res.text}")
                CLUSTER_CACHE_LOGGER_.error(err_str)
                return res.status_code, {'message': err_str}

        if method == HTTPMethod.POST:
            try:
                res = requests.post(url=url, headers=self._headers,
                                data=json.dumps(data) if data else None,
                                json=json.dumps(json_val) if json_val else None,
                                timeout=5,
                                verify=False)
            except requests.exceptions.Timeout as te:
                err_str = (f"POST request failed with status code "
                           f"{res.status_code}, message: {res.text}")
                CLUSTER_CACHE_LOGGER_.error(err_str)
                raise te
            if res.status_code in [HTTPStatus.OK,
                                   HTTPStatus.ACCEPTED,
                                   HTTPStatus.CREATED,
                                   HTTPStatus.NO_CONTENT]:
                return res.status_code, res.json()
            else:
                err_str = (f"POST request failed with status code "
                           f"{res.status_code}, message: {res.text}")
                CLUSTER_CACHE_LOGGER_.error(err_str)
                return res.status_code, {'message': err_str}
        if method == HTTPMethod.DELETE:
            try:
                res = requests.delete(url=url, headers=self._headers,
                                  data=json.dumps(data) if data else None,
                                  json=json.dumps(json_val) if json_val else None,
                                  timeout=5,
                                  verify=False)
            except requests.exceptions.Timeout as te:
                err_str = (f"DELETE request failed with status code "
                           f"{res.status_code}, message: {res.text}")
                CLUSTER_CACHE_LOGGER_.error(err_str)
                raise te
            if res.status_code in [HTTPStatus.OK,
                                   HTTPStatus.ACCEPTED,
                                   HTTPStatus.CREATED,
                                   HTTPStatus.NO_CONTENT]:
                return res.status_code, res.json()
            else:
                err_str = f"DELETE request failed with status code {res.status_code}, message: {res.text}"
                CLUSTER_CACHE_LOGGER_.error(err_str)
                return res.status_code, {'message': err_str}
        return HTTPStatus.NOT_IMPLEMENTED, {'message': f"HTTP Method {method} not implemented"}

    def _wait_for_task_completion(self, task_uuid, task_info_str="", timeout=20):
        start_time = time.time()
        while (time.time() < (start_time + timeout)):
            task_url = self.prism_rest_ep + 'tasks/' + task_uuid
            status_code, task_status_json = self._request_cluster(task_url)
            if status_code == HTTPStatus.OK:
                if task_status_json['progress_status'] == TaskStatus.FAILED:
                    err = {'message': f'Task UUID {task_uuid} failed '
                                      f'on the cluster. Task: {task_info_str}'}
                    CLUSTER_CACHE_LOGGER_.error(err['message'])
                    return HTTPStatus.EXPECTATION_FAILED, err
                elif task_status_json['progress_status'] == TaskStatus.SUCCEEDED:
                    msg = {'message': f'Task UUID {task_uuid} succeeded '
                                      f'on the cluster. Task: {task_info_str}'}
                    # Delete the VM from the powered ON VMs
                    return HTTPStatus.OK, msg
                # If the task is not completed
                elif task_status_json['progress_status'] in [TaskStatus.QUEUED,
                                                             TaskStatus.SCHEDULED,
                                                             TaskStatus.RUNNING]:
                    pass
            else:
                err = {'message': f'Querying task UUID {task_uuid} returned '
                                  f' status code {status_code} on the cluster.'
                                  f'Task: {task_info_str}'}
                CLUSTER_CACHE_LOGGER_.error(err['message'])
                return status_code, err
            time.sleep(2)
        return HTTPStatus.REQUEST_TIMEOUT, {'message': f'Waiting for task UUID'
                                                       f' {task_uuid} timed out'}

    def remove_vm_nic(self, vm_name=None,
                      uuid=None) -> typing.Tuple[HTTPStatus, dict]:
        """Function which removes a (or all) NIC from VMs on this cluster.
        Creates a Task on the Cluster and waits for the task to finish.

        Args:
            vm_name (str): Name of the VM to search in the cache
            uuid (str): UUID of the VM to search in the cache

        Returns:
            HTTPStatus Object, dict: Status of the request and the dict \
                containing the message
        """
        from http import HTTPStatus
        if not vm_name and not uuid:
            exc = {'message': "Please provide any one of VM name or UUID"}
            CLUSTER_CACHE_LOGGER_.exception(exc['message'])
            return HTTPStatus.BAD_REQUEST, exc
        vm_obj = None

        if uuid:
            with self.vm_cache_lock:
                vm_obj = self.vm_cache.get(uuid, None)
                if vm_obj is None:
                    vm_obj = self.power_off_vms.get(uuid, None)
                    if not vm_obj:
                        msg = {'message': f"VM with name UUID {uuid}"
                                          f" not found in the cache"}
                        CLUSTER_CACHE_LOGGER_.error(msg['message'])
                        return HTTPStatus.NOT_FOUND, msg
                    else:
                        CLUSTER_CACHE_LOGGER_.info(f"VM with UUID {uuid} Name "
                                                   f"{vm_obj.name} is powered "
                                                   f"OFF. Attempting to remove"
                                                   f" the NIC.")
                else:
                    CLUSTER_CACHE_LOGGER_.info(f"Attempting to hot-remove the "
                                               f"NIC from the VM with UUID {uuid}"
                                               f" Name {vm_obj.name}")
        else:
            info = self._map_vm_name_to_obj(vm_name)
            if not info:
                msg = {'message': f"VM with name {vm_name} not found in the cache"}
                CLUSTER_CACHE_LOGGER_.error(msg['message'])
                return HTTPStatus.NOT_FOUND, msg
            uuid = info[0]
            vm_obj = info[2]
            if vm_obj.power_state == PowerState.ON:
                CLUSTER_CACHE_LOGGER_.info(f"Attempting to hot-remove the NIC "
                                           f"from the VM with UUID {uuid} Name"
                                           f" {vm_obj.name}")
            else:
                CLUSTER_CACHE_LOGGER_.info(f"VM with UUID {uuid} Name "
                                           f"{vm_obj.name} is powered OFF. "
                                           f"Attempting to remove the NIC.")
        # Now we have a populated vm_obj
        # for each of the NICs in the VM, delete it
        data = {"vm_uuid": vm_obj.uuid}
        url = self.prism_rest_ep + VM_ENDPOINT + vm_obj.uuid + '/nics'
        # TODO Can handle this better
        status = HTTPStatus.SERVICE_UNAVAILABLE
        message = {'message': "Failed to remove NIC from VM"}
        for nic_info in vm_obj.nics:
            nic_id = nic_info['mac_address']
            # if nic_info['is_connected'] is True:
            data["nic_id"] = nic_id
            nic_del_url = url + f'/{nic_id}'
            resp_json = {}
            try:
                status, resp_json = self._request_cluster(nic_del_url, data,
                                                          method=HTTPMethod.DELETE)
            except Exception as ex:
                CLUSTER_CACHE_LOGGER_.exception(ex)
                return status, resp_json
            task_uuid = resp_json['task_uuid']
            tis = f"Removing NIC {nic_id} from VM {vm_obj.name}, UUID {vm_obj.uuid}"
            CLUSTER_CACHE_LOGGER_.info(f"Triggered task {task_uuid}: '{tis}' "
                                       f"on cluster {self.name}")
            status, message = self._wait_for_task_completion(task_uuid=task_uuid,
                                                             task_info_str=tis)
            if status == HTTPStatus.OK:
                CLUSTER_CACHE_LOGGER_.info(f"RemoveNIC:TASK_SUCC for cluster "
                                           f"{self.name} for VM {vm_obj.name},"
                                           f" UUID {vm_obj.uuid}, NIC: {nic_id}")
            else:
                CLUSTER_CACHE_LOGGER_.warning(f"RemoveNIC:TASK_FAIL for cluster"
                                              f" {self.name} for VM {vm_obj.name},"
                                              f" UUID {vm_obj.uuid}, NIC: {nic_id}")
        return status, message

    def cleanup(self):
        """Cleanup the cache for the cluster
        """
        CLUSTER_CACHE_LOGGER_.info(f"Cleaning up the cache for Cluster {self.name}")
        with self.vm_cache_lock:
            self.vm_cache.clear()
            self.hosts.clear()
            self.power_off_vms.clear()
            self.vms_not_following_naming_conv.clear()
            self.vms_added_since_last_cache_build.clear()
            self.skipped_vms.clear()
            self.utilized_resources[ClusterKeys.CORES] = 0
            self.utilized_resources[ClusterKeys.MEMORY] = 0
            self.vm_count = 0
            self.powered_off_vm_count = 0
            self.cache_state = CacheState.PENDING
            self.cache_build_done_ts = 0
            self._threadpool_exec.shutdown(cancel_futures=True)
        return

    def get_health_status(self):
        """Get the health status of the cluster. Returns percentage of memory and cores still available
            Returns:
                dict: Dictionary containing the health status of the cluster
        """
        from caching.server_constants import HealthStatus
        if self.cluster_info_populated:
            status_dict = {
                # This is in MiB
                "absolute_available_memory": self.abs_available_memory,
                # The cores is not a useful metric, as the cores are volatile.
                "absolute_available_cores": self.abs_available_cores,
                # Return the calculated perc used for memory and cores
                "memory_perc": round((self.abs_utilized_resources[ClusterKeys.MEMORY] /
                                self.abs_available_memory) * 100, 2),
                "cores_perc": round((self.abs_utilized_resources[ClusterKeys.CORES] /
                                self.abs_available_cores) * 100, 2)
            }
            core_hs = HealthStatus.get_health_status(status_dict['cores_perc'])
            memory_hs = HealthStatus.get_health_status(status_dict['memory_perc'])
            status_dict['memory_state'] = memory_hs[2] if memory_hs != HealthStatus.UNKNOWN else HealthStatus.UNKNOWN
            status_dict['core_state'] = core_hs[2] if core_hs != HealthStatus.UNKNOWN else HealthStatus.UNKNOWN
            return status_dict
        else:
            return {
                "message": "Cluster information not populated yet",
                "health_status": {
                    'memory_state': HealthStatus.UNKNOWN,
                    'core_state': HealthStatus.UNKNOWN
                }
            }

    def get_updated_health_status(self, new_vm_cores, new_vm_mem):
        """Get the updated health status of the cluster. Returns percentage of memory and cores still available if we provision a particular VM.
            Args:
                new_vm_cores (int): Cores used by the new VM
                new_vm_mem (int): Memory used by the new VM
            Returns:
                dict: Dictionary containing the health status of the cluster
        """
        if self.cluster_info_populated:
            return {
                "memory_perc": 100 - (self.abs_utilized_resources[ClusterKeys.MEMORY] /
                                self.abs_available_memory) * 100,
                "cores_perc": 100 - (self.abs_utilized_resources[ClusterKeys.CORES] /
                                self.abs_available_cores) * 100,
                "new_memory_perc": 100 - ((self.abs_utilized_resources[ClusterKeys.MEMORY] + new_vm_mem) /
                                self.abs_available_memory) * 100,
                "new_cores_perc": 100 - ((self.abs_utilized_resources[ClusterKeys.CORES] + new_vm_cores) /
                                self.abs_available_cores) * 100
            }
        else:
            return {
                "message": "Cluster information not populated yet"
            }
