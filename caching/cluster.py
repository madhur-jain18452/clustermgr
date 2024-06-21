"""Class Cluster
    Logic to fetch, list, categorize and cache VMs running on a cluster

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""

import urllib3
import requests
import logging
import time
import typing

from concurrent import futures
from threading import Lock
from datetime import datetime
from http import HTTPStatus

from caching.NuVM import NuVM
from caching.server_constants import PRISM_PORT, PRISM_REST_FINAL_EP, \
    basic_auth_header, HTTPS, VM_ENDPOINT, generate_query_string, \
    CacheState, PowerState, HOSTS_EP, CLUSTER_EP, check_vm_name_to_skip
from tools.helper import convert_mb_to_gb, convert_bytes_to_gb
from custom_exceptions.exceptions import InconsistentCacheError
from common.constants import NuVMKeys, ClusterKeys, TaskStatus

urllib3.disable_warnings()

# Setting up the logging
CLUSTER_CACHE_LOGGER_ = logging.getLogger(__name__)
CLUSTER_CACHE_LOGGER_.setLevel(logging.DEBUG)
handler = logging.FileHandler("cluster.log", mode='w')
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
        # TODO Have UUID
        self.name = name
        self._ip_address = ip
        self.available_memory = 0 #  Stored in Bytes
        self.available_cores = 0
        self.id = ""
        self.cluster_uuid = ""
        self.hosts = []

        self.cache_state = CacheState.PENDING
        self.prism_rest_ep = (HTTPS + self._ip_address + f":{str(PRISM_PORT)}" +
                              PRISM_REST_FINAL_EP)

        self._username = username
        self._password = password
        self._headers = basic_auth_header(self._username, self._password)
        self._threadpool_exec = futures.ThreadPoolExecutor(max_workers=15)

        # Variables specific to the management framework
        self.cache_build_done_ts = None
        self.utilized_resources = {ClusterKeys.CORES: 0, ClusterKeys.MEMORY: 0}

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

        # self._fetch_cluster_host_info()

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
        mem = 0
        cores = 0
        resp = requests.get(self.prism_rest_ep + HOSTS_EP,
                            headers=self._headers, verify=False)
        if resp.status_code == HTTPStatus.OK:
            resp_json = resp.json()
            for entity in resp_json['entities']:
                CLUSTER_CACHE_LOGGER_.info(entity)
                # mem += entity['memory_capacity_in_bytes']
                # cores += entity['num_cpu_cores'] * entity['num_cpu_threads']
                # self.hosts.append(entity)
        # Stored in Bytes
        self.available_memory = mem
        self.available_cores = cores

        # Get the Cluster UUID
        resp = requests.get(self.prism_rest_ep + CLUSTER_EP,
                            headers=self._headers, verify=False)
        if resp.status_code == HTTPStatus.OK:
            resp_json = resp.json()
            self.cluster_uuid = resp_json['cluster_uuid']
            self.id = resp_json['id']
        pass

    def summary(self, summary_verbosity=0, print_summary=False) -> typing.Optional[typing.Dict]:
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
                    NuVMKeys.NAME: self.name,
                    "count_powered_off_vm": self.powered_off_vm_count,
                }
                if summary_verbosity > 1:
                    cluster_info["list_powered_off_vm"] = list(self.power_off_vms.keys())
                    cluster_info["list_powered_on_vm"] = list(self.vm_cache.keys())
                if summary_verbosity > 2:
                    cluster_info['vm_info'] = {}
                    for vm_uuid, each_vm in self.vm_cache.items():
                        cluster_info['vm_info'][vm_uuid] = each_vm.summary(print_summary=print_summary)
                return cluster_info

    # def to_json(self, resources=False, no_owner=False, sort_by_mem=False, sort)

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
        self.vms_not_following_naming_conv[timestamp].append((uuid, vm_name))

    def get_vm_with_no_prefix(self) -> list:
        """Returns a new VM whch does not have a recognizable prefix to the cache
            Returns
                list of tuple(uuid, name) of the VM whose name does not \
                conform to the naming convention
        """
        biggest_ts = 0
        for ts, _ in self.vms_not_following_naming_conv.items():
            if ts > biggest_ts:
                biggest_ts = ts
        return self.vms_not_following_naming_conv[biggest_ts]

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
            res = requests.get(self.prism_rest_ep + VM_ENDPOINT +
                               generate_query_string(extra_requests),
                               headers=self._headers, verify=False)
            CLUSTER_CACHE_LOGGER_.info(f"VMs on the cluster  "
                                       f"{self.name} fetched!")
            return res.json()['entities']
        except Exception as ex:
            CLUSTER_CACHE_LOGGER_.exception(f"Exception occurred: {ex}."
                                            f"\n\t\tResponse: {res.status_code}")
            raise ex

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

        try:
            all_vm_config = self._fetch_vms_on_cluster()
        except Exception as ex:
            ex_str = (f"Exception occurred when fetching VMs on the cluster "
                      f"{self.name}. Setting the cache_state to "
                      f"{CacheState.to_str(CacheState.PENDING)}")
            print(ex_str)
            CLUSTER_CACHE_LOGGER_.error(ex_str)
            self.cache_state = CacheState.PENDING
            return

        with self.vm_cache_lock:
            for vm_config in all_vm_config:
                uuid = vm_config["uuid"]
                name = vm_config["name"]
                if check_vm_name_to_skip(name):
                    CLUSTER_CACHE_LOGGER_.debug(f"Skipped caching VM {name}, uuid {uuid}")
                    # It is possible that the name might have changed
                    if uuid in self.skipped_vms:
                        if self.skipped_vms[uuid]['name'] != name:
                            CLUSTER_CACHE_LOGGER_.info(f"(TEMPLATE VM Name "
                                                       f"change alert) TO_SKIP"
                                                       f" VM UUID {uuid} old_name: "
                                                       f"{self.skipped_vms[uuid]}"
                                                       f" -> new_name:{name}")
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
                        continue
                    # If it was already cached, no changes
                    if uuid in self.vm_cache:
                        if uuid in self.vms_added_since_last_cache_build:
                            self.vms_added_since_last_cache_build.remove(uuid)
                        continue
                    # If the VM is a new VM altogether
                    else:
                        self.vm_count += 1
                        self.utilized_resources[ClusterKeys.CORES] += \
                            vm_config["num_cores_per_vcpu"] * vm_config["num_vcpus"]
                        self.utilized_resources[ClusterKeys.MEMORY] += vm_config['memory_mb']

                        self.vm_cache[uuid] = NuVM(vm_config, self.name)

                        CLUSTER_CACHE_LOGGER_.debug(f"Added VM {vm_config['name']}"
                                                    f" to the cache")
                        self.vms_added_since_last_cache_build.add(uuid)
                # If the VM is turned OFF
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
                            continue
                        # If it is a new VM altogether
                        else:
                            self.power_off_vms[uuid] = NuVM(vm_config, self.name)
                            self.powered_off_vm_count += 1
                    # The VM was already in the cache tracking powered down VMs
                    else:
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
        self._check_consistency()
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
        mem_util = 0
        memory = self.utilized_resources['memory']

        CLUSTER_CACHE_LOGGER_.debug(f"The memory consumed is {self.utilized_resources['memory']} and the cores consumed is {self.utilized_resources['cores']}")

        for _, vm_obj in self.vm_cache.items():
            res = vm_obj.to_json()["total_resources_used"]
            cores_util += res[NuVMKeys.CORES_USED]
            mem_util += res[NuVMKeys.MEMORY_USED]
        if cores_util != self.utilized_resources['cores']:
            raise InconsistentCacheError("The number of cores utilized by VMs"
                                         " and stored in the cache"
                                         "are different.")
        if mem_util != memory:
            raise InconsistentCacheError(f"The memory utilized by VMs"
                                         " and stored in the cache "
                                         f"are different. Stored: {memory}"
                                         f" Utilized: {mem_util}")
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
                        msg = {'message': f"VM with UUID {uuid} is already powered OFF"}
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
                    msg = {'message': f"VM with name {vm_name} is already powered OFF"}
                    CLUSTER_CACHE_LOGGER_.info(msg['message'])
                    return HTTPStatus.OK, msg
                else:
                    vm_obj = info[2]
                    uuid = info[0]
            else:
                err = {'message': f"VM with name {vm_name} does not exist in the cluster {self.name}"}
                CLUSTER_CACHE_LOGGER_.error(err['message'])
                return HTTPStatus.NOT_FOUND, err
        # Now we have a populated vm_obj
        data = {
            "host_uuid": vm_obj.host_uuid,
            "transition": PowerState.OFF.upper(),
            "uuid": vm_obj.uuid
        }
        import json
        import time
        loop_count = 10
        url = self.prism_rest_ep + VM_ENDPOINT + vm_obj.uuid + '/set_power_state'

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        auth_val = self._headers['Authorization']
        headers['Authorization'] = auth_val

        resp = requests.post(url, data=json.dumps(data), headers=headers, verify=False)

        if resp.status_code == HTTPStatus.CREATED:
            task_uuid = resp.json()['task_uuid']
            CLUSTER_CACHE_LOGGER_.info(f"Created the task {task_uuid} to POWER"
                                       f" OFF the VM {vm_name} UUID {uuid}")
            while True and loop_count:
                task_stat = requests.get(self.prism_rest_ep + 'tasks/' +
                                         task_uuid, headers=headers,
                                         verify=False)
                if task_stat.status_code == HTTPStatus.OK:
                    task_status_json = task_stat.json()

                    if task_status_json['progress_status'] == TaskStatus.FAILED:
                        err = {'message': f"Task UUID {task_uuid} failed "
                                          f"on the cluster"}
                        CLUSTER_CACHE_LOGGER_.error(err['message'])
                        return HTTPStatus.EXPECTATION_FAILED, err
                    elif task_status_json['progress_status'] == TaskStatus.SUCCEEDED:
                        # Delete the VM from the powered ON VMs
                        with self.vm_cache_lock:

                            vm_json = vm_obj.to_json()
                            prev_res = vm_json['total_resources_used']
                            core_diff = prev_res[NuVMKeys.CORES_USED]
                            mem_diff = prev_res[NuVMKeys.MEMORY_USED]

                            print(f"Previous Resources was "
                                  f"{self.utilized_resources[ClusterKeys.CORES]} cores"
                                  f" and {self.utilized_resources[ClusterKeys.MEMORY]}"
                                  f" memory")
                            self.utilized_resources[ClusterKeys.CORES] -= core_diff
                            self.utilized_resources[ClusterKeys.MEMORY] -= mem_diff
                            print(f"Now Resources was "
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
                                "message": f"Task {task_uuid} to power OFF VM"
                                           f" {vm_name} UUID {uuid} succeeded.",
                                "vm_config": vm_obj.to_json()
                            }
                            CLUSTER_CACHE_LOGGER_.info(msg['message'])

                            # Update the VM state in cache to process shutdown
                            vm_obj.process_power_off()
                            self._check_consistency()
                        return HTTPStatus.OK, msg
                    time.sleep(2)
            err = {'message': f"Task UUID {task_uuid} checking failed, timed out"}
            CLUSTER_CACHE_LOGGER_.error(err['message'])
            return HTTPStatus.EXPECTATION_FAILED, err
        err = {'message': f"Task to power OFF VM {vm_name} UUID {uuid} could not"
                          f" be created. Status Code: {resp.status_code}"}
        CLUSTER_CACHE_LOGGER_.error(err['message'])
        return HTTPStatus.SERVICE_UNAVAILABLE, err

