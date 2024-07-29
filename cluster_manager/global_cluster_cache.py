"""Global ClusterCache: Stores the data and acts as a cohesion between the Users and clusters
 (and VMs)

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""
import json
import logging
import os
import threading
import time
import typing

from collections import OrderedDict
from concurrent import futures
from copy import deepcopy
from http import HTTPStatus
from string import ascii_lowercase as alc

from caching.cluster import Cluster
from caching.server_constants import CacheState
from tools.helper import convert_mb_to_gb
from common.constants import UserKeys
from custom_exceptions.exceptions import SameTimestampError, ActionAlreadyPerformedError
from users.user import User


DEFAULT_DEVIATION_RETAIN_VALUE = 5000

def setup_logger():
    """Sets up the logger for the Global ClusterCache
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler("cmgr_global_cache.log", mode='w')
    formatter = logging.Formatter("%(filename)s:%(lineno)d - %(asctime)s %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

class GlobalClusterCache(object):
    """Class that acts as a cohesion of Clusters and Users.
        This is a singleton class, and acts as single source of truth for
        all the cluster entities
    """
    # Singleton
    _g_cluster_mgr_instance = None
    _g_cmgr_lock = threading.Lock()

    # Actually performs and tracks caching for each cluster
    _threadpool = futures.ThreadPoolExecutor(max_workers=15)

    # Actual global cache for cluster and its lock
    global_cluster_cache = {}
    global_cluster_cache_lock = threading.Lock()
    cache_build_done = {}

    # Actual global cache for user and its lock
    global_user_cache = {}
    global_user_cache_lock = threading.Lock()
    # Map from user prefixes to the owner users
    user_prefix_email_map = {}

    # Lists of cluster and user objects
    cluster_obj_list = []
    user_obj_list = []

    def __new__(cls, *args, **kwargs):
        if not cls._g_cluster_mgr_instance:
            with cls._g_cmgr_lock:
                if not cls._g_cluster_mgr_instance:
                    cls._g_cluster_mgr_instance = super(GlobalClusterCache, cls).__new__(cls)
        return cls._g_cluster_mgr_instance

    def __init__(self, cluster_list=None, user_list=None, cache_clusters=True):
        """Parse and process raw list of clusters and users JSON
            Builds Users cache -> Builds Cluster cache -> Maps CVMs and the Users
        """
        if not getattr(self, '_initialized', False):
            setup_logger()
            self.logger = logging.getLogger(__name__)
            start_time = time.time()

            # Triggers the caching process
            if cluster_list:
                for e_cl in cluster_list:
                    cluster_obj = Cluster(e_cl["name"], e_cl.get("ip"), e_cl["user"],
                                          e_cl["password"])
                    self.cluster_obj_list.append(cluster_obj)
            if user_list:
                for e_usr in user_list:
                    user_obj = User(e_usr)
                    self.user_obj_list.append(user_obj)

            for ii in alc:
                self.user_prefix_email_map[ii] = {}

            # List of all the VMs which do not match any prefix
            # We will send a notification for this.
            # TODO Store it in the time-series database to get the diff in easy
            # way for the time schedule
            self.timed_deviations = OrderedDict()
            print("Building user cache")
            self._build_user_cache()
            print("Building user cache DONE")
            print("Building cluster cache")
            # if cache_clusters:
            self._caching_thread = threading.Thread(target=self.rebuild_cache,
                                                    kwargs={'all_clusters': True,
                                                            'cluster_name': None,
                                                            'initial_build': True})
            # self._caching_thread.daemon = True
            self._caching_thread.start()
            # FIXME
            self._caching_thread.join()

            print("Clusters cache build done.")
            global_total_vms = 0
            for cname, cobj in self.global_cluster_cache.items():
                running, stopped, templated = cobj.get_vm_list()
                total_now = len(running) + len(stopped) + len(templated)
                global_total_vms += total_now
                print(f"\tTracking {total_now} VMs on the cluster '{cname}'")
            if self.cluster_obj_list and self.user_obj_list:
                print(f"Cached {len(self.cluster_obj_list)} clusters, "
                      f"{len(self.user_obj_list)} users "
                      f"and processed {global_total_vms} VMs in {time.time() - start_time:<.3f}"
                      " seconds")
            self._initialized = True
        
    # Functions related to summarizing the CacheState
    def summary(self, print_summary=False) -> typing.Optional[typing.Dict]:
        """Summarizes the cache state of the clusters and users.
            Args:
                print_summary (bool): If True, prints the summary on the console
            Returns:
                dict: Summary of the cache state, contains User_cache and cluster_cache
        """
        if print_summary:
            print("\n\nCLUSTERS :")
            with self.global_cluster_cache_lock:
                for _, cluster_obj in self.global_cluster_cache.items():
                    cluster_obj.summary(summary_verbosity=1, print_summary=print_summary)
            print("\n\nUSERS :")
            user_count = 0
            with self.global_user_cache_lock:
                for _, user_obj in self.global_user_cache.items():
                    user_count += 1
                    user_obj.summary(summary_verbosity=2, print_summary=print_summary)
            print(f"Managing {user_count} users.")
            return None
        else:
            cache_summary = {'cluster_cache': {}, 'user_cache': {}}
            with self.global_cluster_cache_lock:
                for cname, cluster_obj in self.global_cluster_cache.items():
                    cache_summary['cluster_cache'][cname] = cluster_obj.summary(
                        summary_verbosity=2,
                        print_summary=print_summary
                    )
                    # self.logger.debug(f"SUMMARY REQUESTED, current:{cache_summary}")
            with self.global_user_cache_lock:
                for email, user_obj in self.global_user_cache.items():
                    cache_summary['user_cache'][email] = user_obj.summary(
                        print_summary=print_summary
                    )
            return cache_summary

    # Functions related to clusters, cluster cache rebuilds
    def _build_cluster_cache(self, cluster_obj) -> str:
        """This is a helper function which builds the cache for each cluster
            And then adds each cluster object into a higher cache
            The exact structure of cache is as follows:
                Global Cluster Cache (global_cluster_cache): Caches all the
                clusters
                    Each cluster has its own cache of VMs (NDB and Non-NDB)
                        Each NDB VM will track its own DB Server VMs (also
                        tracked by the cluster one level up)
            Args:
                cluster_obj: Object of class Cluster
            Returns:
                str: Name of the cluster whose cache is done

        """
        deleted_vm_uuid_list = []
        if cluster_obj.cache_state in [CacheState.READY, CacheState.PENDING]:
            self.logger.info(f"Start (re)Building cache for cluster {cluster_obj.name}")
            deleted_vm_uuid_list = cluster_obj.build_refresh_cache()
        # TODO If already building, wait for it to complete (?) Can we have async here?
        with self.global_cluster_cache_lock:
            self.global_cluster_cache[cluster_obj.name] = cluster_obj
        for each_deleted_vm_uuid in deleted_vm_uuid_list:
            self.logger.info(f"VM with UUID {each_deleted_vm_uuid} "
                             f"detected as deleted in the cluster {cluster_obj.name}")
            vm_obj = cluster_obj.vm_cache.get(each_deleted_vm_uuid, None)
            if not vm_obj:
                vm_obj = cluster_obj.power_off_vms.get(each_deleted_vm_uuid, None)
            owner_email = vm_obj.owner_email
            owner_obj = self.global_user_cache.get(owner_email) if owner_email is not None else None
            if owner_obj:
                self.logger.info(f"VM with UUID {each_deleted_vm_uuid} owner: {owner_email}")
                owner_obj.process_deleted_vm(each_deleted_vm_uuid)
            cluster_obj.process_deleted_vm(each_deleted_vm_uuid)
        self.logger.info(f"Cache (Re)Build for cluster {cluster_obj.name} done!")
        return cluster_obj.name

    def rebuild_cache(self, all_clusters=True, cluster_name=None,
                      initial_build=False):
        """Rebuilds cache for all/one of the clusters.
        Works asynchronously.
        Also performs detection of deleted VMs and process them accordingly

        Args:
            all_clusters (bool): Builds cache for all the available clusters.\
                Takes precedence over cluster_name
            cluster_name (str): rebuilds cache for a single cluster
            initial_build (bool): If initial build, do not run detection of
                deleted VM logic. If some Cluster Cache is pending, that will be
                handled inside the cluster logic.

        Returns:
            None
        """
        if all_clusters:
            caching_futures_list = []
            for cluster_obj in self.cluster_obj_list:
                self.cache_build_done[cluster_obj.name] = False
                self.logger.info("Cache rebuild started for the cluster "
                                       f"{cluster_obj.name}")
                caching_futures_list.append(self._threadpool.submit(
                    self._build_cluster_cache, cluster_obj))
            # Each thread in the pool work on exactly one cluster object
            for fut in futures.as_completed(caching_futures_list):
                try:
                    cluster_name = fut.result()
                    self.logger.info(f"Cache build done for the cluster {cluster_name}. "
                                     f"Trying mapping VM and users for the cluster.")
                    self.map_vm_and_users_track_resources(cluster_name)
                    self.cache_build_done[cluster_name] = True
                except Exception as ex:
                    self.logger.exception(f"Exception occurred: {ex}")
                    raise ex
        else:
            if not cluster_name:
                self.logger.error("Rebuild cache for specific cluster "
                                        "requested but name not provided.")
                return
            cluster_obj = self.global_cluster_cache.get(cluster_name, None)
            if cluster_obj is not None:
                self.logger.debug(f"Started cache build for the cluster "
                                        f"{cluster_obj.name}")
                self._threadpool.submit(self._build_cluster_cache, cluster_obj)
            else:
                self.logger.error("Cluster with name '{}' not found in cache. "
                                        "Please verify that cluster information"
                                        " exists in the config.".format(cluster_name))
                return
            self.map_vm_and_users_track_resources(cluster_name)
            return
        return

    # Functions related to Users, user cache rebuilds
    def _build_user_cache(self):
        """Builds the cache for all the users."""
        # Since we already have the __new__ method protecting the data
        # we don't need lock, However, just for safety measure, i am adding
        with self.global_cluster_cache_lock:
            # Since the global_cluster_manager object is created only once
            # we don't need to check if the user already exists here
            # (it will be always be created exactly once)
            # However, when we add a new user altogether after the system has
            # started, then we need check
            for each_user in self.user_obj_list:
                self.global_user_cache[each_user.email] = each_user
                for each_pref in each_user.prefixes:
                    pref_to_use = each_pref.lower()
                    bucket_key = pref_to_use[0]
                    bucket = self.user_prefix_email_map[bucket_key]
                    if pref_to_use in bucket:
                        if bucket[pref_to_use] != each_user.email:
                            self.logger.error(f"Prefix '{pref_to_use}', "
                                              f"intended for user "
                                              f"{each_user.email} "
                                              f"already exists in the "
                                              f"cache for user "
                                              f"'{bucket[pref_to_use]}'."
                                              f" Skipping prefix add.")
                        # In case of duplicate prefix for the same user, ignore
                        continue
                    bucket[pref_to_use] = each_user.email
        self.logger.info(f"User cache build done!, the map is : {self.user_prefix_email_map}")

    def update_prefix(self, user_email, prefix, op) -> bool:
        """Updates the prefixes for a user.
            For adding new prefix, we check if it can be added to the cache
                and then update the User object.

            For removing a prefix, we delete from the cache first.
                There should be at least one valid prefix remaining for the user.

            The prefixes are always converted to lowercase.
            Both the operations are idempotent.

            Args:
                user_email (str): Email of the user to be updated
                prefix (str): Prefix to be updated for the user
                op (str): "add" or "remove"
            Raises:
                Exception: If the operation is neither "add" nor "remove"
            Returns:
                bool: True if operation is successful, False otherwise
        """
        user_obj = self.global_user_cache.get(user_email, None)
        if user_obj is None:
            self.logger.error(f"User with email '{user_email}' not "
                                    f"found in the cache for updating "
                                    f"prefix. Skipping update.")
            return True
        # Convert the prefix to lowercase
        pref_to_use = prefix.lower()
        if op == "add":
            bucket = self.user_prefix_email_map[pref_to_use[0]]
            # Check if the same prefix already exists for other user
            if pref_to_use in bucket:
                if bucket[pref_to_use] != user_email:
                    self.logger.error(f"Prefix '{pref_to_use}' intended "
                                            f"for user ;{user_email}' already "
                                            f"exists in the cache for other "
                                            f"user: {bucket[pref_to_use]}. "
                                            f"Skipping adding.")
                    return False
                else:
                    self.logger.info(f"Prefix '{pref_to_use}' for user "
                                           f"'{user_email}' already exists. "
                                           f"Skipping.")
                    return True
            self.user_prefix_email_map[pref_to_use[0]][pref_to_use] = user_email
            # FIXME Get the list of UUIDs here and update accordingly
            user_obj.update_prefix(op=op, prefix=pref_to_use)
            return True
        elif op == "remove":
            bucket = self.user_prefix_email_map[pref_to_use[0]]
            if pref_to_use in bucket:
                if bucket[pref_to_use] != user_email:
                    self.logger.error(f"Prefix '{pref_to_use}' intended "
                                            f"for user '{user_email}' already "
                                            f"exists in the cache for other "
                                            f"user: {bucket[pref_to_use]}. "
                                            f"Skipping removal.")
                    return False
                else:
                    del bucket[pref_to_use]
                    # Since we will usually have very few prefixes
                    # -- O(n) should not matter much
                    deleted_uuid_list = user_obj.update_prefix(op=op, prefix=pref_to_use)
                    self.logger.debug(deleted_uuid_list)
                    for (uuid, parent_cluster_name, _) in deleted_uuid_list:
                        cobj = self.global_cluster_cache[parent_cluster_name]
                        vm_obj = cobj.vm_cache.get(uuid, None)
                        if not vm_obj: 
                            vm_obj = cobj.power_off_vms.get(uuid, None)
                        if vm_obj:
                            vm_obj.set_owner(None, None)
                    self.logger.info(f"Prefix '{pref_to_use}' for user "
                                           f"'{user_email}' removed "
                                           f"successfully.")
                    return True
            # Prefix does not exist
            return True
        else:
            raise Exception(f"Operation {op} is invalid for update_prefix")

    # Cohesive functions
    def map_vm_and_users_track_resources(self, cluster_name):
        """This function maps users and VMs.
            Also updates the resources used by each VM and consequently the users
        """
        cluster_obj = self.global_cluster_cache[cluster_name]
        self.logger.debug(f"Trying mapping VMs for the cluster {cluster_name}")
        while not cluster_obj.is_cache_ready():
            self.logger.warning(f"The cluster {cluster_name} cache is "
                                      f"still building. Try again later.")
            time.sleep(1)
        ts = int(time.time())

        with cluster_obj.vm_cache_lock:
            for uuid, vm_obj in cluster_obj.vm_cache.items():
                if vm_obj.owner == None:
                    # self.logger.debug(f"Cluster: {cluster_name} - "
                    #                   f"UUID under process is {uuid} :"
                    #                   f"{vm_obj.name}")
                    # Get the bucket key
                    bucket_key = vm_obj.name.lower()[0]
                    # Get all the prefixes in the bucket
                    bucket = self.user_prefix_email_map.get(bucket_key)
                    if not bucket:
                        continue
                    pref_found = False
                    for pref, email in bucket.items():
                        if vm_obj.name.lower().startswith(pref):
                            pref_found = True
                            user_obj = self.global_user_cache[email]
                            owner_name = user_obj.name
                            # Check if the VM has the same owner, if not, update
                            vm_obj.set_owner(owner_name, email)
                            # Update the resources used by the cluster if not already
                            user_obj.update_vm_resources(vm_obj.to_json())
                            break
                    if not pref_found:
                        cluster_obj.add_vm_with_no_prefix(ts, vm_obj.name, vm_obj.uuid)
                        self.logger.debug(f"Cluster {cluster_name} - "
                                          "No prefix matched for the "
                                          f"VM {vm_obj.name}")
                else:
                    # If VM already has owner, update if there is a diff in the consumption
                    user_obj = self.global_user_cache[vm_obj.owner_email]
                    user_obj.update_vm_resources(vm_obj.to_json())
            for uuid, vm_obj in cluster_obj.power_off_vms.items():
                # Since the VMs is off, it is not going to make an effect if there is no owner
                if vm_obj.owner == None:
                    # Get the bucket key
                    bucket_key = vm_obj.name.lower()[0]
                    # Get all the prefixes in the bucket
                    bucket = self.user_prefix_email_map.get(bucket_key)
                    if not bucket:
                        continue
                    pref_found = False
                    for pref, email in bucket.items():
                        if vm_obj.name.lower().startswith(pref):
                            pref_found = True
                            user_obj = self.global_user_cache[email]
                            owner_name = user_obj.name
                            # Check if the VM has the same owner, if not, update
                            vm_obj.set_owner(owner_name, email)
                            # Update the resources used by the cluster if not already
                            user_obj.update_vm_resources(vm_obj.to_json())
                            break
                    if not pref_found:
                        cluster_obj.add_vm_with_no_prefix(ts, vm_obj.name, vm_obj.uuid)
                        self.logger.debug(f"Cluster {cluster_name} - "
                                                f"No prefix matched for the "
                                                f"(PoweredOFF) VM {vm_obj.name}")
        self.logger.info(f"Mapping for cluster {cluster_name} done! Cache is now READY!")

    # Functions relating to serving the REST API
    def get_clusters(self) -> typing.List:
        """Returns the list of all the clusters in the cache"""
        cluster_name_list = list()
        for _, cobj in self.global_cluster_cache.items():
            cluster_name_list.append(cobj.json())
        return cluster_name_list

    def get_users(self, skip_util=True) -> typing.List:
        """Returns the list of all the users in the cache
            Args:
                skip_util (bool): If True, skips the utilization information
            Returns:
                list: List of all the users in the cache
        """
        user_list = list()
        for _, user_obj in self.global_user_cache.items():
            user_info = user_obj.to_json()
            if skip_util:
                del user_info[UserKeys.GLOBAL_UTILIZED]
                del user_info[UserKeys.VM_UTILIZED]
                del user_info[UserKeys.CLUSTER_UTILIZED]
                user_list.append(user_info)
            else:
                user_list.append(user_info)
        return sorted(user_list, key=lambda x: x[UserKeys.EMAIL])

    def get_cluster_info(self, cluster_name, arguments) -> typing.Dict:
        """Returns the information about the cluster.
            Args:
                cluster_name (str): Name of the cluster
                arguments (dict): Additional arguments to be passed
            Returns:
                dict: Information about the cluster
        """
        cluster_info_dict = dict()
        include_template_vms = eval(arguments.get("include_template_vms", False))
        if eval(arguments.get("resources", 'False')):
            return self._get_vms_resources_sorted(
                        cluster_name=cluster_name,
                        count=int(arguments.get('count', -1)),
                        sort_by_cores=bool(arguments.get('sorted_core', False)),
                        sort_by_mem=bool(arguments.get('sorted_mem', False))
                    )
        else:
            vm_list = None
            health_status = None
            with self.global_cluster_cache_lock:
                cluster_obj = self.global_cluster_cache.get(cluster_name, None)
                if cluster_obj:
                    vm_list = cluster_obj.get_vm_list()
                    health_status = cluster_obj.get_health_status()
            if vm_list:
                cluster_info_dict['running_vm'] = vm_list[0]
                cluster_info_dict['stopped_vm'] = vm_list[1]
                if include_template_vms:
                    cluster_info_dict['template_vm'] = vm_list[2]
            cluster_info_dict['health_status'] = health_status
        return cluster_info_dict

    def add_update_user(self, user_info,
                        is_patch=False
                    ) -> typing.Optional[typing.Tuple[typing.List, typing.List, bool]]:
        """Adds a new user/update existing user in the cache.

            Args:
                user_info: Dict object that stores all the information about\
                    the user
            Returns:
                list, bool: List of prefixes that failed to be added, and if \
                the user was successfully added to the cache
        """
        user_obj = None
        method = "PATCH" if is_patch else "POST"

        failed_add_pref_list = []
        failed_remove_pref_list = []
        vms_to_untrack = []
        done = False
        # Get the user object or create new
        with self.global_user_cache_lock:
            if user_info[UserKeys.EMAIL] in self.global_user_cache:
                if not is_patch:
                    # Should have been a PATCH request, not POST
                    return
                user_obj = self.global_user_cache[user_info[UserKeys.EMAIL]]
            else:
                if is_patch:
                    # Should have been a POST request, not PATCH
                    return
                user_obj = User(user_info)
                self.global_user_cache[user_info[UserKeys.EMAIL]] = user_obj

        # Now process the prefixes in the prefix-user mapping cache
        # add_prefixes will be present for both POST and PATCH
        for pref in user_info.get(UserKeys.ADD_NEW_PREF, []):
            pref = pref.lower()
            updated = self.update_prefix(user_email=user_info[UserKeys.EMAIL],
                                         prefix=pref, op='add')
            if updated:
                if is_patch:
                    user_obj.update_prefix(prefix=pref, op='add')
                self.logger.info(f"{method} Added prefix '{pref}' for "
                                       f"the user {user_info[UserKeys.EMAIL]}")
            else:
                # If any of the prefix update fails, check if needs to be
                # removed from the cached user object and process the VMs accordingly
                failed_add_pref_list.append(pref)
                self.logger.error(f"{method} Could not add prefix '{pref}'"
                                        f"for the user {user_info[UserKeys.EMAIL]}")
                vm_list_untrack = user_obj.update_prefix(prefix=pref, op='remove')
                if vm_list_untrack: 
                    vms_to_untrack.extend(vm_list_untrack)

        # REMOVE_NEW_PREF will be present only for the PATCH requests
        # If remove_prefixes is successful, get the list of VMs which are
        # tracked by this prefix and remove their owner
        for pref in user_info.get(UserKeys.REMOVE_NEW_PREF, []):
            pref = pref.lower()
            updated = self.update_prefix(user_email=user_info[UserKeys.EMAIL],
                                         prefix=pref, op='remove')
            if updated:
                self.logger.info(f"{method} Removed prefix '{pref}' for"
                                       f" the user {user_info[UserKeys.EMAIL]}")
                vm_list_untrack = user_obj.update_prefix(prefix=pref, op='remove')
                if vm_list_untrack:
                    vms_to_untrack.extend(vm_list_untrack)
            else:
                # Since removal failed -- the VMs are still tracked by the prefix
                failed_remove_pref_list.append(pref)
                self.logger.error(f"{method} Could not remove prefix "
                                        f"'{pref}' for the user "
                                        f"{user_info[UserKeys.EMAIL]}")
        # For a PATCH request, also check if the quota/name needs to be patched
        # FIXME update for the VMs in case of name change?
        if is_patch:
            if UserKeys.NAME in user_info:
                user_obj.update_name(user_info[UserKeys.NAME])
            if UserKeys.QUOTA in user_info:
                user_obj.update_resource_quota(user_info[UserKeys.QUOTA])
        done = True

        if vms_to_untrack:
            for (uuid, parent_cluster_name, vm_name) in vms_to_untrack:
                cobj = self.global_cluster_cache[parent_cluster_name]
                with cobj.vm_cache_lock:
                    vm_obj = cobj.vm_cache.get(uuid, None)
                    if not vm_obj:
                        vm_obj = cobj.power_off_vms.get(uuid, None)
                    if vm_obj:
                        vm_obj.set_owner(None, None)
                        self.logger.info(f"Untracking the VM {vm_name} for user {user_info[UserKeys.EMAIL]}")
        for cname, _ in self.global_cluster_cache.items():
            self.map_vm_and_users_track_resources(cname)
        return failed_add_pref_list, failed_remove_pref_list, done

    def add_cluster(self, cluster_info):
        """Adds a new cluster to the cache
            Args:
                cluster_info (dict): Information about the cluster
        """
        if cluster_info['name'] in self.global_cluster_cache:
            self.logger.error(f"Cluster with name {cluster_info['name']} already exists in the cache")
            raise ActionAlreadyPerformedError(f"Cluster with name {cluster_info['name']} already exists in the cache")
        cluster_obj = Cluster(cluster_info["name"], cluster_info["ip"],
                            cluster_info["user"], cluster_info["password"])
        self.cluster_obj_list.append(cluster_obj)
        self.global_cluster_cache[cluster_info['name']] = cluster_obj
        self.rebuild_cache(all_clusters=False, cluster_name=cluster_info["name"])

    def untrack_cluster(self, cluster_name):
        """Untracks the cluster from the cache
        """
        self.logger.info(f"Removing the cluster {cluster_name} from the cache")

        with self.global_cluster_cache_lock:
            if cluster_name in self.global_cluster_cache:
                self.global_cluster_cache[cluster_name].cleanup()
                del self.global_cluster_cache[cluster_name]
            else:
                self.logger.error(f"Cluster {cluster_name} not found in the cache")
                return

        with self.global_user_cache_lock:
            for email, user_obj in self.global_user_cache.items():
                self.logger.debug(f"Processing removal the cluster {cluster_name} for user {email}")
                user_obj.untrack_cluster(cluster_name)
        self.logger.info(f"Cluster {cluster_name} removed from the cache")

        return

    def perform_cluster_vm_power_change(self,
                                        cluster_name,
                                        vm_info
                                        ) -> typing.Tuple[HTTPStatus, typing.Dict]:
        """Common function to change the power change of a VM for a cluster.
            Currently only powers off a VM.
            Args:
                cluster_name (str): Name of the cluster on which the VM resides
                vm_info (dict): Contains 'uuid' or 'name' for a VM
            Returns:
                Tuple(HTTPStatus, dict)
        """
        # TODO Pause the Cache refresh in this time
        cluster_obj = None
        from http import HTTPStatus
        with self.global_cluster_cache_lock:
            cluster_obj = self.global_cluster_cache.get(cluster_name, None)
            if not cluster_obj:
                err = {"message: "f"Cluster with name {cluster_name} not found"
                       f" in the cache"}
                self.logger.error(err)
                return HTTPStatus.NOT_FOUND, err
        status, msg = cluster_obj.power_down_vm(vm_name=vm_info.get('name', None),
                                                uuid=vm_info.get('uuid', None))
        from http import HTTPStatus
        if status == HTTPStatus.OK:
            # We get this if the POWER OFF was successful -- process it
            # for the concerned user as well.
            if 'vm_config' in msg:
                with self.global_user_cache_lock:
                    if msg['vm_config']['owner_email'] is None:
                        self.logger.info(f"Could not back-propagate the "
                                         f"VM {vm_info['name']}, UUID "
                                         f"{vm_info['uuid']} to Owner as "
                                         "the owner of the VM is unknown!")
                    user_obj = self.global_user_cache.get(
                        msg['vm_config']['owner_email'], None)
                    if user_obj:
                        self.logger.info("Back-propagating the Resources"
                                               f"change for VM "
                                               f"{msg['vm_config']["name"]}"
                                               f" UUID {msg['vm_config']['uuid']} "
                                               f"to its owner {user_obj.email}")
                        user_obj.update_vm_resources(msg['vm_config'])
                    else:
                        self.logger.error(f"VM {msg['vm_config']['uuid']} has no owner!!")
        return status, msg

    def perform_cluster_vm_nic_remove(self,
                                      cluster_name,
                                      vm_info
                                      ) -> typing.Tuple[HTTPStatus, typing.Dict]:
        """Common function to change the power change of a VM for a cluster.
            Currently only powers off a VM.
            Args:
                cluster_name (str): Name of the cluster on which the VM resides
                vm_info (dict): Contains 'uuid' or 'name' for a VM
        """
        # TODO Pause the Cache refresh in this time
        cluster_obj = None
        from http import HTTPStatus
        with self.global_cluster_cache_lock:
            cluster_obj = self.global_cluster_cache.get(cluster_name, None)
            if not cluster_obj:
                err = {"message": f"Cluster with name {cluster_name} not found in the cache"}
                self.logger.error(err['message'])
                return HTTPStatus.NOT_FOUND, err
        # VM NIC removal does not affect the user
        return cluster_obj.remove_vm_nic(vm_name=vm_info.get('name', None),
                                         uuid=vm_info.get('uuid', None))

    def get_all_vms_for_user(self, email, cluster=None,
                        include_powered_off_vms=False
                        ) -> typing.Tuple[typing.List, HTTPStatus]:
        """Returns list of VMs for one particular user
            Args:
                email (str): Email of the user
                cluster (str): Filter the list of VMs as per the cluster on \
                    which they run
            Returns:
                list of the VMs for the user
        """
        if cluster:
            if cluster not in self.global_cluster_cache:
                self.logger.error(f"Cluster {cluster} not found in the cache")
                return [], HTTPStatus.NOT_FOUND
        with self.global_user_cache_lock:
            user_obj = self.global_user_cache.get(email, None)
            if user_obj:
                uuid_info_map = user_obj.to_json()[UserKeys.VM_UTILIZED]
                if cluster:
                    return [
                        {
                            "uuid": uuid,
                            "name": res_info['name'],
                            "cores": res_info['cores'],
                            "memory": res_info['memory'],
                            "parent_cluster": res_info['parent_cluster'],
                            "power_state": res_info['power_state']
                        }
                        for uuid, res_info in uuid_info_map.items()
                        if res_info['parent_cluster'] == cluster
                    ], HTTPStatus.OK
                return [{
                        "uuid": uuid,
                        "name": res_info['name'],
                        "cores": res_info['cores'],
                        "memory": res_info['memory'],
                        "parent_cluster": res_info['parent_cluster'],
                        "power_state": res_info['power_state']
                    } for uuid, res_info in uuid_info_map.items()], HTTPStatus.OK
            else:
                self.logger.error(f"User with email {email} not found in the cache")
        return [], HTTPStatus.NOT_FOUND

    def _get_vms_without_prefix(self, cluster_name=None) -> typing.Dict:
        """Helper function to list the VMs whose owners could not be established.
        Args:
            cluster_name (Str|optional): Name of the cluster to filter
        Returns:
            dict: Maps the clusters to the list of the VMs whose owners could\
                not be determined based on the prfixes.
        """
        clusters_vm_without_prefix = {}
        with self.global_cluster_cache_lock:
            if cluster_name is not None:
                if cluster_name in self.global_cluster_cache:
                    clusters_vm_without_prefix[cluster_name] = self.global_cluster_cache.\
                        get(cluster_name).get_vm_with_no_prefix()
                else:
                    self.logger.error(f"Cluster with name {cluster_name} does not "
                                            f"exist in the cache!!")
                return clusters_vm_without_prefix
            else:
                for cname, cluster_obj in self.global_cluster_cache.items():
                    clusters_vm_without_prefix[cname] = cluster_obj.get_vm_with_no_prefix()
        return clusters_vm_without_prefix

    def _get_vms_resources_sorted(self, cluster_name=None, count=-1,
                                 sort_by_cores=False,
                                 sort_by_mem=False
            ) -> typing.Dict:
        """Helper function to list the VMs sorted by the resources they are using.
        Args:
            cluster_name (Optional|str): Name of the cluster to filter
            count (int): Number of VMs to return. If -1, returns all.
            sort_by_cores (bool): If True, sorts the VMs by the cores they are using
            sort_by_mem (bool): If True, sorts the VMs by the mem they are using
        """
        vm_using_resources_sorted_list = {}
        with self.global_cluster_cache_lock:
            if cluster_name is not None:
                if cluster_name in self.global_cluster_cache:
                    vm_using_resources_sorted_list[cluster_name] = {}
                    vm_using_resources_sorted_list[cluster_name]["running_vm"] = \
                        self.global_cluster_cache[cluster_name].get_vm_using_resources_sorted(
                        count=count,
                        sort_by_cores=sort_by_cores,
                        sort_by_mem=sort_by_mem
                    )
                else:
                    self.logger.error(f"Cluster with name {cluster_name} "
                                            f"does not exist in the cache!!")
                return vm_using_resources_sorted_list
            else:
                # If a cluster is not specified, we will return a dict with
                # each cluster_name as key
                for cname, cluster_obj in self.global_cluster_cache.items():
                    vm_using_resources_sorted_list[cname] = \
                        cluster_obj.get_vm_using_resources_sorted(
                            count=count,
                            sort_by_cores=sort_by_cores,
                            sort_by_mem=sort_by_mem)
        return vm_using_resources_sorted_list

    def _get_deviating_users(self, email=None) -> typing.Dict:
        """Helped function that iterates over all the users in the cache and
            returns a Map of email to the quota deviations of the user
            Args:
                email (optional): To get deviations of a single user
            Returns:
                Dict: Mapping email of user to their quota deviations
        """
        deviating_users = {}
        with (self.global_user_cache_lock):
            if email is not None:
                if email in self.global_user_cache:
                    user_obj = self.global_user_cache[email]
                    is_deviating, deviations, quotas = \
                        user_obj.is_over_utilizing_quota()
                    if not is_deviating:
                        self.logger.info(f"User {email} requested for "
                                               "quota over-utilization, no "
                                               "utilization detected.")
                        return {}
                    else:
                        deviating_users[email] = {'deviations': deviations, 'quotas': quotas}
            else:
                for email, user_obj in self.global_user_cache.items():
                    is_deviating, deviations, quotas = user_obj.is_over_utilizing_quota()
                    if is_deviating:
                        deviating_users[email] = {'deviations': deviations, 'quotas': quotas}
        return deviating_users

    def get_deviating_items(self, get_users_over_util=True,
                            include_vm_without_prefix=True,
                            get_vm_resources_per_cluster=False,
                            email=None,
                            cluster_name=None,
                            count=-1, sort_by_cores=False,
                            sort_by_mem=False, print_summary=False,
                            retain_diff=False
                            ) -> typing.Optional[typing.Tuple[dict, dict, dict]]:
        """Returns all the deviating items on the cluster
        Args:
            get_users_over_util (bool): If True, returns the users who are over-utilizing
            include_vm_without_prefix (bool): If True, returns the VMs without prefix
            get_vm_resources_per_cluster (bool): If True, returns the VMs sorted by resources
            email (str): Email of the user to filter the deviations
            cluster_name (str): Name of the cluster to filter the deviations
            count (int): Number of VMs to return. If -1, returns all.
            sort_by_cores (bool): If True, sorts the VMs by the cores they are using
            sort_by_mem (bool): If True, sorts the VMs by the mem they are using
            print_summary (bool): If True, prints the summary on the console
            retain_diff (bool): If True, retains the diff in the cache
        Returns:
            Tuple(dict, dict, dict): Deviating users, VMs per cluster, VMs without prefix
        """
        users_over_utilizing_quota = {}
        vm_resources_per_cluster = {}
        vm_without_prefix = {}
        if (not get_users_over_util and not include_vm_without_prefix and
                not get_vm_resources_per_cluster):
            self.logger.error("At least one of the deviating items must"
                                    " be specified.")
            return None
        if get_users_over_util:
            users_over_utilizing_quota = self._get_deviating_users(email=email)
        if get_vm_resources_per_cluster:
            if cluster_name:
                vm_resources_per_cluster[cluster_name] = self._get_vms_resources_sorted(
                    cluster_name=cluster_name,
                    count=count,
                    sort_by_cores=sort_by_cores,
                    sort_by_mem=sort_by_mem
                )
            else:
                vm_resources_per_cluster = self._get_vms_resources_sorted(
                    count=count,
                    sort_by_cores=sort_by_cores,
                    sort_by_mem=sort_by_mem
                )
        if include_vm_without_prefix:
            vm_without_prefix = self._get_vms_without_prefix(
                cluster_name=cluster_name
            )

        if retain_diff:
            ts = int(time.time())
            self.logger.debug(f"Adding entry in the timed_deviations: at ts: {ts}")
            self.timed_deviations[ts] = {}
            if get_users_over_util:
                self.timed_deviations[ts]['users_over_util'] = users_over_utilizing_quota
            if get_vm_resources_per_cluster:
                self.timed_deviations[ts]['vm_using_per_cluster'] = vm_resources_per_cluster
            if include_vm_without_prefix:
                self.timed_deviations[ts]['vm_without_prefix'] = vm_without_prefix
            while len(self.timed_deviations) > int(os.environ.get('deviations_cache_retain', str(DEFAULT_DEVIATION_RETAIN_VALUE))):
                self.logger.info(f"There are {len(self.timed_deviations)} "
                                       "deviation entries. Pruning to reach "
                                       f"{os.environ.get('deviations_cache_retain')}")
                self.timed_deviations.popitem(last=False)
            # self.logger.debug(f"The entry in the timed_deviations: at ts is {json.dumps(self.timed_deviations[ts], indent=4)}")
            # print(f"The entry in the timed_deviations: at ts is {json.dumps(self.timed_deviations[ts], indent=4)}")

        if print_summary:
            json.dumps(users_over_utilizing_quota, indent=4)
            json.dumps(vm_resources_per_cluster, indent=4)
            json.dumps(vm_without_prefix, indent=4)
        return users_over_utilizing_quota, vm_resources_per_cluster, vm_without_prefix

    # Functions helping the Cluster Monitor
    def get_timed_deviations(self, start_ts,
                           end_ts
                           ) -> typing.Tuple[int, typing.Dict, int, typing.Dict]:
        """Retuns the consolidated list of the quota and VM deviations, which
            has not changed from start_ts and end_ts.
            If exact timestamps do not exist, returns the data at closest \
                matching timestamps
            Args:
                start_ts (int | None): Older timestamp to check for the diff
                end_ts (int): Newer timestamp to check
            Returns:
                tuple(int, dict, int, dict): actual_old_ts, old_deviations,\
                    actual_new_ts, new_deviations
            Raises:
                SameTimestampError
        """
        if not start_ts:
            start_ts = int(time.time())

        # self.logger.debug(json.dumps(self.timed_deviations))
    
        # Get the closest time stamp to start_ts in the cache
        closest_start_ts = min(self.timed_deviations.keys(), key=lambda ts: abs(start_ts - ts))

        # Get the closest time-stamped data to end_ts
        closest_end_ts = min(self.timed_deviations.keys(), key=lambda ts: abs(end_ts - ts))

        if closest_start_ts == closest_end_ts:
            err_str = (f"The latest time: {closest_end_ts} and the earliest "
                       f"time: {closest_start_ts} are same!")
            self.logger.error(err_str)
            raise SameTimestampError(err_str)
        old_devs = self.timed_deviations[closest_start_ts]
        new_devs = self.timed_deviations[closest_end_ts]

        return closest_start_ts, old_devs, closest_end_ts, new_devs

    def dump_user_config(self, file_name):
        """Dumps the user configuration to a file
        """
        user_json_list = []
        with self.global_user_cache_lock:
            for _, user_obj in self.global_user_cache.items():
                u_json = user_obj.to_json()
                json_dump = {
                    "name": u_json[UserKeys.NAME],
                    "email": u_json[UserKeys.EMAIL],
                    "prefix": u_json[UserKeys.PREFIX],
                    "quota": [{
                        cname: {
                            'cores': quota_info['cores'],
                            'memory': convert_mb_to_gb(quota_info['memory'])
                        }
                    } for cname, quota_info in u_json[UserKeys.CLUSTER_QUOTA].items()]
                }
                global_quota = {
                    'global' : {}
                }
                self.logger.info(u_json[UserKeys.GLOBAL_QUOTA])
                if 'cores' in u_json[UserKeys.GLOBAL_QUOTA]:
                    global_quota['global']['cores'] = u_json[UserKeys.GLOBAL_QUOTA]['cores']
                if 'memory' in u_json[UserKeys.GLOBAL_QUOTA]:
                    global_quota['global']['memory'] = convert_mb_to_gb(u_json[UserKeys.GLOBAL_QUOTA]['memory'])
                json_dump['quota'].append(global_quota)
                user_json_list.append(json_dump)
        dump_data = {"users": user_json_list}
        try:
            with open(file_name, 'w') as f:
                f.write(json.dumps(dump_data, indent=4))
        except Exception as ex:
            raise ex
        return True

    def dump_cluster_config(self, file_name):
        """Dumps the cluster configuration to a file
        """
        cluster_config_list = []
        with self.global_cluster_cache_lock:
            for _, c_obj in self.global_cluster_cache.items():
                cluster_config_list.append(c_obj.json(get_user_creds=True))
        dump_data = {"clusters": cluster_config_list}
        try:
            with open(file_name, 'w') as f:
                f.write(json.dumps(dump_data, indent=4))
        except Exception as ex:
            raise ex
        return True

    def remove_user(self,
                    user_email) -> int:
        """Removes the user from the cache
            Args:
                user_email (str): Email of the user to be removed
            Returns:
                int: 0 if successful, else 1
            """
        vms_to_untrack = []
        with self.global_user_cache_lock:
            user_obj = self.global_user_cache.get(user_email, None)
            user_prefixes = user_obj.prefixes
            if user_obj:
                for user_pref in user_prefixes:
                    vm_list_untrack = user_obj.update_prefix(prefix=user_pref, op='remove')
                    if vm_list_untrack: 
                        vms_to_untrack.extend(vm_list_untrack)
                    del self.user_prefix_email_map[user_pref[0].lower()][user_pref]
                del self.global_user_cache[user_email]
            else:
                self.logger.error(f"User with email {user_email} not found in the cache")
                return 1
            if vms_to_untrack:
                for (uuid, parent_cluster_name, vm_name) in vms_to_untrack:
                    cobj = self.global_cluster_cache[parent_cluster_name]
                    with cobj.vm_cache_lock:
                        vm_obj = cobj.vm_cache.get(uuid, None)
                        if not vm_obj:
                            vm_obj = cobj.power_off_vms.get(uuid, None)
                        if vm_obj:
                            vm_obj.set_owner(None, None)
                            self.logger.info(f"Untracking the VM {vm_name} for user {user_email}")
            return 0
