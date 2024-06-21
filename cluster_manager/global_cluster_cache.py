"""Global ClusterManager acts as a cohesion between the Users and clusters
 (and VMs)

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""
import typing
import threading
import logging
import time
import json

from concurrent import futures
from string import ascii_lowercase as alc
from copy import deepcopy
from collections import OrderedDict

from caching.cluster import Cluster
from users.user import User
from common.constants import UserKeys, ONE_DAY_IN_SECS
from caching.server_constants import CacheState


GLOBAL_MGR_LOGGER = logging.getLogger(__name__)
GLOBAL_MGR_LOGGER.setLevel(logging.DEBUG)
handler = logging.FileHandler(f"{__name__}.log", mode='w')
formatter = logging.Formatter("%(filename)s:%(lineno)d - %(asctime)s %(levelname)s - %(message)s")
handler.setFormatter(formatter)
GLOBAL_MGR_LOGGER.addHandler(handler)


class GlobalClusterCache:
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
    GLOBAL_CLUSTER_CACHE = {}
    GLOBAL_CLUSTER_CACHE_LOCK = threading.Lock()
    cache_build_done = {}

    # Actual global cache for user and its lock
    GLOBAL_USER_CACHE = {}
    GLOBAL_USER_CACHE_LOCK = threading.Lock()
    # Map from user prefixes to the owner users
    USER_PREFIX_EMAIL_MAP = {}

    # Lists of cluster and user objects
    cluster_obj_list = []
    user_obj_list = []

    def __new__(cls, *args, **kwargs):
        if cls._g_cluster_mgr_instance is None:
            with cls._g_cmgr_lock:
                if cls._g_cluster_mgr_instance is None:
                    cls._g_cluster_mgr_instance = super().__new__(cls)
        return cls._g_cluster_mgr_instance

    def __init__(self, cluster_list, user_list, cache_clusters=True):
        """Parse and process raw list of clusters and users JSON
            Builds Users cache -> Builds Cluster cache -> Maps CVMs and the Users
        """
        start_time = time.time()
        # Triggers the caching process
        for e_cl in cluster_list:
            cluster_obj = Cluster(e_cl["name"], e_cl.get("ip"), e_cl["user"],
                                  e_cl["password"])
            self.cluster_obj_list.append(cluster_obj)
        for e_usr in user_list:
            user_obj = User(e_usr)
            self.user_obj_list.append(user_obj)
        for ii in alc:
            self.USER_PREFIX_EMAIL_MAP[ii] = {}

        self.current_user_vm_map = {}
        self.old_user_vm_map = {}

        # List of all the VMs which do not match any prefix
        # We will send a notification for this.
        # TODO Store it in the time-series database to get the diff in easy
        # way for the time schedule
        self.timed_offenses = OrderedDict()
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
        for cname, cobj in self.GLOBAL_CLUSTER_CACHE.items():
            running, stopped = cobj.get_vm_list()
            total_now = len(running) + len(stopped)
            global_total_vms += total_now
            print(f"\tTracking {total_now} VMs on the cluster '{cname}'")
        print(f"Cached {len(cluster_list)} clusters, {len(user_list)} users "
              f"and processed {global_total_vms} VMs in {time.time() - start_time:<.3f}"
              " seconds")
        

    def summary(self, print_summary=False) -> typing.Optional[typing.Dict]:
        if print_summary:
            print("\n\nCLUSTERS :")
            with self.GLOBAL_CLUSTER_CACHE_LOCK:
                for _, cluster_obj in self.GLOBAL_CLUSTER_CACHE.items():
                    cluster_obj.summary(summary_verbosity=1, print_summary=print_summary)
            print("\n\nUSERS :")
            user_count = 0
            with self.GLOBAL_USER_CACHE_LOCK:
                for _, user_obj in self.GLOBAL_USER_CACHE.items():
                    user_count += 1
                    user_obj.summary(summary_verbosity=2, print_summary=print_summary)
            print(f"Managing {user_count} users.")
            return None
        else:
            cache_summary = {'cluster_cache': {}, 'user_cache': {}}
            with self.GLOBAL_CLUSTER_CACHE_LOCK:
                for cname, cluster_obj in self.GLOBAL_CLUSTER_CACHE.items():
                    cache_summary['cluster_cache'][cname] = cluster_obj.summary(
                        summary_verbosity=2,
                        print_summary=print_summary
                    )
                    GLOBAL_MGR_LOGGER.debug(f"SUMMARY REQUESTED, current:{cache_summary}")
            with self.GLOBAL_USER_CACHE_LOCK:
                for email, user_obj in self.GLOBAL_USER_CACHE.items():
                    cache_summary['user_cache'][email] = user_obj.summary(
                        print_summary=print_summary
                    )
            return cache_summary

    # Functions related to clusters, cluster cache rebuilds
    def _build_cluster_cache(self, cluster_obj) -> str:
        """This is a helper function which builds the cache for each cluster
            And then adds each cluster object into a higher cache
            The exact structure of cache is as follows:
                Global Cluster Cache (GLOBAL_CLUSTER_CACHE): Caches all the
                clusters
                    Each cluster has its own cache of VMs (NDB and Non-NDB)
                        Each NDB VM will track its own DB Server VMs (also
                        tracked by the cluster one level up)
            Args:
                cluster_obj: Object of class Cluster
            Returns:
                str: Name of the cluster whose cache is done

        """
        if cluster_obj.cache_state in [CacheState.READY, CacheState.PENDING]:
            GLOBAL_MGR_LOGGER.info(f"Start (re)Building cache for cluster {cluster_obj.name}")
            cluster_obj.build_refresh_cache()
        # TODO If already building, wait for it to complete (?) Can we have async here?
        with self.GLOBAL_CLUSTER_CACHE_LOCK:
            self.GLOBAL_CLUSTER_CACHE[cluster_obj.name] = cluster_obj
        GLOBAL_MGR_LOGGER.info(f"Cache (Re)Build for cluster {cluster_obj.name} done!")
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
                GLOBAL_MGR_LOGGER.info("Cache rebuild started for the cluster "
                                       f"{cluster_obj.name}")
                caching_futures_list.append(self._threadpool.submit(
                    self._build_cluster_cache, cluster_obj))
            # Each thread in the pool work on exactly one cluster object
            for fut in futures.as_completed(caching_futures_list):
                try:
                    cluster_name = fut.result()
                    GLOBAL_MGR_LOGGER.info(f"Cache build done for the cluster {cluster_name}. "
                                           f"Trying mapping VM and users for the cluster.")
                    self.map_vm_and_users_track_resources(cluster_name)
                    self.cache_build_done[cluster_name] = True
                except Exception as ex:
                    GLOBAL_MGR_LOGGER.exception(f"Exception occurred: {ex}")
                    raise ex
            if not initial_build:
                self.detect_deleted_vms()
        else:
            if not cluster_name:
                GLOBAL_MGR_LOGGER.error("Rebuild cache for specific cluster "
                                        "requested but name not provided.")
                return
            cluster_obj = self.GLOBAL_CLUSTER_CACHE.get(cluster_name, None)
            if cluster_obj is not None:
                GLOBAL_MGR_LOGGER.debug(f"Started cache build for the cluster "
                                        f"{cluster_obj.name}")
                self._threadpool.submit(self._build_cluster_cache, cluster_obj)
            else:
                GLOBAL_MGR_LOGGER.error("Cluster with name '{}' not found in cache. "
                                        "Please verify that cluster information"
                                        " exists in the config.".format(cluster_name))
                return
            self.map_vm_and_users_track_resources(cluster_name)
            if not initial_build:
                self.detect_deleted_vms()
            return
        return

    def get_vms_without_prefix(self, cluster_name=None) -> dict:
        clusters_vm_without_prefix = {}
        with self.GLOBAL_CLUSTER_CACHE_LOCK:
            if cluster_name is not None:
                if cluster_name in self.GLOBAL_CLUSTER_CACHE:
                    clusters_vm_without_prefix[cluster_name] = self.GLOBAL_CLUSTER_CACHE.\
                        get(cluster_name).get_vm_with_no_prefix()
                else:
                    GLOBAL_MGR_LOGGER.error(f"Cluster with name {cluster_name} does not "
                                            f"exist in the cache!!")
                return clusters_vm_without_prefix
            else:
                for cname, cluster_obj in self.GLOBAL_CLUSTER_CACHE.items():
                    clusters_vm_without_prefix[cname] = cluster_obj.get_vm_with_no_prefix()
        return clusters_vm_without_prefix

    def get_vms_resources_sorted(self, cluster_name=None, count=-1,
                                 sort_by_cores=False,
                                 sort_by_mem=False
            ) -> typing.Dict:
        vm_using_resources_sorted_list = {}
        with self.GLOBAL_CLUSTER_CACHE_LOCK:
            if cluster_name is not None:
                if cluster_name in self.GLOBAL_CLUSTER_CACHE:
                    vm_using_resources_sorted_list[cluster_name] = {}
                    # If a cluster is specified, we will return a list
                    vm_using_resources_sorted_list[cluster_name]["running_vm"] = \
                        self.GLOBAL_CLUSTER_CACHE[cluster_name].get_vm_using_resources_sorted(
                        count=count,
                        sort_by_cores=sort_by_cores,
                        sort_by_mem=sort_by_mem
                    )
                else:
                    GLOBAL_MGR_LOGGER.error(f"Cluster with name {cluster_name} "
                                            f"does not exist in the cache!!")
                return vm_using_resources_sorted_list
            else:
                # If a cluster is not specified, we will return a dict with
                # each cluster_name as key
                for cname, cluster_obj in self.GLOBAL_CLUSTER_CACHE.items():
                    vm_using_resources_sorted_list[cname] = \
                        cluster_obj.get_vm_using_resources_sorted(
                            count=count,
                            sort_by_cores=sort_by_cores,
                            sort_by_mem=sort_by_mem)
        return vm_using_resources_sorted_list

    def get_user_offenses(self, email=None) -> dict:
        offending_users = {}
        with (self.GLOBAL_USER_CACHE_LOCK):
            if email is not None:
                if email in self.GLOBAL_USER_CACHE:
                    user_obj = self.GLOBAL_USER_CACHE[email]
                    is_offending, offenses = \
                        user_obj.is_over_utilizing_quota()
                    if not is_offending:
                        GLOBAL_MGR_LOGGER.info(f"User {email} requested for "
                                               "quota over-utilization, no "
                                               "utilization detected.")
                        return {}
                    else:
                        offending_users[email] = offenses
            else:
                for email, user_obj in self.GLOBAL_USER_CACHE.items():
                    is_offending, offenses = user_obj.is_over_utilizing_quota()
                    if is_offending:
                        offending_users[email] = offenses
        return offending_users

    def get_offending_items(self, get_users_over_util=True,
                            include_vm_without_prefix=True,
                            get_vm_resources_per_cluster=False,
                            email=None,
                            cluster_name=None,
                            count=-1, sort_by_cores=False,
                            sort_by_mem=False, print_summary=False,
                            retain_diff=False
                            ) -> typing.Optional[typing.Tuple[dict, dict, dict]]:

        users_over_utilizing_quota = {}
        vm_resources_per_cluster = {}
        vm_without_prefix = {}
        if (not get_users_over_util and not include_vm_without_prefix and
                not get_vm_resources_per_cluster):
            GLOBAL_MGR_LOGGER.error("At least one of the offending items must"
                                    " be specified.")
            return None
        if get_users_over_util:
            users_over_utilizing_quota = self.get_user_offenses(email=email)
        if get_vm_resources_per_cluster:
            if cluster_name:
                vm_resources_per_cluster[cluster_name] = self.get_vms_resources_sorted(
                    cluster_name=cluster_name,
                    count=count,
                    sort_by_cores=sort_by_cores,
                    sort_by_mem=sort_by_mem
                )
            else:
                vm_resources_per_cluster = self.get_vms_resources_sorted(
                    count=count,
                    sort_by_cores=sort_by_cores,
                    sort_by_mem=sort_by_mem
                )
        if include_vm_without_prefix:
            vm_without_prefix = self.get_vms_without_prefix(
                cluster_name=cluster_name
            )

        if retain_diff:
            ts = int(time.time())
            self.timed_offenses[ts] = {}
            if get_users_over_util:
                self.timed_offenses[ts]['users_over_util'] = get_users_over_util
            if get_vm_resources_per_cluster:
                self.timed_offenses[ts]['vm_using_per_cluster'] = vm_resources_per_cluster
            if include_vm_without_prefix:
                self.timed_offenses[ts]['vm_without_prefix'] = vm_without_prefix

        if print_summary:
            json.dumps(users_over_utilizing_quota, indent=4)
            json.dumps(vm_resources_per_cluster, indent=4)
            json.dumps(vm_without_prefix, indent=4)
        return users_over_utilizing_quota, vm_resources_per_cluster, vm_without_prefix

    # Functions related to Users, user cache rebuilds
    def _build_user_cache(self):
        # Since we already have the __new__ method protecting the data
        # we don't need lock, However, just for safety measure, i am adding
        with self.GLOBAL_CLUSTER_CACHE_LOCK:
            # Since the global_cluster_manager object is created only once
            # we don't need to check if the user already exists here
            # (it will be always be created exactly once)
            # However, when we add a new user altogether after the system has
            # started, then we need check
            for each_user in self.user_obj_list:
                self.GLOBAL_USER_CACHE[each_user.email] = each_user
                for each_pref in each_user.prefixes:
                    pref_to_use = each_pref.lower()
                    bucket_key = pref_to_use[0]
                    bucket = self.USER_PREFIX_EMAIL_MAP[bucket_key]
                    if pref_to_use in bucket:
                        if bucket[pref_to_use] != each_user.email:
                            GLOBAL_MGR_LOGGER.error(f"Prefix '{pref_to_use}', "
                                                    f"intended for user "
                                                    f"{each_user.email} "
                                                    f"already exists in the "
                                                    f"cache for user "
                                                    f"'{bucket[pref_to_use]}'."
                                                    f" Skipping prefix add.")
                        # In case of duplicate prefix for the same user, ignore
                        continue
                    bucket[pref_to_use] = each_user.email

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
        user_obj = self.GLOBAL_USER_CACHE.get(user_email, None)
        if user_obj is None:
            GLOBAL_MGR_LOGGER.error(f"User with email '{user_email}' not "
                                    f"found in the cache for updating "
                                    f"prefix. Skipping update.")
            return True
        # Convert the prefix to lowercase
        pref_to_use = prefix.lower()
        if op == "add":
            bucket = self.USER_PREFIX_EMAIL_MAP[pref_to_use[0]]
            # Check if the same prefix already exists for other user
            if pref_to_use in bucket:
                if bucket[pref_to_use] != user_email:
                    GLOBAL_MGR_LOGGER.error(f"Prefix '{pref_to_use}' intended "
                                            f"for user ;{user_email}' already "
                                            f"exists in the cache for other "
                                            f"user: {bucket[pref_to_use]}. "
                                            f"Skipping adding.")
                    return False
                else:
                    GLOBAL_MGR_LOGGER.info(f"Prefix '{pref_to_use}' for user "
                                           f"'{user_email}' already exists. "
                                           f"Skipping.")
                    return True
            self.USER_PREFIX_EMAIL_MAP[pref_to_use[0]][pref_to_use] = user_email
            user_obj.update_prefix(op=op, prefix=prefix)
            return True
        elif op == "remove":
            bucket = self.USER_PREFIX_EMAIL_MAP[pref_to_use[0]]
            if pref_to_use in bucket:
                if bucket[pref_to_use] != user_email:
                    GLOBAL_MGR_LOGGER.error(f"Prefix '{pref_to_use}' intended "
                                            f"for user '{user_email}' already "
                                            f"exists in the cache for other "
                                            f"user: {bucket[pref_to_use]}. "
                                            f"Skipping removal.")
                    return False
                else:
                    del bucket[pref_to_use]
                    # Since we will usually have very few prefixes
                    # -- O(n) should not matter much
                    user_obj.update_prefix(op=op, prefix=prefix)
                    GLOBAL_MGR_LOGGER.info(f"Prefix '{pref_to_use}' for user "
                                           f"'{user_email}' removed "
                                           f"successfully.")
                    return True
            # Prefix does not exist
            return True
        else:
            raise Exception(f"Operation {op} is invalid for update_prefix")

    def map_vm_and_users_track_resources(self, cluster_name):
        """This function maps users and VMs.
            Also updates the resources used by each VM and consequently the users
        """
        cluster_obj = self.GLOBAL_CLUSTER_CACHE[cluster_name]
        GLOBAL_MGR_LOGGER.debug(f"Trying mapping VMs for the cluster {cluster_name}")
        if cluster_obj.is_cache_ready():
            ts = int(time.time())

            with cluster_obj.vm_cache_lock:
                for uuid, vm_obj in cluster_obj.vm_cache.items():
                    GLOBAL_MGR_LOGGER.debug(f"Cluster: {cluster_name} - "
                                            f"UUID under process is {uuid} :"
                                            f" {vm_obj.name}")
                    # Get the bucket key
                    bucket_key = vm_obj.name.lower()[0]
                    # Get all the prefixes in the bucket
                    bucket = self.USER_PREFIX_EMAIL_MAP[bucket_key]
                    pref_found = False
                    for pref, email in bucket.items():
                        if vm_obj.name.lower().startswith(pref):
                            pref_found = True
                            user_obj = self.GLOBAL_USER_CACHE[email]
                            owner_name = user_obj.name
                            # Check if the VM has the same owner, if not, update
                            vm_obj.add_owner(owner_name, email)
                            # Update the resources used by the cluster if not already
                            user_obj.update_vm_resources(vm_obj.to_json())

                            # Store a copy of this mapping within the
                            # cache to detect deleted VMs
                            if email not in self.current_user_vm_map:
                                self.current_user_vm_map[email] = {}
                            self.current_user_vm_map[email][vm_obj.uuid] = cluster_name

                            break
                    if not pref_found:
                        cluster_obj.add_vm_with_no_prefix(ts, vm_obj.name, vm_obj.uuid)
                        GLOBAL_MGR_LOGGER.warning(f"Cluster {cluster_name} - "
                                                  f"No prefix matched for the "
                                                  f"VM {vm_obj.name}")
                GLOBAL_MGR_LOGGER.info(f"Mapping for cluster {cluster_name} done! Cache is now READY!")
        else:
            GLOBAL_MGR_LOGGER.warning(f"The cluster {cluster_name} cache is "
                                      f"still building. Try again later.")

    def detect_deleted_vms(self):
        for email, vm_cluster_map in self.current_user_vm_map.items():
            old_vm_cluster_map = self.old_user_vm_map.get(email, dict())
            newly_deleted_vm = set(old_vm_cluster_map.keys()) - set(vm_cluster_map.keys())
            if newly_deleted_vm:
                GLOBAL_MGR_LOGGER.info(f"VMs deleted for user {email}: {', '.join(newly_deleted_vm)}")
                user_obj = self.GLOBAL_USER_CACHE[email]
                for each_deleted_vm_uuid in newly_deleted_vm:
                    try:
                        user_obj.process_deleted_vm(each_deleted_vm_uuid)
                    except Exception as ex:
                        GLOBAL_MGR_LOGGER.exception(ex)
                        raise ex
                    # Process the deleted VM for its cluster
                    cluster_name = old_vm_cluster_map.get(each_deleted_vm_uuid, None)
                    if cluster_name:
                        parent_cluster_obj = self.GLOBAL_CLUSTER_CACHE.get(cluster_name)
                        try:
                            parent_cluster_obj.process_deleted_vm(each_deleted_vm_uuid)
                        except Exception as ex:
                            GLOBAL_MGR_LOGGER.exception(ex)
                            raise ex
                    else:
                        GLOBAL_MGR_LOGGER.debug(f"VM with UUID {each_deleted_vm_uuid} is not yet tracked by any cluster.")
        self.old_user_vm_map = deepcopy(self.current_user_vm_map)

    # Functions relating to serving the REST API
    def list_clusters(self) -> typing.List:
        cluster_name_list = list()
        for cname in self.GLOBAL_CLUSTER_CACHE.keys():
            cluster_name_list.append(cname)
        return cluster_name_list

    def list_users(self, skip_util=True) -> typing.List:
        user_list = list()
        for _, user_obj in self.GLOBAL_USER_CACHE.items():
            user_info = user_obj.to_json()
            if skip_util:
                del user_info[UserKeys.GLOBAL_UTILIZED]
                del user_info[UserKeys.VM_UTILIZED]
                del user_info[UserKeys.CLUSTER_UTILIZED]
                user_list.append(user_info)
            else:
                user_list.append(user_info)
        return user_list

    def cluster_info(self, cluster_name, arguments) -> typing.Dict:
        cluster_info_dict = dict()
        if eval(arguments.get("resources", 'False')):
            return self.get_vms_resources_sorted(
                        cluster_name=cluster_name,
                        count=int(arguments.get('count', -1)),
                        sort_by_cores=bool(arguments.get('sorted_core', False)),
                        sort_by_mem=bool(arguments.get('sorted_mem', False))
                    )
        else:
            vm_list = None
            with self.GLOBAL_CLUSTER_CACHE_LOCK:
                cluster_obj = self.GLOBAL_CLUSTER_CACHE.get(cluster_name, None)
                if cluster_obj:
                    vm_list = cluster_obj.get_vm_list()
            if vm_list:
                cluster_info_dict['running_vm'] = vm_list[0]
                cluster_info_dict['stopped_vm'] = vm_list[1]
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
        done = False
        with self.GLOBAL_USER_CACHE_LOCK:
            if user_info[UserKeys.EMAIL] in self.GLOBAL_CLUSTER_CACHE:
                if not is_patch:
                    # Should have been a PATCH request, not POST
                    return
                user_obj = self.GLOBAL_CLUSTER_CACHE[user_info[UserKeys.EMAIL]]
            else:
                if is_patch:
                    # Should have been a POST request, not PATCH
                    return
                user_obj = User(user_info)
                self.GLOBAL_USER_CACHE[user_info[UserKeys.EMAIL]] = user_obj

        # Now process the prefixes
        # add_prefixes will be present for both POST and PATCH
        new_prefix_to_add = user_info.get(UserKeys.ADD_NEW_PREF, [])
        for pref in user_info.get(UserKeys.ADD_NEW_PREF, []):
            print(pref)
            updated = self.update_prefix(user_email=user_info[UserKeys.EMAIL],
                                         prefix=pref, op='add')
            if updated:
                GLOBAL_MGR_LOGGER.info(f"{method} Added prefix '{pref}' for"
                                       f"the user {user_info[UserKeys.EMAIL]}")
            else:
                failed_add_pref_list.append(pref)
                GLOBAL_MGR_LOGGER.error(f"{method} Could not add prefix '{pref}'"
                                        f"for the user {user_info[UserKeys.EMAIL]}")
        # Will be present only for the PATCH requests
        for pref in user_info.get(UserKeys.REMOVE_NEW_PREF, []):
            updated = self.update_prefix(user_email=user_info[UserKeys.EMAIL],
                                         prefix=pref, op='remove')
            if updated:
                GLOBAL_MGR_LOGGER.info(f"{method} Removed prefix '{pref}' for"
                                       f" the user {user_info[UserKeys.EMAIL]}")
            else:
                failed_remove_pref_list.append(pref)
                GLOBAL_MGR_LOGGER.error(f"{method} Could not remove prefix "
                                        f"'{pref}' for the user "
                                        f"{user_info[UserKeys.EMAIL]}")
        # If a PATCH request, now check if the quota/name needs to be patched
        if is_patch:
            if UserKeys.NAME in user_info:
                user_obj.update_name(user_info[UserKeys.NAME])
            if UserKeys.QUOTA in user_info:
                user_obj.update_resource_quota(user_info[UserKeys.QUOTA])
        done = True
        # for pref in failed_pref_list:
        #     user_obj.update_prefix(prefix=pref, op='remove')
        return failed_add_pref_list, failed_remove_pref_list, done

    def process_cluster_vm_power_change(self, cluster_name, vm_info):
        """Common function to change the power change of a VM for a cluster.
            Currently only powers off a VM.
            Args:
                cluster_name (str): Name of the cluster on which the VM resides
                vm_info (dict): Contains 'uuid' or 'name' for a VM
        """
        # TODO Pause the Cache refresh in this time
        cluster_obj = None
        from http import HTTPStatus
        with self.GLOBAL_CLUSTER_CACHE_LOCK:
            cluster_obj = self.GLOBAL_CLUSTER_CACHE.get(cluster_name, None)
            if not cluster_obj:
                err = {"message: "f"Cluster with name {cluster_name} not found"
                       f" in the cache"}
                GLOBAL_MGR_LOGGER.error(err)
                return HTTPStatus.NOT_FOUND, err
        status, msg = cluster_obj.power_down_vm(vm_name=vm_info.get('name', None),
                                                uuid=vm_info.get('uuid', None))
        from http import HTTPStatus
        if status == HTTPStatus.OK:
            # We get this if the POWER OFF was successful -- process it
            # for the concerned user as well.
            if 'vm_config' in msg:
                with self.GLOBAL_USER_CACHE_LOCK:
                    if msg['vm_config']['owner_email'] is None:
                        GLOBAL_MGR_LOGGER.info(f"Could not back-propagate the "
                                               f"VM {vm_info['name']}, UUID "
                                               f"{vm_info['uuid']} to Owner as "
                                               "the owner of the VM is unknown!")
                    user_obj = self.GLOBAL_USER_CACHE.get(
                        msg['vm_config']['owner_email'], None)
                    if user_obj:
                        GLOBAL_MGR_LOGGER.info("Back-propagating the Resources"
                                               f"change for VM "
                                               f"{msg['vm_config']["name"]}"
                                               f" UUID {msg['vm_config']['uuid']} "
                                               f"to its owner {user_obj.email}")
                        user_obj.update_vm_resources(msg['vm_config'])
                    else:
                        GLOBAL_MGR_LOGGER.error(f"User with email {user_obj.email}"
                                                " not found in the CACHE!!")
        return status, msg

    def get_continued_offense(self, timediff=ONE_DAY_IN_SECS, from_ts=None):
        """Retuns the consolidated list of the quota and VM offenses, which
            has not changed in last 'timediff' seconds. Default: 1 Day
            Args:
                timediff (int): Number of seconds to check for the difference in
                from_ts (int | None): Latest timestamp to check for the diff
            Returns:
                tuple(dict, dict)
        """
        if not from_ts:
            from_ts = int(time.time())

        # Get the closest time stamp to from_ts in the cache
        closest_ts_current = min(self.timed_offenses.keys(), key=lambda ts: abs(from_ts - ts))

        # Get the closest time stamp to (from_ts - timediff)
        to_ts = from_ts - timediff
        closest_ts_old = min(self.timed_offenses.keys(), key=lambda ts: abs(to_ts - ts))

        if closest_ts_current == closest_ts_old:
            GLOBAL_MGR_LOGGER.error("The latest time and the earliest timestamp before the difference is same")
            return {}
        old_offenses = self.timed_offenses[to_ts]
        new_offenses = self.timed_offenses[from_ts]
    
        # Get the users who are still over-utilizing their resources
        old_over_util_users = old_offenses.get('users_over_util', {})
        new_over_util_users = new_offenses.get('users_over_util', {})
        users_still_over_util = set(old_over_util_users.keys()).intersection(set(new_over_util_users))
        # TODO Can return the list of the users who are not over-utilizing any-more as well.
        current_user_offenses = {}
        for email in users_still_over_util:
            current_user_offenses[email] = {}
            current_user_offenses[email]['resources'] = new_over_util_users[email]
            # Get the list of the VMs owned by this user
            current_user_offenses[email]['vms'] = self.GLOBAL_USER_CACHE[email].to_json()[UserKeys.VM_UTILIZED]

        # Get the list of VMs which still do not conform to the prefix requirement
        current_vm_without_pref = {}
        old_vm_without_pref = old_offenses.get('vm_without_prefix', {})
        new_vm_without_pref = new_offenses.get('vm_without_prefix', {})
        common_cluster_names = set(old_vm_without_pref.keys()).intersection(set(new_vm_without_pref.keys()))

        for cname in common_cluster_names:
            common_uuids = {}
            new_vms = new_vm_without_pref[cname]
            old_vms = old_vm_without_pref[cname]

            # Convert to sets to get the common UUIDs
            new_vm_uuid_set = {uuid for uuid, _ in new_vms}
            old_vm_uuid_set = {uuid for uuid, _ in old_vms}
            # Get common UUIDs
            common_uuid_s = old_vm_uuid_set.intersection(new_vm_uuid_set)
            # Get the old and new names for the VMs that are still offending
            for uuid in common_uuid_s:
                old_vm_name = next(name for uid, name in old_vms if uid == uuid)
                new_vm_name = next(name for uid, name in new_vms if uid == uuid)
                common_uuids[uuid] = {'old_vm_name': old_vm_name, 'new_vm_name': new_vm_name}

            current_vm_without_pref[cname] = common_uuids

        return current_user_offenses, current_vm_without_pref

    def get_vms_for_user(self, email, cluster=None) -> typing.List:
        """Returns list of VMs for a particular user
            Args:
                email (str): Email of the user
                cluster (str): Filter the list of VMs as per the cluster on \
                    which they run
            Returns:
                list of the VMs for the user
        """
        with self.GLOBAL_USER_CACHE_LOCK:
            user_obj = self.GLOBAL_USER_CACHE[email]
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
                ]
            return [{
                    "uuid": uuid,
                    "name": res_info['name'],
                    "cores": res_info['cores'],
                    "memory": res_info['memory'],
                    "parent_cluster": res_info['parent_cluster'],
                    "power_state": res_info['power_state']
                } for uuid, res_info in uuid_info_map.items()]
