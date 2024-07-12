"""Class NuVM
    Interface to work with the Nutanix VMs (NuVM) running on top of AHV/ESXi.
    Allows differentiating between NDB and non-NDB VMs

    Can be extended to work with any type of VMs

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""

import threading
import urllib3
import requests
import typing
import logging

from concurrent import futures
from copy import deepcopy
from http import HTTPStatus

from caching.server_constants import basic_auth_header, PowerState, HTTPS
from tools.helper import convert_mb_to_gb

urllib3.disable_warnings()
def setup_logger():
    logger = logging.getLogger(__name__)
    if not logger.hasHandlers():
        logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler(f"cmgr_NuVM.log", mode='a')
        formatter = logging.Formatter("%(name)s:%(lineno)d - %(asctime)s %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

NDB_SUBSTR = ["ndb", "era"]

DBSERVER_REQUEST_PARAMS = {
    "load-dbserver-cluster": True,
    "load-databases": False,
    "load-clones": True,
    "detailed": False,
    "lead-metrics": False,
    "time-zone": "UTC"
}


def subst_exists(name, str_list_to_check) -> typing.Tuple[bool, str]:
    """
    Args:
        name (str): Name of the VM
        str_list_to_check (list(str)): List of the strings to check in name

    Returns:
        Tuple(Bool, str): True and the substring if found, else \
            False and empty str
    """
    for subst in str_list_to_check:
        if subst in name.lower():
            return True, subst
    return False, ""


class NuVM:

    # TODO username and Password handling; maybe we can store it in the memory
    #  after verifying with a list of all available permutations
    # TODO Should we parse the config_json and extract info; or just store the
    #  whole thing? -- As of now, storing the whole raw JSON
    def __init__(self, config_json, parent_cluster_name):
        """Sets up the Nutanix VMs based on the config received from
        the PRISM element

        Args:
            config_json (dict): Dictionary containing info about the VM
            parent_cluster_name (str): Name of the cluster running the VM
        """
        setup_logger()
        self.logger = logging.getLogger(__name__)
        # Instance private attributes
        self._config = config_json
        # nuvm_logger.info(config_json)
        self._parent_cluster = parent_cluster_name
        self.nics = self._config.get("vm_nics", [])
        self._ip_address = self.nics[0].get("ip_address") \
            if self.nics and "ip_address" in self.nics[0] else None
        self._threadpool = futures.ThreadPoolExecutor(max_workers=5)

        # FUTURE Resource utilization tracking for the managed DBServers
        self.owner = None
        self.owner_email = None
        self.managed_resources = {}
        self.total_cpu_used_managed = 0
        self.total_mem_used_managed = 0

        # Generic Public attributes
        self.is_reachable = False
        self.power_state = self._config.get("power_state", PowerState.OFF)
        self.name = self._config["name"]
        self.uuid = self._config["uuid"]
        #  Powered OFF VMs do not have a host UUID
        self.host_uuid = self._config.get("host_uuid", None)

        # Resources used by the VM itself
        self.cores_used_per_vcpu = self._config["num_cores_per_vcpu"]
        self.num_vcpu = self._config["num_vcpus"]
        # Storing the memory in MBs
        self.memory = self._config["memory_mb"]

        # FUTURE
        self.vg_list = []
        self.disk_list = []

        # NDB Specific values
        self.is_ndb_cvm = False
        self.db_vm_cache_lock = threading.Lock()
        self.managed_db_vm_list = set()

        if self._ip_address:
            # TODO Out of all assigned IPs, check which IPs are actually usable
            pass
        self.logger.debug(f"Added VM {self.name[:30]:30},"
                                 f" parent cluster {self._parent_cluster},"
                                 f" UUID {self.uuid} to the cache")

    def refresh_https_conn(self):
        pass

    def summary(self, print_summary=False) -> typing.Optional[typing.Dict]:
        """Print/Return the summary of the VM
        Args:
            print_summary (bool): Set to True to print to STDOUT
        Returns:
            dict object of the VM
        """
        if print_summary:
            print(f"\tVM {self.name[:30]:<30} : "
                  f"{self.owner_email[:30] if self.owner_email else '':<30} : "
                  f"{self.num_vcpu * self.cores_used_per_vcpu} CPUs and "
                  f"{convert_mb_to_gb(self.memory)} GB Memory - PowerState: "
                  f"{self.power_state}")
        else:
            return self.to_json()

    def set_owner(self, owner_name, owner_email):
        if self.owner_email == owner_email:
            return
        self.owner = owner_name
        self.owner_email = owner_email
        self.logger.info(f"Updated owner for VM {self.name[:30]:30}, "
                                f"parent cluster {self._parent_cluster}, UUID"
                                f" {self.uuid} -> {owner_email} | {owner_name}")

    def check_ndb_cvm(self) -> typing.Tuple[bool, bool]:
        """Function to check if the VM is an NDB Controller VM
            Works by pinging the "dbservers" endpoint of the ERA
            If returns a valid value, then it is an Era CVM, else None

        Returns:
            unreachable (bool): True if the VM is unreachable or refuses\
             to connect
            is_era (bool): True if the VM is an NDB CVM
        """
        # If VM is powered ON
        if self.power_state == PowerState.ON:
            # And has an IP address
            # TODO Get the data by contacting the PG store (Probably thats
            # best way to do it -- as it will not be dependent on the REST)
            if self._ip_address:
                try:
                    # Try contact the ERA endpoint for that VM
                    # TODO check with all possible combinations of username and pass
                    res = requests.get(HTTPS + self._ip_address + '/era/v0.9/dbservers',
                                       headers={'Authorization': f'Basic '
                                                                 f'{basic_auth_header(
                                                                     "admin",
                                                                     "Nutanix.1")}'},
                                       verify=False, timeout=2
                                       )
                    self.logger.debug("IP: {}, Status: {}".format(
                        self._ip_address, res.status_code))
                    # self.logger.debug(res.text)
                    # If the password is accepted -- or wrong, it is an ERA CVM
                    if res.status_code in [HTTPStatus.UNAUTHORIZED,
                                           HTTPStatus.OK,
                                           HTTPStatus.ACCEPTED]:
                        self.logger.info(f"VM '{self.name}': Replied"
                                                f"with status code "
                                                f"{res.status_code} -> "
                                                f"{res.reason}")
                        self.is_reachable = True
                        self.is_ndb_cvm = True

                        # If the connection was successful, add the DB Server VMs
                        # Since ERA server is not SPOT, we don't rely on it
                        if res.status_code in [HTTPStatus.OK, HTTPStatus.ACCEPTED]:
                            for each_dict in res.json():
                                self.managed_db_vm_list.add(each_dict["name"])
                            return self.is_reachable, self.is_ndb_cvm

                except (ConnectionRefusedError, ConnectionError,
                        ConnectionAbortedError,
                        urllib3.exceptions.NewConnectionError,
                        urllib3.exceptions.MaxRetryError,
                        requests.exceptions.ConnectionError) as conn_ex:
                    self.logger.exception(f"The VM '{self.name}' - IP "
                                                 f"{self._ip_address}: Connection err")
                    #                              f" error: {conn_ex}")
                    self.is_reachable = True
                    self.is_ndb_cvm = False
                    pass
                except Exception as ex:  # If some other exception occur
                    self.logger.exception(f"Exception occurred for the IP: "
                                                 f"{self._ip_address}: {ex}")
                    self.is_reachable = True
                    self.is_ndb_cvm = False
                    pass
            else:
                # Check if the name contains the ERA strings
                self.logger.warning(f"NuVM {self.name} - Valid IP "
                                           f"does not exist. Checking if "
                                           f"the name contains any of "
                                           f"{",".join(NDB_SUBSTR)}")
                ndb_subst_exist, subst = subst_exists(self.name, NDB_SUBSTR)
                if ndb_subst_exist:
                    self.logger.info(f"NuVM {self.name} - "
                                            f"contains {subst}. "
                                            f"Categorizing as unreachable "
                                            f"ERA Server.")
                    return True, True
                # If neither works, the status cannot be determined
                # and the status will be ambiguous
                return False, False
        else:
            self.is_reachable = False
            self.logger.warning(
                f"NuVM {self.name} - is powered off. Checking if the name "
                f"contains any of {",".join(NDB_SUBSTR)}")
            ndb_subst_exist, subst = subst_exists(self.name, NDB_SUBSTR)
            if ndb_subst_exist:
                self.logger.info(
                    f"NuVM {self.name} - contains {subst}. Categorizing"
                    f" as unreachable ERA Server.")
                self.is_ndb_cvm = True
            else:
                # The status cannot be determined and the
                # status will be ambiguous
                self.is_ndb_cvm = False
        return self.is_reachable, self.is_ndb_cvm

    def get_resources_used(self) -> dict:
        """Should be called only after successful completion of cache builkd.
        Returns:
            dict containing the resource utilization
        """
        return {
            "total_cores_used": self.num_vcpu * self.cores_used_per_vcpu,
            "total_mem_used_mb": self.memory
        }

    def to_json(self) -> dict:
        """"Utility function returning JSON representation of the NuVM object
            Returns:
                dict
        """
        json_obj = dict()

        json_obj["cluster"] = self._parent_cluster
        json_obj["uuid"] = self.uuid
        json_obj["name"] = self.name
        json_obj["power_state"] = self.power_state
        json_obj["is_ndb_cvm"] = self.is_ndb_cvm
        json_obj["owner"] = self.owner
        json_obj["owner_email"] = self.owner_email
        json_obj["nics"] = self.nics
        json_obj["total_resources_used"] = self.get_resources_used()

        return deepcopy(json_obj)

    def process_power_off(self):
        """Sets the status of the VM as powered OFF and updates self resources
         accordingly
         """
        self.power_state = PowerState.OFF
        self.memory = 0
        self.cores_used_per_vcpu = 0
        self.num_vcpu = 0
        self.host_uuid = None

    def process_power_on(self, memory, num_cpu, cores_per_cpu, host_uuid):
        """Sets the status of the VM as powered ON and updates resources
             in the object accordingly
        """
        self.power_state = PowerState.ON
        self.memory = memory
        self.cores_used_per_vcpu = cores_per_cpu
        self.num_vcpu = num_cpu
        self.host_uuid = host_uuid
