"""
Defines the constants used in the codebase

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""

class UserKeys(object):
    """Keys used in the User class"""
    # Standard Keys to be used
    NAME = "name"
    EMAIL = "email"
    PREFIX = "prefix"

    # Related to updates
    NEW_NAME = "new_name"
    ADD_NEW_PREF = "add_prefixes"
    REMOVE_NEW_PREF = "remove_prefixes"

    # Resource-Specific Keys used in User class
    QUOTA = "quota"
    GLOBAL_QUOTA = "global_resources_quota"
    CLUSTER_QUOTA = "cluster_quota"
    GLOBAL_UTILIZED = "global_utilized_resources"
    CLUSTER_UTILIZED = "cluster_utilized_resources"
    VM_UTILIZED = "vm_utilized_resources"


class NuVMKeys(object):
    """Keys used in the NuVM class"""
    PARENT_CLUSTER = "cluster"
    NICS = "vm_nics"
    IP_ADDR = "ip_address"
    NAME = "name"
    UUID = "uuid"
    MEMORY = "memory_mb" #  This is received from PRISM
    COUNT_VCPU = "num_vcpus"
    CORES_PER_VCPU = "num_cores_per_vcpu"
    POWER_STATE = "power_state"
    OWNER = "owner"
    OWNER_EMAIL = "owner_email"
    TOTAL_RESOURCES_ALLOCATED = "total_resources_used"
    CORES_USED = "total_cores_used"
    MEMORY_USED = "total_mem_used_mb"


class Resources(object):
    """Related to the resources"""
    MEMORY = "memory"
    CORES = "cores"

class ClusterKeys(object):
    CORES = "cores"
    MEMORY = "memory"

class TaskStatus (object):
    QUEUED = "Queued"
    SCHEDULED = "Scheduled"
    RUNNING = "Running"
    FAILED = "Failed"
    ABORTED = "Aborted"
    SUSPENDED = "Suspended"
    SUCCEEDED = "Succeeded"


ONE_DAY_IN_SECS = 24 * 60 * 60

