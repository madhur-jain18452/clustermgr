"""File containing all the constants used to contact the SERVERS/VMs/
    Generic functions

This file is structured in following organization:
    REQUEST constants
    PRISM constants
    ERA/NDB constants
    Generic functions

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""

import re

from base64 import b64encode
from urllib import parse
from enum import Enum

# Constants for REQUESTS
HEADER = 'Accept: application/json'

HTTP = "http://"
HTTPS = "https://"


class CacheState(Enum):
    PENDING = 0
    BUILDING = 1
    REBUILDING = 2
    READY = 3

    @staticmethod
    def to_str(value) -> str:
        match value:
            case CacheState.PENDING:
                return "PENDING"
            case CacheState.BUILDING:
                return "BUILDING"
            case CacheState.REBUILDING:
                return "REBUILDING"
            case CacheState.READY:
                return "READY"
        return "Undefined"


# Constants to interact with PRISM Element
PRISM_PORT = 9440
PRISM_SERVICES_ENDPOINT = "/PrismGateway/services/"
PRISM_REST_SERVICE_EP = "rest/"
PRISM_REST_VERSION = "v2.0/"
VM_ENDPOINT = "vms/"
CLUSTER_EP = "cluster/"
HOSTS_EP = "hosts/"

PRISM_REST_FINAL_EP = PRISM_SERVICES_ENDPOINT + PRISM_REST_SERVICE_EP + PRISM_REST_VERSION


class PowerState(object):
    ON = "on"
    OFF = "off"
    UNKNOWN = "unknown"


# Constants to interact with ERA/NDB Server
ERA_AUTH_USERS = ["admin"]
ERA_PASS = ["Nutanix/4u", "Nutanix.1", "Nutanix.123"]
ERA_USER = "era"
ERA_ENDPOINT = "/era/v0.9"
ERA_DBSERVER_EP = "/dbservers"
CLUSTER_EP = "/cluster"
ERA_SERVER_NAME_REGEX = r"^[a-zA-Z]+(?:[_\.][a-zA-Z]+)*_(era|ndb)(?:[a-zA-Z0-9_-]*)?$"
DND_SERVER_NAME_REGEX = r"do_not_delete|dont_delete|dnd|do_not_delete_era|dont_delete_era|dnd_era|do_not_delete_ndb|dont_delete_ndb|dnd_ndb"


class DBServerStatus(object):
    """This is common for both the Servers and Clones"""
    READY = "READY"
    UP = "UP"
    POWER_OFF = "POWERED_OFF"
    DELETED = "DELETED_FROM_CLUSTER"


class DBServerJSONKeys(object):
    """Higher level keys in config JSON for DBServer"""
    PROPERTIES = "properties"
    PROTECTION_DOMAIN_ID = "protectionDomainId"
    PROTECTION_DOMAIN = "protectionDomain"
    DATABASES = "databases"
    CLONES = "clones"


class DBServerPropertyKeys(object):
    """Lower level keys in 'properties' inside config JSON for DBServer"""
    VM_CORE_COUNT = "vm_core_count"
    VM_CPU_COUNT = "vm_cpu_count"
    VM_IP_LIST = "vm_ip_address_list"
    IS_ERA_CREATED = "isEraCreated"


class DBStatus(object):
    """This the DB status of Clone"""
    READY = "READY"


# Generic functions
def basic_auth_header(username, password):
    token = b64encode(f"{username}:{password}".encode('utf-8')).decode("ascii")
    return f'Basic {token}'


def generate_query_string(options):
    if not options:
        return ""
    query_string = parse.urlencode(options)
    return f"?{query_string}"


def check_vm_name_to_skip(name):
    # Define the regex pattern
    pattern = re.compile(r'era[._-]*agent|TEMPLATE', re.IGNORECASE)
    
    # Check if the name matches the pattern
    if pattern.match(name):
        return True
    return False


def is_dnd(name):
    # Define the regex pattern
    pattern = re.compile(DND_SERVER_NAME_REGEX, re.IGNORECASE)

    # Check if the name matches the pattern
    if pattern.match(name):
        return True
    return False


class DeviationKeys(object):
    DEVIATING_VM = "vms"
    RES_TRACKER = "resources"
    DEVIATING_USERS = "users"

class HealthStatus(object):
    """To calculate the health status of the cluster based on the health score
        First value is the lower bound (inclusive)
        second value is the upper bound (exclusive) (except CRITICAL and OVERSUBSCRIBED)
        third value is the status str
        fourth value is the color
    """
    HEALTHY = (0, 75, "HEALTHY", "green")
    DEGRADED = (75, 90, "DEGRADED", "yellow")
    CRITICAL = (90, 100, "CRITICAL", "red")
    OVERSUBSCRIBED = (100, float('inf'), "OVERSUBSCRIBED", "red")
    UNKNOWN = "UNKNOWN"
    NOT_APPLICABLE = "NOT_APPLICABLE"

    def get_health_status(score):
        if HealthStatus.HEALTHY[0] <= score < HealthStatus.HEALTHY[1]:
            return HealthStatus.HEALTHY
        elif HealthStatus.DEGRADED[0] <= score < HealthStatus.DEGRADED[1]:
            return HealthStatus.DEGRADED
        elif HealthStatus.CRITICAL[0] <= score <= HealthStatus.CRITICAL[1]:
            return HealthStatus.CRITICAL
        elif HealthStatus.OVERSUBSCRIBED[0] < score < HealthStatus.OVERSUBSCRIBED[1]:
            return HealthStatus.OVERSUBSCRIBED
        return HealthStatus.UNKNOWN

