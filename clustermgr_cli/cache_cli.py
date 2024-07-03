"""
Defines all the CLIs defined for the cache

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""

import click
import json
import requests

from colorama import Fore, Style, init
from http import HTTPStatus
from prettytable import PrettyTable
from urllib import parse

from .constants import CACHE_EP, LOCAL_ENDPOINT, CLI_HEADERS
from tools.helper import convert_mb_to_gb

init(autoreset=True)

@click.group()
def cache():
    """Display combined information about the USERS, CLUSTERS and VMs -- and the quotas
    """
    pass


@cache.command(name="go")
@click.option("--resources", '-r', is_flag=True, help="Show resources associated with the VMs")
@click.option("--cluster", "-c", help='Filter by Cluster')
@click.option("--show-orphan-vms", "-s", is_flag=True, help="Show the VMs who don't have a Owner yet.")
def get_deviations(resources, cluster, show_orphan_vms):
    """List of Users over-utilizing quota, VMs whose owners are unknown, show resources consumed for these VMs
    """

    deviation_url = LOCAL_ENDPOINT + CACHE_EP + "/deviations"
    args = {
        'resources': resources,
        'cluster': cluster if cluster else None
    }
    res = requests.get(deviation_url + '?' + parse.urlencode(args), headers=CLI_HEADERS)
    
    response_json = res.json()
    
    # TODO Show VMs that are consuming most resources for this user
    click.echo("\n\nList of Users over-utilizing their quotas:\nShown values are the count of resources being over-utilized.")
    pt = PrettyTable(["Sr. No.", "User Email", "Cluster Name/Global", "Quota", "Resources Utilization"])
    pt.align["Sr. No."] = 'r'
    pt.align["Quota"] = "l" # Left align as we are dumping a JSON
    pt.align["Resources Utilization"] = "l" # Left align as we are dumping a JSON
    sr_no = 1

    def _parse_over_util(cluster_res_info, cname):
        mem_over_util = False
        core_over_util = False
        mem_quota = '-'
        mem_util = '-'
        core_quota = '-'
        core_util = '-'

        mem_quota = cluster_res_info['quotas'].get(cname, {}).get('memory', '-')
        if mem_quota != '-':
            mem_quota = convert_mb_to_gb(mem_quota)
        mem_util = cluster_res_info['deviations'].get(cname, {}).get('memory', '-')
        if mem_util != '-':
            mem_util = convert_mb_to_gb(mem_util)
        if mem_util != '-' and mem_quota != '-' and mem_util > mem_quota:
            mem_over_util = True

        core_quota = cluster_res_info['quotas'].get(cname, {}).get('cores', '-')
        core_util = cluster_res_info['deviations'].get(cname, {}).get('cores', '-')
        if core_util != '-' and core_quota != '-' and core_util > core_quota:
            core_over_util = True
        return mem_quota, mem_util, mem_over_util, core_quota, core_util, core_over_util

    for email, cluster_res_info in response_json.get("users", {}).items():
        email_added = False
        if "global" in cluster_res_info['deviations']:
            email_added = True
            mem_quota, mem_util, mem_over_util, core_quota, core_util, core_over_util = _parse_over_util(cluster_res_info, 'global')
            global_quota_str = "{} cores, {} GB".format(core_quota, mem_quota)
            global_util_str = "{}{} cores{}, {}{} GB{}".format(Fore.RED if core_over_util else Fore.WHITE, core_util, Style.RESET_ALL, Fore.RED if mem_over_util else Fore.WHITE, mem_util, Style.RESET_ALL)
            row = [sr_no, email, "Global", global_quota_str, global_util_str]
            pt.add_row(row)
        for cname, resources_info in cluster_res_info['deviations'].items():
            sub_str = 1
            if cname != 'global':
                row = []
                mem_quota, mem_util, mem_over_util, core_quota, core_util, core_over_util = _parse_over_util(cluster_res_info, cname)
                
                cluster_quota_str = "{} cores, {} GB".format(core_quota, mem_quota)
                cluster_util_str = "{}{} cores, {} GB{}".format(Fore.RED if mem_over_util or core_over_util else Fore.WHITE, core_util, mem_util, Style.RESET_ALL)
                if not email_added:
                    row = [sr_no, email, cname, cluster_quota_str, cluster_util_str]
                    email_added = True
                else:
                    row = [str(sr_no) + '.' + str(sub_str), "", cname, cluster_quota_str, cluster_util_str]
                pt.add_row(row)
        sr_no += 1
    click.echo(pt)

    vm_resources = response_json.get("resources", {})
    
    if show_orphan_vms:
        click.echo("\n\n\nList of VMs whose owners could not be established")
        for cname, vm_list in response_json.get("vms", {}).items():
            sr_no = 1
            click.echo(f"\nCluster: {cname}\n\tTotal count of VMs whose owner could not be established: {len(vm_list)}")
            columns = (['Sr. No.', 'VM Name', 'UUID'])
            if resources:
                columns.extend(["Cores", "Memory (in GB)"])
            ptx = PrettyTable(columns)
            ptx.align['VM Name'] = 'l' # Left align the VM Names
            for vtuple in vm_list:
                row = [sr_no, vtuple[1], vtuple[0]]
                if resources:
                    vm_list = vm_resources.get(cname, [])
                    for each_vm in vm_list:
                        # print(each_vm['uuid'], vtuple[0])
                        if each_vm['uuid'] == vtuple[0]:
                            row.extend([each_vm['total_resources_used']['total_cores_used'],
                                        convert_mb_to_gb(each_vm['total_resources_used']['total_mem_used_mb'])])
                            break
                    if len(row) == 3:
                        # These are powered OFF VMs -- we do not track their consumption
                        row.extend(["-", "-"])
                ptx.add_row(row)
                sr_no += 1
            click.echo(ptx)

@cache.command(name="refresh")
def force_refresh_cache():
    """Force refreshing of cache
    """
    refresh_url = LOCAL_ENDPOINT + CACHE_EP + "/refresh"
    res = None
    try:
        res = requests.put(refresh_url, timeout=5, headers=CLI_HEADERS)
    except requests.exceptions.Timeout:
        click.echo("Timed out. Please check the logs.")
    if res.status_code == HTTPStatus.OK:
        click.echo(res.json()['message'])


@cache.command(name="update-retention")
@click.argument("retain_deviation", type=int)
def update_retention_deviations(retain_deviation):
    """Update the number of timestamps stored in the in-mem TSDB
    """
    deviation_url = LOCAL_ENDPOINT + CACHE_EP + "/deviations/retain"
    args = {'retain_deviation': retain_deviation}
    res = requests.put(deviation_url, json=json.dumps(args), headers=CLI_HEADERS)
    # click.echo(res.json()['message'])
    click.echo(res)

