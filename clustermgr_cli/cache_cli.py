"""
Defines all the CLIs defined for the cache

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""

import click
import json
import requests

from prettytable import PrettyTable
from http import HTTPStatus
from urllib import parse

from .constants import CACHE_EP, LOCAL_ENDPOINT
from tools.helper import convert_mb_to_gb


@click.group()
def cache():
    """Display combined information about the USERS, CLUSTERS and VMs -- and the quotas
    """
    pass


@cache.command(name="go")
@click.option("--resources", '-r', is_flag=True, help="Show resources associated with the VMs")
@click.option("--cluster", "-c", help='Filter by Cluster')
@click.option("--show-orphan-vms", "-s", is_flag=True, help="Show the VMs who don't have a Owner yet.")
def get_offenses(resources, cluster, show_orphan_vms):
    """List of Users over-utilizing quota, VMs whose owners are unknown, show resources consumed for these VMs
    """

    offense_url = LOCAL_ENDPOINT + CACHE_EP + "/offenses"
    args = {
        'resources': resources,
        'cluster': cluster if cluster else None
    }
    res = requests.get(offense_url + '?' + parse.urlencode(args))
    
    response_json = res.json()
    
    # TODO Show VMs that are consuming most resources for this user
    click.echo("\n\nList of Users over-utilizing their quotas:\nShown values are the count of resources being over-utilized.")
    pt = PrettyTable(["Sr. No.", "User Email", "Cluster Name", "Global", "Resources Over-utilized by"])
    pt.align["Sr. No."] = 'r'
    pt.align["Resources Over-utilized by"] = "l" # Left align as we are dumping a JSON
    sr_no = 1
    for email, cluster_res_info in response_json.get("users", {}).items():
        email_added = False
        if "global" in cluster_res_info:
            email_added = True
            row = [sr_no, email, "-", "Yes", json.dumps(cluster_res_info['global'], indent=2)]
            pt.add_row(row)
        for cname, resources_info in cluster_res_info.items():
            sub_str = 1
            if cname != 'global':
                row = []
                if not email_added:
                    row = [sr_no, email, cname, "", json.dumps(resources_info, indent=2)]
                    email_added = True
                else:
                    row = [str(sr_no) + '.' + str(sub_str), "", cname, "", json.dumps(resources_info, indent=2)]
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
                columns.extend(["Cores Allocated", "Memory Allocated (in GB)"])
            ptx = PrettyTable(columns)
            ptx.align['VM Name'] = 'l' # Left align the VM Names
            for vtuple in vm_list:
                row = [sr_no, vtuple[1], vtuple[0]]
                if resources:
                    vm_list = vm_resources.get(cname, [])
                    for each_vm in vm_list:
                        if each_vm == vtuple[1]:
                            row.extend([each_vm['total_resources_used']['total_cores_used'],
                                        convert_mb_to_gb(each_vm['total_resources_used']['total_mem_used_mb'])])
                ptx.add_row(row)
                sr_no += 1
            print(pt)

@cache.command(name="update-ret")
@click.option("--retain-offense", '-r', is_flag=True, help="Update the count of timestamps that are retained in the system")
def get_offenses(retain_offense):
    offense_url = LOCAL_ENDPOINT + CACHE_EP + "/offenses/refresh_rate"
    args = {}
    if retain_offense:
        args['retain_offense'] = retain_offense
    res = requests.patch(offense_url, json=json.dumps(args))
    print(res.json()['message'])

