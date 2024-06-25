"""
Defines all the CLIs defined for the Users

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""


import click
import json
import prettytable
import requests

from http import HTTPStatus

from .constants import USER_EP, LOCAL_ENDPOINT
from tools.helper import convert_mb_to_gb

BASE_USERS_EP = LOCAL_ENDPOINT + USER_EP

@click.group()
def user():
    """CLI to access information of a particular User
    """
    pass

@user.command()
@click.option('--prefix', '-p', is_flag=True, help="Show the prefixes registered for the user")
@click.option('--quota', '-q', is_flag=True, help="Show the quotas enforced for the user")
def list(prefix, quota):
    """Lists all the Users in the system"""
    # TODO Add option for showing quota
    res = requests.get(LOCAL_ENDPOINT + USER_EP)
    columns = ['Name', 'Email']
    if prefix:
        columns.append("Prefixes")
    if quota:
        columns.extend(["Global cores quota", "Global memory quota", "Per Cluster Enforced Quota"])
    pt = prettytable.PrettyTable(columns)
    if quota:
        pt.align["Per Cluster Enforced Quota"] = 'l'
    
    for each_data in res.json():
        data = [each_data['name'], each_data['email']]
        if prefix:
            data.append(each_data['prefix'])
        if quota:
            cluster_quota_str = ""
            if each_data.get('cluster_quota', {}):
                for cname, quota in each_data['cluster_quota'].items():
                    mem_val = quota.get('memory', '-')
                    if mem_val is not str:
                        mem_val = convert_mb_to_gb(mem_val)
                    cluster_quota_str += f"'{cname}': {quota.get('cores', '-')} cores, {mem_val} GB memory\n"
            else:
                cluster_quota_str = "No quota per cluster"
            data.extend([each_data['global_resources_quota']['cores'],
                         convert_mb_to_gb(each_data['global_resources_quota']['memory']),
                         cluster_quota_str])
        pt.add_row(data)
    click.echo(pt)

@user.command()
@click.argument('name')
@click.argument('email')
@click.argument('prefixes', type=str)
@click.option('--total', type=(int, float), required=True, multiple=False, help="Total quota: cores=<number_of_cores> memory=<memory_in_gb>")
@click.option('--cluster', type=(str, int, float), multiple=True, help="Per-cluster quota: cores=<number_of_cores> memory=<memory_in_gb>")
@click.option('--flush', is_flag=True, help="Flush the user list to a file after successful addition. Default is all_users.json")
@click.option('--file',  type=str, default='all_users.json', help="Name of the file to flush the users list.")
def add(name, email, prefixes, total, cluster, flush, file):
    """Add a new user to the system"""
    # TODO Add a flush option to store the information of the user permanently
    user_url = BASE_USERS_EP + "/" + email
    user_info = {
        'name': name,
        'prefix': prefixes,
        'quota': [
            {
                'global': {
                    'cores': total[0],
                    'memory': total[1]
                }
            }
        ],
        'flush': flush,
        'file': file
    }
    if cluster:
        for cname, core, mem in cluster:
            user_info['quota'].append({cname: {'cores': core, 'memory': mem}})
    res = requests.post(user_url, json=json.dumps(user_info))
    if res.status_code in [HTTPStatus.ACCEPTED, HTTPStatus.OK]:
        click.echo(f"User '{name}' successfully added to the cache")

@user.command()
@click.argument('email')
@click.option('--name', type=str, help="Update the name of the user")
@click.option('--remove-prefixes', type=str, default="", help="Comma-separated list of prefixes to be removed")
@click.option('--add-prefixes', type=str, default="", help="Comma-separated list of prefixes to be added")
@click.option('--total', type=(int, float), multiple=False, help="Total quota: cores=<number_of_cores> memory=<memory_in_gb>.\n\tNOTE: This will over-write current quota values.\n\tTo skip update for a particular type, provide -1.\n\tE.g. --total -1 5 to update only total memory quota to 5GB with same number of cores")
@click.option('--cluster', type=(str, int, float), multiple=True, help="Per-cluster quota: cores=<number_of_cores> memory=<memory_in_gb>\n\tNOTE: This will over-write current quota values. Will add the cluster quota if it does not exist.\n\tTo skip update for a particular type, provide -1.\n\tE.g. --cluster <cluster_name> -1 5 to update only total memory quota to 5GB with same number of cores")
def update(email, name, remove_prefixes, add_prefixes, total, cluster):
    """Update any information of a particular user
    """
    if not name and not remove_prefixes and not add_prefixes and not total and not cluster:
        click.echo("Please provide at least one category to update for the user.")
        return
    user_url = BASE_USERS_EP + "/" + email
    user_info = {}
    if name:
        user_info['new_name'] = name
    if remove_prefixes:
        user_info['remove_prefixes'] = remove_prefixes
    if add_prefixes:
        user_info['add_prefixes'] = add_prefixes
    if total or cluster:
        user_info['quota'] = {}
        if total:
            total_dict = {}
            if total[0] != -1:
                total_dict['cores'] = total[0]
            if total[1] != -1:
                total_dict['memory'] = total[1]
            user_info['quota']['global'].append(total_dict)
        if cluster:
            for cname, core, mem in cluster:
                cluster_dict = {}
                if core != -1:
                    cluster_dict['cores'] = core
                if mem != -1:
                    cluster_dict['memory'] = mem
                user_info['quota'][cname] = cluster_dict
    res = requests.patch(user_url, json=json.dumps(user_info))
    if res.status_code in [HTTPStatus.ACCEPTED, HTTPStatus.OK]:
        click.echo(f"User '{email}' successfully added to the cache")

@user.command()
@click.argument('email')
@click.option("--resources", "-r", is_flag=True, help="Show the resources used for each VM")
@click.option("--cluster", "-c", help="Filter the list as per cluster")
def list_vms(email, resources, cluster):
    """List VMs for a particular user
    """
    from caching.cluster import PowerState, convert_mb_to_gb
    user_url = BASE_USERS_EP + "/" + email + "/vms"
    if cluster:
        user_url += f"?cluster={cluster}"
    res = requests.get(user_url)
    if res.status_code == HTTPStatus.NOT_FOUND:
        click.echo(f"User with email {email} "
                   f"{f'or cluster with name {cluster}' if cluster else ''}"
                    "not found in the cache. Please recheck.")
        return
    res_json = res.json()
    columns = ["Sr. No.", "VM Name", "UUID", "Cluster", "Status"]
    if resources:
        columns.extend(["Cores consumed", "Memory consumed (in GB)"])
    pt = prettytable.PrettyTable(columns)
    sr_no = 1
    for vm_info in res_json:
        data = [sr_no, vm_info['name'], vm_info['uuid'], vm_info['parent_cluster'], "RUNNING" if vm_info['power_state'] == PowerState.ON else "STOPPED"]
        if resources:
            data.extend([vm_info['cores'], convert_mb_to_gb(vm_info['memory'])])
        pt.add_row(data)
        sr_no += 1
    click.echo(f"List of VMs for the user {email} {'on ' + cluster if cluster else ''}: ")
    click.echo(pt)

