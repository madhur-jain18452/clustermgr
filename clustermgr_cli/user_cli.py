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
from urllib import parse

from .constants import USER_EP, LOCAL_ENDPOINT, CLI_HEADERS, CLUSTER_EP
from tools.helper import convert_mb_to_gb, BINARY_CONVERSION_FACTOR

BASE_USERS_EP = LOCAL_ENDPOINT + USER_EP

def _check_cluster_overutil_confirm_with_owner(cluster, new_core, new_mem):
    url = LOCAL_ENDPOINT + CLUSTER_EP + f'/{cluster}/utilization?cores={new_core}&memory={new_mem}'
    res = requests.get(url, headers=CLI_HEADERS)
    if res.status_code == HTTPStatus.OK:
        response = res.json()
        if response['cores_over_sub'] or response['mem_over_sub']:
            click.echo(f"Cluster '{cluster}' will be over-subscribed after allowing this quota")
            if not click.confirm("Do you still want to continue?"):
                click.echo(f"Skipping the cluster {cluster} quota addition")
                return False
    return True

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
    res = requests.get(LOCAL_ENDPOINT + USER_EP, headers=CLI_HEADERS)
    columns = ['Sr. No.', 'Name', 'Email']
    if prefix:
        columns.append("Prefixes")
    if quota:
        columns.extend(["Global cores quota", "Global memory quota", "Per Cluster Enforced Quota"])
    pt = prettytable.PrettyTable(columns)
    if quota:
        pt.align["Per Cluster Enforced Quota"] = 'l'
    sr_no = 1
    for each_data in res.json():
        data = [sr_no, each_data['name'], each_data['email']]
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
        sr_no += 1
        pt.add_row(data)
    click.echo(pt)

@user.command()
@click.argument('name')
@click.argument('email')
@click.argument('prefixes', type=str)
@click.option('--total', type=(int, float), required=True, multiple=False, help="Total quota: cores=<number_of_cores> memory=<memory_in_gb>")
@click.option('--cluster', type=(str, int, float), multiple=True, help="Per-cluster quota: cores=<number_of_cores> memory=<memory_in_gb>")
@click.option('--flush', is_flag=True, help="Flush the user list to a file after successful addition. Default is all_users.json")
@click.option('--file', type=str, default='all_users.json', help="Name of the file to flush the users list.")
@click.option('--yes', '-y', hidden=True, is_flag=True)
def add(name, email, prefixes, total, cluster, flush, file, yes):
    """Add a new user to the system"""
    list_pref = prefixes.split(',')
    user_url = BASE_USERS_EP + "/" + email
    user_info = {
        'name': name,
        'prefix': [x.strip() for x in list_pref],
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
            # Check if there will be over-subscription. If --yes passed, skip
            params = {}
            params['cores'] = core
            params['memory'] = mem * 1024
            if not yes:
                if not _check_cluster_overutil_confirm_with_owner(cname, core, mem):
                    return
            user_info['quota'].append({cname: {'cores': core, 'memory': mem}})
    res = requests.post(user_url, json=json.dumps(user_info), headers=CLI_HEADERS)
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
        list_rem = remove_prefixes.split(',')
        user_info['remove_prefixes'] = [x.strip() for x in list_rem]
    if add_prefixes:
        list_add = add_prefixes.split(',')
        user_info['add_prefixes'] = [x.strip() for x in list_add]
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
                    cluster_dict['memory'] = mem * BINARY_CONVERSION_FACTOR
                user_info['quota'][cname] = cluster_dict
    res = requests.patch(user_url, json=json.dumps(user_info), headers=CLI_HEADERS)
    if res.status_code in [HTTPStatus.ACCEPTED, HTTPStatus.OK]:
        click.echo(f"User '{email}' successfully updated in the cache")

@user.command()
@click.argument('email')
@click.option("--resources", "-r", is_flag=True, help="Show the resources used for each VM")
@click.option("--cluster", "-c", help="Filter the list as per cluster")
def list_vms(email, resources, cluster):
    """List VMs for a particular user
    """
    from caching.cluster import PowerState
    from tools.helper import convert_mb_to_gb
    user_url = BASE_USERS_EP + "/" + email + "/vms"
    if cluster:
        user_url += f"?cluster={cluster}"
    res = requests.get(user_url, headers=CLI_HEADERS)
    if res.status_code == HTTPStatus.NOT_FOUND:
        click.echo(f"User with email {email} "
                   f"{f'or cluster with name {cluster}' if cluster else ''}"
                    "not found in the cache. Please recheck.")
        return

    cores_consumed = 0
    memory_consumed = 0

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
        if vm_info['power_state'] == PowerState.ON:
            cores_consumed += vm_info['cores']
            memory_consumed += convert_mb_to_gb(vm_info['memory'])
        pt.add_row(data)
        sr_no += 1
    click.echo(f"List of VMs for the user {email} {'on ' + cluster if cluster else ''}: ")
    click.echo(pt)
    click.echo(f"\nTotal cores consumed  : {cores_consumed}\nTotal Memory consumed : {memory_consumed} GB")


@user.command('remove')
@click.argument('email')
def remove_user_cli(email):
    """Remove a user from the system"""
    user_url = BASE_USERS_EP + "/" + email
    res = requests.delete(user_url, headers=CLI_HEADERS)
    if res.status_code in [HTTPStatus.ACCEPTED, HTTPStatus.OK]:
        click.echo(f"User '{email}' successfully removed from the cache")
