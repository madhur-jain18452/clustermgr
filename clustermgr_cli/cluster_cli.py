"""
Defines all the CLIs defined for a cluster

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

from caching.server_constants import PowerState, HealthStatus
from .constants import CLUSTER_EP, LOCAL_ENDPOINT, CLI_HEADERS
from tools.helper import convert_mb_to_gb


@click.group()
def cluster():
    """Information about the tracked clusters
    """
    pass


@cluster.command(name="list")
def list_clusters():
    """List of all the clusters in the cache
    """
    res = requests.get(LOCAL_ENDPOINT + CLUSTER_EP, headers=CLI_HEADERS)
    cluster_list = res.json()
    pt = PrettyTable(['Cluster Name', 'IP Address'])
    for info in cluster_list:
        pt.add_row([info['name'], info['ip']])
    click.echo(pt)

@cluster.command(name="info")
@click.argument('cluster_name')
def cluster_info(cluster_name):
    """Gets the memory consumed by the running VM for a cluster
    """
    res = requests.get(LOCAL_ENDPOINT + CLUSTER_EP + f'/{cluster_name}', headers=CLI_HEADERS)
    if res.status_code == HTTPStatus.NOT_FOUND:
        click.echo(f"Cluster with name {cluster_name} not found in the cache!")
        return
    cluster_info = res.json()
    if 'message' in cluster_info:
        click.secho(cluster_info['message'], fg='red')
        return
    mem_state = cluster_info['health_status'].get('memory_state', HealthStatus.UNKNOWN)
    mem_perc = cluster_info['health_status'].get('memory_perc', 0.0)
    if mem_state == HealthStatus.HEALTHY[2]:
        click.secho(f"Cluster '{cluster_name}' Memory state is HEALTHY.\n\tMemory Used by VMs: {mem_perc:.3f}%", fg=HealthStatus.HEALTHY[3])
    elif mem_state == HealthStatus.DEGRADED[2]:
        click.secho(f"Cluster '{cluster_name}' Memory state is DEGRADED.\n\tMemory Used by VMs: {mem_perc:.3f}%", fg=HealthStatus.CRITICAL[3])
    elif mem_state == HealthStatus.CRITICAL[2]:
        click.secho(f"Cluster '{cluster_name}' Memory state is CRITICAL.\n\tMemory Used by VMs: {mem_perc:.3f}%", fg=HealthStatus.UNHEALTHY[3])
    else:
        click.secho(f"Cluster '{cluster_name}' Memory state is UNKNOWN.", fg='red')

@cluster.command(name="add")
@click.argument('cluster_name')
@click.argument('ip')
@click.argument('user')
@click.argument('password')
def add_cluster(cluster_name, ip, user, password):
    """Add a new cluster to the cache
    """
    cluster_info = {
        'name': cluster_name,
        'ip': ip,
        'user': user,
        'password': password
    }
    res = requests.post(LOCAL_ENDPOINT + CLUSTER_EP, json=json.dumps(cluster_info), headers=CLI_HEADERS)
    if res.status_code == HTTPStatus.OK:
        click.echo(f"Cluster {cluster_name} added successfully")
    else:
        click.echo(res.json())

@cluster.command(name="remove")
@click.argument('cluster_name')
def add_cluster(cluster_name):
    """Remove a cluster from the cache
    """
    res = requests.delete(LOCAL_ENDPOINT + CLUSTER_EP + f'/{cluster_name}', headers=CLI_HEADERS)
    if res.status_code == HTTPStatus.OK:
        click.echo(f"Cluster {cluster_name} removed successfully")
    else:
        click.echo(res.json())


@cluster.command(name='list-vms')
@click.argument('cluster_name')
@click.option('--resources', '-r', is_flag=True, help="Show the resources "
                                                      "consumed by the 'running' VMs"
                                                      " (only) on this cluster")
@click.option('--no-owner', '--no', is_flag=True, help="List the VMs which do"
                                                       " not have an owner")
@click.option('--show-owner', '--so', is_flag=True, help="Show owners of the VMs")
@click.option('--sorted-mem', '--sm', is_flag=True, help="Sort the list as per"
                                                         " the memory consumed")
@click.option('--sorted-core', '--sc', is_flag=True, help="Sort the list as "
                                                          "per the cores utilized")
@click.option('--count', '-c', default=-1, type=click.INT, help="Return top X "
                                                                "VMs as per resource consumption")
@click.option('--powered-off', '--po', is_flag=True, help="List the VMs "
                                                          "which are powered off")
@click.option('--include-template-vms', is_flag=True, help="List the templated VMs")
@click.option('--show-nics', '--sn', is_flag=True, help="Show NICs attached to a VM")
def list_vms(cluster_name, resources, no_owner, sorted_mem, sorted_core,
             count, powered_off, show_owner, include_template_vms,
             show_nics):
    """List of all the VMs on a particular cluster
    """
    params = {
        'resources': resources,
        'no_owner': no_owner,
        'count': count,
        'include_template_vms': include_template_vms
    }
    if powered_off and (resources or sorted_core or sorted_mem):
        click.echo("Please provide either --powered-off or any of resources "
                   "or sort-by-resource. (Powered OFF VMs are not allocated any"
                   " resources.)")
        return
    if sorted_mem and sorted_core:
        click.echo("Please pass either --sorted-core or --sorted-mem")
        return
    if show_owner and no_owner:
        click.echo("Please pass either --show-owner or --no-owner")
        return

    if sorted_core:
        resources = True
        params['resources'] = True
        params['sorted_core'] = True
    if sorted_mem:
        resources = True
        params['resources'] = True
        params['sorted_mem'] = True
    
    columns = ['Sr. No.', 'VM Name']

    query_str = parse.urlencode(params)
    
    url = LOCAL_ENDPOINT + CLUSTER_EP + '/' + cluster_name + '/vms?' + query_str

    res = requests.get(url, headers=CLI_HEADERS)
    if res.status_code == HTTPStatus.NOT_FOUND:
        click.echo(f"Cluster with name {cluster_name} not found in the cache!")
        return
    response = res.json()
    if powered_off:
        print("List of Powered OFF VMs:")
        if show_owner:
            columns.extend(['Owner', 'Owner Email'])
        if show_nics:
            columns.extend(['NIC information'])
        pt_po = PrettyTable(columns)
        if show_nics:
            pt_po.align['NIC information'] = 'l'
        sr_no = 1
        for each_vm in response.get("stopped_vm", []):
            data = [sr_no, each_vm['name']]
            if no_owner:
                if each_vm['owner']:
                    continue
            elif show_owner:
                data.extend([each_vm['owner'] if each_vm['owner'] else '-',
                             each_vm['owner_email'] if each_vm['owner_email'] else '-'])
            if show_nics:
                data.extend([json.dumps(each_vm['nics'])])
            pt_po.add_row(data)
            sr_no += 1
        click.echo(pt_po)
        return
    else:
        # if Non-powered OFF VMs are requested, we will get a list of only
        # running VMs which has the cluster name as key -->
        # {'cluster_name': [{}, {}, ... ]}
        if resources:
            columns.extend(["Cores Allocated", "Memory Allocated (in GB)"])
            if show_owner:
                columns.extend(['Owner', 'Owner Email'])
            if show_nics:
                columns.extend(['NIC information'])
            pt = PrettyTable(columns)
            if show_nics:
                pt.align['NIC information'] = 'l'
            for _, vm_state in response.items():
                sr_no = 1
                for each_vm in vm_state["running_vm"]:
                    data = [sr_no, each_vm['name'],
                            each_vm['total_resources_used']['total_cores_used'],
                            convert_mb_to_gb(each_vm['total_resources_used']['total_mem_used_mb'])
                        ]
                    if no_owner:
                        if each_vm['owner']:
                            continue
                    elif show_owner:
                        data.extend([each_vm['owner'] if each_vm['owner'] else '-',
                                     each_vm['owner_email'] if each_vm['owner_email'] else '-'])
                    if show_nics:
                        data.extend([json.dumps(each_vm['nics'])])
                    pt.add_row(data)
                    sr_no += 1
                if include_template_vms:
                    # For template VMs, we are not storing the resources info
                    for each_vm in vm_state.get("template_vm", []):
                        if each_vm['power_state'] == PowerState.ON:
                            data = [sr_no, each_vm['name'] + ' *',
                                    str(each_vm['num_cores_per_vcpu'] * each_vm['num_vcpus'])+' *',
                                    str(convert_mb_to_gb(each_vm['memory_mb'])+' *')
                                ]
                            if no_owner:
                                continue
                            elif show_owner:
                                data.extend(['-', '-'])
                            if show_nics:
                                data.extend(['-'])
                            pt.add_row(data)
                            sr_no += 1
            click.echo(f"Cluster {cluster_name} - Running VM list with resources : ")
            click.echo(pt)
            return
        # If resources are not requested, we get a Dict with list of running_vm
        # and stopped_vms as keys:
        # {"running_vm": [{}, {}, ...], "stopped_vm": [{}, {}, ...]}
        # We do not show the resources in this
        else:
            sr_no = 1
            columns.extend(["State"])
            if show_owner:
                columns.extend(['Owner', 'Owner Email'])
            if show_nics:
                columns.extend(['NIC Information'])
            pt = PrettyTable(columns)
            if show_nics:
                pt.align['NIC Information'] = 'l'
            for each_vm in response.get('running_vm', []):
                data = [sr_no, each_vm['name'], "RUNNING"]
                if no_owner:
                    if each_vm['owner']:
                        continue
                elif show_owner:
                    data.extend([each_vm['owner'] if each_vm['owner'] else '-',
                                each_vm['owner_email'] if each_vm['owner_email'] else '-'])
                if show_nics:
                    data.extend([json.dumps(each_vm['nics'])])
                pt.add_row(data)
                sr_no += 1
            for each_vm in response.get('stopped_vm', []):
                data = [sr_no, each_vm['name'], "STOPPED"]
                if no_owner:
                    if each_vm['owner']:
                        continue
                elif show_owner:
                    data.extend([each_vm['owner'] if each_vm['owner'] else '-',
                                each_vm['owner_email'] if each_vm['owner_email'] else '-'])
                if show_nics:
                    data.extend([json.dumps(each_vm['nics'])])
                pt.add_row(data)
                sr_no += 1
            if include_template_vms:
                # For template VMs, we are not storing the resources info
                for each_vm in response.get("template_vm", []):
                    data = [sr_no, each_vm['name'] + '*', "TEMPLATE"]
                    if no_owner:
                        continue
                    elif show_owner:
                        data.extend(['-', '-'])
                    if show_nics:
                        data.extend(['-'])
                    pt.add_row(data)
                    sr_no += 1
        click.echo(pt)

@cluster.command(name='vm-power')
@click.argument('cluster_name')
@click.option('--off', is_flag=True, default=True, help="Power OFF a VM")
@click.option('--uuid', help="UUID of the VM to change Power State")
@click.option('--name', help="Name of the VM to change Power State")
def power_off_vm(cluster_name, off, uuid, name):
    """Sends request to change the power state of a VM running on this cluster
    """
    if not uuid and not name:
        click.echo("At least one of the UUID or VM Name should be mentioned")
        return
    power_state_url = LOCAL_ENDPOINT + CLUSTER_EP + f'/{cluster_name}/vms/power_state'
    params = {
        'uuid': uuid,
        'name': name,
    }
    if off:
        params['new_power_state'] = 'off'
    import json
    res = requests.post(power_state_url, json=json.dumps(params), headers=CLI_HEADERS)
    if res.status_code in [HTTPStatus.NOT_FOUND,
                           HTTPStatus.BAD_REQUEST,
                           HTTPStatus.EXPECTATION_FAILED,
                           HTTPStatus.SERVICE_UNAVAILABLE]:
        response = res.json()['resp']
        click.echo(response['message'])
    elif res.status_code == HTTPStatus.OK:
        response = res.json()['resp']
        click.echo(response['message'])
    else:
        click.echo(res.json())


@cluster.command(name='vm-nic')
@click.argument('cluster_name')
@click.option('--remove', '-r', is_flag=True, default=True, help="Power OFF a VM")
@click.option('--uuid', help="UUID of the VM to change Power State")
@click.option('--name', help="Name of the VM to change Power State")
def remove_vm_nic(cluster_name, remove, uuid, name):
    """Sends request to change the power state of a VM running on this cluster
    """
    if not uuid and not name:
        click.echo("At least one of the UUID or VM Name should be mentioned")
        return
    vm_nic_url = LOCAL_ENDPOINT + CLUSTER_EP + f'/{cluster_name}/vms/nics'
    params = {
        'uuid': uuid,
        'name': name,
    }
    if remove:
        params['nic_operation'] = 'remove'
    res = requests.delete(vm_nic_url, json=json.dumps(params), headers=CLI_HEADERS)
    if res.status_code in [HTTPStatus.NOT_FOUND,
                           HTTPStatus.BAD_REQUEST,
                           HTTPStatus.EXPECTATION_FAILED,
                           HTTPStatus.SERVICE_UNAVAILABLE]:
        response = res.json()['resp']
        click.echo(response['message'])
    elif res.status_code == HTTPStatus.OK:
        response = res.json()['resp']
        click.echo(response['message'])
    else:
        click.echo(res.json())


@cluster.command(name='check-util')
@click.argument('cluster_name')
@click.option('--cores', '-c', type=int, help="Proposed the CORES allocation")
@click.option('--memory', '-m', type=float, help="Proposed the Mem allocation")
def check_over_utilization(cluster_name, cores, memory):
    """Verify the utilization of the cluster AFTER allocating X resources
    """
    init()
    if not cores and not memory:
        click.echo("Please provide either cores or Memory to check the utilization")
        return
    params = {}
    if cores:
        params['cores'] = cores
    if memory:
        params['memory'] = memory * 1024
    url = LOCAL_ENDPOINT + CLUSTER_EP + f'/{cluster_name}/utilization?' + parse.urlencode(params)
    res = requests.get(url, headers=CLI_HEADERS)
    if res.status_code == HTTPStatus.OK:
        response = res.json()
        click.secho(f"After allocating {cores} cores and {memory} MB memory, "
            f"the cluster health status is - "
            f"CORES: ", nl=False)
        click.secho(f"{response['new_cores_perc']:.2f}% {response['cores_status'][2]}", fg=response['cores_status'][3], nl=False)
        click.secho(f" Memory: ", nl=False)
        click.secho(f"{response['new_memory_perc']:.2f}% {response['mem_status'][2]}", fg=response['mem_status'][3])
    else:
        click.echo(res.json())

