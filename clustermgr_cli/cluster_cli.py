import click 
from prettytable import PrettyTable
import requests
from http import HTTPStatus
from urllib import parse


from .constants import CLUSTER_EP, LOCAL_ENDPOINT
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
    res = requests.get(LOCAL_ENDPOINT + CLUSTER_EP)
    cluster_list = res.json()
    pt = PrettyTable(['Cluster Name'])
    for name in cluster_list:
        pt.add_row([name])
    click.echo(pt)


@cluster.command()
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
def list_vms(cluster_name, resources, no_owner, sorted_mem, sorted_core,
             count, powered_off, show_owner):
    """List of all the VMs on a particular cluster
    """
    params = {
        'resources': resources,
        'no_owner': no_owner,
        'count': count
    }
    if powered_off and (resources or sorted_core or sorted_mem):
        click.echo("Please provide either --powered-off or any of resources "
                   "or sort-by-resource. (Powered OFF VMs are not alloted any"
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

    res = requests.get(url)
    if res.status_code == HTTPStatus.NOT_FOUND:
        click.echo("Cluster with name {} not found in the cache!")
        return
    response = res.json()
    if powered_off:
        print("List of Powered OFF VMs:")
        if show_owner:
            columns.extend(['Owner', 'Owner Email'])
        pt_po = PrettyTable(columns)
        sr_no = 1
        for each_vm in response.get("stopped_vm", []):
            data = [sr_no, each_vm['name']]
            if no_owner:
                if each_vm['owner']:
                    continue
            elif show_owner:
                data.extend([each_vm['owner'] if each_vm['owner'] else '-',
                             each_vm['owner_email'] if each_vm['owner_email'] else '-'])
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
            pt = PrettyTable(columns)
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
            pt = PrettyTable(columns)
            for each_vm in response.get('running_vm', []):
                data = [sr_no, each_vm['name'], "RUNNING"]
                if no_owner:
                    if each_vm['owner']:
                        continue
                elif show_owner:
                    data.extend([each_vm['owner'] if each_vm['owner'] else '-',
                                each_vm['owner_email'] if each_vm['owner_email'] else '-'])
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
    print(power_state_url)
    res = requests.post(power_state_url, json=json.dumps(params))
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

