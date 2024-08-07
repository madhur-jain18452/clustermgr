"""
Defines all the CLIs defined for the general tool

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""

import click
import json
import requests
from http import HTTPStatus
from prettytable import PrettyTable

from .constants import TOOL_EP, LOCAL_ENDPOINT, CLI_HEADERS
from tools.helper import convert_mb_to_gb


@click.group()
def tool():
    """Information about the tool as a whole
    """
    pass


@tool.command(name="list-schedules")
def list_schedules():
    """List of all the schedules running in the tool
    """
    res = requests.get(LOCAL_ENDPOINT + TOOL_EP, headers=CLI_HEADERS)
    list_funcs = res.json()
    pt = PrettyTable(["Sr. No.", "Function", "Running every"])
    sr_no = 1
    for each_func in list_funcs:
        pt.add_row([sr_no, each_func['name'],
                    f"{each_func['interval']} {each_func['unit']}"])
        sr_no += 1
    click.echo(pt)

@tool.command(name="override-dnd")
@click.option('--yes', is_flag=True, help="Allow overriding the DND for powering off the VMs")
@click.option('--no', is_flag=True, help="Do not override the DND for powering off the VMs")
def update_override_dnd(yes, no):
    """Update the Override flag to consider DND VMs for powering off
    """
    if yes and no:
        click.secho("Cannot provide both the options", fg='red')
        return
    if not yes and not no:
        click.secho("Provide either of 'yes' or 'no'", fg='red')
        return
    json_body = {'new_override_str': 'true' if yes else 'false' if no else 'false'}
    res = requests.put(LOCAL_ENDPOINT + TOOL_EP + "/override_dnd", headers=CLI_HEADERS, json=json.dumps(json_body))
    if res.status_code == 200:
        click.secho(res.json()['message'], fg='green')
    else:
        click.secho(res.json()['message'], fg='red')


@tool.command(name="update-cache-refresh")
@click.argument("new_frequency_str")
def update_cache_refresh_timings(new_frequency_str):
    """Update the frequency at which the cache refreshes itself
    """
    body = {'new_freq_str': new_frequency_str}
    res = requests.put(LOCAL_ENDPOINT + TOOL_EP + "/cache_refresh",
                       json=json.dumps(body), headers=CLI_HEADERS)
    click.echo(res.json()['message'])

@tool.command(name="update-dev-refresh")
@click.argument("new_frequency_str")
def update_cache_refresh_timings(new_frequency_str):
    """Update the frequency at which the cache refreshes the Deviations
    """
    body = {'new_freq_str': new_frequency_str}
    res = requests.put(LOCAL_ENDPOINT + TOOL_EP + "/deviation_refresh",
                       json=json.dumps(body), headers=CLI_HEADERS)
    click.echo(res.json()['message'])

@tool.command(name="update-mail-freq")
@click.argument("new_frequency_str")
def update_cache_refresh_timings(new_frequency_str):
    """Update the frequency at the warning mails are sent
    """
    body = {'new_freq_str': new_frequency_str}
    res = requests.put(LOCAL_ENDPOINT + TOOL_EP + "/mail_frequency",
                       json=json.dumps(body), headers=CLI_HEADERS)
    click.echo(res.json()['message'])

@tool.command(name="update-action-freq")
@click.argument("new_frequency_str")
def update_cache_refresh_timings(new_frequency_str):
    """Update the frequency at the actions are taken on the deviations
    """
    body = {'new_freq_str': new_frequency_str}
    res = requests.put(LOCAL_ENDPOINT + TOOL_EP + "/action_frequency",
                       json=json.dumps(body), headers=CLI_HEADERS)
    click.echo(res.json()['message'])

@tool.command(name="dump-user")
@click.option("--file", type=str, help="Name of the file to dump the user config")
def dump_user_config(file):
    """Dump the user config to a file
    """
    body = {}
    if file:
        body = {'dump_file': file}
    res = requests.post(LOCAL_ENDPOINT + TOOL_EP + "/dump_user_config",
                        json=json.dumps(body), headers=CLI_HEADERS)
    click.echo(res.json()['message'])

@tool.command(name="dump-cluster")
@click.option("--file", type=str, help="Name of the file to dump the user config")
def dump_cluster_config(file):
    """Dump the cluster config to a file
    """
    body = {}
    if file:
        body = {'dump_file': file}
    res = requests.post(LOCAL_ENDPOINT + TOOL_EP + "/dump_cluster_config",
                        json=json.dumps(body), headers=CLI_HEADERS)
    click.echo(res.json()['message'])

@tool.command(name="eval")
@click.option("--off", is_flag=True, help="Turn the eval mode off")
@click.option("--on", is_flag=True, help="Turn the eval mode on")
def dump_cluster_config(off, on):
    """Dump the cluster config to a file
    """
    if off and on:
        click.secho("Cannot provide both the options", fg='red')
        return
    if not off and not on:
        click.secho("Provide either of 'on' or 'off'", fg='red')
        return
    body = {'new_state': 'off' if off else 'on' if on else 'on'}
    res = requests.put(LOCAL_ENDPOINT + TOOL_EP + "/eval_mode",
                        json=json.dumps(body), headers=CLI_HEADERS)
    if res.status_code != HTTPStatus.OK:
        click.secho(res.json()['message'], fg='red')
    click.echo(res.json()['message'])
