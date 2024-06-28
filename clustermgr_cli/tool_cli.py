"""
Defines all the CLIs defined for the general tool

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""

import click
import json
import requests

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
    """List of all the clusters in the cache
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

@tool.command(name="update-cache-refresh")
@click.argument("new_frequency_str")
def update_cache_refresh_timings(new_frequency_str):
    """Update the frequency at which the cache refreshes itself
    """
    body = {'new_freq_str': new_frequency_str}
    res = requests.put(LOCAL_ENDPOINT + TOOL_EP + "/cache_refresh",
                       json=json.dumps(body), headers=CLI_HEADERS)
    click.echo(res.json()['message'])

@tool.command(name="update-offense-refresh")
@click.argument("new_frequency_str")
def update_cache_refresh_timings(new_frequency_str):
    """Update the frequency at which the cache refreshes the Offenses
    """
    body = {'new_freq_str': new_frequency_str}
    res = requests.put(LOCAL_ENDPOINT + TOOL_EP + "/offense_refresh",
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
    """Update the frequency at the actions are taken on the offenses
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
