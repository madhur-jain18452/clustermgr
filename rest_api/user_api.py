"""
Defines all the REST APIs for the cluster.

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""

import json

from flask import Blueprint, jsonify, request, render_template
from http import HTTPStatus, HTTPMethod

from cluster_manager.global_cluster_cache import GlobalClusterCache

user_blue_print = Blueprint('user', __name__)


@user_blue_print.route("/users", methods=[HTTPMethod.GET])
def list_users():
    global_cache = GlobalClusterCache()
    if 'Accept' in request.headers and request.headers['Accept'] == 'application/json':
        return jsonify(global_cache.get_users()), HTTPStatus.OK
    else:
        return render_template('users.html', users=global_cache.get_users())


@user_blue_print.route("/users/<email>", methods=[HTTPMethod.POST,
                                                  HTTPMethod.PATCH])
def add_update_user(email):
    global_cache = GlobalClusterCache()
    request_args = json.loads(request.get_json())
    # Parse the comma-sep prefix list and clean
    if request.method == HTTPMethod.POST:
        # For a post request, Prefixes are passed as a list of strings
        list_pref = request_args.get('prefix')
        if list_pref is None:
            return jsonify({'message': 'Prefixes are required for a new user.'}), HTTPStatus.BAD_REQUEST
        request_args['add_prefixes'] = list_pref
    elif request.method == HTTPMethod.PATCH:
        if 'remove_prefixes' in request_args:
            temp_list = request_args['remove_prefixes']
            prefix_ls = []
            for each_pr in temp_list:
                prefix_ls.append(each_pr.strip())
            request_args['remove_prefixes'] = prefix_ls
        if 'add_prefixes' in request_args:
            temp_list = request_args['add_prefixes']
            prefix_ls = []
            for each_pr in temp_list:
                prefix_ls.append(each_pr.strip())
            request_args['add_prefixes'] = prefix_ls
    request_args['email'] = email

    is_patch=True if request.method == HTTPMethod.PATCH else False
    message = None
    val = global_cache.add_update_user(request_args, is_patch=is_patch)
    addition_failed = False
    flush_failed = False
    file_name = None
    if val:
        if not val[2]:
            addition_failed = True
            message = f"User {email} COULD NOT be {'updated' if is_patch else 'added'} in the cache."
        else: #  If successfully added / updated the user in the cache
           to_flush = request_args.get('flush', False)
           if to_flush:
            file_name = request_args.get('file', 'all_users.json')
            users_list = global_cache.get_users()
            if file_name.endswith(".json"):
                try:
                    with open(file_name, 'w') as fileh:
                        fileh.write(json.dumps(users_list, indent=4))
                except Exception as ex:
                    print(ex)
                    flush_failed = True
        if not addition_failed:
            message = f"User {email} {'updated' if is_patch else 'added'} successfully in the cache."
            if not flush_failed:
                if val[0] or val[1]:
                    message += f"\tPrefixes that failed to add: '{', '.join(val[0])}."
                    f"\n\tPrefixes that failed to remove: {', '.join(val[1])}"
                return jsonify({'message': message}), HTTPStatus.ACCEPTED
            else:
                message += f"\nFailed to flush the list of users to {file_name}."
                return jsonify({'message': message}), HTTPStatus.INTERNAL_SERVER_ERROR
        else:
            message = f"User {email} COULD NOT be {'updated' if is_patch else 'added'} in the cache."
    else:
        message = f"User {email} COULD NOT be {'updated' if is_patch else 'added'} in the cache."
    return jsonify({'message': message}), HTTPStatus.INTERNAL_SERVER_ERROR


@user_blue_print.route("/users/<email>/vms", methods=[HTTPMethod.GET])
def list_user_vms(email):
    global_cache = GlobalClusterCache()
    cname = request.args.get('cluster')
    vm_list, status = global_cache.get_all_vms_for_user(email, cname)
    return jsonify(vm_list), status
