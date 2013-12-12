# Copyright 2011 Gridcentric Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
An extension module for novaclient that allows the `nova` application access to
the cobalt API extensions.
"""

import os
import base64
import re
import json
import sys

from novaclient import utils
from novaclient import base
from novaclient import exceptions
from novaclient.v1_1 import servers
from novaclient.v1_1 import shell

from . import agent

# Add new client capabilities here. Each key is a capability name and its value
# is the list of API capabilities upon which it depends.

CAPABILITIES = {'user-data': ['user-data'],
                'launch-name': ['launch-name'],
                'security-groups': ['security-groups'],
                'num-instances': ['num-instances'],
                'availability-zone': ['availability-zone'],
                'bless-name': ['bless-name'],
                'launch-key': ['launch-key'],
                'import-export': ['import-export'],
                'scheduler-hints': ['scheduler-hints'],
                'install-policy': ['install-policy'],
                'get-policy': ['get-policy'],
                'supports-volumes': ['supports-volumes'],
                'launch-nics': ['launch-nics'],
                }

CAPS_HELP = {'user-data': 'Live-image-start will honor --user-data.',
             'launch-name': 'Live-image-start will honor --name.',
             'security-groups': 'Live-image-start will honor --security-groups.',
             'num-instances': 'Live-image-start will honor --num-instances.',
             'availability-zone': 'Live-image-start will honor --availability-zone.',
             'bless-name': 'Live-image-create will honor --name.',
             'launch-key': 'Live-image-start will honor --key-name.',
             'import-export': 'Live-image-import/export supported by API.',
             'scheduler-hints': 'Live-image-start will honor --hint.',
             'install-policy': 'Install-policy supported by API.',
             'get-policy': 'Get-policy supported by API.',
             'supports-volumes': 'Instances with volumes attached (boot or hotplug) supported for live-image-*.',
             'launch-nics': 'Live-image-start will honor --nic',
            }

def __pre_parse_args__():
    pass

def __post_parse_args__(args):
    pass

def _print_server(cs, server, minimal=False):
    # (dscannell): Note that the following method was taken from the main
    # novaclient code base. We duplicate it here to protect ourselves from
    # changes in the method signatures between versions of the novaclient.

    # By default when searching via name we will do a
    # findall(name=blah) and do a REST /details which is not the same
    # as a .get() and doesn't get the information about flavors and
    # images. This fixes it as we redo the call with the id which does a
    # .get() to get all informations.
    if not 'flavor' in server._info:
        server = shell._find_server(cs, server.id)

    networks = server.networks
    info = server._info.copy()
    for network_label, address_list in networks.items():
        info['%s network' % network_label] = ', '.join(address_list)

    flavor = info.get('flavor', {})
    flavor_id = flavor.get('id', '')
    if minimal:
        info['flavor'] = flavor_id
    else:
        info['flavor'] = shell._find_flavor(cs, flavor_id).name

    image = info.get('image', {})
    if image:
        image_id = image.get('id', '')
        if minimal:
            info['image'] = image_id
        else:
            try:
                info['image'] = '%s (%s)' \
                        % (shell._find_image(cs, image_id).name, image_id)
            except:
                info['image'] = '%s (%s)' % ("Image not found", image_id)
    else: # Booted from volume
        info['image'] = "Attempt to boot from volume - no image supplied"

    info.pop('links', None)
    info.pop('addresses', None)

    utils.print_dict(info)

def _find_server(cs, server):
    """ Returns a server by name or ID. """
    return utils.find_resource(cs.cobalt, server)

def parse_nics_arg(arg_nics):
    """
    Taken from python-novaclient to parse the nic argument to live-image-start
    the same way novalcient parses it for 'boot'.
    """
    nics = []
    for nic_str in arg_nics:
        err_msg = ("Invalid nic argument '%s'. Nic arguments must be of the "
                   "form --nic <net-id=net-uuid,v4-fixed-ip=ip-addr,"
                   "port-id=port-uuid>, with at minimum net-id or port-id "
                   "specified." % nic_str)
        nic_info = {"net-id": "", "v4-fixed-ip": "", "port-id": ""}

        for kv_str in nic_str.split(","):
            try:
                k, v = kv_str.split("=", 1)
            except ValueError as e:
                raise exceptions.CommandError(err_msg)

            if k in nic_info:
                nic_info[k] = v
            else:
                raise exceptions.CommandError(err_msg)

        if not nic_info['net-id'] and not nic_info['port-id']:
            raise exceptions.CommandError(err_msg)

        nics.append(nic_info)
    return nics

def inherit_args(inherit_from_fn):
    """Decorator to inherit all of the utils.arg decorated agruments from
    another function.
    """
    def do_inherit(fn):
        if hasattr(inherit_from_fn, 'arguments'):
            fn.arguments = inherit_from_fn.arguments
        return fn
    return do_inherit

#### ACTIONS ####
@utils.arg('name', metavar='<name>', help='The name for the new server')
@utils.arg('--live-image', metavar='<live image>', help="Live-image ID (see 'nova live-image-list')")
@utils.arg('--user-data', metavar='<user-data>', default=None,
           help='User data file to pass to be exposed by the metadata server')
@utils.arg('--security-groups', metavar='<security groups>', default=None, help='Comma separated list of security group names.')
@utils.arg('--availability-zone', metavar='<availability zone>', default=None, help='The availability zone for instance placement.')
@utils.arg('--num-instances', metavar='<number>', default='1', help='Launch multiple instances at a time')
@utils.arg('--key-name', metavar='<key name>', default=None, help='Key name of keypair that should be created earlier with the command keypair-add')
@utils.arg('--params', action='append', default=[], metavar='<key=value>', help='Guest parameters to send to vms-agent')
@utils.arg('--hint', action='append', dest='_scheduler_hints', default=[], metavar='<key=value>',
            help="Send arbitrary key/value pairs to the scheduler for custom use.")
@utils.arg('--nic',
    metavar="<net-id=net-uuid,v4-fixed-ip=ip-addr,port-id=port-uuid>",
    action='append',
    dest='nics',
    default=[],
    help="Create a NIC on the server. "
         "Specify option multiple times to create multiple NICs. "
         "net-id: attach NIC to network with this UUID "
         "(required if no port-id), "
         "v4-fixed-ip: IPv4 fixed address for NIC (optional), "
         "port-id: attach NIC to port with this UUID "
         "(required if no net-id)")
def do_live_image_start(cs, args):
    """Start a new instance from a live-image."""
    if not args.live_image:
        raise exceptions.CommandError("you need to provide a live-image ID")
    server = _find_server(cs, args.live_image)
    guest_params = {}
    for param in args.params:
        components = param.split("=")
        if len(components) > 0:
            guest_params[components[0]] = "=".join(components[1:])

    if args.user_data:
        user_data = open(args.user_data)
    else:
        user_data = None

    if args.security_groups:
        security_groups = args.security_groups.split(',')
    else:
        security_groups = None

    if args.availability_zone:
        availability_zone = args.availability_zone
    else:
        availability_zone = None

    scheduler_hints = {}
    if args._scheduler_hints:
        for hint in args._scheduler_hints:
            key, _sep, value = hint.partition('=')
            # NOTE(vish says): multiple copies of the same hint will result in
            # a list of values
            if key in scheduler_hints:
                if isinstance(scheduler_hints[key], basestring):
                    scheduler_hints[key] = [scheduler_hints[key]]
                scheduler_hints[key] += [value]
            else:
                scheduler_hints[key] = value

    nics = parse_nics_arg(args.nics)

    launch_servers = cs.cobalt.start_live_image(server,
        name=args.name,
        user_data=user_data,
        guest_params=guest_params,
        security_groups=security_groups,
        availability_zone=availability_zone,
        num_instances=int(args.num_instances),
        key_name=args.key_name,
        scheduler_hints=scheduler_hints,
        networks=nics)

    for server in launch_servers:
        _print_server(cs, server)

@utils.arg('live_image', metavar='<live image>', help="Live-image ID (see 'nova live-image-list')")
@utils.arg('--name', metavar='<name>', default=None, help='The name for the new server')
@utils.arg('--user-data', metavar='<user-data>', default=None,
           help='User data file to pass to be exposed by the metadata server')
@utils.arg('--security-groups', metavar='<security groups>', default=None, help='Comma separated list of security group names.')
@utils.arg('--availability-zone', metavar='<availability zone>', default=None, help='The availability zone for instance placement.')
@utils.arg('--num-instances', metavar='<number>', default='1', help='Launch multiple instances at a time')
@utils.arg('--key-name', metavar='<key name>', default=None, help='Key name of keypair that should be created earlier with the command keypair-add')
@utils.arg('--params', action='append', default=[], metavar='<key=value>', help='Guest parameters to send to vms-agent')
@utils.arg('--hint', action='append', dest='_scheduler_hints', default=[], metavar='<key=value>',
            help="Send arbitrary key/value pairs to the scheduler for custom use.")
def do_launch(cs, args):
    """DEPRECATED! Use live-image-start instead."""
    args.nics = []
    do_live_image_start(cs, args)

@utils.arg('server', metavar='<instance>', help="Name or ID of server.")
@utils.arg('name', metavar='<name>', help="Name of live-image.")
def do_live_image_create(cs, args):
    """Creates a new live-image from a running instance."""
    server = _find_server(cs, args.server)
    blessed_servers = cs.cobalt.create_live_image(server, args.name)
    for server in blessed_servers:
        _print_server(cs, server)

@utils.arg('server', metavar='<instance>', help="Name or ID of server.")
@utils.arg('--name', metavar='<name>', default=None, help="Name of live-image.")
def do_bless(cs, args):
    """DEPRECATED! Use live-image-create instead."""
    do_live_image_create(cs, args)

@utils.arg('live_image', metavar='<live-image>', help="ID or name of the live-image")
def do_live_image_delete(cs, args):
    """Delete a live image."""
    server = _find_server(cs, args.live_image)
    cs.cobalt.delete_live_image(server)

@inherit_args(do_live_image_delete)
def do_discard(cs, args):
    """DEPRECATED! Use live-image-delete instead."""
    do_live_image_delete(cs, args)

@utils.arg('server', metavar='<instance>', help="ID or name of the instance to migrate")
@utils.arg('--dest', metavar='<destination host>', default=None, help="Host to migrate to")
def do_cobalt_migrate(cs, args):
    """Migrate an instance using VMS."""
    server = _find_server(cs, args.server)
    cs.cobalt.migrate(server, args.dest)

@inherit_args(do_cobalt_migrate)
def do_gc_migrate(cs, args):
    """DEPRECATED! Use cobalt-migrate instead."""
    do_cobalt_migrate(cs, args)

def _print_list(servers):
    id_col = 'ID'
    columns = [id_col, 'Name', 'Status', 'Networks']
    formatters = {'Networks':utils._format_servers_list_networks}
    utils.print_list(servers, columns, formatters)

@utils.arg('live_image', metavar='<live-image>', help="ID or name of the live-image")
def do_live_image_servers(cs, args):
    """List instances started from this live-image."""
    server = _find_server(cs, args.live_image)
    _print_list(cs.cobalt.list_live_image_servers(server))

@inherit_args(do_live_image_servers)
def do_list_launched(cs, args):
    """DEPRECATED! Use live-image-servers instead."""
    do_live_image_servers(cs, args)

@utils.arg('server', metavar='<server>', help="ID or name of the instance")
def do_live_image_list(cs, args):
    """List the live images of this instance."""
    server = _find_server(cs, args.server)
    _print_list(cs.cobalt.list_live_images(server))

@inherit_args(do_live_image_list)
def do_list_blessed(cs, args):
    """DEPRECATED! Use live-image-list instead."""
    do_live_image_list(cs, args)

@utils.arg('server', metavar='<live-image>', help="ID or name of the live-image")
@utils.arg('output', metavar='<output>', default=None, help="Name of a file to write the exported data to.")
def do_live_image_export(cs, args):
    """Export a live-image"""
    server = _find_server(cs, args.server)
    result = server.export()

    json_result = json.dumps(result, sort_keys=True, indent=4, separators=(',', ': ')) + '\n'

    with file(args.output, 'w') as f:
        f.write(json_result)

    print "Instance data is being exported to image %s" %(result['export_image_id'])

@utils.arg('data_filename', metavar='<data-filename>',
                              help="A file containing the exported server data")
@utils.arg('--override', metavar='<override>',
                      help="Semicolon-separated list of parameters to override")
def do_live_image_import(cs, args):
    """Import a live-image"""

    # The override option can be something like this:
    # export_image_id=THE-ID;security_groups=sg1,sg2;fields.display_name=foo

    # Read in the contents of the server data (should be a JSON file)
    with open(args.data_filename, 'r') as f:
        data = json.load(f)

    if args.override is not None:
        def override(key, value, data):
            if '.' in key:
                parent, _, key = key.partition('.')
                override(key, value, data[parent])
            else:
                if isinstance(data[key], list):
                    value = value.split(',')
                data[key] = value
        for override_arg in args.override.split(';'):
            key, value = override_arg.split('=', 1)
            override(key, value, data)

    server = cs.cobalt.import_instance(data)
    _print_server(cs, server)

@utils.arg('policy_filename', metavar='<policy-filename>',
           help='Path to file containing vmspolicyd policy definitions')
@utils.arg('--wait', dest='wait', action='store_true', default=False,
           help='Block until the new policy has been successfully installed on all hosts')
def do_install_policy(cs, args):
    """Distribute policy definitions to all cobalt hosts."""
    with open(args.policy_filename, 'r') as policy_file:
        cs.cobalt.install_policy(policy_file.read(), args.wait)

@utils.arg('server', metavar='<instance>', help="Name or ID of server.")
def do_get_policy(cs, args):
    """Get the applied domain policy from vmspolicyd."""
    server = _find_server(cs, args.server)
    for line in server.get_policy():
        print line

@utils.arg('--all', dest='all', action='store_true', default=False,
           help='List all capabilities, enabled or not.')
def do_cobalt_capabilities(cs, args):
    """Display Cobalt capabilities supported by the API."""
    caps = dict(CAPS_HELP)
    # Watch out for sloppiness
    for k in [ x for x in CAPABILITIES.keys() if x not in CAPS_HELP ]:
        caps[k] = ''
    if not args.all:
        if not hasattr(cs.cobalt, 'capabilities'):
            cs.cobalt.setup_capabilities()
        elide = list(set(CAPABILITIES.keys()) - set(cs.cobalt.capabilities))
        for k in elide:
            del(caps[k])
    for cap, help in caps.items():
        print'    %-20s  %s' % (cap, help)

@utils.arg('server', metavar='<instance>', help="ID or name of the instance to install on")
@utils.arg('--user',
     default='root',
     metavar='<user>',
     help="The login user.")
@utils.arg('--key_path',
     default=None,
     metavar='<key_path>',
     help="The path to the private key.")
@utils.arg('--agent_location',
     default=None,
     metavar='<agent_location>',
     help="Install packages from a custom location.")
@utils.arg('--agent_version',
     default=None,
     metavar='<agent_version>',
     help="Install a specific agent version.")
@utils.arg('--ip',
     default=None,
     metavar='<ip>',
     help="Instance IP address to use (defaults to first ssh-able).")
def do_cobalt_install_agent(cs, args):
    """Install the agent onto an instance."""
    server = _find_server(cs, args.server)
    server.install_agent(args.user,
                         args.key_path,
                         location=args.agent_location,
                         version=args.agent_version,
                         ip=args.ip)

@inherit_args(do_cobalt_install_agent)
def do_gc_install_agent(cs, args):
    """ DEPRECATED! Use cobalt-install-agent instead."""
    do_cobalt_install_agent(cs, args)

class CoServer(servers.Server):
    """
    A server object extended to provide cobalt capabilities
    """

    def launch(self, *args, **kwargs):
        """ Deprecated. Please use the start_live_image(...). """
        return self.start_live_image(*args, **kwargs)

    def start_live_image(self, target=None, name=None, user_data=None, guest_params={},
               security_groups=None, availability_zone=None, num_instances=1,
               key_name=None, scheduler_hints={}, networks=None):
        return self.manager.launch(self,
                                   target=target,
                                   name=name,
                                   user_data=user_data,
                                   guest_params=guest_params,
                                   security_groups=security_groups,
                                   availability_zone=availability_zone,
                                   num_instances=num_instances,
                                   key_name=key_name,
                                   scheduler_hints=scheduler_hints,
                                   networks=networks)

    def bless(self, *args, **kwargs):
        """ Deprecated. Please use create_live_image(...). """
        return self.create_live_image(*args, **kwargs)

    def create_live_image(self, name=None):
        return self.manager.bless(self, name)

    def discard(self, *args, **kwargs):
        """ Deprecated. Please use delete_live_image(...). """
        self.delete_live_image(*args, **kwargs)

    def delete_live_image(self):
        self.manager.discard(self)

    def migrate(self, dest=None):
        self.manager.migrate(self, dest)

    def list_launched(self, *args, **kwargs):
        """ Deprecated. Please use list_servers(...)."""
        return self.list_servers(*args, **kwargs)

    def list_servers(self):
        return self.manager.list_launched(self)

    def list_blessed(self, *args, **kwargs):
        """ Deprecated. Please use list_live_images(...)."""
        return self.list_live_images(*args, **kwargs)

    def list_live_images(self):
        return self.manager.list_blessed(self)

    def export(self):
        return self.manager.export(self)

    def install_agent(self, user, key_path, location=None,
                        version=None, ip=None):
        self.manager.install_agent(self, user, key_path, location=location,
                                    version=version, ip=ip)

    def get_policy(self):
        return self.manager.get_policy(self)

class CoServerManager(servers.ServerManager):
    resource_class = CoServer

    def __init__(self, client, *args, **kwargs):
        servers.ServerManager.__init__(self, client, *args, **kwargs)

        # Make sure this instance is available as cobalt.
        if not(hasattr(client, 'cobalt')):
            setattr(client, 'cobalt', self)

        # We also attach to the client as 'gridcentric' to be backwards
        # compatible.
        if not(hasattr(client, 'gridcentric')):
            setattr(client, 'gridcentric', self)

    # Capabilities must be computed lazily because self.api.client isn't
    # available in __init__

    def setup_capabilities(self):
        api_caps = self.get_info()['capabilities']
        self.capabilities = [cap for cap in CAPABILITIES.keys() if \
                all([api_req in api_caps for api_req in CAPABILITIES[cap]])]

    def satisfies(self, requirements):
        if not hasattr(self, 'capabilities'):
            self.setup_capabilities()

        return set(requirements) <= set(self.capabilities)

    def get_info(self):
        url = '/gcinfo'
        res = self.api.client.get(url)[1]
        return res

    def launch(self, *args, **kwargs):
        """ Deprecated. Please use start_live_image(...). """
        return self.start_live_image(*args, **kwargs)

    def start_live_image(self, server, target=None, name=None, user_data=None,
               guest_params={}, security_groups=None, availability_zone=None,
               num_instances=1, key_name=None, scheduler_hints={},
               networks=None):
        # NOTE: We no longer support target in the backend, so this
        # parameter is silent dropped. It exists only in the kwargs
        # so as not to break existing client.
        params = {'guest': guest_params,
                  'security_groups': security_groups,
                  'availability_zone': availability_zone,
                  'scheduler_hints': scheduler_hints,
                  'num_instances': num_instances,
                  'key_name': key_name}

        if name != None:
            params['name'] = name

        if user_data:
            if hasattr(user_data, 'read'):
                real_user_data = user_data.read()
            elif isinstance(user_data, unicode):
                real_user_data = user_data.encode('utf-8')
            else:
                real_user_data = user_data

            params['user_data'] = base64.b64encode(real_user_data)

        # (dscannell): Taken from python-novaclient
        if networks is not None:
            # NOTE(tr3buchet): nics can be an empty list
            all_net_data = []
            for nic_info in networks:
                net_data = {}
                # if value is empty string, do not send value in body
                if nic_info.get('net-id'):
                    net_data['uuid'] = nic_info['net-id']
                if nic_info.get('v4-fixed-ip'):
                    net_data['fixed_ip'] = nic_info['v4-fixed-ip']
                if nic_info.get('port-id'):
                    net_data['port'] = nic_info['port-id']
                all_net_data.append(net_data)
            if len(all_net_data) > 0:
                params['networks'] = all_net_data

        header, info = self._action("gc_launch", base.getid(server), params)
        return [self.get(server['id']) for server in info]

    def bless(self, *args, **kwargs):
        """ Deprecated. Please use create_live_image(...). """
        return self.create_live_image(*args, **kwargs)

    def create_live_image(self, server, name=None):
        params = {'name': name}
        header, info = self._action("gc_bless", base.getid(server), params)
        return [self.get(server['id']) for server in info]

    def discard(self, *args, **kwargs):
        """ Deprecated. Please use delete_live_iamge(...). """
        return self.delete_live_image(*args, **kwargs)

    def delete_live_image(self, server):
        return self._action("gc_discard", base.getid(server))

    def migrate(self, server, dest=None):
        params = {}
        if dest != None:
            params['dest'] = dest
        return self._action("gc_migrate", base.getid(server), params)

    def list_launched(self, *args, **kwargs):
        """ Deprecated. Please use list_live_image_servers(...)."""
        return self.list_live_image_servers(*args, **kwargs)

    def list_live_image_servers(self, server):
        header, info = self._action("gc_list_launched", base.getid(server))
        return [self.get(server['id']) for server in info]

    def list_blessed(self, *args, **kwargs):
        """ Deprecated. Please use list_live_images(...). """
        return self.list_live_images(*args, **kwargs)

    def list_live_images(self, server):
        header, info = self._action("gc_list_blessed", base.getid(server))
        return [self.get(server['id']) for server in info]

    def export(self, server):
        header, info = self._action("gc_export", server.id)
        return info

    def import_instance(self, data):
        url = "/gc-import-server"
        body = {'data': data}

        return self._create(url, body, 'server')

    def install_policy(self, policy_ini_string, wait):
        url = "/gcpolicy"
        body = {
            "policy_ini_string": policy_ini_string,
            "wait": wait,
        }

        return self.api.client.post(url, body=body)

    def get_policy(self, server):
        header, info = self._action("co_get_policy", base.getid(server))
        return info

    def install_agent(self, server, user, key_path, location=None,
                        version=None, ip=None):
        agent.install(server, user, key_path, location=location,
                        version=version, ip=ip)
