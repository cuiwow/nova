# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright (c) 2012 Citrix Systems, Inc.
# Copyright 2010 OpenStack LLC.
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
Management class for host-related functions (start, reboot, etc).
"""

import logging
import json
import random

from nova import context
from nova import db
from nova import exception
from nova.virt.xenapi import vm_utils

LOG = logging.getLogger(__name__)


class Host(object):
    """
    Implements host related operations.
    """
    def __init__(self, session):
        self.XenAPI = session.get_imported_xenapi()
        self._session = session

    def host_power_action(self, host, action):
        """Reboots or shuts down the host."""
        args = {"action": json.dumps(action)}
        methods = {"reboot": "host_reboot", "shutdown": "host_shutdown"}
        response = call_xenhost(self._session, methods[action], args)
        return response.get("power_action", response)

    def host_maintenance_mode(self, host, mode):
        """Start/Stop host maintenance window. On start, it triggers
        guest VMs evacuation."""
        host_list = [host_ref for host_ref in
                     self._session.call_xenapi('host.get_all') \
                     if host_ref != self._session.get_xenapi_host()]
        ctxt = context.get_admin_context()
        for vm_ref, vm_rec in vm_utils.VMHelper.list_vms(self._session):
            for host_ref in host_list:
                try:
                    self._session.call_xenapi('VM.pool_migrate',
                                              vm_ref, host_ref, {})
                    instance_uuid = vm_rec['other_config']['nova_uuid']
                    instance = db.instance_get_by_uuid(ctxt, instance_uuid)
                    db.instance_update(ctxt, instance.id, host=host)
                    break
                except self.XenAPI.Failure:
                    LOG.exception('Unable to migrate VM %(vm_ref)s'
                                  'from %(host)s' % locals())
        return mode

    def set_host_enabled(self, host, enabled):
        """Sets the specified host's ability to accept new instances."""
        args = {"enabled": json.dumps(enabled)}
        response = call_xenhost(self._session, "set_host_enabled", args)
        return response.get('status', response)


class HostState(object):
    """Manages information about the XenServer host this compute
    node is running on.
    """
    def __init__(self, session):
        super(HostState, self).__init__()
        self._session = session
        self._stats = {}
        self.update_status()

    def get_host_stats(self, refresh=False):
        """Return the current state of the host. If 'refresh' is
        True, run the update first.
        """
        if refresh:
            self.update_status()
        return self._stats

    def update_status(self):
        """Since under Xenserver, a compute node runs on a given host,
        we can get host status information using xenapi.
        """
        LOG.debug(_("Updating host stats"))
        data = call_xenhost(self._session, "host_data", {})
        if data:
            try:
                # Get the SR usage
                sr_ref = vm_utils.VMHelper.safe_find_sr(self._session)
            except exception.NotFound as e:
                # No SR configured
                LOG.error(_("Unable to get SR for this host: %s") % e)
                return
            sr_rec = self._session.call_xenapi("SR.get_record", sr_ref)
            total = int(sr_rec["virtual_allocation"])
            used = int(sr_rec["physical_utilisation"])
            data["disk_total"] = total
            data["disk_used"] = used
            data["disk_available"] = total - used
            host_memory = data.get('host_memory', None)
            if host_memory:
                data["host_memory_total"] = host_memory.get('total', 0)
                data["host_memory_overhead"] = host_memory.get('overhead', 0)
                data["host_memory_free"] = host_memory.get('free', 0)
                data["host_memory_free_computed"] = host_memory.get(
                                                    'free-computed', 0)
                del data['host_memory']
            self._stats = data


def call_xenhost(session, method, arg_dict):
    """There will be several methods that will need this general
    handling for interacting with the xenhost plugin, so this abstracts
    out that behavior.
    """
    # Create a task ID as something that won't match any instance ID
    task_id = random.randint(-80000, -70000)
    XenAPI = session.get_imported_xenapi()
    try:
        task = session.async_call_plugin("xenhost", method, args=arg_dict)
        task_result = session.wait_for_task(task, str(task_id))
        if not task_result:
            task_result = json.dumps("")
        return json.loads(task_result)
    except ValueError:
        LOG.exception(_("Unable to get updated status"))
        return None
    except XenAPI.Failure as e:
        LOG.error(_("The call to %(method)s returned "
                    "an error: %(e)s.") % locals())
        return e.details[1]
