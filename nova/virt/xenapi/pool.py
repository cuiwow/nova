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
Management class for Pool-related functions (join, eject, etc).
"""

import urlparse

from nova import db
from nova import exception
from nova import flags
from nova import log as logging
from nova import rpc
from nova.common import cfg
from nova.compute import aggregate_states
from nova.virt.xenapi import vm_utils

LOG = logging.getLogger("nova.virt.xenapi.pool")

xenapi_pool_opts = [
    cfg.BoolOpt('use_join_force',
               default=True,
               help='To use for hosts with different CPUs'),
    ]

FLAGS = flags.FLAGS
FLAGS.add_options(xenapi_pool_opts)


class ResourcePool(object):
    """
    Implements resource pool operations.
    """
    def __init__(self, session):
        self.XenAPI = session.get_imported_xenapi()
        self._session = session

    def add_to_aggregate(self, context, aggregate, host, **kwargs):
        """Add a compute host to an aggregate."""
        if len(aggregate.hosts) == 1:
            # this is the first host of the pool -> make it master
            try:
                pool_ref = self._session.call_xenapi("pool.get_all")[0]
                self._session.call_xenapi("pool.set_name_label",
                                          pool_ref, aggregate.name)
            except self.XenAPI.Failure, e:
                LOG.error(_("Unable to set up pool: %(e)s.") % locals())
                raise exception.AggregateError(aggregate_id=aggregate.id,
                                               action='add_to_aggregate',
                                               reason=str(e.details))
            # save metadata so that we can find the master again:
            # the password should be encrypted, really.
            url = FLAGS.xenapi_connection_url
            values = {
                'operational_state': aggregate_states.ACTIVE,
                'metadata': {
                     'master_hostname': urlparse.urlparse(url).hostname,
                     'master_username': FLAGS.xenapi_connection_username,
                     'master_password': FLAGS.xenapi_connection_password,
                     },
                }
            db.aggregate_update(context, aggregate.id, values)
        else:
            # at this point, if this host is not the master, then an rpc.cast
            # to the master is required to make the pool-join. Otherwise
            # call the pool-join directly.
            master = aggregate.metadetails['master_hostname']
            mehost = urlparse.urlparse(FLAGS.xenapi_connection_url).hostname
            if master != mehost:
                # send rpc cast to master, asking to add the following
                # host with specified credentials.
                # NOTE: password in clear is not great, but it'll do for now
                rpc.cast(context, FLAGS.compute_topic,
                         {"method": "add_to_aggregate",
                          "args": {"aggregate": aggregate,
                                   "host": host,
                                   "url": FLAGS.xenapi_connection_url,
                                   "user": FLAGS.xenapi_connection_username,
                                   "pass": FLAGS.xenapi_connection_password,
                                   "compute": vm_utils.get_this_vm_uuid(), },
                         })
            else:
                # use xenapi session with no fringes to make the join
                session = self.XenAPI.Session(kwargs.get('url'))
                session.login_with_password(kwargs.get('user'),
                                            kwargs.get('pass'))
                try:
                    url = FLAGS.xenapi_connection_url
                    urlparse.urlparse(url).hostname
                    pool_join = FLAGS.use_join_force and \
                                session.xenapi.join_force or \
                                session.xenapi.join
                    pool_join(FLAGS.xenapi_connection_username,
                              FLAGS.xenapi_connection_password)
                except self.XenAPI.Failure, e:
                    LOG.warning(_("Pool-Join failed: %(e)s") % locals())
                    raise exception.AggregateError(
                                          aggregate_id=aggregate.id,
                                          action='add_to_aggregate',
                                          reason='Unable to join %(host)s'
                                          'in the pool' % locals())

    def remove_from_aggregate(self, context, aggregate, host, **kwargs):
        try:
            host_ref = self._session.call_xenapi("host.get_by_name_label",
                                                 host)
            self._session.call_xenapi("pool.eject", host_ref)
        except self.XenAPI.Failure, e:
            LOG.error(_("Error during xenapi call: %(e)s.") % locals())
            raise exception.AggregateError(aggregate_id=aggregate.id,
                                           action='remove_from_aggregate',
                                           reason=str(e.details))
