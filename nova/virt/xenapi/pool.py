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
        host_ref = session.get_xenapi_host()
        host_rec = session.call_xenapi('host.get_record', host_ref)
        self._host_name = host_rec['hostname']
        self._host_addr = host_rec['address']
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
                     'master_compute': host,
                     'master_address': self._host_addr,
                     'master_hostname': self._host_name,
                     'master_username': FLAGS.xenapi_connection_username,
                     'master_password': FLAGS.xenapi_connection_password,
                     },
                }
            db.aggregate_update(context, aggregate.id, values)
        else:
            # at this point, if this host is not the master, then an rpc.cast
            # to the master is required to make the pool-join. Otherwise
            # call the pool-join directly.
            master_compute = aggregate.metadetails['master_compute']
            if master_compute != host:
                # send rpc cast to master, asking to add the following
                # host with specified credentials.
                # NOTE: password in clear is not great, but it'll do for now
                forward_request(context, "add_to_aggregate", master_compute,
                                aggregate, host, self._host_addr)
            elif master_compute != host:
                # this is the master ->  do a pool-join
                session = self.XenAPI.Session(kwargs.get('url'))
                session.login_with_password(kwargs.get('user'),
                                            kwargs.get('pass'))
                try:
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
                                          reason='Unable to join %(host)s '
                                          'in the pool' % locals())

    def remove_from_aggregate(self, context, aggregate, host, **kwargs):
        master_compute = aggregate.metadetails.get('master_compute')
        if master_compute == FLAGS.host and master_compute != host:
            # this is the master -> Instruct it to eject a host from the pool
            try:
                host_ref = self._session.get_xenapi_host()
                self._session.call_xenapi("pool.eject", host_ref)
            except self.XenAPI.Failure, e:
                LOG.error(_("Error during xenapi call: %(e)s.") % locals())
                raise exception.AggregateError(aggregate_id=aggregate.id,
                                               action='remove_from_aggregate',
                                               reason=str(e.details))
        elif master_compute == host:
            # Remove master from its own pool -> destroy pool
            # only if the master is on its own, otherwise
            # raise fault. Destroying a pool made only by
            # master is fictional
            try:
                pool_ref = self._session.call_xenapi('pool.get_all')[0]
                self._session.call_xenapi('pool.set_name_label', pool_ref, '')
                metadata_keys = ['master_address', 'master_hostname',
                                 'master_compute', 'master_username',
                                 'master_password', ]
                for key in metadata_keys:
                    db.aggregate_metadata_delete(context, aggregate.id, key)
            except self.XenAPI.Failure, e:
                    LOG.warning(_("Pool-Join failed: %(e)s") % locals())
                    raise exception.AggregateError(
                                          aggregate_id=aggregate.id,
                                          action='remove_from_aggregate',
                                          reason='Unable to eject %(host)s '
                                          'from the pool; pool not empty'
                                          % locals())
        elif master_compute and master_compute != host:
            # A master exists -> forward pool-eject request to master
            forward_request(context, "remove_from_aggregate", master_compute,
                            aggregate, host, self._host_addr)
        else:
            # this shouldn't have happened
            raise exception.AggregateError(aggregate_id=aggregate.id,
                                           action='remove_from_aggregate',
                                           reason='Unable to eject %(host)s '
                                           'from the pool; No master found'
                                           % locals())


def forward_request(context, request_type, recipient,
                    aggregate, sender_host, sender_address):
    #tmp_url = urlparse.urlparse(FLAGS.xenapi_connection_url)
    #sender_url = urlparse.urlunparse(tmp_url) 
    rpc.cast(context,
             db.queue_get_for(context, FLAGS.compute_topic, recipient),
             {"method": request_type,
              "args": {"aggregate": aggregate,
                       "host": sender_host,
                       "url": FLAGS.xenapi_connection_url,
                       "user": FLAGS.xenapi_connection_username,
                       "pass": FLAGS.xenapi_connection_password,
                       "compute": vm_utils.get_this_vm_uuid(), },
             })

