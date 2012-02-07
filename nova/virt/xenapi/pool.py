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

import json
import urlparse

from nova import db
from nova import exception
from nova import flags
from nova import log as logging
from nova import rpc
from nova.compute import aggregate_states
from nova.openstack.common import cfg
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
            self._init_pool(aggregate.id, aggregate.name)
            # save metadata so that we can find the master again:
            # the password should be encrypted, really.
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
            # the pool is already up and running, we need to figure out
            # whether we can serve the request from this host or not.
            master_compute = aggregate.metadetails['master_compute']
            if master_compute == FLAGS.host and master_compute != host:
                # this is the master ->  do a pool-join
                # To this aim, nova compute on the slave has to go down.
                # NOTE: it is assumed that ONLY nova compute is running now
                self._join_slave(aggregate.id, host,
                                 kwargs.get('compute_uuid'),
                                 kwargs.get('url'), kwargs.get('user'),
                                 kwargs.get('passwd'))
            elif master_compute and master_compute != host:
                # send rpc cast to master, asking to add the following
                # host with specified credentials.
                # NOTE: password in clear is not great, but it'll do for now
                forward_request(context, "add_aggregate_host", master_compute,
                                aggregate.id, host, self._host_addr)

    def remove_from_aggregate(self, context, aggregate, host, **kwargs):
        master_compute = aggregate.metadetails.get('master_compute')
        if master_compute == FLAGS.host and master_compute != host:
            # this is the master -> instruct it to eject a host from the pool
            self._eject_slave(aggregate.id)
        elif master_compute == host:
            # Remove master from its own pool -> destroy pool only if the
            # master is on its own, otherwise raise fault. Destroying a
            # pool made only by master is fictional
            self._clear_pool(aggregate.id)
            # Metadata clear-out
            metadata_keys = ['master_address', 'master_hostname',
                             'master_compute', 'master_username',
                             'master_password', ]
            for key in metadata_keys:
                db.aggregate_metadata_delete(context, aggregate.id, key)
        elif master_compute and master_compute != host:
            # A master exists -> forward pool-eject request to master
            forward_request(context, "remove_aggregate_host", master_compute,
                            aggregate.id, host, self._host_addr)
        else:
            # this shouldn't have happened
            raise exception.AggregateError(aggregate_id=aggregate.id,
                                           action='remove_from_aggregate',
                                           reason=_('Unable to eject %(host)s '
                                           'from the pool; No master found')
                                           % locals())

    def _join_slave(self, aggregate_id, host, compute_uuid, url, user, passwd):
        """Joins a slave into a XenServer resource pool."""
        try:
            args = {'compute_uuid': compute_uuid,
                    'url': url,
                    'user': user,
                    'password': passwd,
                    'force': json.dumps(FLAGS.use_join_force),
                    'master_addr': self._host_addr,
                    'master_user': FLAGS.xenapi_connection_username,
                    'master_pass': FLAGS.xenapi_connection_password, }
            task = self._session.async_call_plugin('xenhost',
                                                   'host_join', args)
            self._session.wait_for_task(task)
        except self.XenAPI.Failure, e:
            LOG.error(_("Pool-Join failed: %(e)s") % locals())
            raise exception.AggregateError(aggregate_id=aggregate_id,
                                           action='add_to_aggregate',
                                           reason=_('Unable to join %(host)s '
                                                  'in the pool') % locals())

    def _eject_slave(self, aggregate_id):
        """Eject a slave from a XenServer resource pool."""
        try:
            host_ref = self._session.get_xenapi_host()
            self._session.call_xenapi("pool.eject", host_ref)
        except self.XenAPI.Failure, e:
            LOG.error(_("Pool-eject failed: %(e)s") % locals())
            raise exception.AggregateError(aggregate_id=aggregate_id,
                                           action='remove_from_aggregate',
                                           reason=str(e.details))

    def _init_pool(self, aggregate_id, aggregate_name):
        try:
            pool_ref = self._session.call_xenapi("pool.get_all")[0]
            self._session.call_xenapi("pool.set_name_label",
                                      pool_ref, aggregate_name)
        except self.XenAPI.Failure, e:
            LOG.error(_("Unable to set up pool: %(e)s.") % locals())
            raise exception.AggregateError(aggregate_id=aggregate_id,
                                           action='add_to_aggregate',
                                           reason=str(e.details))

    def _clear_pool(self, aggregate_id):
        try:
            pool_ref = self._session.call_xenapi('pool.get_all')[0]
            self._session.call_xenapi('pool.set_name_label', pool_ref, '')
        except self.XenAPI.Failure, e:
            LOG.error(_("Pool-eject failed: %(e)s") % locals())
            raise exception.AggregateError(aggregate_id=aggregate_id,
                                           action='remove_from_aggregate',
                                           reason=_('Unable to eject %(host)s '
                                              'from the pool; pool not empty')
                                              % locals())


def forward_request(context, request_type, master,
                    aggregate_id, slave_compute, slave_address):
    """Casts add/remove requests to the pool master."""
    # replace the address from the xenapi connection url
    # because this might be 169.254.0.1, i.e. xenapi
    sender_url = swap_xapi_host(FLAGS.xenapi_connection_url, slave_address)
    rpc.cast(context, db.queue_get_for(context, FLAGS.compute_topic, master),
             {"method": request_type,
              "args": {"aggregate_id": aggregate_id,
                       "host": slave_compute,
                       "url": sender_url,
                       "user": FLAGS.xenapi_connection_username,
                       "passwd": FLAGS.xenapi_connection_password,
                       "compute_uuid": vm_utils.get_this_vm_uuid(), },
             })


def swap_xapi_host(url, host_addr):
    temp_url = urlparse.urlparse(url)
    _, sep, port = temp_url.netloc.partition(':')
    return url.replace(url.netloc, '%s%s%s' % (host_addr, sep, port))

