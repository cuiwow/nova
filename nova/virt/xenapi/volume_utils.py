# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright (c) 2010 Citrix Systems, Inc.
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
Helper methods for operations related to the management of volumes,
and storage repositories
"""

import re
import string

from nova import db
from nova import context
from nova import exception
from nova import flags
from nova import log as logging
from nova import utils
from nova.virt.xenapi import HelperBase

FLAGS = flags.FLAGS
LOG = logging.getLogger("nova.virt.xenapi.volume_utils")


class StorageError(Exception):
    """To raise errors related to SR, VDI, PBD, and VBD commands"""

    def __init__(self, message=None):
        super(StorageError, self).__init__(message)


class VolumeHelper(HelperBase):
    """
    The class that wraps the helper methods together.
    """

    @classmethod
    def create_sr(cls, session, label, params):

        LOG.debug(_("creating sr within volume_utils"))
        type = params['sr_type']
        del params['sr_type']
        LOG.debug(_('type is = %s') % type)
        if 'name_description' in params:
            desc = params['name_description']
            LOG.debug(_('name = %s') % desc)
            del params['name_description']
        else:
            desc = ''

        try:
            sr_ref = session.get_xenapi().SR.create(
                        session.get_xenapi_host(),
                        params,
                        '0', label, desc, type, '', False, {})
            LOG.debug(_('Created %(label)s as %(sr_ref)s.') % locals())
            return sr_ref

        except cls.XenAPI.Failure, exc:
            LOG.exception(exc)
            raise StorageError(_('Unable to create Storage Repository'))

    @classmethod
    def introduce_sr(cls, session, sr_uuid, label, params):

        LOG.debug(_("introducing sr within volume_utils"))
        type = params['sr_type']
        del params['sr_type']
        LOG.debug(_('type is = %s') % type)
        if 'name_description' in params:
            desc = params['name_description']
            LOG.debug(_('name = %s') % desc)
            del params['name_description']
        else:
            desc = ''
        if 'id' in params:
            del params['id']
        LOG.debug(params)

        try:
            sr_ref = session.get_xenapi().SR.introduce(sr_uuid,
                                                       label,
                                                       desc,
                                                       type,
                                                       '',
                                                       False,
                                                       params,)
            LOG.debug(_('Introduced %(label)s as %(sr_ref)s.') % locals())

            #Create pbd
            LOG.debug(_('Creating pbd for SR'))
            pbd_ref = cls.create_pbd(session, sr_ref, params)
            LOG.debug(_('Plugging SR'))
            #Plug pbd
            session.get_xenapi().PBD.plug(pbd_ref)
            session.get_xenapi().SR.scan(sr_ref)
            return sr_ref

        except cls.XenAPI.Failure, exc:
            LOG.exception(exc)
            raise StorageError(_('Unable to introduce Storage Repository'))

    @classmethod
    def forget_sr(cls, session, sr_uuid):
        """
        Forgets the storage repository without destroying the VDIs within
        """
        try:
            sr_ref = session.get_xenapi().SR.get_by_uuid(sr_uuid)
        except cls.XenAPI.Failure, exc:
            LOG.exception(exc)
            raise StorageError(_('Unable to get SR using uuid'))

        LOG.debug(_('Forgetting SR %s...') % sr_ref)

        try:
            cls.unplug_pbds(session, sr_ref)
            sr_ref = session.get_xenapi().SR.forget(sr_ref)

        except cls.XenAPI.Failure, exc:
            LOG.exception(exc)
            raise StorageError(_('Unable to forget Storage Repository'))

    @classmethod
    def find_sr_by_uuid(cls, session, sr_uuid):
        """
        Return the storage repository given a uuid.
        """
        sr_refs = session.get_xenapi().SR.get_all()
        for sr_ref in sr_refs:
            sr_rec = session.get_xenapi().SR.get_record(sr_ref)
            if sr_rec['uuid'] == sr_uuid:
                return sr_ref
        return None

    @classmethod
    def find_sr_from_vbd(cls, session, vbd_ref):
        """Find the SR reference from the VBD reference"""
        try:
            vdi_ref = session.get_xenapi().VBD.get_VDI(vbd_ref)
            sr_ref = session.get_xenapi().VDI.get_SR(vdi_ref)
        except cls.XenAPI.Failure, exc:
            LOG.exception(exc)
            raise StorageError(_('Unable to find SR from VBD %s') % vbd_ref)
        return sr_ref

    @classmethod
    def create_vbd(cls, session, vm_ref, vdi_ref, userdevice, bootable):
        """Create a VBD record.  Returns a Deferred that gives the new
        VBD reference."""
        vbd_rec = {}
        vbd_rec['VM'] = vm_ref
        vbd_rec['VDI'] = vdi_ref
        vbd_rec['userdevice'] = str(userdevice)
        vbd_rec['bootable'] = bootable
        vbd_rec['mode'] = 'RW'
        vbd_rec['type'] = 'disk'
        vbd_rec['unpluggable'] = True
        vbd_rec['empty'] = False
        vbd_rec['other_config'] = {}
        vbd_rec['qos_algorithm_type'] = ''
        vbd_rec['qos_algorithm_params'] = {}
        vbd_rec['qos_supported_algorithms'] = []
        LOG.debug(_('Creating VBD for VM %(vm_ref)s,'
                ' VDI %(vdi_ref)s ... ') % locals())
        vbd_ref = session.call_xenapi('VBD.create', vbd_rec)
        LOG.debug(_('Created VBD %(vbd_ref)s for VM %(vm_ref)s,'
                ' VDI %(vdi_ref)s.') % locals())
        return vbd_ref

    @classmethod
    def create_pbd(cls, session, sr_ref, params):
        pbd_rec = {}
        pbd_rec['host'] = session.get_xenapi_host()
        pbd_rec['SR'] = sr_ref
        pbd_rec['device_config'] = params
        pbd_ref = session.get_xenapi().PBD.create(pbd_rec)
        return pbd_ref

    @classmethod
    def unplug_pbds(cls, session, sr_ref):
        pbds = []
        try:
            pbds = session.get_xenapi().SR.get_PBDs(sr_ref)
        except cls.XenAPI.Failure, exc:
            LOG.warn(_('Ignoring exception %(exc)s when getting PBDs'
                    ' for %(sr_ref)s') % locals())
        for pbd in pbds:
            try:
                session.get_xenapi().PBD.unplug(pbd)
            except cls.XenAPI.Failure, exc:
                LOG.warn(_('Ignoring exception %(exc)s when unplugging'
                        ' PBD %(pbd)s') % locals())

    @classmethod
    def introduce_vdi(cls, session, sr_ref, vdi_uuid=None):
        """Introduce VDI in the host"""
        try:
            session.get_xenapi().SR.scan(sr_ref)
            if vdi_uuid:
                LOG.debug("vdi_uuid: %s" % vdi_uuid)
                vdi_ref = session.get_xenapi().VDI.get_by_uuid(vdi_uuid)
            else:
                vdi_ref = (session.get_xenapi().SR.get_VDIs(sr_ref))[0]
        except cls.XenAPI.Failure, exc:
            LOG.exception(exc)
            raise StorageError(_('Unable to introduce VDI on SR %s') % sr_ref)

        try:
            vdi_rec = session.get_xenapi().VDI.get_record(vdi_ref)
            LOG.debug(vdi_rec)
            LOG.debug(type(vdi_rec))
        except cls.XenAPI.Failure, exc:
            LOG.exception(exc)
            raise StorageError(_('Unable to get record'
                                 ' of VDI %s on') % vdi_ref)

        if vdi_rec['managed']:
            # We do not need to introduce the vdi
            return vdi_ref

        try:
            return session.get_xenapi().VDI.introduce(
                vdi_rec['uuid'],
                vdi_rec['name_label'],
                vdi_rec['name_description'],
                vdi_rec['SR'],
                vdi_rec['type'],
                vdi_rec['sharable'],
                vdi_rec['read_only'],
                vdi_rec['other_config'],
                vdi_rec['location'],
                vdi_rec['xenstore_data'],
                vdi_rec['sm_config'])
        except cls.XenAPI.Failure, exc:
            LOG.exception(exc)
            raise StorageError(_('Unable to introduce VDI for SR %s')
                                % sr_ref)

    @classmethod
    def purge_sr(cls, session, sr_ref):
        try:
            sr_rec = session.get_xenapi().SR.get_record(sr_ref)
            vdi_refs = session.get_xenapi().SR.get_VDIs(sr_ref)
        except StorageError, ex:
            LOG.exception(ex)
            raise StorageError(_('Error finding vdis in SR %s') % sr_ref)

        for vdi_ref in vdi_refs:
            try:
                vbd_refs = session.get_xenapi().VDI.get_VBDs(vdi_ref)
            except StorageError, ex:
                LOG.exception(ex)
                raise StorageError(_('Unable to find vbd for vdi %s') % \
                                   vdi_ref)
            if len(vbd_refs) > 0:
                return

        cls.forget_sr(session, sr_rec['uuid'])

    @classmethod
    def parse_volume_info(cls, device_path, mountpoint, dev_params):
        """
        Parse device_path and mountpoint as they can be used by XenAPI.
        In particular, the mountpoint (e.g. /dev/sdc) must be translated
        into a numeric literal.
        FIXME(armando):
        As for device_path, currently cannot be used as it is,
        because it does not contain target information. As for interim
        solution, target details are passed either via Flags or obtained
        by iscsiadm. Long-term solution is to add a few more fields to the
        db in the iscsi_target table with the necessary info and modify
        the iscsi driver to set them.
        """

        target_host = _get_target_host(dev_params['target_portal'])
        target_port = _get_target_port(dev_params['target_portal'])

        volume_info = {}
        volume_info['id'] = dev_params['id']
        volume_info['target'] = target_host
        volume_info['port'] = target_port
        volume_info['targetIQN'] = dev_params['target_iqn']

        if  'auth_method' in dev_params and \
             dev_params['auth_method'] == 'CHAP':
            volume_info['chapuser'] = dev_params['auth_username']
            volume_info['chappassword'] = dev_params['auth_password']

        LOG.debug(_("Volume = %(id)s,host = %(target)s,\
                  port = %(port)s,iqn = %(targetIQN)s") % volume_info)
        return volume_info

    @classmethod
    def mountpoint_to_number(cls, mountpoint):
        """Translate a mountpoint like /dev/sdc into a numeric"""
        if mountpoint.startswith('/dev/'):
            mountpoint = mountpoint[5:]
        if re.match('^[hs]d[a-p]$', mountpoint):
            return (ord(mountpoint[2:3]) - ord('a'))
        elif re.match('^vd[a-p]$', mountpoint):
            return (ord(mountpoint[2:3]) - ord('a'))
        elif re.match('^[0-9]+$', mountpoint):
            return string.atoi(mountpoint, 10)
        else:
            LOG.warn(_('Mountpoint cannot be translated: %s'), mountpoint)
            return -1


def _get_target_host(iscsi_string):
    """Retrieve target host"""
    if iscsi_string:
        return iscsi_string[0:iscsi_string.find(':')]
    elif iscsi_string is None and FLAGS.target_host:
        return FLAGS.target_host


def _get_target_port(iscsi_string):
    """Retrieve target port"""
    if iscsi_string:
        return iscsi_string[iscsi_string.find(':') + 1:]
    elif  iscsi_string is None and FLAGS.target_port:
        return FLAGS.target_port
