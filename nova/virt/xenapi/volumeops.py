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
Management class for Storage-related functions (attach, detach, etc).
"""

from nova import exception
from nova.openstack.common import log as logging
from nova.virt.xenapi import vm_utils
from nova.virt.xenapi import volume_utils


LOG = logging.getLogger(__name__)


class VolumeOps(object):
    """
    Management class for Volume-related tasks
    """

    def __init__(self, session):
        self._session = session

    def _forget_sr_if_present(self, sr_uuid):
        sr_ref = volume_utils.find_sr_by_uuid(self._session, sr_uuid)
        if sr_ref is None:
            LOG.debug(_('SR %s not found in the xapi database') % sr_uuid)
            return
        try:
            volume_utils.forget_sr(self._session, sr_uuid)
        except volume_utils.StorageError, exc:
            LOG.exception(exc)
            raise exception.NovaException(_('Could not forget SR'))

    def attach_volume(self, connection_info, instance_name, mountpoint,
                      hotplug=True):
        """Attach volume storage to VM instance"""

        vm_ref = vm_utils.vm_ref_or_raise(self._session, instance_name)

        # NOTE: No Resource Pool concept so far
        LOG.debug(_("Attach_volume: %(connection_info)s, %(instance_name)s,"
                " %(mountpoint)s") % locals())

        driver_type = connection_info['driver_volume_type']
        if driver_type not in ['iscsi', 'xensm']:
            raise exception.VolumeDriverNotFound(driver_type=driver_type)

        connection_data = connection_info['data']
        dev_number = volume_utils.get_device_number(mountpoint)

        self._connect_volume(connection_data, dev_number, instance_name,
                            vm_ref, hotplug=hotplug)

        LOG.info(_('Mountpoint %(mountpoint)s attached to'
                ' instance %(instance_name)s') % locals())

    def _connect_volume(self, connection_data, dev_number, instance_name,
                       vm_ref, hotplug=True):

        description = 'Disk-for:%s' % instance_name
        uuid, label, sr_params = volume_utils.parse_sr_info(connection_data,
                                                            description)

        # Introduce SR
        try:
            sr_ref = volume_utils.introduce_sr_unless_present(
                self._session, uuid, label, sr_params)
            LOG.debug(_('Introduced %(label)s as %(sr_ref)s.') % locals())
        except self._session.XenAPI.Failure, exc:
            LOG.exception(exc)
            raise volume_utils.StorageError(
                                _('Unable to introduce Storage Repository'))

        vdi_uuid = None
        target_lun = None
        if 'vdi_uuid' in connection_data:
            vdi_uuid = connection_data['vdi_uuid']
        elif 'target_lun' in connection_data:
            target_lun = connection_data['target_lun']
        else:
            vdi_uuid = None

        # Introduce VDI  and attach VBD to VM
        try:
            vdi_ref = volume_utils.introduce_vdi(self._session, sr_ref,
                                                 vdi_uuid, target_lun)
        except volume_utils.StorageError, exc:
            LOG.exception(exc)
            self._forget_sr_if_present(uuid)
            raise Exception(_('Unable to create VDI on SR %(sr_ref)s for'
                    ' instance %(instance_name)s') % locals())

        try:
            vbd_ref = vm_utils.create_vbd(self._session, vm_ref, vdi_ref,
                                          dev_number, bootable=False,
                                          osvol=True)
        except self._session.XenAPI.Failure, exc:
            LOG.exception(exc)
            self._forget_sr_if_present(uuid)
            raise Exception(_('Unable to use SR %(sr_ref)s for'
                              ' instance %(instance_name)s') % locals())

        if hotplug:
            try:
                self._session.call_xenapi("VBD.plug", vbd_ref)
            except self._session.XenAPI.Failure, exc:
                LOG.exception(exc)
                self._forget_sr_if_present(uuid)
                raise Exception(_('Unable to attach volume to instance %s')
                                % instance_name)

    def detach_volume(self, connection_info, instance_name, mountpoint):
        """Detach volume storage to VM instance"""

        vm_ref = vm_utils.vm_ref_or_raise(self._session, instance_name)

        # Detach VBD from VM
        LOG.debug(_("Detach_volume: %(instance_name)s, %(mountpoint)s")
                % locals())
        device_number = volume_utils.mountpoint_to_number(mountpoint)
        try:
            vbd_ref = vm_utils.find_vbd_by_number(self._session, vm_ref,
                                                  device_number)
        except volume_utils.StorageError, exc:
            LOG.exception(exc)
            raise Exception(_('Unable to locate volume %s') % mountpoint)

        try:
            vm_rec = self._session.call_xenapi("VM.get_record", vm_ref)
            sr_ref = volume_utils.find_sr_from_vbd(self._session, vbd_ref)
            if vm_rec['power_state'] != 'Halted':
                vm_utils.unplug_vbd(self._session, vbd_ref)
        except volume_utils.StorageError, exc:
            LOG.exception(exc)
            raise Exception(_('Unable to detach volume %s') % mountpoint)
        try:
            vm_utils.destroy_vbd(self._session, vbd_ref)
        except volume_utils.StorageError, exc:
            LOG.exception(exc)
            raise Exception(_('Unable to destroy vbd %s') % mountpoint)

        # Forget SR only if no other volumes on this host are using it
        try:
            volume_utils.purge_sr(self._session, sr_ref)
        except volume_utils.StorageError, exc:
            LOG.exception(exc)
            raise Exception(_('Error purging SR %s') % sr_ref)

        LOG.info(_('Mountpoint %(mountpoint)s detached from'
                ' instance %(instance_name)s') % locals())
