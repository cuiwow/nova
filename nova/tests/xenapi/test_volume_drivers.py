# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright (c) 2013 Citrix Systems, Inc.
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


from nova import test
from nova.virt.xenapi import volume_drivers


class TestConstructor(test.TestCase):
    def test_constructor_stores_variables(self):
        drv = volume_drivers.LegacyDriver(
            volumeops="volumeops", session="session")

        self.assertEquals("volumeops", drv.volumeops)
        self.assertEquals("session", drv.session)
