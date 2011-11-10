# Copyright (c) 2011 Openstack, LLC.
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


import nova.scheduler
from nova.scheduler.filters import abstract_filter


class InstanceMetadataFilter(abstract_filter.AbstractHostFilter):
    """HostFilter based on InstanceTypeExtraSpecs records and
    instance properties.
    """
    def instance_metadata_to_filter(self, instance_metadata):
        """Use instance_metadata to filter hosts."""
        return (self._full_name(), instance_metadata)

    def _satisfies_extra_specs(self, capabilities, instance_extra_specs):
        """Check that the capabilities provided by the compute service
        satisfy the extra specs associated with the instance type.
        """
        if instance_extra_specs is None:
            return True
        # NOTE(lorinh): For now, we are just checking exact matching on the
        # values. Later on, we want to handle numerical
        # values so we can represent things like number of GPU cards
        # version of hypervisor etc.
        try:
            for key, value in instance_extra_specs.iteritems():
                if capabilities[key] != value:
                    return False
        except KeyError:
            return False
        return True

    def filter_hosts(self, zone_manager, query):
        """Return a list of hosts that can create instance_metadata."""
        instance_metadata = query
        instance_type = instance_metadata.get("instance_type")
        instance_properties = instance_metadata.get("instance_properties")

        selected_hosts = []
        for host, services in zone_manager.service_states.iteritems():
            capabilities = services.get('compute', {})
            if not capabilities:
                continue
            extra_specs = 'extra_specs' in instance_type and \
                           instance_type['extra_specs']

            if ((instance_properties['hypervisor'] == \
                 capabilities['hypervisor']) and \
                 self._satisfies_extra_specs(capabilities, extra_specs)):
                selected_hosts.append((host, capabilities))
        return selected_hosts
