# Copyright (C) 2023 Canonical
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Ubuntu MaaS 2.x CLI support code."""

import logging
import subprocess

logger = logging.getLogger()


class ConfigureMaasStorage:
    def __init__(self, maas_profile, node_id):
        self.maas_profile = maas_profile
        self.node_id = node_id

    def call_cmd(self):
        """subprocess placeholder"""
        pass

    def call_cmd_json(self):
        """subprocess placeholder"""
        pass

    def clear_storage_config(self):
        blockdevice_set = self.read_blockdevices(
            self.maas_profile, self.node_id
        )
        for blockdevice in blockdevice_set:
            if blockdevice["type"] == "virtual":
                continue
            for partition in blockdevice["partitions"]:
                self.call_cmd(
                    [
                        "maas",
                        self.maas_profile,
                        "partition",
                        "delete",
                        self.node_id,
                        str(blockdevice["id"]),
                        str(partition["id"]),
                    ]
                )
            if blockdevice["filesystem"] is not None:
                if blockdevice["filesystem"]["mount_point"] is not None:
                    self.call_cmd_json(
                        [
                            "maas",
                            self.maas_profile,
                            "block-device",
                            "unmount",
                            self.node_id,
                            str(blockdevice["id"]),
                        ]
                    )
                self.call_cmd_json(
                    [
                        "maas",
                        self.maas_profile,
                        "block-device",
                        "unformat",
                        self.node_id,
                        blockdevice["id"],
                    ]
                )
                self.call_cmd_json(
                    [
                        "maas",
                        self.maas_profile,
                        "block-device",
                        "unformat",
                        self.node_id,
                        str(blockdevice["id"]),
                    ]
                )

    def mount_blockdevice(self, blockdevice_id, mount_point):
        self.call_cmd_json(
            [
                "maas",
                self.maas_profile,
                "block-device",
                "mount",
                self.node_id,
                blockdevice_id,
                f"mount_point={mount_point}",
            ]
        )

    def mount_partition(self, blockdevice_id, partition_id, mount_point):
        self.call_cmd_json(
            [
                "maas",
                self.maas_profile,
                "partition",
                "mount",
                self.node_id,
                blockdevice_id,
                partition_id,
                f"mount_point={mount_point}",
            ]
        )

    def format_partition(self, blockdevice_id, partition_id, fstype, label):
        self.call_cmd_json(
            [
                "maas",
                self.maas_profile,
                "partition",
                "format",
                self.node_id,
                blockdevice_id,
                partition_id,
                f"fstype={fstype}",
                f"label={label}",
            ]
        )

    def create_partition(self, blockdevice_id, size=None):
        command = [
            "maas",
            self.maas_profile,
            "partitions",
            "create",
            self.node_id,
            blockdevice_id,
        ]
        if size is not None:
            command.append(f"size={size}")
        return self.call_cmd_json(command)

    def set_boot_disk(self, blockdevice_id):
        self.call_cmd(
            [
                "maas",
                self.maas_profile,
                "block-device",
                "set-boot-disk",
                self.node_id,
                blockdevice_id,
            ]
        )

    def update_blockdevice(self, blockdevice_id, opts=None):
        """Update a block-device.

        :param str self.maas_profile: The maas cli profile to use.
        :param str self.node_id: The self.node_id of the machine.
        :param str blockdevice_id: The id of the block-device.
        :param dict opts: A dictionary of options to apply.
        :returns: The updated MAAS API block-device dictionary.
        """
        command = [
            "maas",
            self.maas_profile,
            "block-device",
            "update",
            self.node_id,
            blockdevice_id,
        ]
        if opts is not None:
            for k, v in opts.items():
                command.append(f"{k}={v}")
        return self.call_cmd_json(command)

    def format_blockdevice(self, blockdevice_id, fstype, label):
        self.call_cmd_json(
            [
                "maas",
                self.maas_profile,
                "block-device",
                "format",
                self.node_id,
                blockdevice_id,
                f"fstype={fstype}",
                f"label={label}",
            ]
        )

    def get_cache_set(self, blockdevice_id, partition_id):
        command = [
            "maas",
            self.maas_profile,
            "bcache-cache-sets",
            "read",
            self.node_id,
        ]

        cache_sets = self.call_cmd_json(command)

        for cache_set in cache_sets:
            # multiple cache devices per cache set are not supported
            # by bcache upstream and not supported in MAAS as well
            cache_device = cache_set["cache_device"]
            backing_type = cache_device["type"]
            backing_device_id = cache_device["id"]

            if backing_type not in ["physical", "partition"]:
                raise Exception(
                    "Unknown backing device type %s" % (backing_type)
                )
            elif (
                backing_type == "physical"
                and blockdevice_id == backing_device_id
            ):
                return cache_set
            elif (
                backing_type == "partition"
                and partition_id == backing_device_id
            ):
                return cache_set

        # no cache sets
        return None

    def create_cache_set(self, blockdevice_id, partition_id):
        command = [
            "maas",
            self.maas_profile,
            "bcache-cache-sets",
            "create",
            self.node_id,
        ]
        if blockdevice_id is not None:
            command.append(f"cache_device={blockdevice_id}")
        else:
            command.append(f"cache_partition={partition_id}")
        return self.call_cmd_json(command)

    def create_bcache(
        self,
        name,
        cache_set_id,
        blockdevice_id,
        partition_id,
        cache_mode,
    ):
        command = [
            "maas",
            self.maas_profile,
            "bcaches",
            "create",
            self.node_id,
            f"name={name}",
            f"cache_set={cache_set_id}",
            f"cache_mode={cache_mode}",
        ]
        if blockdevice_id is not None:
            command.append(f"backing_device={blockdevice_id}")
        else:
            command.append(f"backing_partition={partition_id}")
        return self.call_cmd_json(command)

    def read_bcaches(self):
        command = ["maas", self.maas_profile, "bcaches", "read", self.node_id]
        return self.call_cmd_json(command)

    def create_volume_group(self, name, block_devices=None, partitions=None):
        command = [
            "maas",
            self.maas_profile,
            "volume-groups",
            "create",
            self.node_id,
            f"name={name}",
        ]

        if block_devices:
            for device in block_devices:
                command.append(f"block_devices={device}")
        if partitions:
            for partition in partitions:
                command.append(f"partitions={partition}")
        return self.call_cmd_json(command)

    def create_logical_volume(self, volume_group, name=None, size=None):
        command = [
            "maas",
            self.maas_profile,
            "volume-group",
            "create-logical-volume",
            self.node_id,
            str(volume_group),
            f"name={name}",
            f"size={size}",
        ]
        return self.call_cmd_json(command)

    def read_blockdevices(self):
        command = [
            "maas",
            self.maas_profile,
            "block-devices",
            "read",
            self.node_id,
        ]
        return self.call_cmd_json(command)

    def get_disksize_real_value(self, value):
        """Sizes can use M, G, T suffixes."""
        try:
            real_value = str(int(value))
            return real_value
        except ValueError as error:
            for n, suffix in enumerate(["M", "G", "T"]):
                if value[-1].capitalize() == suffix:
                    return str(int(float(value[:-1]) * 1000 ** (n + 2)))
            raise error

    def round_disk_size(self, disk_size):
        return round(float(disk_size) / (5 * 1000**3)) * (5 * 1000**3)

    def humanized_size(self, num, system_unit=1024):
        for suffix in ["", "K", "M", "G", "T", "P", "E", "Z"]:
            if num < system_unit:
                return "%d%s" % (round(num), suffix)
            num = num / system_unit
        return "%dY" % (round(num))

    def setup_lvm_vg(self, disk_device_to_blockdevice, partition_map):
        """Setup LVM volume groups on a specific machine."""
        vgs = {}
        for vg in self.entries_of_type(disk_config, "lvm_volgroup"):
            logging.info(
                "setting up volume group %s on %s", vg["id"], self.node_id
            )
            block_devices = []
            partitions = []
            self.append_disk_or_partition(
                vg["devices"],
                block_devices,
                partitions,
                disk_device_to_blockdevice,
                partition_map,
            )
            new_vg = self.create_volume_group(
                self.self.maas_profile,
                self.node_id,
                name=vg["name"],
                block_devices=block_devices,
                partitions=partitions,
            )
            vgs[vg["name"]] = new_vg["id"]
        return vgs

    def setup_lvm_lv(
        self, disk_config, disk_device_to_blockdevice, partition_map, vgs
    ):
        """Setup LVM logical volumes on a specific machine."""
        lv_to_blockdevice = {}
        for lv in self.entries_of_type(disk_config, "lvm_partition"):
            logging.info(
                "setting up logical volume %s on %s", lv["id"], self.node_id
            )
            if lv["volgroup"] in vgs:
                new_lv = self.create_logical_volume(
                    self.self.maas_profile,
                    self.node_id,
                    name=lv["name"],
                    volume_group=vgs[lv["volgroup"]],
                    size=self.get_disksize_real_value(lv["size"]),
                )
                lv_to_blockdevice[lv["id"]] = new_lv
        return lv_to_blockdevice

    def partition_disks(self, disk_config, disk_device_to_blockdevice):
        """Partition the disks on a specific machine."""
        # Find and create the partitions on this disk
        partitions = self.entries_of_type(disk_config, "partition")
        partitions = sorted(partitions, key=lambda k: k["number"])
        # maps config partition ids to maas partition ids
        partition_map = {}
        for partition in partitions:
            disk_maas_id = disk_device_to_blockdevice[partition["device"]][
                "id"
            ]
            logging.info("creating partition %s", partition["id"])
            # If size is not specified, all avaiable space is used
            if "size" not in partition or not partition["size"]:
                disksize_value = None
            else:
                disksize_value = self.get_disksize_real_value(partition["size"])

            partition_id = self.create_partition(
                self.self.maas_profile,
                self.node_id,
                str(disk_maas_id),
                size=disksize_value,
            )["id"]
            partition_map[partition["id"]] = {
                "partition_id": partition_id,
                "blockdevice_id": disk_maas_id,
            }
        return partition_map

    def update_disks(self, disk_config, disk_device_to_blockdevice):
        """Update the settings for disks on a machine.

        :param str self.node_id: The self.node_id of the machine.
        :param list disk_config: The disk config for a machine.
        :param dict disk_device_to_blockdevice: maps config disk ids
            to maas API block-devices.
        """
        for disk in self.entries_of_type(disk_config, "disk"):
            if "boot" in disk and disk["boot"]:
                logging.warn(
                    "Setting boot disk only applies to"
                    " legacy (non-EFI) booting systems!"
                )
                self.set_boot_disk(
                    self.self.maas_profile,
                    self.node_id,
                    str(disk_device_to_blockdevice[disk["id"]]["id"]),
                )
            if "name" in disk:
                self.update_blockdevice(
                    self.self.maas_profile,
                    self.node_id,
                    str(disk_device_to_blockdevice[disk["id"]]["id"]),
                    opts={"name": disk["name"]},
                )

    def setup_storage(self, machine_info, config):
        """Setup storage on a specific machine."""
        self.node_id = str(machine_info["self.node_id"])
        logging.info("Clearing previous storage configuration")
        self.clear_storage_config(self.self.maas_profile, self.node_id)
        config_disk_to_blockdevice = self.map_bucket_disks_to_machine_disks(
            machine_info
        )
        disk_device_to_blockdevice = self.map_disk_device_to_blockdevice(
            config["disks"], config_disk_to_blockdevice
        )
        # apply updates to the disks.
        self.update_disks(
            self.node_id, config["disks"], disk_device_to_blockdevice
        )
        # partition disks and keep map of config partitions
        # to partition ids in maas
        partition_map = self.partition_disks(
            self.node_id, config["disks"], disk_device_to_blockdevice
        )
        # DM devices are treated just like disks by the api
        disk_device_to_blockdevice.update(raid_to_blockdevice)
        # setup bcaches first, to make volumes on top of bcache possible
        self.setup_bcaches(
            self.node_id,
            config["disks"],
            disk_device_to_blockdevice,
            partition_map,
        )
        lv_to_blockdevice = self.setup_lvm_lv(
            self.node_id,
            config["disks"],
            disk_device_to_blockdevice,
            partition_map,
            vgs=self.setup_lvm_vg(
                self.node_id,
                config["disks"],
                disk_device_to_blockdevice,
                partition_map,
            ),
        )
        disk_device_to_blockdevice.update(lv_to_blockdevice)
        # format volumes and create mount points
        self.apply_formats(
            self.node_id,
            config["disks"],
            partition_map,
            disk_device_to_blockdevice,
        )
        self.create_mounts(
            self.node_id,
            config["disks"],
            partition_map,
            disk_device_to_blockdevice,
        )
