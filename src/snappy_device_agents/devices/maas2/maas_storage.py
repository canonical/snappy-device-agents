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

from snappy_device_agents.devices import ProvisioningError

logger = logging.getLogger()


class ConfigureMaasStorage:
    def __init__(self, maas_user, node_id):
        self.maas_user = maas_user
        self.node_id = node_id

    def call_cmd(self, cmd):
        """subprocess placeholder"""
        self._logger_info("Acquiring node")
        proc = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False
        )
        if proc.returncode:
            self._logger_error(f"maas error running: {' '.join(cmd)}")
            raise ProvisioningError(proc.stdout.decode())

    def clear_storage_config(self):
        blockdevice_set = self.read_blockdevices()
        for blockdevice in blockdevice_set:
            if blockdevice["type"] == "virtual":
                continue
            for partition in blockdevice["partitions"]:
                self.call_cmd(
                    [
                        "maas",
                        self.maas_user,
                        "partition",
                        "delete",
                        self.node_id,
                        str(blockdevice["id"]),
                        str(partition["id"]),
                    ]
                )
            if blockdevice["filesystem"] is not None:
                if blockdevice["filesystem"]["mount_point"] is not None:
                    self.call_cmd(
                        [
                            "maas",
                            self.maas_user,
                            "block-device",
                            "unmount",
                            self.node_id,
                            str(blockdevice["id"]),
                        ]
                    )

                self.call_cmd(
                    [
                        "maas",
                        self.maas_user,
                        "block-device",
                        "unformat",
                        self.node_id,
                        blockdevice["id"],
                    ]
                )
                self.call_cmd(
                    [
                        "maas",
                        self.maas_user,
                        "block-device",
                        "unformat",
                        self.node_id,
                        str(blockdevice["id"]),
                    ]
                )

    def mount_blockdevice(self, blockdevice_id, mount_point):
        self.call_cmd(
            [
                "maas",
                self.maas_user,
                "block-device",
                "mount",
                self.node_id,
                blockdevice_id,
                f"mount_point={mount_point}",
            ]
        )

    def mount_partition(self, blockdevice_id, partition_id, mount_point):
        self.call_cmd(
            [
                "maas",
                self.maas_user,
                "partition",
                "mount",
                self.node_id,
                blockdevice_id,
                partition_id,
                f"mount_point={mount_point}",
            ]
        )

    def format_partition(self, blockdevice_id, partition_id, fstype, label):
        self.call_cmd(
            [
                "maas",
                self.maas_user,
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
        cmd = [
            "maas",
            self.maas_user,
            "partitions",
            "create",
            self.node_id,
            blockdevice_id,
        ]
        if size is not None:
            cmd.append(f"size={size}")
        return self.call_cmd(cmd)

    def set_boot_disk(self, blockdevice_id):
        self.call_cmd(
            [
                "maas",
                self.maas_user,
                "block-device",
                "set-boot-disk",
                self.node_id,
                blockdevice_id,
            ]
        )

    def update_blockdevice(self, blockdevice_id, opts=None):
        """Update a block-device.

        :param str self.maas_user: The maas cli profile to use.
        :param str self.node_id: The self.node_id of the machine.
        :param str blockdevice_id: The id of the block-device.
        :param dict opts: A dictionary of options to apply.
        :returns: The updated MAAS API block-device dictionary.
        """
        cmd = [
            "maas",
            self.maas_user,
            "block-device",
            "update",
            self.node_id,
            blockdevice_id,
        ]
        if opts is not None:
            for k, v in opts.items():
                cmd.append(f"{k}={v}")
        return self.call_cmd(cmd)

    def format_blockdevice(self, blockdevice_id, fstype, label):
        self.call_cmd(
            [
                "maas",
                self.maas_user,
                "block-device",
                "format",
                self.node_id,
                blockdevice_id,
                f"fstype={fstype}",
                f"label={label}",
            ]
        )

    def get_cache_set(self, blockdevice_id, partition_id):
        cmd = [
            "maas",
            self.maas_user,
            "bcache-cache-sets",
            "read",
            self.node_id,
        ]

        cache_sets = self.call_cmd(cmd)

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
        cmd = [
            "maas",
            self.maas_user,
            "bcache-cache-sets",
            "create",
            self.node_id,
        ]
        if blockdevice_id is not None:
            cmd.append(f"cache_device={blockdevice_id}")
        else:
            cmd.append(f"cache_partition={partition_id}")
        return self.call_cmd(cmd)

    def create_bcache(
        self,
        name,
        cache_set_id,
        blockdevice_id,
        partition_id,
        cache_mode,
    ):
        cmd = [
            "maas",
            self.maas_user,
            "bcaches",
            "create",
            self.node_id,
            f"name={name}",
            f"cache_set={cache_set_id}",
            f"cache_mode={cache_mode}",
        ]
        if blockdevice_id is not None:
            cmd.append(f"backing_device={blockdevice_id}")
        else:
            cmd.append(f"backing_partition={partition_id}")
        return self.call_cmd(cmd)

    def read_bcaches(self):
        cmd = ["maas", self.maas_user, "bcaches", "read", self.node_id]
        return self.call_cmd(cmd)

    def read_blockdevices(self):
        cmd = [
            "maas",
            self.maas_user,
            "block-devices",
            "read",
            self.node_id,
        ]
        return self.call_cmd(cmd)

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
                disksize_value = self.get_disksize_real_value(
                    partition["size"]
                )

            partition_id = self.create_partition(
                self.self.maas_user,
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
                    self.self.maas_user,
                    self.node_id,
                    str(disk_device_to_blockdevice[disk["id"]]["id"]),
                )
            if "name" in disk:
                self.update_blockdevice(
                    self.self.maas_user,
                    self.node_id,
                    str(disk_device_to_blockdevice[disk["id"]]["id"]),
                    opts={"name": disk["name"]},
                )

    def setup_bcaches(
        self, disk_config, disk_device_to_blockdevice, partition_map
    ):
        for bcache in self.entries_of_type(disk_config, "bcache"):
            logging.info(
                "setting up bcache %s on %s", bcache["id"], self.node_id
            )
            cache_device = bcache["cache_device"]
            partition = partition_map.get(cache_device)
            if partition is not None:
                partition_id = partition["partition_id"]
                blockdevice_id = None
            else:
                blockdevice_id = disk_device_to_blockdevice[cache_device]["id"]
                partition_id = None
            # see if one already exists for that backing block device
            # or partition
            cache_set = self.get_cache_set(
                self.user_id, self.node_id, blockdevice_id, partition_id
            )
            if not cache_set:
                cache_set = self.create_cache_set(
                    self.user_id, self.node_id, blockdevice_id, partition_id
                )
            backing_device = bcache["backing_device"]
            partition = partition_map.get(backing_device)
            if partition is not None:
                partition_id = partition["partition_id"]
                blockdevice_id = None
            else:
                blockdevice_id = disk_device_to_blockdevice[backing_device][
                    "id"
                ]
                partition_id = None
            maas_bcache = self.create_bcache(
                self.user_id,
                self.node_id,
                bcache["name"],
                cache_set["id"],
                blockdevice_id,
                partition_id,
                bcache["cache_mode"],
            )
            disk_device_to_blockdevice[bcache["id"]] = maas_bcache[
                "virtual_device"
            ]

    def apply_formats(
        self, disk_config, partition_map, disk_device_to_blockdevice
    ):
        """Apply formats on the volumes of a specific machine."""
        # Format the partitions we created, or disks!
        for _format in self.entries_of_type(disk_config, "format"):
            logging.info("applying format %s", _format["id"])
            if _format["volume"] in partition_map:
                partition_info = partition_map[_format["volume"]]
                self.format_partition(
                    self.user_id,
                    self.node_id,
                    str(partition_info["blockdevice_id"]),
                    str(partition_info["partition_id"]),
                    _format["fstype"],
                    _format["label"],
                )
            else:
                device_info = disk_device_to_blockdevice[_format["volume"]]
                self.format_blockdevice(
                    self.user_id,
                    self.node_id,
                    str(device_info["id"]),
                    _format["fstype"],
                    _format["label"],
                )

    def create_mounts(
        self, disk_config, partition_map, disk_device_to_blockdevice
    ):
        """Create mounts on a specific machine."""
        # Create mounts for the formatted partitions
        for mount in self.entries_of_type(disk_config, "mount"):
            logging.info("applying mount %s", mount["id"])
            volume_name = mount["device"][:-7]  # strip _format
            if volume_name in partition_map:
                partition_info = partition_map[volume_name]
                self.mount_partition(
                    self.user_id,
                    self.node_id,
                    str(partition_info["blockdevice_id"]),
                    str(partition_info["partition_id"]),
                    mount["path"],
                )
            else:
                device_info = disk_device_to_blockdevice[volume_name]
                self.mount_blockdevice(
                    self.user_id,
                    self.node_id,
                    str(device_info["id"]),
                    mount["path"],
                )

    def setup_storage(self, machine_info, config):
        """Setup storage on a specific machine."""
        self.node_id = str(machine_info["self.node_id"])
        logging.info("Clearing previous storage configuration")
        self.clear_storage_config()
        config_disk_to_blockdevice = self.map_bucket_disks_to_machine_disks(
            machine_info
        )
        disk_device_to_blockdevice = self.map_disk_device_to_blockdevice(
            config["disks"], config_disk_to_blockdevice
        )
        # apply updates to the disks.
        self.update_disks(config["disks"], disk_device_to_blockdevice)
        # partition disks and keep map of config partitions
        # to partition ids in maas
        partition_map = self.partition_disks(
            config["disks"], disk_device_to_blockdevice
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
