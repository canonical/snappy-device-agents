# Copyright (C) 2017-2023 Canonical
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

"""Ubuntu Riverside devices support code."""

import json
import logging
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path

import yaml

from snappy_device_agents.devices import ProvisioningError, RecoveryError

logger = logging.getLogger()


class Riverside:

    """Device Agent for Riverside devices."""

    def __init__(self, config, job_data):
        with open(config) as configfile:
            self.config = yaml.safe_load(configfile)
        with open(job_data) as j:
            self.job_data = json.load(j)
        self.agent_name = self.config.get("agent_name")
        self.mount_point = Path("/mnt") / self.agent_name

    def _run_control(self, cmd, timeout=60):
        """
        Run a command on the control host over ssh

        :param cmd:
            Command to run
        :param timeout:
            Timeout (default 60)
        :returns:
            Return output from the command, if any
        """
        control_host = self.config.get("control_host")
        control_user = self.config.get("control_user", "ubuntu")
        ssh_cmd = [
            "ssh",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "{}@{}".format(control_user, control_host),
            cmd,
        ]

        try:
            output = subprocess.check_output(
                ssh_cmd, stderr=subprocess.STDOUT, timeout=timeout
            )
        except subprocess.CalledProcessError as e:
            raise ProvisioningError(e.output)
        return output

    def _copy_to_control(self, local_file, remote_file):
        """
        Copy a file to the control host over ssh

        :param local_file:
            Local filename
        :param remote_file:
            Remote filename
        """
        control_host = self.config.get("control_host")
        control_user = self.config.get("control_user", "ubuntu")
        ssh_cmd = [
            "scp",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            local_file,
            "{}@{}:{}".format(control_user, control_host, remote_file),
        ]
        try:
            output = subprocess.check_output(ssh_cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            raise ProvisioningError(e.output)
        return output

    def provision(self):
        try:
            url = self.job_data["provision_data"]["url"]
        except KeyError:
            raise ProvisioningError(
                'You must specify a "url" value in '
                'the "provision_data" section of '
                "your job_data"
            )
        cmd = self.config.get("control_switch_local_cmd", "stm -ts")

        self._run_control(cmd)
        time.sleep(5)
        logger.info("Flashing Test image")
        try:
            self.flash_test_image(url)
            with self.remote_mount():
                logger.info("Creating Test User")
                self.create_user()
            self.run_post_provision_script()
            logger.info("Booting Test Image")
            cmd = self.config.get("control_switch_device_cmd", "stm -dut")
            self._run_control(cmd)
            self.hardreset()
            self.check_test_image_booted()
        except Exception:
            raise

    def flash_test_image(self, url):
        """
        Flash the image at :image_url to the sd card.

        :param url:
            URL to download the image from
        :raises ProvisioningError:
            If the command times out or anything else fails.
        """
        # First unmount, just in case
        self.unmount_writable_partition()

        test_device = self.config["test_device"]

        cmd = (
            f"(set -o pipefail; curl -sf {url} | zstdcat| "
            f"sudo dd of={test_device} bs=16M)"
        )
        logger.info("Running: %s", cmd)
        try:
            # XXX: I hope 30 min is enough? but maybe not!
            self._run_control(cmd, timeout=1800)
        except Exception:
            raise ProvisioningError("timeout reached while flashing image!")
        try:
            self._run_control("sync")
        except Exception:
            # Nothing should go wrong here, but let's sleep if it does
            logger.warn("Something went wrong with the sync, sleeping...")
            time.sleep(30)
        try:
            self._run_control(
                "sudo hdparm -z {}".format(self.config["test_device"]),
                timeout=30,
            )
        except Exception:
            raise ProvisioningError(
                "Unable to run hdparm to rescan " "partitions"
            )

    def _get_part_labels(self):
        test_device = self.config["test_device"]
        lsblk_data = self._run_control(
            "lsblk -o NAME,LABEL -J {}".format(test_device)
        )
        print(lsblk_data)
        lsblk_json = json.loads(lsblk_data.decode())
        # List of (name, label) pairs
        return [
            (x.get("name"), self.mount_point / x.get("label"))
            for x in lsblk_json["blockdevices"][0]["children"]
            if x.get("name") and x.get("label")
        ]

    @contextmanager
    def remote_mount(self):
        mount_list = self._get_part_labels()
        # Sometimes the labels don't show up to lsblk right away
        if not mount_list:
            print("No valid partitions found, retrying...")
            time.sleep(10)
            mount_list = self._get_part_labels()
        for dev, mount in mount_list:
            try:
                self._run_control("sudo mkdir -p {}".format(mount))
                self._run_control("sudo mount /dev/{} {}".format(dev, mount))
            except Exception:
                # If unmountable or any other error, go on to the next one
                mount_list.remove((dev, mount))
                continue
        try:
            yield self.mount_point
        finally:
            for _, mount in mount_list:
                self._run_control("sudo umount {}".format(mount))

    def hardreset(self):
        """
        Reboot the device.

        :raises RecoveryError:
            If the command times out or anything else fails.

        .. note::
            This function runs the commands specified in 'reboot_script'
            in the config yaml.
        """
        for cmd in self.config.get("reboot_script", []):
            logger.info("Running %s", cmd)
            try:
                subprocess.check_call(cmd.split(), timeout=120)
            except Exception:
                raise RecoveryError("timeout reaching control host!")

    def unmount_writable_partition(self):
        try:
            self._run_control(
                "sudo umount {}*".format(self.config["test_device"]),
                timeout=30,
            )
        except KeyError:
            raise RecoveryError("Device config missing test_device")
        except Exception:
            # We might not be mounted, so expect this to fail sometimes
            pass

    def create_user(self):
        """Create user account for default ubuntu user"""
        base = self.mount_point
        remote_tmp = Path("/tmp") / self.agent_name
        try:
            data_path = Path(__file__).parent / "../../data/riverside"
            # Cloud-init user data file is already in the image,
            # in one of the 2 locations:
            # /var/lib/cloud/seed/nocloud/user-data
            # /etc/cloud/cloud.cfg.d/user-data.cfg

            # Workaround network issues with AGX and IGX to make sure we
            # get the assigned IP associated to the device
            self._run_control("mkdir -p {}".format(remote_tmp))
            self._copy_to_control(data_path / "network-config.cfg", remote_tmp)
            cmd = (
                f"sudo cp {remote_tmp}/network-config.cfg "
                f"{base}/writable/etc/cloud/cloud.cfg.d/"
            )
            self._run_control(cmd)
            cmd = (
                f"sudo sed -i 's/<ip>/{format(self.config['device_ip'])}/' "
                f"{base}/writable/etc/cloud/cloud.cfg.d/network-config.cfg"
            )
            self._run_control(cmd)
            self._configure_sudo()
        except Exception:
            raise ProvisioningError("Error creating user files")

    def _configure_sudo(self):
        # Setup sudoers data
        sudo_data = "ubuntu ALL=(ALL) NOPASSWD:ALL"
        sudo_path = "{}/writable/etc/sudoers.d/ubuntu".format(self.mount_point)
        self._run_control(
            "sudo bash -c \"echo '{}' > {}\"".format(sudo_data, sudo_path)
        )

    def check_test_image_booted(self):
        logger.info("Checking if test image booted.")
        started = time.time()
        # Retry for a while since we might still be rebooting
        test_username = self.job_data.get("test_data", {}).get(
            "test_username", "ubuntu"
        )
        test_password = self.job_data.get("test_data", {}).get(
            "test_password", "ubuntu"
        )
        while time.time() - started < 1200:
            try:
                time.sleep(10)
                cmd = [
                    "sshpass",
                    "-p",
                    test_password,
                    "ssh-copy-id",
                    "-o",
                    "StrictHostKeyChecking=no",
                    "-o",
                    "UserKnownHostsFile=/dev/null",
                    "{}@{}".format(test_username, self.config["device_ip"]),
                ]
                subprocess.check_output(
                    cmd, stderr=subprocess.STDOUT, timeout=60
                )
                return True
            except Exception:
                pass
        # If we get here, then we didn't boot in time
        raise ProvisioningError("Failed to boot test image!")

    def run_post_provision_script(self):
        # Run post provision commands on control host if there are any, but
        # don't fail the provisioning step if any of them don't work
        for cmd in self.config.get("post_provision_script", []):
            logger.info("Running %s", cmd)
            try:
                self._run_control(cmd)
            except Exception:
                logger.warn("Error running %s", cmd)
