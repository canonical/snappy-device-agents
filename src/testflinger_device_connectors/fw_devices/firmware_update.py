"""Base class for flashing firmware on devices"""

import subprocess
import logging
from testflinger_device_connectors.fw_devices.dmi import Dmi
from testflinger_device_connectors.fw_devices import *
from testflinger_device_connectors import logmsg


SSH_OPTS = "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
target_device_username = "ubuntu"


def all_subclasses(cls):
    return cls.__subclasses__() + [
        g for s in cls.__subclasses__() for g in all_subclasses(s)
    ]


def detect_device(
    ip: str, user: str, password: str = "", **options
) -> AbstractDevice:
    """
    Detect device's firmware upgrade type by checking on DMI data

    :ip:        DUT IP
    :user:      DUT user
    :password:  DUT password (default=blank)
    :return:    device class object
    :rtype:     an instance of a class that implements AbstractDevice
    """
    temp_device = LVFSDevice(ip, user, password)
    run_ssh = temp_device.run_cmd
    devices = all_subclasses(AbstractDevice)
    try:
        dmi_chassis_vendor = "sudo cat /sys/class/dmi/id/chassis_vendor"
        dmi_chassis_type = "sudo cat /sys/class/dmi/id/chassis_type"
        rc1, vendor_string, stderr1 = run_ssh(
            dmi_chassis_vendor, raise_stderr=False
        )
        rc2, type_string, stderr2 = run_ssh(
            dmi_chassis_type, raise_stderr=False
        )

        err_msg = ""
        if rc1 != 0:
            err_msg = vendor_string + stderr1 + "\n"
        if rc2 != 0:
            err_msg = err_msg + type_string + stderr2
        if err_msg:
            err_msg = (
                "Unable to detect device vendor/type due to lacking of dmi info.\n"
                + err_msg
            )
            logmsg(logging.ERROR, err_msg)
            raise SystemExit(err_msg)

        type_index = int(type_string)
        upgrade_type = Dmi.chassis_types[type_index]
        try:
            dev = [
                dev
                for dev in devices
                if dev.fw_update_type in upgrade_type
                and any(x == vendor_string for x in dev.vendor)
            ][0]
            logmsg(logging.INFO, f"{ip} is a {vendor_string} {dev.__name__}")

        except IndexError:
            err_msg = f"{vendor_string} {Dmi.chassis_names[type_index]} Device is not in current support scope"
            logmsg(logging.ERROR, err_msg)
            raise SystemExit(err_msg)

        if issubclass(dev, LVFSDevice):
            return dev(ip, user, password)
        elif issubclass(dev, OEMDevice):
            if not (
                "bmc_ip" in options
                and "bmc_user" in options
                and "bmc_password" in options
            ):
                raise SystemExit(
                    "Please provide $BMC_IP, $BMC_USER, $BMC_PASSWORD for this device"
                )
            return dev(
                options.get("bmc_ip"),
                options.get("bmc_user"),
                options.get("bmc_password"),
            )
    except subprocess.CalledProcessError as e:
        logmsg(logging.ERROR, e.output)
        raise RuntimeError(e.output)

