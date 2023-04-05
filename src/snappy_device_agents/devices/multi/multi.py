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

"""Ubuntu multi-device support code."""

import json
import logging
import os
import time

from snappy_device_agents.devices import ProvisioningError

logger = logging.getLogger()


class Multi:

    """Device Agent for multi-device"""

    def __init__(self, config, job_data, client):
        """Initialize the multi-device agent.

        :param config: path to the config file
        :param job_data: path to the job data file
        :param client: client object for talking to the Testflinger server
        """
        self.config = config
        self.job_data = job_data
        self.agent_name = self.config.get("agent_name")
        self.mount_point = os.path.join("/mnt", self.agent_name)
        self.client = client
        self.jobs = []

    def provision(self):
        """Provision the multi-device agent by creating the specified jobs"""
        self.create_jobs()

        # Wait for all jobs to reach the "allocated" state
        unallocated = self.jobs.copy()

        # Set default timeout to allocate all devices to 2 hours
        allocation_timeout = self.job_data.get(
            "allocation_timeout", 2 * 60 * 60
        )
        start_time = time.time()

        while unallocated:
            time.sleep(10)
            for job in unallocated:
                state = self.client.get_status(job)
                if state == "allocated":
                    unallocated.remove(job)
                    break
                if state in ("cancelled", "complete"):
                    logger.error(
                        "Job %s failed to allocate, cancelling remaining jobs",
                        job,
                    )
                    self.cancel_jobs(self.jobs)
                    raise ProvisioningError("Unable to allocate all devices")
            # Timeout if we've been waiting too long for devices to allocate
            if time.time() - start_time > allocation_timeout:
                self.cancel_jobs(self.jobs)
                raise ProvisioningError(
                    "Timed out waiting for devices to allocate"
                )

    def save_job_list_file(self):
        """
        get the "device_info" dict from the results for each job in self.jobs
        Create a job_list.json file with a list of jobs that looks like:
        [
            {
                "job_id": "1234",
                "device_info": {
                    "device_ip": "10.1.1.1"
                }
            },
            {
                "job_id": "5678",
                "device_info": {
                    "device_ip": "10.1.1.2"
            }
        ]
        """
        job_list = []
        for job in self.jobs:
            device_info = self.client.get_results(job).get("device_info")
            job_list.append(
                {
                    "job_id": job,
                    "device_info": device_info,
                }
            )
        with open("job_list.json", "w") as json_file:
            json.dump(job_list, json_file)

    def create_jobs(self):
        """Create the jobs for the multi-device agent"""
        jobs_list = self.job_data.get("provision_data", {}).get("jobs")
        if not jobs_list:
            raise ProvisioningError(
                "You must specify a list of 'jobs' in "
                "the 'provision_data' section of "
                "your job."
            )

        logger.info("Creating test jobs")
        for job in jobs_list:
            try:
                job_id = self.client.submit_job(job)
            except OSError as exc:
                logger.exception("Unable to create job: %s", job_id)
                self.cancel_jobs(self.jobs)
                raise ProvisioningError(
                    f"Unable to create job: {job_id}"
                ) from exc

            logger.info("Created job %s", job_id)
            self.jobs.append(job_id)

    def cancel_jobs(self, jobs):
        """Cancel all jobs in the specified list of job_ids"""
        for job in jobs:
            try:
                self.client.cancel_job(job)
            except OSError:
                logger.exception("Unable to cancel job: %s", job)
