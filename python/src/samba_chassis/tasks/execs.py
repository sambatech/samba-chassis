#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) SambaTech. All rights reserved.

created_at: 10-JUL-2018
updated_at: 10-JUL-2018
"""
import math
from datetime import datetime, timedelta

from samba_chassis import logging
_logger = logging.getLogger(__name__)


class TaskExecution(object):
    """Encapsulation of a task execution command."""

    def __init__(self, exec_id, task, attr, attempts, created_at, message, timeout, job_id, job_name):
        """
        Initiate object.

        :param exec_id: Command id.
        :param task: Task to be executed.
        :param attr: Attributes to be passed as arguments.
        :param attempts: Number of attempts already made.
        :param created_at: Datetime of creation.
        :param message: Queue message issuing the execution command.
        :param timeout: Command timeout.
        :param job_id: Job id for service logging.
        :param job_name: Job name for service logging.

        """
        self.exec_id = exec_id
        self.task = task
        self.attr = attr
        self.attempts = attempts
        self.created_at = created_at
        self.message = message
        self.timeout = timeout
        self.results = None
        self.thread = None
        self.disabled = False
        self.postpone_num = 0
        self.job_id = job_id
        self.job_name = job_name

    def execute(self):
        """Run task."""
        res = self.task.run(self.attr, self.attempts - 1, job_id=self.job_id, job_name=self.job_name)
        if not self.disabled:
            self.results = res

    def get_deadline(self):
        """Return execution command deadline."""
        return self.created_at + timedelta(seconds=int(self.timeout / 2) * (self.postpone_num + 1))

    def postpone(self, queue_handler):
        """Postpone execution command deadline."""
        new_timeout = int(math.ceil((self.get_deadline() - datetime.utcnow()).total_seconds())) + self.timeout
        _logger.info("POSTPONE: {} for {} {}".format(new_timeout, self.task.name, self.exec_id),
                     job_id=self.job_id, job_name=self.job_name)
        return queue_handler.postpone(self.message, new_timeout)

