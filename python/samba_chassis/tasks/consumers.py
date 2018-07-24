#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) SambaTech. All rights reserved.

created_at: 10-JUL-2018
updated_at: 10-JUL-2018
"""
import threading
import json
import time
import math
from datetime import datetime, timedelta
from samba_chassis import logging
from samba_chassis.tasks.execs import TaskExecution


def _enum(*sequential, **named):
    """
    Enum creation function.

    :param sequential: Arguments for a sequential enum;
    :param named:
    :return: Returns enumerator.
    """
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)


class TaskConsumer(object):
    """Consumer that runs tasks and monitor their execution."""
    _logger = logging.get(__name__)

    statuses = _enum("STOPPED", "STOPPING", "RUNNING", "ERROR")

    def __init__(self, queue_handler, task_map, workers=1, unknown_tasks_retries=10,
                 unknown_tasks_delay=10, task_execution_class=TaskExecution, max_workers=None,
                 scale_factor=100, when_window=60):
        """
        Initiate consumer.

        :param queue_handler: Queue to receive and send task commands.
        :param task_map: Map containing all registered tasks.
        :param workers: Number of concurrent tasks to be ran by this consumer.
        :param unknown_tasks_retries: Number of max retries for unknown tasks messages.
        :param unknown_tasks_delay: Delay for retrying unknown tasks messages.
        :param task_execution_class: Class to use for task exec commands.
        :param max_workers: Max number of workers for dynamic scaling. If None, scaling is off.
        :param when_window: Time in seconds to offset datetime defined executions.
        """
        self.queue_handler = queue_handler
        self.task_map = task_map
        self.workers = workers
        self.def_workers = workers
        self.max_workers = max_workers
        self.unknown_tasks_retries = unknown_tasks_retries
        self.unknown_tasks_delay = unknown_tasks_delay
        self.scale_factor = scale_factor

        self._task_execution_class = task_execution_class

        self.consumer_thread = None

        self._status_lock = threading.Lock()
        self.status = self.statuses.STOPPED

        self._on_going_lock = threading.Lock()
        self._on_going_tasks = {}
        self.when_window = when_window

    def start(self):
        """Start consumer."""
        self._logger.info("STARTING_TASK_CONSUMER")
        if self.status == self.statuses.STOPPED:
            if self.consumer_thread is not None and self.consumer_thread.is_alive():
                # wait for thread to die
                while self.consumer_thread.is_alive():
                    time.sleep(1)

            self.status = self.statuses.RUNNING
            self.consumer_thread = threading.Thread(target=self.loop)
            self.consumer_thread.start()
            return

        with self._status_lock:
            if self.status == self.statuses.STOPPING:
                self.status = self.statuses.RUNNING

    def stop(self, force=False):
        """
        Stop the consumer.

        :param force: Flag that determines whether to stop immediately or wait for current task to stop.
        """
        self._logger.info("STOPPING_TASK_CONSUMER")
        if force:
            self.status = self.statuses.STOPPED
        else:
            self.status = self.statuses.STOPPING

    def loop(self):
        """Main loop for monitoring thread."""
        self._logger.debug("Entering loop with status {}".format(self.status))
        while self.status != self.statuses.STOPPED:
            with self._on_going_lock:
                self._logger.debug("{} tasks executing".format(len(self._on_going_tasks)))
                # Monitor and process ongoing tasks.
                self._process_on_going_tasks()
                # Stop consumer if it is stopping and there are no more on going tasks.
                with self._status_lock:
                    if len(self._on_going_tasks) == 0 and self.status == self.statuses.STOPPING:
                        self.status = self.statuses.STOPPED
                # Check if should scale number of workers
                self._process_scaling()
                # Get new tasks if consumer is running and there are less on going tasks than max.
                if len(self._on_going_tasks) < self.workers and self.status == self.statuses.RUNNING:
                    tasks = self._get_new_tasks(self.workers - len(self._on_going_tasks))
                    self._run_tasks(tasks)
            # Sleep for one second
            time.sleep(1)
        self._logger.debug("Getting out of loop")
        self.status = self.statuses.STOPPED

    def _process_scaling(self):
        """Scale number of workers if enabled and queue len is greater or lower than limit."""
        if self.max_workers is None:
            return
        try:
            num_tasks = self.queue_handler.queue_len()
            upper_limit = self.workers*self.scale_factor + int(self.scale_factor/2)
            lower_limit = self.workers * self.scale_factor - int(self.scale_factor / 2)
            if num_tasks > upper_limit and self.workers < self.max_workers:
                self.workers += 1
            elif num_tasks < lower_limit and self.workers > self.def_workers:
                self.workers -= 1
        except:
            self._logger.exception("SCALING_ERROR")

    def _process_on_going_tasks(self):
        """Monitor and process on going tasks."""
        bye_bye_tasks = []
        for exec_id in self._on_going_tasks:
            task_exec = self._on_going_tasks[exec_id]
            # Check if done
            if task_exec.results is not None:
                # Process results
                self._process_task_results(task_exec, bye_bye_tasks)
                continue
            if not task_exec.thread.is_alive():
                # Thread finished without results, that's not good :(
                self._process_dead_thread(task_exec, bye_bye_tasks)
                continue
            # Check if outdated
            if datetime.utcnow() > task_exec.get_deadline():
                # Postpone deadline
                if not task_exec.postpone(self.queue_handler):
                    self._postpone_failed(task_exec, bye_bye_tasks)
        # Bye bye
        for exec_id in bye_bye_tasks:
            del self._on_going_tasks[exec_id]

    def _postpone_failed(self, task_exec, bye_bye_tasks):
        """Process failed postpone call."""
        self._logger.error("POSTPONE_FAILURE: {} {}".format(task_exec.task.name, task_exec.exec_id))
        # If postpone failed, resend message
        task_exec.task.issue(task_exec.attr, 0, task_exec.exec_id)
        # Disable
        task_exec.disabled = True
        # Delete original one
        self.queue_handler.done(task_exec.message)
        bye_bye_tasks.append(task_exec.exec_id)

    def _process_task_results(self, task_exec, bye_bye_tasks):
        """Process results."""
        # Results is either true or false
        if task_exec.results is True:
            # The task was a great success!
            self.queue_handler.done(task_exec.message)
        elif task_exec.results is False:
            # Task failed, calc visibility delay
            vis_delay = (datetime.utcnow() - task_exec.created_at).total_seconds() + \
                        task_exec.task.get_delay(task_exec.attempts)
            # Postpone message
            self.queue_handler.postpone(task_exec.message, int(vis_delay))
        # Delete ongoing task
        bye_bye_tasks.append(task_exec.exec_id)

    def _process_dead_thread(self, task_exec, bye_bye_tasks):
        """Process a dead thread to be considered a failed execution."""
        # A dead thread is considered fail
        self._logger.error("DEAD_THREAD: {} {}".format(task_exec.task.name, task_exec.exec_id))
        if task_exec.disabled:
            # Delete message
            self.queue_handler.done(task_exec.message)
        else:
            # Postpone message
            vis_delay = (datetime.utcnow() - task_exec.created_at).total_seconds() + \
                        task_exec.task.get_delay(task_exec.attempts)
            self.queue_handler.postpone(task_exec.message, vis_delay)
        # Delete ongoing task
        bye_bye_tasks.append(task_exec.exec_id)

    def _is_known_task(self, message):
        """Return whether task in message is int task map."""
        return \
            "task_name" in message.message_attributes and \
            "StringValue" in message.message_attributes["task_name"] and \
            message.message_attributes["task_name"]["StringValue"] in self.task_map

    def _passed_when(self, message):
        when = datetime.strptime(message.message_attributes["when"]["StringValue"], "%d/%m/%y %H:%M:%S")
        if datetime.utcnow() > when:
            self._logger.warn(
                "EXEC_PASSED_DUE_DATE: {} ({})>({})".format(
                    message.message_attributes["exec_id"]["StringValue"], datetime.utcnow(), when
                )
            )
        when -= timedelta(seconds=self.when_window)
        return datetime.utcnow() > when

    def _when_to_seconds(self, message):
        when = datetime.strptime(message.message_attributes["when"]["StringValue"], "%d/%m/%y %H:%M:%S") \
               - timedelta(seconds=self.when_window)
        total = (when - datetime.utcnow()).total_seconds()
        return math.ceil(total) if total <= 18000 else 18000

    def _get_new_tasks(self, num):
        """
        Get new tasks for execution.

        TODO: Messages that are in the queue for too long should bet recreated before they expire.
        :param num: Max number of tasks to be received.
        """
        # Retrieve num messages
        messages = self.queue_handler.retrieve(num)
        if len(messages) > 0:
            self._logger.info("RETRIEVED_TASKS: {}/{}".format(len(messages), num))
        # Create a TaskExecution object for each message
        tasks = []
        for message in messages:
            self._logger.debug("Received message:  header = {} body = {}".format(message.message_attributes, message.body))
            # Check if message has a known task
            if not self._is_known_task(message):
                self._logger.warn(
                    "RECEIVED_UNKNOWN_TASK: header = {} attr = {}".format(message.message_attributes, message.body)
                )
                if int(message.attributes['ApproximateReceiveCount']) > self.unknown_tasks_retries:
                    self.queue_handler.done(message)
                else:
                    self.queue_handler.postpone(message, self.unknown_tasks_delay)
                continue
            # Check if the task should be executed later
            if not self._passed_when(message):
                # Postpone it as long as possible
                self.queue_handler.postpone(message, self._when_to_seconds(message))
                continue
            tasks.append(
                self._task_execution_class(
                    exec_id=message.message_attributes["exec_id"]["StringValue"],
                    task=self.task_map[message.message_attributes["task_name"]["StringValue"]],
                    attr=json.loads(message.body),
                    attempts=int(message.attributes['ApproximateReceiveCount']),
                    created_at=datetime.utcnow(),
                    message=message,
                    timeout=self.queue_handler.task_timeout
                )
            )
        # return all objects in a list
        return tasks

    def _run_tasks(self, tasks):
        """Run tasks."""
        for task_exec in tasks:
            self._logger.info("RUNNING_TASK: {} {}".format(task_exec.task.name, task_exec.exec_id),
                              extra=task_exec.attr)
            task_exec.thread = threading.Thread(target=task_exec.execute)
            task_exec.thread.start()
            self._on_going_tasks[task_exec.exec_id] = task_exec

    def get_status(self):
        if (
                self.status != self.statuses.STOPPED and
                (self.consumer_thread is None or not self.consumer_thread.is_alive())
        ):
            return self.statuses.ERROR
        return self.status

