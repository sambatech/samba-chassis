#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) SambaTech. All rights reserved.

created_at: 06-JUN-2018
updated_at: 10-JUL-2018

Task consumer for async and reliable job executions.

With this module it is possible to schedule a task to be ran by
some thread or process in a reliable manner.
It uses AWS's SQS messaging queue service with one queue
shared among all service executions.

To use it you must associate task names with functions to be executed when
a task is retrieved from the queue. Use the method set_task for that. When
a task is retrieved from the queue the class verifies the task name e executes
the function associated with it passing the task attributes as an argument.
This associated function must return true if the work was completed and false
(or raise an error) if not.

Although the queue exists independently from this module's use, it only
starts to listen and send to the queue when it's start function is called
with its proper configuration.

It is possible to define custom values for the maximum of times a work can fail
or throw an error.
"""
import warnings
import random
import math
from samba_chassis import logging, config
from samba_chassis.tasks.execs import TaskExecution
from samba_chassis.tasks.queues import QueueHandler
from samba_chassis.tasks.consumers import TaskConsumer

_logger = logging.getLogger(__name__)


class Task(object):
    """Task representation that defines a task and its operations."""

    _progressions = {
        "NONE": lambda wait_time, retries: 0 if retries == 0 else int(wait_time),
        "GEOMETRIC": lambda wait_time, retries: int(wait_time * math.pow(retries, 2)),
        "ARITHMETIC": lambda wait_time, retries: int(wait_time * retries),
        "RANDOM": lambda wait_time, retries: 0 if retries == 0 else int(wait_time * random.uniform(0.5, 2.0))
    }

    @staticmethod
    def send(task_name, attr, queue_handler, delay=0, exec_id=None, when=None, **kwargs):
        """
        Send a task execution command to queue handler for the selected task.

        :param task_name: Task's name.
        :param attr: Attributes to be passed as arguments.
        :param queue_handler: Queue to send task execution command to.
        :param delay: Time delay before sending command.
        :param exec_id: Command id.
        :param when: Datetime that tells when can the task execute.
        """
        queue_handler.send(task_name, attr, delay, exec_id, when, **kwargs)

    def __init__(self, name, func, queue_handler, max_retries=10, on_fail=None, wait_time=0, wait_progression="NONE"):
        """
        Initiate task.

        :param name: Task's name.
        :param func: Task's function.
        :param queue_handler: Queue to be associated with this task.
        :param max_retries: Maximum number of retries possible.
        :param on_fail: Task to be ran upon task failure.
        :param wait_time: Time to wait between first retry.
        :param wait_progression: Wait time progression after first retry.
        """
        self.name = name
        self.func = func
        self.queue_handler = queue_handler
        self.max_retries = max_retries
        self.on_fail = on_fail
        self.wait_time = wait_time
        if wait_progression not in self._progressions:
            _logger.error("INVALID_PROGRESSION {}".format(wait_progression))
            raise ValueError("INVALID_PROGRESSION {}".format(wait_progression))
        self.wait_progression = wait_progression

    def get_delay(self, retries=0):
        """
        Return delay before next retry.

        :param retries: Number of retries already done.
        :return: Next delay.
        """
        return self._progressions[self.wait_progression](self.wait_time, retries)

    def issue(self, attr, delay=0, exec_id=None, when=None):
        """
        Issue task execution command to the queue handler.

        :param attr: Attributes to be passed as arguments.
        :param delay: Delay in seconds.
        :param exec_id: Command id.
        :param when: Datetime that tells when can the task execute.
        """
        self.queue_handler.send(self.name, attr, delay, exec_id, when)

    def issue_fail(self, attr):
        """
        Issues the on_fail task.

        :param attr: Attributes to be passed as arguments.
        """
        if isinstance(self.on_fail, tuple):
            self.send(self.on_fail[0], attr, self.on_fail[1])
        else:
            self.send(self.on_fail, attr, self.queue_handler)

    def run(self, attr, retries=0, job_id="unknown", job_name="unknown"):
        """
        Run task.

        :param attr: Dictionary with task attributes (data to be used by task).
        :param retries: Number of retries already performed in this command.
        :param job_id: Job id for service logging.
        :param job_name: Job name for service logging.
        :return: True if successful, false otherwise.
        """
        if int(retries) >= self.max_retries:
            try:
                _logger.error("TASK_FAILED {}: {}/{} retries".format(self.name, retries, self.max_retries),
                              job_id=job_id, job_name=job_name)
                self.issue_fail(attr)
            finally:
                return True

        try:
            res = self.func(attr)
            return res if res is not None else True
        except:
            _logger.exception("ERROR_RUNNING_TASK {}".format(self.name), job_id=job_id, job_name=job_name)
            return False


#
# Error classes
#
class ConfigurationError(RuntimeError):
    pass


#
# Attributes
#
_consumer = None
_consumer_p = None
_queue_pool = {}
_tasks = {}
_config = None


#
# Config Layout
#
config_layout = config.ConfigLayout({
    "task_pool": config.ConfigItem(
        type=str,
        rules=[lambda x: True if x.lower() == x else False]
    ),
    "task_timeout": config.ConfigItem(
        default=120,
        type=int,
        rules=[lambda x: True if x > 0 else False]
    ),
    "workers": config.ConfigItem(
        default=3,
        type=int,
        rules=[lambda x: True if x > 0 else False]
    ),
    "unknown_tasks_retries": config.ConfigItem(
        default=50,
        type=int,
        rules=[lambda x: True if x > 0 else False]
    ),
    "unknown_tasks_delay": config.ConfigItem(
        default=10,
        type=int,
        rules=[lambda x: True if x > 0 else False]
    ),
    "max_workers": config.ConfigItem(
        default=6,
        type=int,
        rules=[lambda x: True if x > 0 else False]
    ),
    "scale_factor": config.ConfigItem(
        default=100,
        type=int,
        rules=[lambda x: True if x > 0 else False]
    ),
    "when_window": config.ConfigItem(
        default=300,
        type=int,
        rules=[lambda x: True if x > 0 else False]
    ),
})


#
# Interface methods
#
def config(config_object=None):
    """
    Configure module.

    :param config_object: A configuration object. It must have task_pool, optional parameters are
    task_timeout (default 120), workers (default 3), unknown_tasks_retries (default 50),
    unknown_tasks_delay (default 10), max_workers (default 10), scale_factor (default 100).
    """
    global _config
    if config_object is None:
        _config = config_layout.get(base="tasks")
    else:
        _config = config_layout.get(config_object=config_object)
    # Configure module
    # Create queue handler pool with its names.
    queue_name = _config.task_pool
    _queue_pool[queue_name] = _queue_class(queue_name, _config.task_timeout)

    _logger.debug("Configured tasks module with queue {} and attributes {}".format(queue_name, _config))


def start_consumer(config_object=None):
    """Start module's standard consumer."""
    # Config if necessary
    if config_object is not None:
        config(config_object)

    global _consumer
    _logger.debug("Starting consumer")
    # Evaluate consumer
    if _config is None or not hasattr(_config, "task_pool"):
        _logger.error("UNCONFIGURED_TASK_MODULE")
        raise ConfigurationError("UNCONFIGURED_TASK_MODULE")
    if _consumer is not None and _consumer.status == _consumer_class.statuses.RUNNING:
        _logger.warn("CONSUMER_ALREADY_RUNNING")
        raise warnings.warn("CONSUMER_ALREADY_RUNNING")
    # Start consumer
    queue_name = _config.task_pool
    if _consumer is None:
        _consumer = _consumer_class(
            _queue_pool[queue_name],
            _tasks,
            workers=_config.workers,
            unknown_tasks_retries=_config.unknown_tasks_retries,
            unknown_tasks_delay=_config.unknown_tasks_delay,
            max_workers=_config.max_workers,
            scale_factor=_config.scale_factor,
            when_window=_config.when_window
        )
    _consumer.start()


def stop_consumer():
    """Stop the consumer from receiving new tasks."""
    _logger.debug("Stopping consumer")
    # Evaluate consumer
    if _consumer is None:
        raise RuntimeError("Missing task consumer")
    elif _consumer.status != _consumer_class.statuses.RUNNING:
        warnings.warn("Task consumer already stopping")
    # Stop consumer
    _consumer.stop(force=False)


def is_consumer_running():
    """Return whether the task consumer is running or not."""
    # Evaluate consumer
    if _consumer is None:
        raise RuntimeError("Missing task consumer")
    # Return result
    if _consumer.status == TaskConsumer.statuses.STOPPED:
        return False
    return True


def set_task(task_name, task_function, max_retries=10, on_fail=None, wait_time=10, wait_progression="NONE"):
    """
    Register a task.

    :param task_name: Task name to be used in messaging system.
    :param task_function: Function to be ran as the task.
    :param max_retries: Number of retries upon unsuccessful task executions.
    :param on_fail: Task to be ran after task ultimate failure.
    :param wait_time: Time to wait before first retry.
    :param wait_progression: Progression mode of wait_time after first retry.
    """
    _logger.debug("Registering task {}".format(task_name))
    # Validate arguments
    if task_name in _tasks:
        _logger.warn("REGISTERED_TASK_OVERWRITTEN: {}".format(task_name))
        warnings.warn("REGISTERED_TASK_OVERWRITTEN: {}".format(task_name))
    # Register task
    queue_name = _config.task_pool
    _tasks[task_name] = _task_class(task_name, task_function, _queue_pool[queue_name],
                                    max_retries, on_fail, wait_time, wait_progression)


def task(max_retries=10, on_fail=None, wait_time=10, wait_progression="NONE"):
    """
    Decorator that sets up the function as a task with the function name as its name.

    :param max_retries: Number of retries upon unsuccessful task executions.
    :param on_fail: Task to be ran after task ultimate failure.
    :param wait_time: Time to wait before first retry.
    :param wait_progression: Progression mode of wait_time after first retry.
    """
    def real_dec(func):
        set_task(func.__name__, func, max_retries, on_fail, wait_time, wait_progression)
        return func
    return real_dec


def run(task_name, task_attr, task_pool=None, when=None, **kwargs):
    """
    Send a run task message to queue.

    :param task_name: Task to execute.
    :param task_attr: Attributes to use for execution.
    :param task_pool: The name set for the task pool where all tasks come frmo.
    The default is the current service's name.
    :param when: Datetime that tells when can the task execute.
    """
    # Define service and queue handler
    if task_pool is None:
        task_pool = _config.task_pool

    strict = True
    if task_pool != _config.task_pool:
        strict = False

    queue_name = task_pool
    if queue_name not in _queue_pool:
        _queue_pool[queue_name] = _queue_class(queue_name)
    # Issue task
    if strict and task_name not in _tasks:
        _logger.error(
            "STRICT_TASK_NOT_REGISTERED: {}".format(task_name),
            job_id=kwargs.get("job_id", "unknown"),
            job_name=kwargs.get("job_name", "unknown")
        )
        raise RuntimeError("STRICT_TASK_NOT_REGISTERED: {}".format(task_name))
    _task_class.send(task_name, task_attr, _queue_pool[queue_name], when=when, **kwargs)


def ready():
    """Return module's features readiness."""
    r = {}
    if _queue_pool:
        r["TASK_QUEUES"] = "OK"
    else:
        r["TASK_QUEUES"] = "ERROR"

    if _consumer is None:
        _logger.error("TASK_CONSUMER_IS_NONE")
        r["TASK_CONSUMER"] = "ERROR"
    elif _consumer.get_status() not in [TaskConsumer.statuses.STOPPING, TaskConsumer.statuses.RUNNING]:
        _logger.error("TASK_CONSUMER_BAD_STATUS {}".format(_consumer.get_status()))
        r["TASK_CONSUMER"] = "ERROR"
    else:
        r["TASK_CONSUMER"] = "OK"

    return r


#
# Classes to use on module functions
#
_task_class = Task
_consumer_class = TaskConsumer
_queue_class = QueueHandler


def set_task_class(task_class):
    """Set the task class to be used as standard for this module."""
    global _task_class
    _task_class = task_class


def get_task_class():
    """Get the module's standard task class."""
    return _task_class


def set_consumer_class(consumer_class):
    """Set the task consumer class to be used as standard for this module."""
    global _consumer_class
    _consumer_class = consumer_class


def get_consumer_class():
    """Get the module's standard task consumer class."""
    return _consumer_class


def set_queue_class(queue_class):
    """Set the queue handler class to be used as standard for this module."""
    global _queue_class
    _queue_class = queue_class


def get_queue_class():
    """Get the module's standard queue handler class."""
    return _queue_class
