#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) SambaTech. All rights reserved.

created_at: 06-JUN-2018
updated_at: 10-JUL-2018

Task scheduler for async and reliable job executions.

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
from samba_chassis.tasks.schedulers import TaskScheduler


class Task(object):
    """Task representation that defines a task and its operations."""
    _logger = logging.get(__name__)

    _progressions = {
        "NONE": lambda wait_time, retries: 0 if retries == 0 else int(wait_time),
        "GEOMETRIC": lambda wait_time, retries: int(wait_time * math.pow(retries, 2)),
        "ARITHMETIC": lambda wait_time, retries: int(wait_time * retries),
        "RANDOM": lambda wait_time, retries: 0 if retries == 0 else int(wait_time * random.uniform(0.5, 2.0))
    }

    @staticmethod
    def send(task_name, attr, queue_handler, delay=0, exec_id=None, when=None):
        """
        Send a task execution command to queue handler for the selected task.

        :param task_name: Task's name.
        :param attr: Attributes to be passed as arguments.
        :param queue_handler: Queue to send task execution command to.
        :param delay: Time delay before sending command.
        :param exec_id: Command id.
        :param when: Datetime that tells when can the task execute.
        """
        queue_handler.send(task_name, attr, delay, exec_id, when)

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
            self._logger.error("INVALID_PROGRESSION")
            raise ValueError("INVALID_PROGRESSION")
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

    def run(self, attr, retries=0):
        """
        Run task.

        :param attr: Dictionary with task attributes (data to be used by task).
        :param retries: Number of retries already performed in this command.
        :return: True if successful, false otherwise.
        """
        if int(retries) >= self.max_retries:
            try:
                self._logger.error("TASK_FAILED: {}/{} retries".format(retries, self.max_retries), attr=attr)
                self.issue_fail(attr)
            finally:
                return True

        try:
            res = self.func(attr)
            return res if res is not None else True
        except:
            self._logger.exception("ERROR_RUNNING_TASK")
            return False


#
# Attributes
#
_logger = logging.get(__name__)
_scheduler = None
_scheduler_p = None
_queue_pool = {}
_tasks = {}
_config = None

#
# Config Layout
#
config_layout = config.ConfigLayout({
    "name": config.ConfigItem(
        default="tasks",
        type=str,
        rules=[lambda x: True if x.lower() == x else False]
    ),
    "project": config.ConfigItem(
        default="project",
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


class ConfigurationError(RuntimeError):
    pass


#
# Interface methods
#
def config(config_object=None):
    """
    Configure module.

    :param config_object: A configuration object. It must have name and project, optional parameters are
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
    queue_name = "{}_{}".format(_config.project, _config.name)
    _queue_pool[queue_name] = _queue_class(queue_name, _config.task_timeout)

    _logger.debug("Configured tasks module with queue {} and attributes {}".format(queue_name, _config))


def start_scheduler(config_map=None):
    """Start module's standard scheduler."""
    # Config if necessary
    if config_map is not None:
        config(config_map)

    global _scheduler
    _logger.debug("Starting scheduler")
    # Evaluate scheduler
    if _config is None or not hasattr(_config, "name") or not hasattr(_config, "project"):
        _logger.error("UNCONFIGURED_TASK_MODULE")
        raise ConfigurationError("UNCONFIGURED_TASK_MODULE")
    if _scheduler is not None and _scheduler.status == _scheduler_class.statuses.RUNNING:
        _logger.warn("SCHEDULER_ALREADY_RUNNING")
        raise warnings.warn("SCHEDULER_ALREADY_RUNNING")
    # Start scheduler
    queue_name = "{}_{}".format(_config.project, _config.name)
    if _scheduler is None:
        _scheduler = _scheduler_class(
            _queue_pool[queue_name],
            _tasks,
            workers=_config.workers,
            unknown_tasks_retries=_config.unknown_tasks_retries,
            unknown_tasks_delay=_config.unknown_tasks_delay,
            max_workers=_config.max_workers,
            scale_factor=_config.scale_factor,
            when_window=_config.when_window
        )
    _scheduler.start()


def stop_scheduler():
    """Stop the scheduler from receiving new tasks."""
    _logger.debug("Stopping scheduler")
    # Evaluate scheduler
    if _scheduler is None:
        raise RuntimeError("Missing task scheduler")
    elif _scheduler.status != _scheduler_class.statuses.RUNNING:
        warnings.warn("Task scheduler already stopping")
    # Stop scheduler
    _scheduler.stop(force=False)


def is_scheduler_running():
    """Return whether the task scheduler is running or not."""
    # Evaluate scheduler
    if _scheduler is None:
        raise RuntimeError("Missing task scheduler")
    # Return result
    if _scheduler.status == _scheduler_class.statuses.STOPPED:
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
    queue_name = "{}_{}".format(_config.project, _config.name)
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


def run(task_name, task_attr, service_name=None, project_name=None, when=None):
    """
    Send a run task message to queue.

    :param task_name: Task to execute.
    :param task_attr: Attributes to use for execution.
    :param service_name: Service that runs this task. The default is the current service.
    :param project_name: Service project. The default is the current project.
    :param when: Datetime that tells when can the task execute.
    """
    # Define service and queue handler
    if service_name is None:
        service_name = _config.name
    if project_name is None:
        project_name = _config.project

    strict = True
    if service_name != _config.name or project_name != _config.project:
        strict = False

    queue_name = "{}_{}".format(project_name, service_name)
    if queue_name not in _queue_pool:
        _queue_pool[queue_name] = _queue_class(queue_name)
    # Issue task
    if strict and task_name not in _tasks:
        raise RuntimeError("Strict task not registered")
    _task_class.send(task_name, task_attr, _queue_pool[queue_name], when)


def ready():
    """Return module's features readiness."""
    r = {}
    if _queue_pool:
        r["TASK_QUEUES"] = "OK"
    else:
        r["TASK_QUEUES"] = "ERROR"
    if _scheduler is not None and \
            _scheduler.get_status() in [_scheduler_class.statuses.STOPPING, _scheduler_class.statuses.RUNNING]:
        r["TASK_SCHEDULER"] = "OK"
    else:
        r["TASK_SCHEDULER"] = "ERROR"
    return r

#
# Classes to use on module functions
#
_task_class = Task
_scheduler_class = TaskScheduler
_queue_class = QueueHandler


def set_task_class(task_class):
    """Set the task class to be used as standard for this module."""
    global _task_class
    _task_class = task_class


def get_task_class():
    """Get the module's standard task class."""
    return _task_class


def set_scheduler_class(scheduler_class):
    """Set the task scheduler class to be used as standard for this module."""
    global _scheduler_class
    _scheduler_class = scheduler_class


def get_scheduler_class():
    """Get the module's standard task scheduler class."""
    return _scheduler_class


def set_queue_class(queue_class):
    """Set the queue handler class to be used as standard for this module."""
    global _queue_class
    _queue_class = queue_class


def get_queue_class():
    """Get the module's standard queue handler class."""
    return _queue_class
