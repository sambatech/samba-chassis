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
import boto3
import threading
import warnings
import random
import json
import math
import uuid
import time
from datetime import datetime, timedelta
from samba_chassis import logging, config
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
# Auxiliary constructs
#

def _enum(*sequential, **named):
    """
    Enum creation function.

    :param sequential: Arguments for a sequential enum;
    :param named:
    :return: Returns enumerator.
    """
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)


class ConfigurationError(RuntimeError):
    pass


class QueueHandler(object):
    """Queue handler responsible to communicate with SQS, send and retrieve tasks."""
    _sqs_client = None
    _sqs = None

    def __init__(self, queue_name, task_timeout=120):
        """
        Take queue name and create SQS connection.

        :param queue_name: SQS queue name.
        :param task_timeout: Seconds for message processing deadline.
        """
        _logger.debug("Creating queue handler {}".format(queue_name))
        self.queue_name = queue_name
        self.queue = None
        self.task_timeout = task_timeout

    def connect(self):
        # Setup sqs if yet not set
        if self._sqs_client is None:
            self._sqs_client = boto3.client('sqs')
        if self._sqs is None:
            self._sqs = boto3.resource('sqs')
        # Setup queue if yet not set
        if self.queue is None:
            # Create queue if necessary
            try:
                self.queue = self._sqs.get_queue_by_name(QueueName=self.queue_name)
            except self._sqs_client.exceptions.QueueDoesNotExist:
                _logger.info("CREATING_QUEUE_IN_AWS: {}".format(self.queue_name))
                self.queue = self._sqs.create_queue(
                    QueueName=self.queue_name,
                    Attributes={
                        "ReceiveMessageWaitTimeSeconds": "2",
                        "VisibilityTimeout": "120"
                    }
                )

    def send(self, task_name, task_attr, delay=0, exec_id=None, when=None):
        """
        Send task to SQS queue.

        :param task_name: Task name.
        :param task_attr: Task attributes in dict form.
        :param delay: Time in seconds for the task to become available for execution.
        :param exec_id: Task execution command id.
        :param when: Datetime that tells when can the task execute.
        """
        self.connect()
        _logger.debug("Sending task {}".format(task_name))
        self.queue.send_message(
            MessageBody=json.dumps(task_attr),
            DelaySeconds=delay,
            MessageAttributes={
                "task_name": {'StringValue': task_name, 'DataType': 'String'},
                "exec_id": {'StringValue': str(uuid.uuid4()) if exec_id is None else exec_id, 'DataType': 'String'},
                "when": {
                    'StringValue': datetime.utcnow().strftime("%d/%m/%y %H:%M:%S") if when is None else when.strftime("%d/%m/%y %H:%M:%S"),
                    'DataType': 'String'
                }
            }
        )

    def queue_len(self):
        self.connect()
        self.queue.load()
        return int(self.queue.attributes["ApproximateNumberOfMessages"])

    def retrieve(self, max_number=1):
        """
        Retrieve at most max_number messages.

        :param max_number: Maximum number of messages to be returned.
        :return: Retrieved messages.
        """
        self.connect()
        _logger.debug("Retrieving {} messages".format(max_number))
        # Check max number limit
        if max_number > 10:
            max_number = 10
            warnings.warn("Tried to retrieve more than 10 messages")

        messages = self.queue.receive_messages(
            AttributeNames=[
                'ApproximateReceiveCount',
                'SentTimestamp'
            ],
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=max_number,
            VisibilityTimeout=self.task_timeout,
            WaitTimeSeconds=1
        )
        return messages

    @staticmethod
    def done(message):
        """
        Delete message from queue service.

        :param message: Message to be deleted.
        """
        message.delete()

    @staticmethod
    def postpone(message, new_timeout):
        """
        Postpone message visibility timeline.

        :param message: Message to postpone.
        :param new_timeout: New deadline to set.
        """
        try:
            message.change_visibility(VisibilityTimeout=int(new_timeout))
            return True
        except:
            _logger.exception("VISIBILITY_CHANGE_FAILURE")
            return False


class Task(object):
    """Task representation that defines a task and its operations."""
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
            _logger.error("INVALID_PROGRESSION")
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
                _logger.error("TASK_FAILED: {}/{} retries".format(retries, self.max_retries), attr=attr)
                self.issue_fail(attr)
            finally:
                return True

        try:
            res = self.func(attr)
            return res if res is not None else True
        except:
            _logger.exception("ERROR_RUNNING_TASK")
            return False


class TaskExecution(object):
    """Encapsulation of a task execution command."""
    def __init__(self, exec_id, task, attr, attempts, created_at, message, timeout):
        """
        Initiate object.

        :param exec_id: Command id.
        :param task: Task to be executed.
        :param attr: Attributes to be passed as arguments.
        :param attempts: Number of attempts already made.
        :param created_at: Datetime of creation.
        :param message: Queue message issuing the execution command.
        :param timeout: Command timeout.
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

    def execute(self):
        """Run task."""
        res = self.task.run(self.attr, self.attempts - 1)
        if not self.disabled:
            self.results = res

    def get_deadline(self):
        """Return execution command deadline."""
        return self.created_at + timedelta(seconds=int(self.timeout / 2) * (self.postpone_num + 1))

    def postpone(self, queue_handler):
        """Postpone execution command deadline."""
        new_timeout = int(math.ceil((self.get_deadline() - datetime.utcnow()).total_seconds())) + self.timeout
        _logger.info("POSTPONE: {} for {} {}".format(new_timeout, self.task.name, self.exec_id))
        return queue_handler.postpone(self.message, new_timeout)


class TaskScheduler(object):
    """Scheduler that runs tasks and monitor their execution."""
    statuses = _enum("STOPPED", "STOPPING", "RUNNING", "ERROR")

    def __init__(self, queue_handler, task_map, workers=1, unknown_tasks_retries=10,
                 unknown_tasks_delay=10, task_execution_class=TaskExecution, max_workers=None,
                 scale_factor=100, when_window=60):
        """
        Initiate scheduler.

        :param queue_handler: Queue to receive and send task commands.
        :param task_map: Map containing all registered tasks.
        :param workers: Number of concurrent tasks to be ran by this scheduler.
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

        self.scheduler_thread = None

        self._status_lock = threading.Lock()
        self.status = self.statuses.STOPPED

        self._on_going_lock = threading.Lock()
        self._on_going_tasks = {}
        self.when_window = when_window

    def start(self):
        """Start scheduler."""
        _logger.info("STARTING_TASK_SCHEDULER")
        if self.status == self.statuses.STOPPED:
            if self.scheduler_thread is not None and self.scheduler_thread.is_alive():
                # wait for thread to die
                while self.scheduler_thread.is_alive():
                    time.sleep(1)

            self.status = self.statuses.RUNNING
            self.scheduler_thread = threading.Thread(target=self.loop)
            self.scheduler_thread.start()
            return

        with self._status_lock:
            if self.status == self.statuses.STOPPING:
                self.status = self.statuses.RUNNING

    def stop(self, force=False):
        """
        Stop the scheduler.

        :param force: Flag that determines whether to stop immediately or wait for current task to stop.
        """
        _logger.info("STOPPING_TASK_SCHEDULER")
        if force:
            self.status = self.statuses.STOPPED
        else:
            self.status = self.statuses.STOPPING

    def loop(self):
        """Main loop for monitoring thread."""
        _logger.debug("Entering loop with status {}".format(self.status))
        while self.status != self.statuses.STOPPED:
            with self._on_going_lock:
                _logger.debug("{} tasks executing".format(len(self._on_going_tasks)))
                # Monitor and process ongoing tasks.
                self._process_on_going_tasks()
                # Stop scheduler if it is stopping and there are no more on going tasks.
                with self._status_lock:
                    if len(self._on_going_tasks) == 0 and self.status == self.statuses.STOPPING:
                        self.status = self.statuses.STOPPED
                # Check if should scale number of workers
                self._process_scaling()
                # Get new tasks if scheduler is running and there are less on going tasks than max.
                if len(self._on_going_tasks) < self.workers and self.status == self.statuses.RUNNING:
                    tasks = self._get_new_tasks(self.workers - len(self._on_going_tasks))
                    self._run_tasks(tasks)
            # Sleep for one second
            time.sleep(1)
        _logger.debug("Getting out of loop")
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
            _logger.exception("SCALING_ERROR")

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
        _logger.error("POSTPONE_FAILURE: {} {}".format(task_exec.task.name, task_exec.exec_id))
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
            self.queue_handler.postpone(task_exec.message, vis_delay)
        # Delete ongoing task
        bye_bye_tasks.append(task_exec.exec_id)

    def _process_dead_thread(self, task_exec, bye_bye_tasks):
        """Process a dead thread to be considered a failed execution."""
        # A dead thread is considered fail
        _logger.error("DEAD_THREAD: {} {}".format(task_exec.task.name, task_exec.exec_id))
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
        when = datetime.strptime(message.message_attributes["when"]["StringValue"], "%d/%m/%y %H:%M:%S") \
               - timedelta(seconds=self.when_window)
        print "WHEN: ", when > datetime.utcnow()
        return datetime.utcnow() > when

    def _when_to_seconds(self, message):
        when = datetime.strptime(message.message_attributes["when"]["StringValue"], "%d/%m/%y %H:%M:%S") \
               - timedelta(seconds=self.when_window)
        total = (when - datetime.utcnow()).total_seconds()
        return total if total <= 18000 else 18000

    def _get_new_tasks(self, num):
        """
        Get new tasks for execution.

        TODO: Messages that are in the queue for too long should bet recreated before they expire.
        :param num: Max number of tasks to be received.
        """
        # Retrieve num messages
        messages = self.queue_handler.retrieve(num)
        if len(messages) > 0:
            _logger.info("RETRIEVED_TASKS: {}/{}".format(len(messages), num))
        # Create a TaskExecution object for each message
        tasks = []
        for message in messages:
            _logger.debug("Received message:  header = {} body = {}".format(message.message_attributes, message.body))
            # Check if message has a known task
            if not self._is_known_task(message):
                _logger.warn(
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
            _logger.info("RUNNING_TASK: {} {}".format(task_exec.task.name, task_exec.exec_id), attr=task_exec.attr)
            task_exec.thread = threading.Thread(target=task_exec.execute)
            task_exec.thread.start()
            self._on_going_tasks[task_exec.exec_id] = task_exec

    def get_status(self):
        if (
                self.status != self.statuses.STOPPED and
                (self.scheduler_thread is None or not self.scheduler_thread.is_alive())
        ):
            return self.statuses.ERROR
        return self.status


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
