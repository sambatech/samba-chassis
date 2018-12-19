#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) SambaTech. All rights reserved.

created_at: 11-JUN-2018
updated_at: 11-JUL-2018
"""
from samba_chassis import tasks
from samba_chassis.tasks.consumers import TaskConsumer
from mock import MagicMock, patch, ANY
import unittest
import warnings


class ModuleTest(unittest.TestCase):

    @patch.object(tasks, "_queue_class")
    def test_config(self, q):
        co = MagicMock(
            task_pool="test",
            workers=5
        )
        with warnings.catch_warnings(UserWarning):
            tasks.config(co)
            self.assertEqual(tasks._config.task_pool, "test")
            self.assertEqual(tasks._config.task_timeout, 120)
            self.assertEqual(tasks._config.workers, 5)
            self.assertEqual(tasks._config.unknown_tasks_retries, 50)
            self.assertEqual(tasks._config.unknown_tasks_delay, 10)
            self.assertEqual(tasks._config.max_workers, 6)
            self.assertEqual(tasks._config.scale_factor, 100)
            self.assertEqual(tasks._config.when_window, 300)
            assert "test" in tasks._queue_pool
            q.assert_called_once_with("test", 120)

    @patch.object(tasks, "_logger")
    @patch.object(tasks, "_config")
    @patch.object(tasks, "_consumer")
    @patch.object(tasks, "_consumer_class")
    @patch.object(tasks, "_queue_pool")
    def test_start_consumer(self, *_):
        tasks._config.name = "tasks"
        tasks._config.project = "test"
        tasks._config.workers = 1
        tasks._config.unknown_tasks_retries = 2
        tasks._config.unknown_tasks_delay = 3
        tasks._config.max_workers = 4
        tasks._config.scale_factor = 5
        tasks._config.when_window = 6
        tasks._consumer = None
        tasks.start_consumer()
        tasks._logger.debug.assert_called_once_with("Starting consumer")
        tasks._consumer_class.assert_called_once_with(
            tasks._queue_pool["test_tasks"],
            tasks._tasks,
            workers=1,
            unknown_tasks_retries=2,
            unknown_tasks_delay=3,
            max_workers=4,
            scale_factor=5,
            when_window=6
        )

    @patch.object(tasks, "_logger")
    @patch.object(tasks, "_consumer")
    def test_stop_consumer(self, *_):
        tasks._consumer.status = TaskConsumer.statuses.RUNNING
        tasks._consumer_class = TaskConsumer
        tasks.stop_consumer()
        tasks._logger.debug.assert_called_once_with("Stopping consumer")
        tasks._consumer.stop.assert_called_once_with(force=False)

        tasks._consumer = None
        with self.assertRaises(RuntimeError):
            tasks.stop_consumer()

    @patch.object(tasks, "_consumer")
    def test_is_consumer_running(self, *_):
        tasks._consumer.status = TaskConsumer.statuses.RUNNING
        self.assertTrue(tasks.is_consumer_running())

        tasks._consumer.status = TaskConsumer.statuses.STOPPING
        self.assertTrue(tasks.is_consumer_running())

        tasks._consumer.status = TaskConsumer.statuses.STOPPED
        self.assertFalse(tasks.is_consumer_running())

        tasks._consumer = None
        with self.assertRaises(RuntimeError):
            tasks.is_consumer_running()

    @patch.object(tasks, "_logger")
    @patch.object(tasks, "_tasks")
    @patch.object(tasks, "_task_class")
    @patch.object(tasks, "_config")
    @patch.object(tasks, "_queue_pool")
    def test_set_task(self, *_):
        tasks._tasks = {}
        qh = MagicMock()
        tasks._queue_pool = {"tasks": qh}
        tasks._config.task_pool = "tasks"
        f = MagicMock()
        tasks.set_task("test", f, max_retries=10, on_fail=None, wait_time=10, wait_progression="NONE")
        tasks._task_class.assert_called_once_with(
            "test", f, qh, 10, None, 10, "NONE"
        )

    @patch.object(tasks, "_tasks")
    @patch.object(tasks, "_task_class")
    @patch.object(tasks, "_config")
    @patch.object(tasks, "_queue_pool")
    @patch.object(tasks, "_queue_class")
    def test_run(self, *_):
        qh = MagicMock()
        tasks._tasks = {"test": MagicMock()}
        tasks._queue_pool = {"tasks": qh}
        tasks._config.task_pool = "tasks"
        tasks.run("test", {"one": 1}, task_pool=None, when="23/02/1990 14:00:00")
        tasks._task_class.send.assert_called_once_with(
            "test", {"one": 1}, qh, when="23/02/1990 14:00:00"
        )

        tasks.run("test", {"one": 1}, task_pool="test", when="23/02/1990 14:00:00")
        tasks._queue_class.assert_called_with("test")
        tasks._task_class.send.assert_called_with(
            "test", {"one": 1}, tasks._queue_pool["test"], when="23/02/1990 14:00:00"
        )

        tasks._tasks = {}
        with self.assertRaises(RuntimeError):
            tasks.run("test", {"one": 1}, service_name=None, project_name=None, when="23/02/1990 14:00:00")

    @patch.object(tasks, "_queue_pool")
    @patch.object(tasks, "_consumer")
    def test_ready(self, *_):
        tasks._consumer.get_status.return_value = TaskConsumer.statuses.RUNNING
        tasks._queue_pool = {"test": MagicMock()}
        self.assertEqual(tasks.ready(), {"TASK_QUEUES": "OK", "TASK_CONSUMER": "OK"})

        tasks._consumer = None
        tasks._queue_pool = {}
        self.assertEqual(tasks.ready(), {"TASK_QUEUES": "ERROR", "TASK_CONSUMER": "ERROR"})

    def test_task_class(self):
        tasks._task_class = None
        tasks.set_task_class("Class")
        self.assertEqual(tasks._task_class, "Class")
        self.assertEqual(tasks.get_task_class(), "Class")

    def test_consumer_class(self):
        tasks._consumer_class = None
        tasks.set_consumer_class("Class")
        self.assertEqual(tasks._consumer_class, "Class")
        self.assertEqual(tasks.get_consumer_class(), "Class")

    def test_queue_class(self):
        tasks._queue_class = None
        tasks.set_queue_class("Class")
        self.assertEqual(tasks._queue_class, "Class")
        self.assertEqual(tasks.get_queue_class(), "Class")
