#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) SambaTech. All rights reserved.

created_at: 11-JUN-2018
updated_at: 11-JUL-2018
"""
from samba_chassis import tasks
from samba_chassis.tasks.schedulers import TaskScheduler
from mock import MagicMock, patch, ANY
import unittest
import warnings


class ModuleTest(unittest.TestCase):

    @patch.object(tasks, "_queue_class")
    def test_config(self, q):
        co = MagicMock(
            project="test",
            workers=5
        )
        with warnings.catch_warnings(UserWarning):
            tasks.config(co)
            self.assertEqual(tasks._config.name, "tasks")
            self.assertEqual(tasks._config.project, "test")
            self.assertEqual(tasks._config.task_timeout, 120)
            self.assertEqual(tasks._config.workers, 5)
            self.assertEqual(tasks._config.unknown_tasks_retries, 50)
            self.assertEqual(tasks._config.unknown_tasks_delay, 10)
            self.assertEqual(tasks._config.max_workers, 6)
            self.assertEqual(tasks._config.scale_factor, 100)
            self.assertEqual(tasks._config.when_window, 300)
            assert "test_tasks" in tasks._queue_pool
            q.assert_called_once_with("test_tasks", 120)

    @patch.object(tasks, "_logger")
    @patch.object(tasks, "_config")
    @patch.object(tasks, "_scheduler")
    @patch.object(tasks, "_scheduler_class")
    @patch.object(tasks, "_queue_pool")
    def test_start_scheduler(self, *_):
        tasks._config.name = "tasks"
        tasks._config.project = "test"
        tasks._config.workers = 1
        tasks._config.unknown_tasks_retries = 2
        tasks._config.unknown_tasks_delay = 3
        tasks._config.max_workers = 4
        tasks._config.scale_factor = 5
        tasks._config.when_window = 6
        tasks._scheduler = None
        tasks.start_scheduler()
        tasks._logger.debug.assert_called_once_with("Starting scheduler")
        tasks._scheduler_class.assert_called_once_with(
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
    @patch.object(tasks, "_scheduler")
    def test_stop_scheduler(self, *_):
        tasks._scheduler.status = TaskScheduler.statuses.RUNNING
        tasks._scheduler_class = TaskScheduler
        tasks.stop_scheduler()
        tasks._logger.debug.assert_called_once_with("Stopping scheduler")
        tasks._scheduler.stop.assert_called_once_with(force=False)

        tasks._scheduler = None
        with self.assertRaises(RuntimeError):
            tasks.stop_scheduler()

    @patch.object(tasks, "_scheduler")
    def test_is_scheduler_running(self, *_):
        tasks._scheduler.status = TaskScheduler.statuses.RUNNING
        self.assertTrue(tasks.is_scheduler_running())

        tasks._scheduler.status = tasks._scheduler_class.statuses.STOPPING
        self.assertTrue(tasks.is_scheduler_running())

        tasks._scheduler.status = tasks._scheduler_class.statuses.STOPPED
        self.assertFalse(tasks.is_scheduler_running())

        tasks._scheduler = None
        with self.assertRaises(RuntimeError):
            tasks.is_scheduler_running()

    @patch.object(tasks, "_logger")
    @patch.object(tasks, "_tasks")
    @patch.object(tasks, "_task_class")
    @patch.object(tasks, "_config")
    @patch.object(tasks, "_queue_pool")
    def test_set_task(self, *_):
        tasks._tasks = {}
        qh = MagicMock()
        tasks._queue_pool = {"test_tasks": qh}
        tasks._config.name = "tasks"
        tasks._config.project = "test"
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
        tasks._queue_pool = {"test_tasks": qh}
        tasks._config.name = "tasks"
        tasks._config.project = "test"
        tasks.run("test", {"one": 1}, service_name=None, project_name=None, when="23/02/1990 14:00:00")
        tasks._task_class.send.assert_called_once_with(
            "test", {"one": 1}, qh, "23/02/1990 14:00:00"
        )

        tasks.run("test", {"one": 1}, service_name="other", project_name="test", when="23/02/1990 14:00:00")
        tasks._queue_class.assert_called_with("test_other")
        tasks._task_class.send.assert_called_with(
            "test", {"one": 1}, tasks._queue_pool["test_other"], "23/02/1990 14:00:00"
        )

        tasks._tasks = {}
        with self.assertRaises(RuntimeError):
            tasks.run("test", {"one": 1}, service_name=None, project_name=None, when="23/02/1990 14:00:00")

    @patch.object(tasks, "_queue_pool")
    @patch.object(tasks, "_scheduler")
    def test_ready(self, *_):
        tasks._scheduler.get_status.return_value = TaskScheduler.statuses.RUNNING
        tasks._queue_pool = {"test": MagicMock()}
        self.assertEqual(tasks.ready(), {"TASK_QUEUES": "OK", "TASK_SCHEDULER": "OK"})

        tasks._scheduler = None
        tasks._queue_pool = {}
        self.assertEqual(tasks.ready(), {"TASK_QUEUES": "ERROR", "TASK_SCHEDULER": "ERROR"})

    def test_task_class(self):
        tasks._task_class = None
        tasks.set_task_class("Class")
        self.assertEqual(tasks._task_class, "Class")
        self.assertEqual(tasks.get_task_class(), "Class")

    def test_scheduler_class(self):
        tasks._scheduler_class = None
        tasks.set_scheduler_class("Class")
        self.assertEqual(tasks._scheduler_class, "Class")
        self.assertEqual(tasks.get_scheduler_class(), "Class")

    def test_queue_class(self):
        tasks._queue_class = None
        tasks.set_queue_class("Class")
        self.assertEqual(tasks._queue_class, "Class")
        self.assertEqual(tasks.get_queue_class(), "Class")
