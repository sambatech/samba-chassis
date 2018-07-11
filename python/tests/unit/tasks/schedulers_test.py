#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) SambaTech. All rights reserved.

created_at: 11-JUN-2018
updated_at: 11-JUL-2018
"""
from samba_chassis import tasks
from samba_chassis.tasks import *
from mock import MagicMock, patch, ANY
import unittest
import boto3
from datetime import datetime, timedelta
import warnings


class TaskSchedulerTest(unittest.TestCase):

    @patch("threading.Thread")
    def test_start(self, T):
        qh = MagicMock()
        ts = TaskScheduler(qh, {"test_task": MagicMock()}, max_workers=2)
        self.assertEqual(ts.status, TaskScheduler.statuses.STOPPED)
        ts.start()
        self.assertEqual(ts.status, TaskScheduler.statuses.RUNNING)
        T.assert_called_with(target=ts.loop)
        T().start.assert_called_once()

    def test_stop(self):
        qh = MagicMock()
        ts = TaskScheduler(qh, {"test_task": MagicMock()}, max_workers=2)
        ts.status = TaskScheduler.statuses.RUNNING
        ts.stop()
        self.assertEqual(ts.status, TaskScheduler.statuses.STOPPING)

        ts.stop(force=True)
        self.assertEqual(ts.status, TaskScheduler.statuses.STOPPED)

    @patch.object(tasks.schedulers.TaskScheduler, "_logger")
    @patch("threading.Thread")
    def test_run_tasks(self, T, l):
        qh = MagicMock()
        te = MagicMock()
        te.task.name = "test"
        te.exec_id = "id"
        te.attr = {1: "one"}

        ts = TaskScheduler(qh, {"test_task": MagicMock()}, max_workers=2)
        ts._run_tasks([te])

        l.info.assert_called_with("RUNNING_TASK: test id", extra={1: "one"})
        T.assert_called_with(target=te.execute)
        self.assertEqual(ts._on_going_tasks["id"], te)

    @patch.object(tasks.schedulers.TaskScheduler, "_logger")
    @patch("datetime.datetime")
    def test_get_new_tasks(self, d, l):
        d.utcnow.return_value = "now"
        qh = MagicMock(task_timeout=60)
        ts = TaskScheduler(qh, {"test_task": MagicMock()}, max_workers=2)
        m1 = MagicMock(
            message_attributes={
                "task_name": {"StringValue": "test_task"},
                "exec_id": {"StringValue": "id"},
                "when": {"StringValue": datetime.utcnow().strftime("%d/%m/%y %H:%M:%S")}
            },
            body='{"1": "one"}',
            attributes={'ApproximateReceiveCount': 2}
        )
        m2 = MagicMock(message_attributes="wrong", body="wrong_body")
        qh.retrieve.return_value = [m1, m2]

        res = ts._get_new_tasks(5)
        l.warn.assert_called_with("RECEIVED_UNKNOWN_TASK: header = wrong attr = wrong_body")
        self.assertIsInstance(res[0], TaskExecution)
        self.assertEqual(len(res), 1)

    def test_is_known_task(self):
        qh = MagicMock(task_timeout=60)
        ts = TaskScheduler(qh, {"test_task": MagicMock()}, max_workers=2)

        self.assertTrue(ts._is_known_task(MagicMock(message_attributes={"task_name": {"StringValue": "test_task"}})))
        self.assertFalse(ts._is_known_task(MagicMock(message_attributes="wrong")))

    @patch.object(tasks.schedulers.TaskScheduler, "_logger")
    def test_process_dead_thread(self, l):
        qh = MagicMock(task_timeout=60)
        te = MagicMock(disabled=True, exec_id="id", message="message")
        te.task.name = "test_task"
        ts = TaskScheduler(qh, {"test_task": MagicMock()}, max_workers=2)
        ts._on_going_tasks[te.exec_id] = te
        ts._process_dead_thread(te, [])
        l.error.assert_called_once_with("DEAD_THREAD: test_task id")
        qh.done.assert_called_once_with("message")

    def test_passed_when(self):
        qh = MagicMock(task_timeout=60)
        te = MagicMock(disabled=True, exec_id="id", message="message")
        te.task.name = "test_task"
        ts = TaskScheduler(qh, {"test_task": MagicMock()}, max_workers=2, when_window=60)
        m = MagicMock(
            message_attributes={
                "when": {
                    "StringValue": (datetime.utcnow() + timedelta(seconds=30)).strftime("%d/%m/%y %H:%M:%S")
                }
            }
        )
        self.assertTrue(ts._passed_when(m))
        m = MagicMock(
            message_attributes={
                "when": {
                    "StringValue": (datetime.utcnow() + timedelta(seconds=90)).strftime("%d/%m/%y %H:%M:%S")
                }
            }
        )
        self.assertFalse(ts._passed_when(m))
        with patch.object(ts, "_logger") as l:
            now = (datetime.utcnow() - timedelta(seconds=30))
            m = MagicMock(
                message_attributes={
                    "when": {
                        "StringValue": now.strftime("%d/%m/%y %H:%M:%S")
                    },
                    "exec_id": {
                        "StringValue": "test"
                    }
                }
            )
            self.assertTrue(ts._passed_when(m))
            self.assertEqual(l.warn.call_count, 1)

    def test_when_to_seconds(self):
        qh = MagicMock(task_timeout=60)
        te = MagicMock(disabled=True, exec_id="id", message="message")
        te.task.name = "test_task"
        ts = TaskScheduler(qh, {"test_task": MagicMock()}, when_window=5)
        when = (datetime.utcnow() + timedelta(seconds=30))
        m = MagicMock(
            message_attributes={
                "when": {
                    "StringValue": when.strftime("%d/%m/%y %H:%M:%S")
                }
            }
        )
        self.assertEqual(25, ts._when_to_seconds(m))
        when = (datetime.utcnow() + timedelta(seconds=25000))
        m = MagicMock(
            message_attributes={
                "when": {
                    "StringValue": when.strftime("%d/%m/%y %H:%M:%S")
                }
            }
        )
        self.assertEqual(18000, ts._when_to_seconds(m))

    def test_process_task_results(self):
        qh = MagicMock(task_timeout=60)
        te = MagicMock(disabled=True, exec_id="id", message="message")
        te.task.name = "test_task"
        ts = TaskScheduler(qh, {"test_task": MagicMock()})
        # Success
        task_exec = MagicMock(results=True, exec_id="test", message="Help")
        bt = []
        ts._process_task_results(task_exec, bt)
        qh.done.assert_called_once_with("Help")
        self.assertEqual([task_exec.exec_id], bt)
        # Failure
        task_exec = MagicMock(results=False, exec_id="test", message="Help", created_at=datetime.utcnow(), attempts=1)
        task_exec.task.get_delay.return_value = 10
        bt = []
        ts._process_task_results(task_exec, bt)
        qh.done.assert_called_once_with("Help")
        self.assertEqual([task_exec.exec_id], bt)
        self.assertEqual(qh.postpone.call_count, 1)

    def test_postpone_failed(self):
        qh = MagicMock(task_timeout=60)
        ts = TaskScheduler(qh, {"test_task": MagicMock()})
        te = MagicMock(results=True, exec_id="test", message="Help", disabled=False, attr="attr")
        bt = []
        ts._postpone_failed(te, bt)
        te.task.issue.assert_called_once_with("attr", 0, "test")
        self.assertTrue(te.disabled)
        qh.done.assert_called_once_with("Help")
        self.assertEqual([te.exec_id], bt)

    def test_process_on_going_tasks(self):
        qh = MagicMock(task_timeout=60)
        tt = MagicMock()
        ts = TaskScheduler(qh, {"test_task": tt})
        # Test first check
        te = MagicMock(
            results=True,
            exec_id="test",
            message="Help",
            created_at=datetime.utcnow(),
            attempts=1
        )
        ts._on_going_tasks = {
            "test": te
        }
        with patch.object(ts, "_process_task_results") as p:
            ts._process_on_going_tasks()
            p.assert_called_once_with(te, [])
        # Test second check
        te = MagicMock(
            results=None,
            exec_id="test",
            message="Help",
            created_at=datetime.utcnow(),
            attempts=1
        )
        te.thread.is_alive.return_value = False
        ts._on_going_tasks = {
            "test": te
        }
        with patch.object(ts, "_process_dead_thread") as p:
            ts._process_on_going_tasks()
            p.assert_called_once_with(te, [])
        # Test third check
        te = MagicMock(
            results=None,
            exec_id="test",
            message="Help",
            created_at=datetime.utcnow(),
            attempts=1
        )
        te.thread.is_alive.return_value = True
        te.get_deadline.return_value = datetime.utcnow() - timedelta(seconds=30)
        te.postpone.return_value = False
        ts._on_going_tasks = {
            "test": te
        }
        with patch.object(ts, "_postpone_failed") as p:
            ts._process_on_going_tasks()
            p.assert_called_once_with(te, [])

    def test_loop(self):
        pass
