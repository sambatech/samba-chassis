#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) SambaTech. All rights reserved.

created_at: 11-JUN-2018
updated_at: 10-JUL-2018
"""
from samba_chassis import tasks
from samba_chassis.tasks import *
from mock import MagicMock, patch, ANY
import unittest
import boto3
from datetime import datetime, timedelta
import warnings


class TaskExecutionTest(unittest.TestCase):

    def test_execute(self):
        te = TaskExecution("1", MagicMock(execute=lambda: True), {1: {"one"}}, 1, datetime.utcnow(), MagicMock(), 30)
        te.execute()
        te.task.run.assert_called_with({1: {"one"}}, 0)

    def test_get_deadline(self):
        now = datetime.utcnow()
        te = TaskExecution("1", MagicMock(execute=lambda: True), {1: {"one"}}, 1, now, MagicMock(), 30)
        deadline = te.get_deadline()
        self.assertEqual(deadline, now + timedelta(seconds=15))

        te = TaskExecution("1", MagicMock(execute=lambda: True), {1: {"one"}}, 1, now, MagicMock(), 60)
        te.postpone_num = 3
        deadline = te.get_deadline()
        self.assertEqual(deadline, now + timedelta(seconds=120))

    def test_postpone(self):
        now = datetime.utcnow()
        te = TaskExecution("1", MagicMock(execute=lambda: True), {1: {"one"}}, 1, now, "message", 30)
        qh = MagicMock()
        te.postpone(qh)
        qh.postpone.assert_called_with("message", 45)


class TaskTest(unittest.TestCase):

    def test_send(self, *args):
        qh = MagicMock()
        Task.send("Test", {}, qh, 0, "id")
        qh.send.assert_called_once_with("Test", {}, 0, "id", None)

    @patch.object(tasks, "_logger")
    def test_invalid_progression(self, *args):
        qh = MagicMock()
        with self.assertRaises(ValueError):
            Task("Test", lambda: True, qh, 10, "fail", 10, "WRONG")
            tasks._logger.error.assert_called_with("INVALID_PROGRESSION")

    def test_get_delay(self, *args):
        qh = MagicMock()
        t = Task("Test", lambda: True, qh, 10, "fail", 10, "NONE")
        self.assertEqual(t.get_delay(0), 0)
        self.assertEqual(t.get_delay(3), 10)

        t = Task("Test", lambda: True, qh, 10, "fail", 10, "ARITHMETIC")
        self.assertEqual(t.get_delay(0), 0)
        self.assertEqual(t.get_delay(3), 30)

        t = Task("Test", lambda: True, qh, 10, "fail", 10, "GEOMETRIC")
        self.assertEqual(t.get_delay(0), 0)
        self.assertEqual(t.get_delay(3), 90)

        t = Task("Test", lambda: True, qh, 10, "fail", 10, "RANDOM")
        self.assertEqual(t.get_delay(0), 0)
        self.assertLessEqual(t.get_delay(3), 20)
        self.assertGreaterEqual(t.get_delay(3), 5)

    def test_issue(self, *args):
        qh = MagicMock()
        t = Task("Test", lambda: True, qh, 10, "fail", 10, "NONE")
        t.issue({1: "one"}, 10, "id")
        qh.send.assert_called_once_with("Test", {1: "one"}, 10, "id", None)

    def test_issue_fail(self, *args):
        qh1 = MagicMock()
        qh2 = MagicMock()
        t = Task("Test", lambda: True, qh1, 10, "fail", 10, "NONE")
        t.issue_fail({1: "one"})
        qh1.send.assert_called_with("fail", {1: "one"}, 0, None, None)

        t = Task("Test", lambda: True, qh1, 10, ("fail", qh2), 10, "NONE")
        t.issue_fail({1: "one"})
        qh2.send.assert_called_with("fail", {1: "one"}, 0, None, None)

    def test_run(self, *args):
        qh = MagicMock()

        t = Task("Test", lambda attr: True, qh, 10, "fail", 10, "NONE")
        self.assertTrue(t.run({1: "one"}, 0))

        t = Task("Test", lambda attr: False, qh, 10, "fail", 10, "NONE")
        self.assertFalse(t.run({1: "one"}, 0))

        t = Task("Test", lambda: True, qh, 10, "fail", 10, "NONE")
        self.assertFalse(t.run({1: "one"}, 0))


class QueueHandlerTest(unittest.TestCase):

    @patch.object(boto3, "client")
    @patch.object(boto3, "resource")
    def test_queue(self, *args):
        qh = QueueHandler("test_queue", 60)
        qh.connect()
        self.assertIsInstance(qh.queue, MagicMock)

    @patch.object(boto3, "client")
    @patch.object(boto3, "resource")
    def test_send(self, *args):
        qh = QueueHandler("test_queue", 60)
        qh.connect()
        qh.send("test", {1: "one"}, 10, "id")
        qh.queue.send_message.assert_called_once_with(
            DelaySeconds=10,
            MessageAttributes={
                'exec_id': {
                    'DataType': 'String',
                    'StringValue': 'id'
                },
                'when': {
                    'DataType': 'String',
                    'StringValue': datetime.utcnow().strftime("%d/%m/%y %H:%M:%S")
                },
                'task_name': {
                    'DataType': 'String',
                    'StringValue': 'test'
                }
            },
            MessageBody='{"1": "one"}'
        )

    @patch.object(boto3, "client")
    @patch.object(boto3, "resource")
    def test_retrieve(self, *args):
        qh = QueueHandler("test_queue", 60)
        qh.connect()
        with warnings.catch_warnings(record=True):
            qh.retrieve(50)
        qh.queue.receive_messages.assert_called_once_with(
            AttributeNames=[
                'ApproximateReceiveCount',
                'SentTimestamp'
            ],
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=10,
            VisibilityTimeout=60,
            WaitTimeSeconds=1,
        )

    def test_done(self, *args):
        m = MagicMock()
        QueueHandler.done(m)
        m.delete.assert_called_once()

    @patch.object(boto3, "client")
    @patch.object(boto3, "resource")
    def test_postpone(self, *args):
        m = MagicMock()
        self.assertTrue(QueueHandler.postpone(m, 60))
        m.change_visibility.called_once_with(VisibilityTimeout=60)

        m.change_visibility.side_effect = Exception()
        self.assertFalse(QueueHandler.postpone(m, 60))


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

        l.info.assert_called_with("RUNNING_TASK: test id", attr={1: "one"})
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
        l.warn.assert_called_once_with("RECEIVED_UNKNOWN_TASK: header = wrong attr = wrong_body")
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
        pass

    def test_when_to_seconds(self):
        pass

    def test_process_task_results(self):
        pass

    def test_postpone_failed(self):
        pass

    def test_process_on_going_tasks(self):
        pass

    def test_loop(self):
        pass


class ModuleTest(unittest.TestCase):

    def test_config(self):
        pass

    def test_start_scheduler(self):
        pass

    def test_stop_scheduler(self):
        pass

    def test_is_scheduler_running(self):
        pass

    def test_set_task(self):
        pass

    def test_task(self):
        pass

    def test_run(self):
        pass

    def test_ready(self):
        pass

    def test_task_class(self):
        pass

    def test_scheduler_class(self):
        pass

    def test_queue_class(self):
        pass