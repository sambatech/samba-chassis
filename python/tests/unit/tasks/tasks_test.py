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