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