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