#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) SambaTech. All rights reserved.

created_at: 10-JUL-2018
updated_at: 10-JUL-2018
"""
import boto3
import warnings
import json
import uuid
from datetime import datetime

import logging
_logger = logging.getLogger(__name__)


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
