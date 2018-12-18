#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) SambaTech. All rights reserved.

created_at: 09-JUL-2018
updated_at: 09-JUL-2018

"""
from samba_chassis import logging
from samba_chassis.logging import stackdriver
from mock import MagicMock, patch
import unittest


class LoggingTest(unittest.TestCase):

    def test_loggin(self):
        logger = logging.getLogger("test")
        logger.addHandler(stackdriver.ContainerEngineHandler())
        logger.setLevel("DEBUG")
        logger.debug("test log record")

    @patch("math.modf")
    def test_format_stackdriver_json(self, modf):
        record = MagicMock(
            thread=1,
            levelname="lname",
            pathname="pname",
            lineno=1,
            exc_text="exc_text"
        )
        record.name = "module name"
        modf.return_value = 1, 1

        self.assertEqual(
            '{"_mock_call_count": 0, "timestamp": {"seconds": 1, "nanos": 1000000000}, "exception": ["exc_text"], "module": "module name", "_mock_call_args": null, "_spec_class": null, "file": "pname", "message": "test message", "_mock_called": false, "line": 1, "_mock_name": null, "method_calls": [], "_mock_new_name": "", "_mock_methods": null, "severity": "lname", "thread": 1, "_mock_wraps": null, "_mock_side_effect": null, "_mock_delegate": null, "_mock_new_parent": null, "_spec_signature": null, "_mock_parent": null, "_mock_call_args_list": [], "_mock_mock_calls": [], "_spec_set": null, "_mock_unsafe": false}',
            stackdriver._format_stackdriver_json(record, "test message")
        )
