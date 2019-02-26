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
            '{"message": "test message", "timestamp": {"seconds": 1, "nanos": 1000000000}, "thread": 1, "severity": "lname", "module": "module name", "file": "pname", "line": 1, "_mock_parent": null, "_mock_name": null, "_mock_new_name": "", "_mock_new_parent": null, "_spec_class": null, "_spec_set": null, "_spec_signature": null, "_mock_methods": null, "_mock_wraps": null, "_mock_delegate": null, "_mock_called": false, "_mock_call_args": null, "_mock_call_count": 0, "_mock_call_args_list": [], "_mock_mock_calls": [], "method_calls": [], "_mock_unsafe": false, "_mock_side_effect": null, "exception": ["exc_text"]}',
            stackdriver._format_stackdriver_json(record, "test message")
        )
