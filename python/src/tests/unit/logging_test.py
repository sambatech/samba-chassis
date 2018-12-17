#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) SambaTech. All rights reserved.

created_at: 09-JUL-2018
updated_at: 09-JUL-2018

"""
import samba_chassis
from samba_chassis import logging
from samba_chassis.logging import stackdriver
from mock import MagicMock, patch
import unittest


config_dict = {
    'version': 1,
    'disable_existing_loggers': False,
    'loggers': {
        'help': {'handlers': ['hell'], 'propagate': True, 'level': 'DEBUG'},
        'default': {'handlers': ['console'], 'propagate': True, 'level': 'DEBUG'}
    },
    'formatters': {
        'default': {'format': '[%(levelname)s] %(name)s: %(message)s'},
        'help': {'format': '[%(asctime)s] %(process)d %(levelname)s %(name)s:%(funcName)s:%(lineno)s - %(message)s'}
    },
    'handlers': {
        'hell': {'formatter': 'help', 'class': 'logging.StreamHandler', 'level': 'DEBUG'},
        'console': {'formatter': 'default', 'class': 'logging.StreamHandler', 'level': 'DEBUG'}
    }
}


class LoggingTest(unittest.TestCase):
    @patch.object(samba_chassis, "dict_from_json")
    @patch.object(samba_chassis, "dict_from_yaml")
    def test_get_set(self, *_):
        samba_chassis.dict_from_json.return_value = config_dict
        samba_chassis.dict_from_yaml.return_value = config_dict

        logging.set("test.json")
        assert logging._loggers == ['default', 'help']
        assert logging.default_logger == 'default'

        logging.set("test.yaml")
        assert logging._loggers == ['default', 'help']
        assert logging.default_logger == 'default'

        help_l = logging.get("help")
        assert "error" in dir(help_l)
        assert "info" in dir(help_l)
        assert "debug" in dir(help_l)
        assert "exception" in dir(help_l)

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
            '{"exception": ["exc_text"], "severity": "lname", "thread": 1, "timestamp": {"seconds": 1, "nanos": 1000000000}, "module": "module name", "file": "pname", "message": "test message", "line": 1}',
            stackdriver._format_stackdriver_json(record, "test message")
        )