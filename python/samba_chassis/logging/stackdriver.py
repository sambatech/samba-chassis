#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) SambaTech. All rights reserved.

created_at: 09-JUL-2018
updated_at: 31-JUL-2018

"""
import logging
import json
import math


class ContainerEngineHandler(logging.StreamHandler):
    """Handler to format log messages the format expected by GKE fluent.
    This handler is written to format messages for the Google Container Engine
    (GKE) fluentd plugin, so that metadata such as log level are properly set.
    """
    def format(self, record):
        message = super(ContainerEngineHandler, self).format(record)
        return _format_stackdriver_json(record, message)


def _format_stackdriver_json(record, message):
    """Helper to format a LogRecord in in Stackdriver fluentd format."""
    subsecond, second = math.modf(record.created)

    payload = {
        'message': message.replace("\"", "'").replace("\n", "; "),
        'timestamp': {
            'seconds': int(second),
            'nanos': int(subsecond * 1e9),
        },
        'thread': record.thread,
        'severity': record.levelname,
        'module': record.name,
        'file': record.pathname,
        'line': record.lineno,
    }

    if record.exc_text is not None:
        # Add exception info if it exists
        payload['exception'] = record.exc_text.replace("\"", "'").split("\n")

    return json.dumps(payload)
