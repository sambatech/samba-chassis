#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) SambaTech. All rights reserved.

created_at: 09-JUL-2018
updated_at: 09-JUL-2018

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


class StackDriverLogger(logging.Logger):
    """Base logger class for using the features in this module."""
    def _log(self, level, msg, args, attr={}, exc_info=None, **kwargs):
        extra = attr.copy()
        extra["attr"] = attr
        super(StackDriverLogger, self)._log(level, msg, args, exc_info, extra)


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

    try:
        payload.update(record.attr)
    except AttributeError:
        logging.warning("Logger without attr")

    return json.dumps(payload)
