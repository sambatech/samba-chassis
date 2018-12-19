#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) SambaTech. All rights reserved.

created_at: 09-JUL-2018
updated_at: 31-JUL-2018

This module provides a logging handler to be used with google's stackdriver and kubernetes engine with fluentd.

To use this module all you need is to add the handler to you logger:

from samba_chassis.logging import stackdriver
logger.addhandler(ContainerEngineHandler())
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


# This itens won't be added as extra information in the stackdriver logging record
_items_to_pop_from_record_copy = [
    'relativeCreated',
    'process',
    'module',
    'funcName',
    'message',
    'filename',
    'levelno',
    'processName',
    'lineno',
    'msg',
    'args',
    'exc_text',
    'name',
    'thread',
    'created',
    'threadName',
    'msecs',
    'pathname',
    'exc_info',
    'levelname'
]


def _is_jsonable(x):
    try:
        json.dumps(x)
        return True
    except (TypeError, OverflowError):
        return False


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

    extra_info = dict(record.__dict__)
    to_pop = []
    for item in extra_info:
        if item in _items_to_pop_from_record_copy or not _is_jsonable(extra_info[item]):
            to_pop.append(item)
    for item in to_pop:
        extra_info.pop(item)

    payload.update(extra_info)

    if record.exc_text is not None:
        # Add exception info if it exists
        payload['exception'] = record.exc_text.replace("\"", "'").split("\n")

    return json.dumps(payload)
