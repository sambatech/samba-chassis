#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) SambaTech. All rights reserved.

created_at: 09-JUL-2018
updated_at: 18-DEC-2018

This module provides a new Logger class more friendly to our micro services model.
It can be configured using samba-chassis config framework to define a service name.
The job id and name will always be present in the logging record.
The getLogger function returns a service friendly logger to simplify use and imports.

To use you need only to import this modules and write in your module:

from samba_chassis import logging
_logger = logging.getLogger(__name__)
"""
import logging
import logging.config


class ServiceLogger(logging.Logger):
    """
    This class is meant to simplify logging in micro services.

    It always adds to extra job_id and job_name.
    """
    def _log(self, level, msg, args, exc_info=None, extra={}, **kwargs):
        extra["job_id"] = kwargs.get("job_id", extra.get("job_id", "unknown"))

        extra["job_name"] = kwargs.get("job_name", extra.get("job_name", "unknown"))

        super(ServiceLogger, self)._log(level, msg, args, exc_info, extra)


_loggerClass = ServiceLogger


def getLogger(name=None):
    """
    Return a logger similar to logging's getLogger but always of _loggerClass class.

    :param name: Logger name. If no name is specified, return the root logger.
    :return: Returns a new logger.
    """
    def_klass = logging.getLoggerClass()
    if def_klass == _loggerClass:
        return logging.getLogger(name)

    logging.setLoggerClass(ServiceLogger)
    new_logger = logging.getLogger(name)
    logging.setLoggerClass(def_klass)
    return new_logger

