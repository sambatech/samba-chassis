#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) SambaTech. All rights reserved.

created_at: 09-JUL-2018
updated_at: 09-JUL-2018

"""
import logging
import logging.config
import samba_chassis


default_logger = logging.getLogger("samba_chassis")
_loggers = []


def set(filename):
    """
    Add a configuration file.

    :param filename: YAML or JSON file describing loggers.
    """
    ext = filename.split(".")[-1]
    if ext in ["json"]:
        config_dict = samba_chassis.dict_from_json(filename)
    elif ext in ["yml", "yaml"]:
        config_dict = samba_chassis.dict_from_yaml(filename)
    else:
        raise ValueError("File extension not supported")

    global _loggers
    _loggers = config_dict["loggers"].keys()
    global default_logger
    default_logger = "default" if "default" in _loggers else _loggers[0]

    logging.config.dictConfig(config_dict)


def get(name):
    """
    Return requested logger or default if it doesn't exist.

    :param name: Logger name.
    :return: Requested logger.
    """
    return logging.getLogger(name) if name in _loggers else logging.getLogger(default_logger)

