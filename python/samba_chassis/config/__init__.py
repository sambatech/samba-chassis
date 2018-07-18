#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) SambaTech. All rights reserved.

created_at: 05-JUL-2018
updated_at: 06-JUL-2018

"""
from collections import namedtuple
import os
import warnings
import samba_chassis

_config_ledger = {}


class ConfigItem(object):
    """
    Configuration item to be used in configuration layouts.

    It provides default value capabilities, can enforce types and restriction rules.
    """
    def __init__(self, default=None, type=None, rules=[]):
        self.default = default
        self.current = default
        self.type = type
        self.rules = rules

    def eval(self):
        if self.type is not None and not isinstance(self.current, self.type):
            return False
        for rule in self.rules:
            if not rule(self.current):
                return False
        return True


class ConfigLayout(object):
    """
    Configuration layout that enforces a set of configuration objects to exist and follow set rules.

    Example of a configuration layout definition:
    ConfigLayout({
        config_a: ConfigItem(
            default="alejandro",
            type=str,
            rules=[lambda x: True if x.startswith("a") else False]
        )
        config_b: ConfigItem(
            default="bernardo",
            type=str,
            rules=[lambda x: True if x.startswith("b") else False]
        )
    })
    """
    def __init__(self, config_dict):
        """
        Initialize object with the configuration dictionary and a simple version of it.

        :param config_dict: Configuration dictionary with ConfigItem elements.
        """
        self.config_dict = config_dict
        self.simple_dict = _simplify(config_dict)
        print "S:", self.simple_dict

    def get(self, config_object=None, base=None, config_entry="default"):
        """
        Get configuration object compliant to the layout.

        :param config_object:
        :param base:
        :param config_entry:
        :return:
        """
        # Set configuration object if necessary
        if config_object is None:
            if base is None:
                config_object = _config_ledger[config_entry]
            else:
                path = base.split(".")
                if config_entry in _config_ledger:
                    config_object = _retrieve(_config_ledger[config_entry], path)
                else:
                    config_object = _objectify("Config", {})
        # Build final configuration object
        for key in self.simple_dict:
            path = key.split(".")
            try:
                self.simple_dict[key].current = _retrieve(config_object, path)
            except KeyError:
                warnings.warn("Layout item {} not found in config object".format(key))
            if not self.simple_dict[key].eval():
                raise ValueError("{} is divergent".format(key))

        return _objectify("LayoutObject", self.config_dict)


def require_env_var(name, default=None, rules=[]):
    """
    Require an environment variable to bet set and follow defined restriction rules.

    Obs. Default doesn't have to follow the restriction rules.
    :param name: Environment variable name.
    :param default: Environment variable default value.
    :param rules: Functions to evaluate the variable's value. Each function must return True if the value passes.
    :return: The environment variable value.
    """
    try:
        env_var = os.environ[name]
        for rule in rules:
            if not rule(env_var):
                raise ValueError("Env var {} does not satisfy rules".format(name))
    except KeyError:
        if default is not None:
            os.environ[name] = default
        else:
            raise ValueError("Env var {} does not exist and there is no default value for it".format(name))
    return os.environ[name]


def get(base=None, config_entry="default", config_layout=None):
    """
    Get configuration object.

    It uses base to establish the root node in the hierarchical tree and config entry to
    choose which configuration object to return. It is possible to force a configuration layout
    by passing it as an argument.
    :param base: hierarchical root for the return object.
    :param config_entry: The data entry to query.
    :param config_layout: A config layout to enforce.
    :return: Configuration object.
    """
    if base is None:
        return _config_ledger[config_entry]
    path = base.split(".")
    ob = _retrieve(_config_ledger[config_entry], path)
    if config_layout is not None:
        return config_layout.get(config_object=ob)
    else:
        return ob


def set(name, config_entry="default"):
    """
    Process external configuration data.

    It is possible to add multiple configuration data by using multiple config entries.
    TODO: Allow multiple adds into the same config entry in a merge sort of way
    :param name: file name (can be yml or json) or "[prefix].env" for use of environment variables.
    :param config_entry: name of the entry in the configuration ledger to add to.
    :return: True upon success, False on failure.
    """
    ext = name.split(".")[-1]
    if ext == "env":
        config_dict = samba_chassis.dict_from_env(name.split(".")[0])
    elif ext in ["json"]:
        config_dict = samba_chassis.dict_from_json(name)
    elif ext in ["yml", "yaml"]:
        config_dict = samba_chassis.dict_from_yaml(name)
    else:
        raise ValueError("File extension not supported")

    alias_dict = _simplify(config_dict)

    config_object = _objectify("Object", config_dict, alias_dict)

    _config_ledger[config_entry] = config_object


def _retrieve(ob, path):
    """
    Retrieve object information from path.

    :param ob: object for retrieval.
    :param path: list defining the path for object tree traversal.
    :return: the object within defined path.
    """
    for target in path:
        if target == "":
            continue
        try:
            index = int(target)
            ob = ob[index]
        except ValueError:
            ob = ob.__dict__[target]
    return ob


def _objectify(name, element, alias_dict={}):
    """
    Transform a dictionary into an immutable python object recursively.

    It also satisfies aliasing references to boot.
    :param name: element's name.
    :param element: element, duh!
    :param alias_dict: alias dictionary.
    :return: an object sub-structure.
    """
    if isinstance(element, dict):
        return namedtuple("Config{}".format(samba_chassis.cap_first(name)), element.keys())(
            *[_objectify(i[0], i[1], alias_dict) for i in element.items()]
        )
    if isinstance(element, list):
        return [_objectify("ListElement", i, alias_dict) for i in element]
    if isinstance(element, tuple):
        return [_objectify("TupleElement", i, alias_dict) for i in element]
    if isinstance(element, ConfigItem):
        return element.current
    try:
        return alias_dict[element] if isinstance(element, basestring) and element[0] == "." else element
    except KeyError:
        warnings.warn("Alias reference {} can't be dereferenced".format(element))
        return element


def _simplify(element, name=""):
    """
    Simplify a dictionary into a map (alias):(non container element) recursively.

    :param name: element's name
    :param element: element, duh!
    :return: a simplified sub-map
    """
    s = {}
    if isinstance(element, dict):
        for key in element:
            s.update(_simplify(element[key], "{}.{}".format(name, key)))
    elif isinstance(element, list):
        for i in range(len(element)):
            s.update(_simplify(element[i], "{}.{}".format(name, i)))
    elif isinstance(element, tuple):
        for i in range(len(element)):
            s.update(_simplify(element[i], "{}.{}".format(name, i)))
    else:
        s[name] = element
    return s



