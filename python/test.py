from collections import namedtuple
import json
import yaml
import os


def get():
    pass


def add(name):
    """
    Process external configuration data.

    :param name: file name (can be yml or json) or "env" for use of environment variables.
    :return: True upon success, False on failure.
    """
    ext = name.split(".")[-1]
    if ext == "env":
        pass
    elif ext in ["json"]:
        pass
    elif ext in ["yml", "yaml"]:
        pass
    else:
        raise ValueError("File extension not supported")


def get_from_env():
    for k in globals():
        if not k.startswith("__") and k in os.environ:
            globals()[k] = os.environ[k]


def get_from_yaml(filename):
    with open(filename) as config_file:
        data = yaml.load(config_file)

    for k in globals():
        if not k.startswith("__") and k in data:
            globals()[k] = data[k]


def get_from_json(filename):
    with open(filename) as config_file:
        data = json.load(config_file)

    for k in globals():
        if not k.startswith("__") and k in data:
            globals()[k] = data[k]



def objectfy(name, element):
    if isinstance(element, dict):
        return namedtuple(name, element.keys())(*[objectfy(*i) for i in element.items()])
    if isinstance(element, list):
        return [objectfy("list_element", i) for i in element]
    if isinstance(element, tuple):
        return [objectfy("tuple_element", i) for i in element]
    return element


def process_aliases():
    pass



















with open("test.yml") as config_file:
    data = yaml.load(config_file)
print data


named = objectfy("Configuration", data)
print named

print named.moduleC.list[0]


