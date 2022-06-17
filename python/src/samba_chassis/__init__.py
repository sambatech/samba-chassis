import json
import yaml
import os


def dict_from_env(prefix=""):
    """
    Return a dictionary taken from the current environment variables.

    TODO: Implement hierarchy among multiple prefixes.
    :param prefix: Prefix that determines valid variables.
    :return: Dictionary with valid environment variable with the prefix removed.
    """
    r = {}
    for key in os.environ:
        if key.startswith(prefix):
            r[key[len(prefix):]] = os.environ[key]
    return r


def dict_from_yaml(filename):
    """Return a dictionary taken from an YAML file."""
    with open(filename) as config_file:
        return yaml.safe_load(config_file)


def dict_from_json(filename):
    """Return a dictionary taken from a JSON file."""
    with open(filename) as config_file:
        return json.load(config_file)


def cap_first(line):
    """
    Capitalize the first letter of each word in snake case format.

    :param line: snake case format line of characters
    :return: the good stuff
    """
    return ''.join(s[:1].upper() + s[1:] for s in line.split('_'))