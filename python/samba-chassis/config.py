from collections import namedtuple
import json
import yaml
import os
import warnings

_config_ledger = {}


class ConfigItem(object):
    def __init__(self, default=None, ctype=None, rules=[]):
        self.default = default
        self.current = default
        self.ctype = ctype
        self.rules = rules

    def eval(self):
        if self.ctype is not None and not isinstance(self.current, self.ctype):
            return False
        for rule in self.rules:
            if not rule(self.current):
                return False
        return True

class ConfigLayout(object):
    def __init__(self, config_dict):
        self.config_dict = config_dict
        self.simple_dict = _simplify("", config_dict)

    def get(self, config_object=None, base=None, config_entry="default"):
        if config_object is None:
            if base is None:
                config_object = _config_ledger[config_entry]
            else:
                path = base.split(".")
                config_object = _retrieve(_config_ledger[config_entry], path)
        for key in self.simple_dict:
            path = key.split(".")
            try:
                self.simple_dict[key].current = _retrieve(config_object, path)
            except KeyError:
                warnings.warn("Layout item {} not found in config object".format(key))
            if not self.simple_dict[key].eval():
                raise ValueError("{} is divergent".format(key))

        return _objectfy("LayoutObject", self.config_dict)


def require_env_var(name, default=None, rules=[]):
    try:
        env_var = os.environ[name]
        for rule in rules:
            if not rule(env_var):
                raise ValueError("Env var {} does not satisfy rules".format(name))
    except KeyError:
        os.environ[name] = default


def get(base=None, config_entry="default", config_layout=None):
    if base is None:
        return _config_ledger[config_entry]
    path = base.split(".")
    ob = _retrieve(_config_ledger[config_entry], path)
    if config_layout is not None:
        return config_layout.get(config_object=ob)
    else:
        return ob


def add(name, config_entry="default"):
    """
    Process external configuration data.

    TODO: Allow multiple adds into the same config entries in a merge sort of way
    :param name: file name (can be yml or json) or "env" for use of environment variables.
    :param config_entry: name of the entry in the configuration ledger to add to.
    :return: True upon success, False on failure.
    """
    ext = name.split(".")[-1]
    if ext == "env":
        config_dict = _dict_from_env()
    elif ext in ["json"]:
        config_dict = _dict_from_json(name)
    elif ext in ["yml", "yaml"]:
        config_dict = _dict_from_yaml(name)
    else:
        raise ValueError("File extension not supported")

    alias_dict = _simplify("", config_dict)

    config_object = _objectfy("Object", config_dict, alias_dict)

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


def _dict_from_env():
    """Return a dictionary taken from the current environment variables."""
    return os.environ


def _dict_from_yaml(filename):
    """Return a dictionary taken from an YAML file."""
    with open(filename) as config_file:
        return yaml.load(config_file)


def _dict_from_json(filename):
    """Return a dictionary taken from a JSON file."""
    with open(filename) as config_file:
        return json.load(config_file)


def _objectfy(name, element, alias_dict={}):
    """
    Transform a dictionary into an immutable python object recursively.

    It also satisfies aliasing references to boot.
    :param name: element's name.
    :param element: element, duh!
    :param alias_dict: alias dictionary.
    :return: an object sub-structure.
    """
    if isinstance(element, dict):
        return namedtuple("Config{}".format(_cap_first(name)), element.keys())(
            *[_objectfy(i[0], i[1], alias_dict) for i in element.items()]
        )
    if isinstance(element, list):
        return [_objectfy("ListElement", i, alias_dict) for i in element]
    if isinstance(element, tuple):
        return [_objectfy("TupleElement", i, alias_dict) for i in element]
    if isinstance(element, ConfigItem):
        return element.current
    return alias_dict[element] if isinstance(element, basestring) and element[0] == "." else element


def _simplify(name, element):
    """
    Simplify a dictionary into a map (alias):(non container element) recursively.

    :param name: element's name
    :param element: element, duh!
    :return: a simplified sub-map
    """
    s = {}
    if isinstance(element, dict):
        for key in element:
            s.update(_simplify("{}.{}".format(name, key), element[key]))
    elif isinstance(element, list):
        for i in range(len(element)):
            s.update(_simplify("{}.{}".format(name, i), element[i]))
    elif isinstance(element, tuple):
        for i in range(len(element)):
            s.update(_simplify("{}.{}".format(name, i), element[i]))
    else:
        s[name] = element
    return s


def _cap_first(line):
    """
    Capitalize the first letter of each word in snake case format.

    :param line: snake case format line of characters
    :return: the good stuff
    """
    return ' '.join(s[:1].upper() + s[1:] for s in line.split('_'))


