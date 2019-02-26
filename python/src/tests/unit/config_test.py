#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) SambaTech. All rights reserved.

created_at: 06-JUL-2018
updated_at: 06-JUL-2018

"""
import pytest
import os
import samba_chassis
from samba_chassis import config
from mock import MagicMock
import unittest


class JobTrackerTests(unittest.TestCase):

    def test_config_item(self):
        c = config.ConfigItem(
                default="alejandro",
                type=str,
                rules=[lambda x: True if x.startswith("a") else False]
            )
        assert c.current == "alejandro"
        c.current = "alejo"
        assert c.eval() is True

        c.current = 10
        assert c.eval() is False

    def test_config_layout(self):
        config._config_ledger["default"] = MagicMock(config_a="allepo", config_b="bill")

        config_a = config.ConfigItem(
            default="alejandro",
            type=str,
            rules=[lambda x: True if x.startswith("a") else False]
        )
        config_b = config.ConfigItem(
            default="bernardo",
            type=str,
            rules=[lambda x: True if x.startswith("b") else False]
        )

        c = config.ConfigLayout({
            "config_a": config_a,
            "config_b": config_b
        })
        assert c.simple_dict == {".config_a": config_a, ".config_b": config_b}

    def test_require_env_var(self):
        os.environ = {"VARC1": 1, "VARC2": 2, "SYSTEM1": 1}
        assert config.require_env_var("SYSTEM1", rules=[lambda x: True if isinstance(x, int) else False]) == 1

        with pytest.raises(ValueError):
            assert config.require_env_var("SYSTEM2", rules=[lambda x: True if isinstance(x, int) else False]) == 1

        assert config.require_env_var("SYSTEM2", default=8) == 8

    def  test_get(self):
        dictionary = {
            "t1": 1,
            "t2": {
                "t3": 3,
                "t4": [4, 5, (6, 7)]
            }
        }
        config._config_ledger["default"] = config._objectify("Test", dictionary)

        ob = config.get("t2")
        assert ob.t3 == 3

    def test_set(self):
        config._config_ledger = {}
        os.environ = {"VARC1": 1, "VARC2": 2, "SYSTEM1": 1}
        config.set("VAR.env")
        ob = config._config_ledger["default"]
        assert ob.C1 == 1

    def test_retrieve(self):
        dictionary = {
            "t1": 1,
            "t2": {
                "t3": 3,
                "t4": [4, 5, (6, 7)]
            }
        }

        ob = config._objectify("Test", dictionary)
        assert config._retrieve(ob, ["t2", "", "t4", "2", "0"]) == 6

    def test_dict_from_env(self):
        os.environ = {"VARC1": 1, "VARC2": 2, "SYSTEM1": 1}
        d = samba_chassis.dict_from_env("VAR")
        assert d == {'C1': 1, 'C2': 2}

    def test_objectify(self):
        dictionary = {
            "t1": 1,
            "t2": {
                "t3": 3,
                "t4": [4, 5, (6, 7)]
            }
        }

        ob = config._objectify("Test", dictionary)
        assert ob.t1 == 1
        assert ob.t2.t3 == 3
        assert ob.t2.t4[0] == 4
        assert ob.t2.t4[2][0] == 6

    def test_simplify(self):
        dictionary = {
            "t1": 1,
            "t2": {
                "t3": 3,
                "t4": [4, 5, (6, 7)]
            }
        }
        assert config._simplify(dictionary) == \
               {'.t1': 1, '.t2.t4.0': 4, '.t2.t4.1': 5,
                '.t2.t4.2.0': 6, '.t2.t4.2.1': 7, '.t2.t3': 3}

    def test_alias(self):
        dictionary = {
            "t1": 1,
            "t2": {
                "t3": 3,
                "t4": [4, 5, (".t1", 7)]
            }
        }
        with pytest.warns(UserWarning):
            ob = config._objectify("Object", dictionary)
        assert ob.t2.t4[2][0] == ".t1"
        ob = config._objectify("Object", dictionary, config._simplify(dictionary))
        assert ob.t2.t4[2][0] == 1

    def test_cap_first(self):
        assert samba_chassis.cap_first("snAke_case") == "SnAkeCase"
