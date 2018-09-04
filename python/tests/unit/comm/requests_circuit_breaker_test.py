#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) SambaTech. All rights reserved.

created_at: 04-SEP-2018
updated_at: 04-SEP-2018

Circuit breaker module test.
"""
import unittest
from mock import MagicMock, patch
from datetime import datetime


class RequestsCircuitBreakerTests(unittest.TestCase):
    @patch("samba_chassis.comm.requests_circuit_breaker._timed_out")
    @patch("requests.request")
    def test_get(self, request, utcnow):
        from samba_chassis.comm import requests_circuit_breaker as rcb
        print rcb.get("http://test.com/api/endpoint")

