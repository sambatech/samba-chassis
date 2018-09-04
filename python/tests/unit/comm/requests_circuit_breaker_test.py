#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) SambaTech. All rights reserved.

created_at: 04-SEP-2018
updated_at: 04-SEP-2018

Circuit breaker module test.
"""
import unittest
from mock import MagicMock, patch, ANY


class RequestsCircuitBreakerTests(unittest.TestCase):
    @patch("samba_chassis.comm.requests_circuit_breaker._timed_out")
    @patch("requests.request")
    def test_get(self, request, utcnow):
        from samba_chassis.comm import requests_circuit_breaker as rcb
        request.return_value = "test"
        response = rcb.get("http://test.com/api/endpoint")
        self.assertEqual(response, "test")

    @patch("samba_chassis.comm.requests_circuit_breaker._timed_out")
    @patch("requests.request")
    def test_post(self, request, utcnow):
        from samba_chassis.comm import requests_circuit_breaker as rcb
        request.return_value = "test"
        response = rcb.post("http://test.com/api/endpoint")
        self.assertEqual(response, "test")

    @patch("samba_chassis.comm.requests_circuit_breaker._timed_out")
    @patch("requests.request")
    def test_put(self, request, utcnow):
        from samba_chassis.comm import requests_circuit_breaker as rcb
        request.return_value = "test"
        response = rcb.put("http://test.com/api/endpoint")
        self.assertEqual(response, "test")

    @patch("samba_chassis.comm.requests_circuit_breaker._timed_out")
    @patch("requests.request")
    def test_delete(self, request, utcnow):
        from samba_chassis.comm import requests_circuit_breaker as rcb
        request.return_value = "test"
        response = rcb.delete("http://test.com/api/endpoint")
        self.assertEqual(response, "test")

    @patch("samba_chassis.comm.requests_circuit_breaker._timed_out")
    @patch("requests.request")
    def test_head(self, request, utcnow):
        from samba_chassis.comm import requests_circuit_breaker as rcb
        request.return_value = "test"
        response = rcb.head("http://test.com/api/endpoint")
        self.assertEqual(response, "test")

    @patch("samba_chassis.comm.requests_circuit_breaker._timed_out")
    @patch("requests.request")
    def test_open(self, request, utcnow):
        from samba_chassis.comm import requests_circuit_breaker as rcb
        request.side_effect = Exception()
        with self.assertRaises(Exception):
            rcb.get("http://test.com/api/endpoint")

        for i in range(0, 9):
            self.assertEqual(rcb._register["http://test.com/api/endpoint"]['fails'], i+1)
            self.assertEqual(rcb._register["http://test.com/api/endpoint"]['successes'], 0)
            self.assertEqual(rcb._register["http://test.com/api/endpoint"]['state'], "CLOSED")
            self.assertEqual(rcb._register["http://test.com"]['fails'], i+1)
            self.assertEqual(rcb._register["http://test.com"]['successes'], 0)
            self.assertEqual(rcb._register["http://test.com"]['state'], "CLOSED")
            with self.assertRaises(Exception):
                rcb.get("http://test.com/api/endpoint")

        self.assertEqual(rcb._register["http://test.com/api/endpoint"]['fails'], 0)
        self.assertEqual(rcb._register["http://test.com/api/endpoint"]['successes'], 0)
        self.assertEqual(rcb._register["http://test.com/api/endpoint"]['state'], "OPENED")
        self.assertEqual(rcb._register["http://test.com"]['fails'], 0)
        self.assertEqual(rcb._register["http://test.com"]['successes'], 0)
        self.assertEqual(rcb._register["http://test.com"]['state'], "OPENED")
