#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Copyright (c) SambaTech. All rights reserved.

created_at: 04-SEP-2018
updated_at: 04-SEP-2018

Circuit breaker module that uses the requests library.

A service client should invoke a remote service via a proxy
that functions in a similar fashion to an electrical circuit
breaker. When the number of consecutive failures crosses a
threshold, the circuit breaker trips, and for the duration of
a timeout period all attempts to invoke the remote service will
fail immediately. After the timeout expires the circuit breaker
allows a limited number of test requests to pass through. If
those requests succeed the circuit breaker resumes normal operation.
Otherwise, if there is a failure the timeout period begins again.

The circuit breaker has 3 distinct states, Closed, Open, and Half-Open:

Closed – When everything is normal, the circuit breaker remains
in the closed state and all calls pass through to the services.
When the number of failures exceeds a predetermined threshold the
breaker trips, and it goes into the Open state.

Open – The circuit breaker returns an error for calls without
executing the function.

Half-Open – After a timeout period, the circuit switches to a
half-open state to test if the underlying problem still exists.
If a single call fails in this half-open state, the breaker is
once again tripped. If it succeeds, the circuit breaker resets
back to the normal closed state.

Obs.: A request failure or success is registered for the entire
url as well as to all parent sub paths.
"""
from samba_chassis import config
import requests
import warnings
from urlparse import urlparse
from datetime import datetime, timedelta


# -------------
# Configuration
# -------------
import logging
_logger = logging.getLogger(__name__)

_config = None

config_layout = config.ConfigLayout({
    "request_timeout": config.ConfigItem(
        default=5,
        type=int,
        rules=[lambda x: True if x > 0 else False]
    ),
    "open_timeout": config.ConfigItem(
        default=10,
        type=int,
        rules=[lambda x: True if x > 0 else False]
    ),
    "closed_fail_limit": config.ConfigItem(
        default=10,
        type=int,
        rules=[lambda x: True if x > 0 else False]
    ),
    "closed_success_limit": config.ConfigItem(
        default=1,
        type=int,
        rules=[lambda x: True if x > 0 else False]
    ),
    "half_fail_limit": config.ConfigItem(
        default=1,
        type=int,
        rules=[lambda x: True if x > 0 else False]
    ),
    "half_success_limit": config.ConfigItem(
        default=10,
        type=int,
        rules=[lambda x: True if x > 0 else False]
    )
})


def config(config_object=None):
    """
    Configure module.

    :param config_object: A configuration object.
    """
    global _config
    if config_object is None:
        _config = config_layout.get(base="circuit_breaker")
    else:
        _config = config_layout.get(config_object=config_object)


with warnings.catch_warnings(UserWarning):
    config()


# ---------------
# Circuit Breaker
# ---------------
class RequestFailed(RuntimeError):
    pass


_register = {}


# INTERFACE
def request(method, url, **kwargs):
    if get_state(url) == 'OPENED':
        raise RequestFailed("CIRCUIT_BREAKER_OPEN")

    try:
        if 'timeout' not in kwargs:
            kwargs['timeout'] = _config.request_timeout
        r = requests.request(method, url, **kwargs)
    except Exception as e:
        # Failed
        _failed(url)
        raise RequestFailed(e.message)
    _succeeded(url)
    return r


def get(url, **kwargs):
    return request('GET', url, **kwargs)


def post(url, **kwargs):
    return request('POST', url, **kwargs)


def put(url, **kwargs):
    return request('PUT', url, **kwargs)


def delete(url, **kwargs):
    return request('DELETE', url, **kwargs)


def head(url, **kwargs):
    return request('HEAD', url, **kwargs)


def get_state(url):

    parsed_url = urlparse(url)
    entry = "{}://{}".format(parsed_url.scheme, parsed_url.netloc)
    state = _get_entry_state(entry)

    for piece in parsed_url.path.split('/'):
        if piece != "":
            entry += "/{}".format(piece)

            entry_state = _get_entry_state(entry)
            if entry_state == "OPENED":
                state = "OPENED"
            elif entry_state == "HALF" and state == "CLOSED":
                state = "HALF"

    return state


# PRIVATES
def _get_entry_state(entry):
    if entry not in _register:
        _add(entry)

    state = _register[entry]['state']

    if state == "OPENED" and _timed_out(entry):
        _half_open(entry)

    return state


def _timed_out(entry):
    return datetime.utcnow() > _register[entry]['timeout']


def _failed(url):
    parsed_url = urlparse(url)

    entry = "{}://{}".format(parsed_url.scheme, parsed_url.netloc)
    _fail(entry)
    for piece in parsed_url.path.split('/'):
        if piece != "":
            entry += "/{}".format(piece)
            _fail(entry)


def _succeeded(url):
    parsed_url = urlparse(url)

    entry = "{}://{}".format(parsed_url.scheme, parsed_url.netloc)
    _success(entry)
    for piece in parsed_url.path.split('/'):
        if piece != "":
            entry += "/{}".format(piece)
            _success(entry)


def _fail(entry):
    if entry not in _register:
        _add(entry)

    _register[entry]['fails'] += 1

    # Verify possible changes of state
    if (
        (_register[entry]['state'] == 'CLOSED' and _register[entry]['fails'] >= _config.closed_fail_limit)
        or
        (_register[entry]['state'] == 'HALF'   and _register[entry]['fails'] >= _config.half_fail_limit)
    ):
        _open(entry)


def _success(entry):
    if entry not in _register:
        _add(entry)

    _register[entry]['successes'] += 1

    if _register[entry]['state'] == 'CLOSED' and _register[entry]['successes'] >= _config.closed_success_limit:
        _clean(entry)
    elif _register[entry]['state'] == 'HALF' and _register[entry]['successes'] >= _config.half_success_limit:
        _close(entry)


def _add(entry):
    _register[entry] = {'state': 'CLOSED', 'fails': 0, 'successes': 0, 'timeout': datetime.utcnow()}


def _clean(entry):
    _register[entry]['fails'] = 0
    _register[entry]['successes'] = 0


def _open(entry):
    _clean(entry)
    _register[entry]['state'] = 'OPENED'
    _register[entry]['timeout'] = datetime.utcnow() + timedelta(seconds=_config.open_timeout)


def _close(entry):
    _clean(entry)
    _register[entry]['state'] = 'CLOSED'


def _half_open(entry):
    _clean(entry)
    _register[entry]['state'] = 'HALF'
