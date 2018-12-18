## Python Microservice Chassis

### Introduction
Software development is an arms race against 
competitors and the clock. We require fast and
high quality deliveries to stay competitive.
Microservices architecture allows developers 
to build small and independent applications and
features capable of providing value to clients
rapidly and adaptively.

When you start the development of a service 
you often spend a significant amount of time 
putting in place the mechanisms to handle 
cross-cutting concerns. It is common to spend 
one or two days, sometimes even longer, setting 
up these mechanisms. If you going to spend months 
or years developing a monolithic application 
then the upfront investment in handling cross-cutting 
concerns is insignificant. The situation is very 
different, however, if you are developing an 
application that has the microservice architecture. 
There are tens or hundreds of services. You will 
frequently create new services, each of which will 
only take days or weeks to develop. You cannot 
afford to spend a few days configuring the mechanisms 
to handle cross-cutting concerns. What is even worse 
is that in a microservice architecture there are 
additional cross-cutting concerns that you have to 
deal with including service registration and discovery, 
and circuit breakers for reliably handling partial 
failure. 

Creating a new microservice should be fast 
and easy. When creating a microservice you must 
handle cross-cutting concerns such as externalized 
configuration, logging, health checks, metrics, 
service registration and discovery, circuit breakers. 
There are also cross-cutting concerns that are 
specific to the technologies that the microservices 
uses. So this project aims in providing a 
microservice chassis framework, which handles 
cross-cutting concerns as well other features that
can be heavily reused.

This is a python version of the project.

### Python package samba_chassis
#### Installation
this package uses setuptools and can be installed
using pip normally using pip:
```bash
~/samba-chassis/python$ pip install .
```
or pipenv (recommended):
```bash
~/samba-chassis/python$ pipenv install .
```

#### Features

##### Circuit Breaker
Present in the comm package, the module 
requests_circuit_breaker implements a circuit 
breaker over the requests package. It should
be used in place of the requests module.

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
  
##### Asynchronous Task Management
The package tasks implements a task consumer for asynchronous 
and reliable job executions.

With this package it is possible to schedule a task to be ran by
some thread or process in a reliable manner.
It uses AWS's SQS messaging queue service with one queue
shared among all service executions.

To use it you must associate task names with functions to be 
executed when a task is retrieved from the queue. Use the 
method set_task for that. When a task is retrieved from the 
queue the class verifies the task name e executes
the function associated with it passing the task attributes 
as an argument. This associated function must return true 
if the work was completed and false (or raise an error) if not.

Although the queue exists independently from this module's 
use, it only starts to listen and send to the queue when it's
start function is called with its proper configuration.

It is possible to define custom values for the maximum of 
times a work can fail or throw an error.

##### Job Tracking
The package jobs implements a set of function to help with
job tracking for asynchronous executions.

##### Configuration
The package config implements a simple way of configuring
python modules and force environment variables to exist.

##### Logging
The package logging provides a new Logger class more 
friendly to our micro services model. It can be configured 
using samba-chassis config framework to define a service name.
The service name as well as a job id and name will always 
be present in the logging record. The getLogger function 
returns a service friendly logger to simplify use and 
imports. To use you need only to import this modules and 
write in your module:
```python
from samba_chassis import logging
_logger = logging.getLogger(__name__) 
```

The package logging.stackdriver provides a Stackdriver log 
handler to be used in a GKE kubernetes cluster.
To use this module all you need is to add the handler to you logger:
```python
from samba_chassis.logging import stackdriver
logger.addhandler(ContainerEngineHandler())
```
 