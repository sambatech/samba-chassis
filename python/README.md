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



  
