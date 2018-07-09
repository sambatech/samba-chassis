import os
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="samba-chassis",
    version="1.0.0",
    author="Vitor Mendes Paisante",
    author_email="vitor.paisante@sambatech.com.br",
    description="Framework for building microservices at SambaTech",
    license="Copyright SambaTech",
    keywords="microsservices framework library",
    url="http://www.sambatech.com.br",
    packages=['samba_chassis'],
    long_description=read('README'),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "Programming Language :: Python :: 2.7"
    ],
    install_requires=["setuptools", "pyyaml"],
    python_requires=">=2.7"
)
