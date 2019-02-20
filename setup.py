#!/usr/bin/env python3.6
import sys

assert sys.version_info[0]==3 and sys.version_info[1]>=6,\
    "Ýou must install and use akf-dbTools with Python version 3.6 or higher"

from distutils.core import setup

setup(
    name='akf_dbTools',
    version='1.0',
    author='jkamlah',
    description='Tools for the Aktienfuehrer Database',
    packages=[''],
)
