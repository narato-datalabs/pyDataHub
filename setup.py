#!/usr/bin/env python

from setuptools import setup, find_packages


# This setup is suitable for "python setup.py develop".

setup(name='pyDataHub',
      version='0.1',
      description='The base libraries for data hub operations in databricks on a datalake store gen2',
      author='Patrick De Block',
      author_email='patrick.deblock@nara.to',
      url='http://www.narato.be/',
      packages=find_packages(),
      )
