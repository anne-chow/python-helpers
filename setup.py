#!/usr/bin/env python3

from distutils.core import setup
from setuptools import find_packages
import re
import codecs
import os.path

# python3 setup.py install
# python3 setup.py bdist_egg bdist_wheel sdist --format gztar,zip

PACKAGE_NAME = 'helpers'
HERE = os.path.dirname(os.path.realpath(__file__))


def find_version(*file_paths):
    print(f"Reading version_file at {os.path.join(HERE, *file_paths)}")
    version_file = codecs.open(os.path.join(HERE, *file_paths), 'r').read()
    version_match = re.search(r"^__version__\s*=\s*['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)

    raise RuntimeError("Unable to find version string.")


setup(
    name=PACKAGE_NAME,
    version=find_version(PACKAGE_NAME, "__init__.py"),

    # only include metis packages
    packages=find_packages(include=[PACKAGE_NAME+"*"]),

    scripts=[
    ],

    install_requires=[
        'simplejson',
        'cachetools',
        'python-dateutil'
    ],

    # optional metadata
    description='Handy Python Helper Functions',
    author='Anne Chow',
    author_email="chow.anne@yahoo.com",
    url='https://www.linkedin.com/in/anne-chow/'
)
