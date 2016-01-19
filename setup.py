#!/usr/bin/env python
# -*- coding: utf-8 -*-


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read().replace('.. :changelog:', '')

requirements = [
    # TODO: put package requirements here
]

test_requirements = [
    # TODO: put package test requirements here
]

setup(
    name='sparkonda',
    version='0.1.1',
    description="Minimalistic utility library to manage conda environments for pyspark jobs on yarn clusters",
    long_description=readme + '\n\n' + history,
    author="Moussa Taifi",
    author_email='@moutai',
    url='https://github.com/moutai/sparkonda',
    packages=[
        'sparkonda',
    ],
    package_dir={'sparkonda':
                 'sparkonda'},
    include_package_data=True,
    install_requires=requirements,
    license="ISCL",
    zip_safe=False,
    keywords='sparkonda',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: ISC License (ISCL)',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',

    ],
    test_suite='tests',
    tests_require=test_requirements
)
