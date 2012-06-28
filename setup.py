#!/usr/bin/env python
from distutils.core import setup

setup(
    name = 'mongoengine-relational',
    description = '''Mixin for MongoEngine that manages both sides of ToOne or ToMany relations''',
    version = 0.1,
    author = 'Paul Uithol - Progressive Company',
    author_email = 'paul.uithol@progressivecompany.nl',
    url = 'http://github.com/ProgressiveCompany/mongoengine-relational',
    packages=['mongoengine_privileges'],
    requires=[
        'mongoengine',
        'bson',
    ],
    install_requires=[
        'mongoengine',
        'bson',
    ],
    classifiers = [
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Framework :: Pyramid',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Utilities'
    ],
)
