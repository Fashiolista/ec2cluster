#!/usr/bin/env python
from setuptools import setup, find_packages
from ec2cluster import __version__

CLASSIFIERS = [
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: BSD License',
    'Natural Language :: English',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
]

install_requires = [
    'mock==1.0.1',
    'unittest2==0.5.1',
    'boto==2.8.0',
    'dnspython==1.10.0',
    'python-crontab==1.2',
    'psycopg2==2.4.5',
    'argh==0.23.1',
]

setup(
    name='ec2cluster',
    version=__version__,
    author='Mike Ryan',
    author_email='mike@fadedink.co.uk',
    license='BSD',
    url='http://github.com/mikery/ec2cluster',
    description='Tools to work with clustered applications (PostgreSQL, Redis) on EC2',
    packages=find_packages(exclude=("tests",)),
    zip_safe=False,
    install_requires=install_requires,
    test_suite='ec2cluster.tests',
    include_package_data=True,
    classifiers=CLASSIFIERS,
    entry_points={
        'console_scripts': [
            'ec2cluster = ec2cluster.cli:main'
        ]
    },
)
