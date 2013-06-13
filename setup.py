#!/usr/bin/env python
from setuptools import setup, find_packages

setup(name='reddit_adzerk',
    description='reddit adzerk integration',
    version='0.1',
    author='Max Goodman',
    author_email='max@reddit.com',
    license='BSD',
    packages=find_packages(),
    install_requires=[
        'r2',
        'requests',
    ],
    entry_points={
        'r2.plugin':
            ['adzerk = reddit_adzerk:Adzerk']
    },
    include_package_data=True,
    zip_safe=False,
)
