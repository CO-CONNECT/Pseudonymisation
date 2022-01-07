import setuptools
import os
import sys

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open('requirements.txt') as f:
    required = f.read().splitlines()

from _version import __version__ as version

    
setuptools.setup(
    name="co-connect-pseudonymise", 
    author="CO-CONNECT",
    version=version,
    author_email="CO-CONNECT@dundee.ac.uk",
    description="CommandLine Interface for pseudonymising data.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/CO-CONNECT/Pseudonymisation",
    entry_points = {
        'console_scripts':[
            'pseudonymise=cli.cli:pseudonymise'
        ],
    },
    packages=setuptools.find_packages(),
    install_requires=required,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
