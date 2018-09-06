#!/usr/bin/env python3.6
import sys
import shlex
import subprocess
from typing import List
from setuptools import setup, find_packages  # type: ignore
from setuptools.command.develop import develop  # type: ignore


def parse_reqs(requirements_file: str) -> List[str]:
    """Get requirements as a list of strings from the file."""
    with open(requirements_file) as reqs:
        return [r for r in reqs if r]


class CustomDevelop(develop):
    """Develop command that actually prepares the development environment."""

    def run(self):
        """Set up the local dev environment fully."""
        super().run()

        for command in [
            'pip install -U pip',
            'pip install -r requirements.txt',
            'pip install -r dev_requirements.txt',
        ]:
            print('\nCustom develop - executing:', command, file=sys.stderr)
            subprocess.check_call(shlex.split(command))


REQUIREMENTS = parse_reqs('requirements.txt')
TEST_REQUIREMENTS = parse_reqs('dev_requirements.txt')


setup(
    name='adstxt',
    version='0.1',
    install_requires=REQUIREMENTS,
    tests_require=TEST_REQUIREMENTS,
    packages=find_packages(exclude=['tests']),
    entry_points={'console_scripts': 'adstxt=adstxt.cli:cli'},

    cmdclass={
        'develop': CustomDevelop,
    }

)
