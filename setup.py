"""
Setup file
"""


import re
import os

from setuptools import setup, find_packages


def get_version(package):
    """Return package version as listed in `__version__` in `init.py`."""
    init_py = open(os.path.join(package, '__init__.py')).read()
    return re.search("__version__ = ['\"]([^'\"]+)['\"]", init_py).group(1)


setup(
  name='Alpyne',
  version=get_version('Alpyne'),
  description='A grid computing framework in python',
  url='https://github.com/mohitudupa/Alpyne.git',
  # Entry points provide cross-platform support and allow pip to create the appropriate form of executable for
  # the target platform.
  # http://python-packaging.readthedocs.io/en/latest/command-line-scripts.html#the-console-scripts-entry-point

  # If there are any packages in the project that need to be installed, specify them here or use find_packages
  # module to let it do for you
  # http://setuptools.readthedocs.io/en/latest/setuptools.html#using-find-packages
  packages=['Alpyne'],
  install_requires=['requests', 'pymongo'],
  # List all the dependencies here. These will be installed by pip when the project is being installed
  # https://packaging.python.org/discussions/install-requires-vs-requirements/#install-requires
)
