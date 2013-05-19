#!/usr/bin/python
# this script just exists for Travis-CI: pypy doesn't support "-j" so I have to check which python
# implementation is running.

import platform, os

if platform.python_implementation() == "PyPy":
    # Travis installs pypy as "python" so we don't need to do anything special to use it.
    os.system("trial txscale")
else:
    os.system("trial -j2 txscale")
