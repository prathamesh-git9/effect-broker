"""Put the repository root on sys.path for the test session.

The crash-matrix test imports the runnable ``examples/`` proof harness to drive a
real killed-worker scenario. ``examples`` is not an installed package, so this
makes it importable as a namespace package in any environment (local or CI),
independent of the current working directory. The worker subprocesses set their
own cwd to the repo root, so only test collection needs this.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
