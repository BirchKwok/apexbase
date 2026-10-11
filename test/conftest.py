"""
Pytest configuration for ApexBase tests
"""

import os
import sys

# Add the apexbase python module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'apexbase', 'python'))

# TEMPORARY release diagnostic: record the test that is about to run so a native
# abort (which leaves no pytest traceback) can be located from the job log.
def pytest_runtest_setup(item):
    import io
    with io.open("pytest-current-test.txt", "w", encoding="utf-8") as handle:
        handle.write(item.nodeid)
