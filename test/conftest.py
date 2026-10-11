"""
Pytest configuration for ApexBase tests
"""

import os
import sys

# Add the apexbase python module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'apexbase', 'python'))

# TEMPORARY release diagnostic: record the test that is about to run and the
# outcome of every finished test, so a native abort (which leaves no pytest
# traceback and no final summary) still names the failing test and its assertion.
def pytest_runtest_setup(item):
    import io
    with io.open("pytest-current-test.txt", "w", encoding="utf-8") as handle:
        handle.write(item.nodeid)


def pytest_runtest_logreport(report):
    import io
    try:
        with io.open("pytest-failures.txt", "a", encoding="utf-8") as handle:
            handle.write(f"[{report.outcome}] {report.when} {report.nodeid}\n")
            if report.failed and report.longreprtext:
                handle.write(report.longreprtext.rstrip() + "\n")
            handle.flush()
    except Exception:
        pass

