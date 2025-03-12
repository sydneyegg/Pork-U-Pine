# Databricks notebook source
# Restart the Python process to clear the import cache.
dbutils.library.restartPython()

import pytest
import os
import sys

# Run all tests in the repository root.
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(notebook_path))
os.chdir(f'/Workspace/{repo_root}')

# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True

# Capture the pytest output
retcode = pytest.main([".", "-p", "no:cacheprovider"])

# Display the pytest output
if retcode != 0:
    print("The pytest invocation failed. See the log above for details.")

# Fail the cell execution if we have any test failures.
assert retcode == 0, 'The pytest invocation failed. See the log above for details.'
