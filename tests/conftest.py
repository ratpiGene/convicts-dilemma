"""Session-scoped pytest configuration.

``tmp_path`` needs a base directory, and any *shared* location for it is a
trap on this machine: both the default (``%TEMP%\\pytest-of-<user>``) and a
static ``--basetemp`` are reused across shells, so when one security
context (sandboxed/elevated shell) creates the directory, a later run from
a normal shell dies with ``PermissionError`` while scanning or wiping it.

The fix is to share nothing: every session gets a brand-new directory via
``mkdtemp`` — created by the context that runs the tests, deleted by that
same context at session end.
"""

import shutil
import tempfile
from pathlib import Path


def pytest_configure(config):
    if config.option.basetemp is None:
        config.option.basetemp = Path(tempfile.mkdtemp(prefix="convicts-dilemma-pytest-"))
        config._owned_basetemp = config.option.basetemp


def pytest_unconfigure(config):
    owned = getattr(config, "_owned_basetemp", None)
    if owned is not None:
        shutil.rmtree(owned, ignore_errors=True)
