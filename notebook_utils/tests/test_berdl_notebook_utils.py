"""Basic package tests to ensure that dependencies are correct.

One or more of these tests will fail if the project dependencies do not
provide all necessary packages for the functions exported by berdl-notebook-utils.
"""

import importlib
import pkgutil
from collections.abc import Generator
from types import ModuleType
from typing import Any

PACKAGE_NAME = "berdl_notebook_utils"


def test_package_importable() -> None:
    """Import the top-level package; if this raises, the whole test fails."""
    importlib.import_module(PACKAGE_NAME)


def test_all_symbols_are_present_and_callable() -> None:
    """Verify that every symbol the package advertises can be accessed.

    This also forces the import of any lazy imports that ``__init__.py`` performs.
    """
    pkg = importlib.import_module(PACKAGE_NAME)

    # check all symbols exported by the package
    for name in pkg.__all__:
        # Ensure the attribute exists
        assert hasattr(pkg, name), f"'{name}' not exported from {PACKAGE_NAME}"


def _iter_submodules(pkg: ModuleType) -> Generator[str, Any, None]:
    """Yield full dotted names of all sub-modules (not packages) under ``pkg``."""
    for _, modname, ispkg in pkgutil.iter_modules(pkg.__path__, pkg.__name__ + "."):
        if not ispkg:
            yield modname


def test_all_submodules_importable() -> None:
    """Check each sub-module to ensure all internal dependencies are satisfied."""
    pkg = importlib.import_module(PACKAGE_NAME)

    for mod_name in _iter_submodules(pkg):
        importlib.import_module(mod_name)
