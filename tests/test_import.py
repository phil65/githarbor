"""Smoke test: the package imports and reports its version."""

import githarbor


def test_package_imports() -> None:
    """The package can be imported and exposes a version string."""
    assert isinstance(githarbor.__version__, str)
