"""Smoke test. Confirms the package imports and exposes a version string."""

import mcp_upload


def test_version_is_a_string() -> None:
    assert isinstance(mcp_upload.__version__, str)
    assert mcp_upload.__version__
