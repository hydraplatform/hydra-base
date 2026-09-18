import sys

import pytest


def test_safe_on_non_windows_platforms():
    """
      Verifies that a Warning is raised on non-Windows platforms
      when attempting to import a Windows-only util and that
      import succeeds on Windows
    """
    if sys.platform.startswith("win"):
        from hydra_base.util.windows import win_get_common_documents
    else:
        with pytest.raises(Warning):
            from hydra_base.util.windows import win_get_common_documents


@pytest.mark.skipif(not sys.platform.startswith("win"), reason="Skipping Windows platform tests")
class TestWindows:

    def test_win_get_common_documents(self):
        """
          Do the Windows utils import correctly, find an appropriate
          windll function, and successfully call this to identify
          a path?
        """
        from hydra_base.util.windows import win_get_common_documents
        common_doc_path = win_get_common_documents()
        assert isinstance(str, common_doc_path)
        assert len(common_doc_path) > 0

