import os

import pytest


os.environ.setdefault("TORBOX_API_KEY", "test-key")
os.environ.setdefault("ENABLE_METADATA", "true")
os.environ.setdefault("RAW_MODE", "false")
os.environ.setdefault("MOUNT_REFRESH_TIME", "normal")


@pytest.fixture(autouse=True)
def isolate_test_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
