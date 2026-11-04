"""Business M6 has explicit signing and cursor trust, independent of OIDC."""

import pytest
from api.config import Settings

from .test_registry_config import environment


def test_history_requires_explicit_signing_and_cursor_config():
    with pytest.raises(ValueError):
        Settings.from_env({**environment(), "BUSINESS_HISTORY_ENABLED": "true"})
    assert Settings.from_env(environment()).history is None
