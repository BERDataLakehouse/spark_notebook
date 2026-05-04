"""Backward-compat shim for ``minio_governance.tenant_management``.

See :mod:`berdl_notebook_utils.minio_governance.__init__` for context.
"""

from __future__ import annotations

import warnings

from berdl_notebook_utils.governance.tenant_management import *  # noqa: F401, F403

warnings.warn(
    "berdl_notebook_utils.minio_governance.tenant_management has been renamed "
    "to berdl_notebook_utils.governance.tenant_management. The old path is "
    "preserved as a compat shim for one release; please migrate your imports.",
    DeprecationWarning,
    stacklevel=2,
)
