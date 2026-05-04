"""Backward-compat shim for ``minio_governance.operations``.

See :mod:`berdl_notebook_utils.minio_governance.__init__` for context.
"""

from __future__ import annotations

import warnings

from berdl_notebook_utils.governance.operations import *  # noqa: F401, F403

warnings.warn(
    "berdl_notebook_utils.minio_governance.operations has been renamed to "
    "berdl_notebook_utils.governance.operations. The old path is preserved "
    "as a compat shim for one release; please migrate your imports.",
    DeprecationWarning,
    stacklevel=2,
)
