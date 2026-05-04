"""Backward-compat shim for the legacy ``minio_governance`` package path.

The package was renamed to :mod:`berdl_notebook_utils.governance` as part
of the Polaris/S3 refactor — the ``MinIO`` prefix no longer reflects what
the API surfaces (the new module governs both MinIO/S3 IAM and Polaris
OAuth credentials).

External downstream consumers — most notably
`tenant-data-browser <https://github.com/BERDataLakehouse/tenant-data-browser>`_,
which has

    from berdl_notebook_utils.minio_governance.operations import ...
    from berdl_notebook_utils.minio_governance.tenant_management import ...

baked into its handlers — would otherwise ``ImportError`` when imported
against this build, breaking the JupyterLab data-dictionary panel.

Keeping the legacy package path importable (re-exporting the new
package's symbols) lets those consumers keep working for one release
while they migrate.  The shim emits a single ``DeprecationWarning`` per
process so legacy callers stay visible in logs.

Remove this shim once every downstream consumer (tenant-data-browser
≥ post-rename release, plus any private code we haven't audited) has
migrated to the ``governance`` package path.
"""

from __future__ import annotations

import warnings

# Re-export every public symbol from the renamed package so callers
# doing ``from berdl_notebook_utils.minio_governance import X`` resolve
# to the same object as ``from berdl_notebook_utils.governance import X``.
from berdl_notebook_utils.governance import *  # noqa: F401, F403
from berdl_notebook_utils.governance import __all__  # noqa: F401

warnings.warn(
    "berdl_notebook_utils.minio_governance has been renamed to "
    "berdl_notebook_utils.governance. The old path is preserved as a "
    "compat shim for one release; please migrate your imports.",
    DeprecationWarning,
    stacklevel=2,
)
