"""Regression test: the ``minio_governance`` package alias must keep working.

The package was renamed to :mod:`berdl_notebook_utils.governance` in the
Polaris/S3 refactor.  External downstream consumers — most notably
``tenant-data-browser`` (the JupyterLab data-dictionary panel) — still
import from the old path and break catastrophically when the alias goes
missing.  This test locks the alias in place so removal has to be
deliberate.

Remove this file together with the
:mod:`berdl_notebook_utils.minio_governance` shim once every downstream
consumer has migrated to the ``governance`` path.
"""

from __future__ import annotations

import warnings


def test_minio_governance_package_aliases_governance() -> None:
    """``from berdl_notebook_utils.minio_governance import X`` must
    resolve to the same object as
    ``from berdl_notebook_utils.governance import X``.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        from berdl_notebook_utils.governance import (
            check_governance_health as new_check,
            get_credentials as new_get_creds,
            get_my_groups as new_groups,
            get_namespace_prefix as new_prefix,
        )
        from berdl_notebook_utils.minio_governance import (
            check_governance_health as old_check,
            get_credentials as old_get_creds,
            get_my_groups as old_groups,
            get_namespace_prefix as old_prefix,
        )

    assert old_check is new_check
    assert old_get_creds is new_get_creds
    assert old_groups is new_groups
    assert old_prefix is new_prefix


def test_minio_governance_operations_submodule_aliases() -> None:
    """``from berdl_notebook_utils.minio_governance.operations import X``
    must resolve to the same object as
    ``from berdl_notebook_utils.governance.operations import X``.

    This is the exact import shape that ``tenant-data-browser``'s
    ``cdm_methods.py`` uses — keeping it green is what keeps the
    JupyterLab Data Dictionary panel functional.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        from berdl_notebook_utils.governance.operations import (
            get_my_groups as new_groups,
            get_namespace_prefix as new_prefix,
        )
        from berdl_notebook_utils.minio_governance.operations import (
            get_my_groups as old_groups,
            get_namespace_prefix as old_prefix,
        )

    assert old_groups is new_groups
    assert old_prefix is new_prefix


def test_minio_governance_tenant_management_submodule_aliases() -> None:
    """Mirror check for the ``tenant_management`` submodule used by
    ``tenant-data-browser`` for tenant detail lookups.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        from berdl_notebook_utils.governance.tenant_management import (
            get_tenant_detail as new_detail,
            get_tenant_members as new_members,
        )
        from berdl_notebook_utils.minio_governance.tenant_management import (
            get_tenant_detail as old_detail,
            get_tenant_members as old_members,
        )

    assert old_detail is new_detail
    assert old_members is new_members


def test_minio_governance_emits_deprecation_warning() -> None:
    """First import of the legacy path must emit a DeprecationWarning.

    The warning is the only signal we have to surface lingering callers
    of the old path; if it ever stops firing we lose visibility into
    who still needs to migrate.
    """
    # Force a fresh import so the module body re-runs and re-emits.
    import importlib
    import sys

    for mod in (
        "berdl_notebook_utils.minio_governance",
        "berdl_notebook_utils.minio_governance.operations",
        "berdl_notebook_utils.minio_governance.tenant_management",
    ):
        sys.modules.pop(mod, None)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", DeprecationWarning)
        importlib.import_module("berdl_notebook_utils.minio_governance")
        importlib.import_module("berdl_notebook_utils.minio_governance.operations")
        importlib.import_module("berdl_notebook_utils.minio_governance.tenant_management")

    legacy_warnings = [
        w for w in caught if issubclass(w.category, DeprecationWarning) and "minio_governance" in str(w.message)
    ]
    assert len(legacy_warnings) >= 3, (
        f"expected one DeprecationWarning per legacy module import; got "
        f"{len(legacy_warnings)}: {[str(w.message) for w in legacy_warnings]}"
    )
