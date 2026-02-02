"""
GroupedS3ContentsManager - A ContentsManager that groups multiple S3 paths under one virtual folder.

This allows a structure like:
    minio/
    ├── my-files/      → S3: bucket/prefix1
    ├── my-sql/        → S3: bucket/prefix2
    ├── kbase-files/   → S3: bucket/prefix3
    └── kbase-sql/     → S3: bucket/prefix4

Instead of having each S3 path as a top-level folder.
"""

import logging
from datetime import datetime, timezone
from typing import Any

from jupyter_server.services.contents.checkpoints import Checkpoints
from jupyter_server.services.contents.manager import ContentsManager
from s3contents import S3ContentsManager
from traitlets import Dict, Unicode

logger = logging.getLogger("berdl.grouped_s3_contents")


class NoOpCheckpoints(Checkpoints):
    """
    A Checkpoints class that does absolutely nothing.
    Useful for S3 backends where we don't want to manage sidecar files or versioning.
    """

    def create_checkpoint(self, contents_mgr, path):
        """Return a dummy checkpoint model."""
        return {
            "id": "checkpoint",
            "last_modified": datetime.now(timezone.utc),
        }

    def restore_checkpoint(self, contents_mgr, checkpoint_id, path):
        pass

    def rename_checkpoint(self, checkpoint_id, old_path, new_path):
        pass

    def delete_checkpoint(self, checkpoint_id, path):
        pass

    def list_checkpoints(self, path):
        """Return empty list of checkpoints."""
        return []


class GroupedS3ContentsManager(ContentsManager):
    """
    A ContentsManager that groups multiple S3ContentsManagers under virtual subdirectories.

    Configuration example:
        c.GroupedS3ContentsManager.managers = {
            "my-files": {"bucket": "cdm-lake", "prefix": "users-general-warehouse/alice"},
            "my-sql": {"bucket": "cdm-lake", "prefix": "users-sql-warehouse/alice"},
        }
        c.GroupedS3ContentsManager.endpoint_url = "http://minio:9000"
        c.GroupedS3ContentsManager.access_key_id = "access_key"
        c.GroupedS3ContentsManager.secret_access_key = "secret_key"
    """

    # We MUST define checkpoints_class because ContentsManager.delete() uses self.checkpoints
    checkpoints_class = NoOpCheckpoints

    # Shared S3 configuration
    endpoint_url = Unicode("", config=True, help="S3/MinIO endpoint URL")
    access_key_id = Unicode("", config=True, help="S3 access key ID")
    secret_access_key = Unicode("", config=True, help="S3 secret access key")
    region_name = Unicode("us-east-1", config=True, help="S3 region name")
    signature_version = Unicode("s3v4", config=True, help="S3 signature version")

    # Manager definitions: {"name": {"bucket": "...", "prefix": "..."}}
    managers = Dict(config=True, help="Dictionary of manager names to S3 configs")

    # Additional kwargs for s3fs
    s3fs_additional_kwargs = Dict(config=True, help="Additional kwargs for s3fs")

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._s3_managers: dict[str, S3ContentsManager] = {}
        self._init_managers()

    def _init_managers(self):
        """Initialize S3ContentsManager instances for each configured path."""
        for name, config in self.managers.items():
            try:
                manager = S3ContentsManager(
                    access_key_id=self.access_key_id,
                    secret_access_key=self.secret_access_key,
                    endpoint_url=self.endpoint_url,
                    bucket=config.get("bucket", ""),
                    prefix=config.get("prefix", ""),
                    signature_version=self.signature_version,
                    region_name=self.region_name,
                    s3fs_additional_kwargs=dict(self.s3fs_additional_kwargs),
                )
                self._s3_managers[name] = manager
                logger.info(f"Initialized S3 manager '{name}' -> {config.get('bucket')}/{config.get('prefix')}")
            except Exception as e:
                logger.error(f"Failed to initialize S3 manager '{name}': {e}")

    def _split_path(self, path: str) -> tuple[str | None, str]:
        """
        Split path into (manager_name, remaining_path).

        Examples:
            "" -> (None, "")
            "my-files" -> ("my-files", "")
            "my-files/notebook.ipynb" -> ("my-files", "notebook.ipynb")
        """
        path = path.strip("/")
        if not path:
            return None, ""

        parts = path.split("/", 1)
        manager_name = parts[0]
        remaining = parts[1] if len(parts) > 1 else ""
        return manager_name, remaining

    def _get_manager(self, name: str) -> S3ContentsManager | None:
        """Get the S3ContentsManager for a given name."""
        return self._s3_managers.get(name)

    def _virtual_root_model(self, content: bool = True) -> dict[str, Any]:
        """Create a virtual directory model for the root, listing all sub-managers."""
        model = {
            "name": "",
            "path": "",
            "type": "directory",
            "writable": False,  # Root itself is not writable
            "created": datetime.now(timezone.utc),
            "last_modified": datetime.now(timezone.utc),
            "mimetype": None,
            "format": None,
            "content": None,
        }

        if content:
            model["format"] = "json"
            model["content"] = [
                {
                    "name": name,
                    "path": name,
                    "type": "directory",
                    "writable": True,
                    "created": datetime.now(timezone.utc),
                    "last_modified": datetime.now(timezone.utc),
                    "mimetype": None,
                    "format": None,
                    "content": None,
                }
                for name in sorted(self._s3_managers.keys())
            ]

        return model

    def _prefix_path_in_model(self, model: dict[str, Any], prefix: str) -> dict[str, Any]:
        """Prefix all paths in a model with the manager name."""
        if model is None:
            return model

        model = model.copy()

        # Prefix the main path
        if model.get("path") is not None:
            original_path = model["path"].strip("/")
            model["path"] = f"{prefix}/{original_path}".strip("/")

        # Prefix paths in content (for directories)
        if model.get("content") and isinstance(model["content"], list):
            model["content"] = [self._prefix_path_in_model(item, prefix) for item in model["content"]]

        return model

    # -------------------------------------------------------------------------
    # ContentsManager API Implementation
    # -------------------------------------------------------------------------

    def get(self, path: str, content: bool = True, type: str = None, format: str = None, **kwargs) -> dict[str, Any]:
        """Get file or directory model."""
        manager_name, remaining_path = self._split_path(path)

        # Root listing - show all virtual folders
        if manager_name is None:
            return self._virtual_root_model(content=content)

        # Get the appropriate manager
        manager = self._get_manager(manager_name)
        if manager is None:
            raise FileNotFoundError(f"No such directory: {manager_name}")

        # Delegate to the S3 manager
        # Note: S3ContentsManager might not support **kwargs depending on version,
        # but we should accept them to satisfy base class.
        # Safest is to try passing known args, or just pass what we have.
        # S3ContentsManager.get(path, content=True, type=None, format=None) typically.
        model = manager.get(remaining_path, content=content, type=type, format=format)
        return self._prefix_path_in_model(model, manager_name)

    def save(self, model: dict[str, Any], path: str) -> dict[str, Any]:
        """Save a file or directory."""
        manager_name, remaining_path = self._split_path(path)

        if manager_name is None:
            raise ValueError("Cannot save to root directory")

        manager = self._get_manager(manager_name)
        if manager is None:
            raise FileNotFoundError(f"No such directory: {manager_name}")

        result = manager.save(model, remaining_path)
        return self._prefix_path_in_model(result, manager_name)

    def delete_file(self, path: str):
        """Delete a file or empty directory."""
        manager_name, remaining_path = self._split_path(path)

        if manager_name is None:
            raise ValueError("Cannot delete root directory")

        manager = self._get_manager(manager_name)
        if manager is None:
            raise FileNotFoundError(f"No such directory: {manager_name}")

        # Prevent deleting the virtual folder itself
        if not remaining_path:
            raise ValueError(f"Cannot delete virtual directory: {manager_name}")

        return manager.delete_file(remaining_path)

    def rename_file(self, old_path: str, new_path: str):
        """Rename a file or directory."""
        old_manager_name, old_remaining = self._split_path(old_path)
        new_manager_name, new_remaining = self._split_path(new_path)

        if old_manager_name is None or new_manager_name is None:
            raise ValueError("Cannot rename root directory")

        if old_manager_name != new_manager_name:
            raise ValueError(f"Cannot move files between different managers: {old_manager_name} -> {new_manager_name}")

        manager = self._get_manager(old_manager_name)
        if manager is None:
            raise FileNotFoundError(f"No such directory: {old_manager_name}")

        return manager.rename_file(old_remaining, new_remaining)

    def file_exists(self, path: str) -> bool:
        """Check if a file exists."""
        manager_name, remaining_path = self._split_path(path)

        if manager_name is None:
            return False  # Root is a directory, not a file

        manager = self._get_manager(manager_name)
        if manager is None:
            return False

        if not remaining_path:
            return False  # Manager root is a directory

        return manager.file_exists(remaining_path)

    def dir_exists(self, path: str) -> bool:
        """Check if a directory exists."""
        manager_name, remaining_path = self._split_path(path)

        if manager_name is None:
            return True  # Root always exists

        manager = self._get_manager(manager_name)
        if manager is None:
            return False

        if not remaining_path:
            return True  # Manager root always exists

        return manager.dir_exists(remaining_path)

    def is_hidden(self, path: str) -> bool:
        """Check if path is hidden."""
        manager_name, remaining_path = self._split_path(path)

        if manager_name is None:
            return False

        manager = self._get_manager(manager_name)
        if manager is None:
            return False

        if not remaining_path:
            return False

        return manager.is_hidden(remaining_path)
