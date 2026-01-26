"""
User Activity Logging for BERDL Notebooks
========================================

This script registers a pre-execution hook with IPython to log every executed cell.
Logs are written directly to the process standard error file descriptor (fd 2),
bypassing any Python-level redirection (like IPython's capture of stderr).

Captured fields:
- timestamp: ISO 8601 timestamp with timezone
- user: Username (from NB_USER or USER env var)
- type: "cell_execution"
- code: The executed code content (truncated)
"""

import json
import os
import sys
from datetime import datetime, timezone

import IPython


def log_to_container_stderr(message):
    """
    Write a message directly to the process stderr (fd 2).
    This ensures logs go to the container log and NOT the notebook output,
    even if sys.stderr is redirected by IPython.
    """
    try:
        # Append newline if missing
        if not message.endswith("\n"):
            message += "\n"

        # Write bytes simply and directly to file descriptor 2 (stderr)
        os.write(2, message.encode("utf-8"))
    except Exception:
        # Fallback to sys.__stderr__ if os.write fails for some reason
        try:
            sys.__stderr__.write(message)
            sys.__stderr__.flush()
        except Exception:
            # If even the fallback stderr logging fails, there is nowhere else reliable to report it.
            # Intentionally suppress this exception to avoid recursive logging failures.
            pass


def log_cell_execution(info):
    """
    Log cell execution details to container stderr as JSON.
    """
    try:
        # Get the code being executed
        code = info.raw_cell

        # Skip empty executions
        if not code or not code.strip():
            return

        # Truncate extremely large code blocks
        if len(code) > 10000:
            code = code[:10000] + "... (truncated)"

        # Construct log entry
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user": os.environ.get("NB_USER") or os.environ.get("USER", "unknown"),
            "type": "cell_execution",
            "code": code,
        }

        # Log directly to container stderr
        log_to_container_stderr(json.dumps(log_entry))

    except Exception as e:
        # Attempt to log the error itself safely
        log_to_container_stderr(f"Error logging user activity: {e}")


def log_cell_error(result):
    """
    Log exceptions to container stderr as JSON after cell execution.
    """
    try:
        if hasattr(result, "error_in_exec") and result.error_in_exec:
            e = result.error_in_exec
            # Construct error log entry
            log_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "user": os.environ.get("NB_USER") or os.environ.get("USER", "unknown"),
                "type": "cell_error",
                "error_type": type(e).__name__,
                "error_message": str(e),
            }
            log_to_container_stderr(json.dumps(log_entry))
    except Exception as e:
        log_to_container_stderr(f"Error logging cell exception: {e}")


def register_hook():
    """Register the pre_run_cell hook with the current IPython session."""
    try:
        ip = IPython.get_ipython()
        if ip:
            ip.events.register("pre_run_cell", log_cell_execution)
            ip.events.register("post_run_cell", log_cell_error)  # Register post-execution hook

            # Log startup event
            startup_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "user": os.environ.get("NB_USER") or os.environ.get("USER", "unknown"),
                "type": "session_start",
                "message": "Activity logging enabled (container stderr)",
            }
            log_to_container_stderr(json.dumps(startup_entry))

    except Exception as e:
        log_to_container_stderr(f"Failed to register activity logging hook: {e}")


# Register immediately on import
register_hook()
