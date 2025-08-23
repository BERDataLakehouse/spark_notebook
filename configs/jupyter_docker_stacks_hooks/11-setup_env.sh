#!/bin/bash
set -e
set -x

# Ensure NB_USER is set to prevent errors
if [ -z "$NB_USER" ]; then
    echo "Error: The NB_USER environment variable must be set." >&2
    exit 1
fi

HOME_DIRECTORY="/home/$NB_USER"

# --- Shell Configuration ---

# Always copy/overwrite the default .bash_profile to ensure a consistent environment.
install /etc/skel/.bash_profile "$HOME_DIRECTORY/.bash_profile"

# Conditionally create .custom_profile for user customizations if it doesn't exist.
# This file is sourced by .bash_profile and preserves user changes across sessions.
CUSTOM_PROFILE="$HOME_DIRECTORY/.custom_profile"
if [ ! -f "$CUSTOM_PROFILE" ]; then
    install "/etc/skel/.custom_profile" "$CUSTOM_PROFILE"
fi

# --- Jupyter Configuration ---

JUPYTER_DIR="$HOME_DIRECTORY/.jupyter"
FAVORITES_DIR="$JUPYTER_DIR/lab/user-settings/@jlab-enhanced/favorites"
AI_CONFIG_FILE="$JUPYTER_DIR/jupyter_ai_config.json"
FAVORITES_FILE="$FAVORITES_DIR/favorites.jupyterlab-settings"

# Create all needed directories at once; -p creates parent directories as needed.
mkdir -p "$FAVORITES_DIR"

# Create Jupyter AI config from a default if it doesn't exist, preserving user changes.
if [ ! -f "$AI_CONFIG_FILE" ]; then
    install /configs/extensions/jupyter_ai_config.json "$AI_CONFIG_FILE"
fi

# Create JupyterLab favorites from a template if it doesn't exist.
# `envsubst` replaces environment variables in the template (e.g., $HOME_DIRECTORY).
if [ ! -f "$FAVORITES_FILE" ]; then
    envsubst < /configs/extensions/favorites.jupyterlab-settings > "$FAVORITES_FILE"
fi

# --- IPython Startup Scripts ---
# Always install the python startup scripts from the /configs/ipython_startup directory.
# This overwrites any user modifications to ensure a consistent, standard environment.
# If any of these scripts don't work, the whole import process will fail silently.
# We could add a step to check each of these scripts if we want as well
# [IPKernelApp] WARNING | Unknown error in handling startup files:
IPYTHON_STARTUP_DIR="$HOME_DIRECTORY/.ipython/profile_default/startup"
SOURCE_SCRIPTS_DIR="/configs/ipython_startup"
mkdir -p "$IPYTHON_STARTUP_DIR"
install "$SOURCE_SCRIPTS_DIR"/*.py "$IPYTHON_STARTUP_DIR"/


# --- Finalization ---

# Recursively set ownership for the entire home directory to the notebook user.
# This is a critical final step to ensure the user has the correct permissions.
# TODO: We may want to consider using a more specific directory after each step if this is too broad and too slow
chown -R "$NB_USER":users "$HOME_DIRECTORY"

# DELETE JOVYAN USER