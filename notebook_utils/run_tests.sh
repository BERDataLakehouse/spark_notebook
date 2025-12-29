#!/bin/bash
set -x

# Get the directory where the script is located
SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
# Change the current working directory to SCRIPT_DIR
cd "$SCRIPT_DIR"

uv pip install --group dev --system

pytest tests/ -v \
  --cov=berdl_notebook_utils \
  --cov-report=term-missing \
  --cov-report=xml
