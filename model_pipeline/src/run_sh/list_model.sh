#!/usr/bin/env bash
set -e

# =====================
# Resolve paths
# =====================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "SCRIPT_DIR: $SCRIPT_DIR"
echo "PROJECT_ROOT: $PROJECT_ROOT"

# =====================
# Python path 
# =====================

export PYTHONPATH="$PROJECT_ROOT"

# =====================
# Variables
# =====================

PYTHON_SCRIPT="$PROJECT_ROOT/src/scripts/register_model.py"
CONFIG_PATH="$PROJECT_ROOT/src/config/config.yaml"

# =====================
# List All Registered Models
# =====================

echo "Listing all registered models..."
echo ""

python "$PYTHON_SCRIPT" \
    --config "$CONFIG_PATH" \
    list