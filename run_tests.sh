#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Ensure all dependencies are installed and synchronized
uv sync --extra test

# Run all tests using pytest via uv run
uv run pytest tests/
