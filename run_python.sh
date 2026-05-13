#!/bin/bash
# Run Python scripts with the project's virtual environment
export PATH="$PWD/.venv/bin:$PATH"
exec python "$@"