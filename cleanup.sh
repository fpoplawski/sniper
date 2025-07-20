#!/bin/bash
set -euo pipefail

isort sniper_main/
black --line-length 79 sniper_main/
autoflake --remove-all-unused-imports --in-place --recursive sniper_main/
flake8 sniper_main/
