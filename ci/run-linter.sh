#!/bin/bash
set -e
set -eo pipefail

echo "Code Styling with (black, flake8, isort)"

source activate databrewery-dev

echo "[flake8]"
flake8 databrewery --exclude=__init__.py

echo "[black]"
black --check -S databrewery

echo "[isort]"
isort --recursive --check-only databrewery

echo "[doc8]"
doc8 docs/source
doc8 *.rst
