#!/bin/bash
set -e
set -eo pipefail

echo "Code Styling with (black, flake8, isort)"

source activate databrewery-dev

echo "[flake8]"
flake8 databrewery --exclude=__init__.py --max-line-length=79 --ignore=C901,W605,W503,F722

echo "[black]"
black --check -S -l 79 databrewery

echo "[isort]"
isort --recursive --check-only -w 79 databrewery

# commented because there are no docs yet
# echo "[doc8]"
# doc8 docs/source
# doc8 *.rst
