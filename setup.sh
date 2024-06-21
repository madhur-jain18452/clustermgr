#!/bin/bash

PYTHON=$(which python3)
PIP=$(which pip3)
VENV="venv"

$PYTHON -m virtualenv --version >/dev/null 2>&1

if [ $? -ne 0 ]; then
  echo "virtualenv not found, installing..."
  $PIP install virtualenv
fi

$PYTHON -m virtualenv "$VENV"

source "$VENV/bin/activate"

if [ -f requirements.txt ]; then
  $PIP install -r requirements.txt
else
  echo "requirements.txt not found, skipping dependency installation."
fi

# Run the application
