#!/bin/sh
echo "Building helpers"

rm -rf dist build venv helpers.egg-info

python3 -m venv venv
. venv/bin/activate && pip install -U pip && pip install -r requirements.txt  && pytest -vs

rm -rf dist build venv helpers.egg-info

echo "Done"
