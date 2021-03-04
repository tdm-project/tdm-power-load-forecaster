#!/bin/sh

cd ${APP_HOME}
. venv/bin/activate
export PYTHONPATH="${APP_HOME}/src"
cd tests/
python3 -m unittest $@
