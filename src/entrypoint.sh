#!/bin/sh

cd ${APP_HOME}
. venv/bin/activate
python3 src/power_load_forecaster.py $@
