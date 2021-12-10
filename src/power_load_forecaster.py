#!/usr/bin/env python
#
#  Copyright 2021, CRS4 - Center for Advanced Studies, Research and Development
#  in Sardinia
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# ---------------------------------------------------------------------------- #

import argparse
import configparser
import logging
import sys
from influxdb import DataFrameClient
from time import time

import forecasting
import continuous_scheduler

# ---------------------------------------------------------------------------- #

INFLUXDB_HOST = 'influxdb'        # INFLUXDB address
INFLUXDB_PORT = 8086               # INFLUXDB port
INFLUXDB_DB = 'Emon'               # INFLUXDB database
INFLUXDB_USER = 'root'             # INFLUXDB username
INFLUXDB_PASS = 'root'             # INFLUXDB password
GPS_LOCATION = '0.0,0.0'

WEATHER_SERVER_URL = 'http://159.65.112.157/api/gfs/Cagliari/T2?date='
WEATHER_START_TIMESTAMP = '2021-03-08 00:00:00'
WEATHER_TS = 'weather'             # Time series containing weather data
FORECAST_TS = 'forecast'           # Time series containing power forecasts
MEASUREMENT_TS = 'emontx3'         # Time series containing measurements
PROCESSED_TS = 'processed'         # Time series containing processed data
WEATHER_FORECAST_INTERVAL = 6      # Hours between two weather forecasts
FORECAST_INTERVAL = 60*60*6        # Seconds between two forecast runs (6 hours)
HORIZON_LENGTH = 72                # Length of the forecast, in hours

APPLICATION_NAME = 'Power_Load_Forecaster'

# ---------------------------------------------------------------------------- #

def str_to_bool(parameter):
    """
    Utility for converting a string to its boolean equivalent.
    """
    if isinstance(parameter, bool):
        return parameter
    if parameter.lower() in {'false', 'f', '0', 'no', 'n'}:
        return False
    elif parameter.lower() in {'true', 't', '1', 'yes', 'y'}:
        return True
    raise ValueError(f'"{parameter}" is not a valid boolean value.')

# ---------------------------------------------------------------------------- #

def configuration_parser(p_args=None):
    pre_parser = argparse.ArgumentParser(add_help=False)

    pre_parser.add_argument(
        '-c', '--config-file', dest='config_file', action='store',
        type=str, metavar='FILE',
        help='specify the config file')

    args, remaining_args = pre_parser.parse_known_args(p_args)

    v_general_config_defaults = {'logging_level': logging.INFO,
                                 'influxdb_host': INFLUXDB_HOST,
                                 'influxdb_port': INFLUXDB_PORT,
                                 'influxdb_database': INFLUXDB_DB,
                                 'influxdb_username': INFLUXDB_USER,
                                 'influxdb_password': INFLUXDB_PASS,
                                 'gps_location': GPS_LOCATION}

    v_specific_config_defaults = {'forecast_interval': FORECAST_INTERVAL,
                                  'horizon_length': HORIZON_LENGTH,
                                  'weather_start_timestamp':
                                                        WEATHER_START_TIMESTAMP,
                                  'weather_server_url': WEATHER_SERVER_URL,
                                  'weather_ts': WEATHER_TS,
                                  'forecast_ts': FORECAST_TS,
                                  'measurement_ts': MEASUREMENT_TS,
                                  'processed_ts': PROCESSED_TS,
                                  'weather_forecast_interval':
                                                      WEATHER_FORECAST_INTERVAL}

    v_config_section_defaults = {'GENERAL': v_general_config_defaults,
                                 APPLICATION_NAME: v_specific_config_defaults}

    # Default config values initialization
    v_config_defaults = {}
    v_config_defaults.update(v_general_config_defaults)
    v_config_defaults.update(v_specific_config_defaults)

    if args.config_file:
        _config = configparser.ConfigParser()
        _config.read_dict(v_config_section_defaults)
        _config.read(args.config_file)

        # Filter out GENERAL options not listed in v_general_config_defaults
        _general_defaults = {_key: _config.get('GENERAL', _key) for _key in
                             _config.options('GENERAL') if _key in
                             v_general_config_defaults}

        # Updates the defaults dictionary with general and application specific
        # options
        v_config_defaults.update(_general_defaults)
        v_config_defaults.update(_config.items(APPLICATION_NAME))

    parser = argparse.ArgumentParser(parents=[pre_parser],
                          description=('Read pulse measurements from InfluxDB, '
                                       'compute power load forecasts and store '
                                       'them to InfluxDB.'),
                          formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.set_defaults(**v_config_defaults)

    parser.add_argument(
        '-l', '--logging-level', dest='logging_level', action='store',
        type=int,
        help='threshold level for log messages (default: {})'
             .format(logging.INFO))

    parser.add_argument(
        '--influxdb-host', dest='influxdb_host', action='store',
        type=str,
        help='hostname or address of the influx database (default: {})'
             .format(INFLUXDB_HOST))

    parser.add_argument(
        '--influxdb-port', dest='influxdb_port', action='store',
        type=int,
        help='port of the influx database (default: {})'.format(INFLUXDB_PORT))

    parser.add_argument(
        '--gps-location', dest='gps_location', action='store',
        type=str,
        help=('GPS coordinates of the sensor expressed as latitude and '
              'longitude (default: {})').format(GPS_LOCATION))

    parser.add_argument(
        '--measurement-ts', dest='measurement_ts', action='store',
        type=str,
        help=('name of the time series containing the pulse measurements '
              '(default: {})').format(MEASUREMENT_TS))

    parser.add_argument(
        '--processed-ts', dest='processed_ts', action='store',
        type=str,
        help=('name of the time series containing the processed pulse '
              'measurements (default: {})').format(PROCESSED_TS))

    parser.add_argument(
        '--forecast-ts', dest='forecast_ts', action='store',
        type=str,
        help=('name of the time series containing the power load forecasts '
              '(default: {})').format(FORECAST_TS))

    parser.add_argument(
        '--weather-ts', dest='weather_ts', action='store',
        type=str,
        help=('name of the time series containing the weather historical data '
              'and forecasts (default: {})').format(WEATHER_TS))

    parser.add_argument(
        '--weather-start-timestamp', dest='weather_start_timestamp',
        action='store', type=str,
        help=('timestamp corresponding to the availability of data from the '
              'web service to download weather historical data '
              'and forecasts (default: {})').format(WEATHER_START_TIMESTAMP))

    parser.add_argument(
        '--weather-server-url', dest='weather_server_url', action='store',
        type=str,
        help=('name of the web service to download weather historical data '
              'and forecasts (default: {})').format(WEATHER_SERVER_URL))

    parser.add_argument(
        '--forecast-interval', dest='forecast_interval', action='store',
        type=int,
        help=('interval, in seconds, between consecutive power load forecasting '
              'runs (default: {} seconds)').format(FORECAST_INTERVAL))

    parser.add_argument(
        '--weather-forecast-interval', dest='weather_forecast_interval',
        action='store', type=int,
        help=('interval, in hours, between consecutive weather forecasts '
              '(default: {} hours)').format(WEATHER_FORECAST_INTERVAL))

    parser.add_argument(
        '--horizon-length', dest='horizon_length', action='store',
        type=int,
        help=('length, in hours, of the forecast horizon '
              '(default: {} hours)').format(HORIZON_LENGTH))

    return parser.parse_args(remaining_args)

# ---------------------------------------------------------------------------- #

def forecasting_task(params: dict):

    logger = params['LOGGER']
    logger.info('Starting forecasting task...')
    start_time = time()

    # Connect to the database "Emon" in InfluxDB
    client = DataFrameClient(host=params['INFLUXDB_HOST'],
                             port=params['INFLUXDB_PORT'],
                             username=params['INFLUXDB_USER'],
                             password=params['INFLUXDB_PASS'],
                             database=params['INFLUXDB_DB'])

    # Retrieve and update weather historical measurements and predictions
    W = forecasting.weather_preprocessing(client, params)

    # Query and preprocess power load measurements
    y, X, Xh = forecasting.preprocessing(client, params, W)

    # Predict power load consumptions
    df_pred = forecasting.forecasting(y, X, Xh, params)

    # Write power load forecasts to InfluxDB
    try:
        # Write data to InfluxDB for the current source
        client.write_points(dataframe=df_pred,
                            database=params['INFLUXDB_DB'],
                            measurement=params['FORECAST_TS'],
                            protocol='line',
                            time_precision='s')
    except Exception as e:
        logger.error(e)
    finally:
        logger.info(f'Forecasting task completed in {round(time()-start_time)} seconds!')
        client.close()

# ---------------------------------------------------------------------------- #

def main():

    # Initializes the default logger
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO)
    logger = logging.getLogger(APPLICATION_NAME)

    # Checks the Python Interpeter version
    if (sys.version_info < (3, 0)):
        logger.error('Python 3 is requested! Leaving the program.')
        sys.exit(-1)

    # Parse arguments
    args = configuration_parser()

    logger.setLevel(args.logging_level)
    logger.info(f'Starting application "{APPLICATION_NAME}"...')
    logger.debug(f'Arguments: {vars(args)}')

    v_latitude, v_longitude = map(float, args.gps_location.split(','))

    v_influxdb_host = args.influxdb_host
    v_influxdb_port = args.influxdb_port

    v_influxdb_database = args.influxdb_database
    v_influxdb_username = args.influxdb_username
    v_influxdb_password = args.influxdb_password

    # Check if "Emon" database exists
    _client = DataFrameClient(host=v_influxdb_host,
                              port=v_influxdb_port,
                              username=v_influxdb_username,
                              password=v_influxdb_password,
                              database=v_influxdb_database)

    _dbs = _client.get_list_database()
    logger.debug(f'List of InfluxDB databases: {_dbs}')
    if v_influxdb_database not in [_d['name'] for _d in _dbs]:
        logger.info(f'InfluxDB database "{v_influxdb_database}" not found. Creating a new one.')
        _client.create_database(v_influxdb_database)

    _client.close()

    # Pack all parameters in a dictionary
    _userdata = {'LOGGER': logger,
                 'LATITUDE': v_latitude,
                 'LONGITUDE': v_longitude,
                 'INFLUXDB_HOST': v_influxdb_host,
                 'INFLUXDB_PORT': v_influxdb_port,
                 'INFLUXDB_USER': v_influxdb_username,
                 'INFLUXDB_PASS': v_influxdb_password,
                 'INFLUXDB_DB': v_influxdb_database,
                 'MEASUREMENT_TS': args.measurement_ts,
                 'PROCESSED_TS': args.processed_ts,
                 'FORECAST_TS': args.forecast_ts,
                 'WEATHER_TS': args.weather_ts,
                 'HORIZON_LENGTH': args.horizon_length,
                 'WEATHER_SERVER_URL': args.weather_server_url,
                 'WEATHER_FORECAST_INTERVAL': args.weather_forecast_interval,
                 'WEATHER_START_TIMESTAMP': args.weather_start_timestamp}

    # Instantiate the scheduler and repeatedly run the "forecasting task"
    # "forecast interval" seconds after its previous execution
    _main_scheduler = continuous_scheduler.MainScheduler()
    _main_scheduler.add_task(forecasting_task, 0, args.forecast_interval, 0,
                             _userdata)
    _main_scheduler.start()

# ---------------------------------------------------------------------------- #

if __name__ == '__main__':
    main()

# ---------------------------------------------------------------------------- #
