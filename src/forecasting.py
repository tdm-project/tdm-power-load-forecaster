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

import pandas as pd
from datetime import datetime, timedelta
from influxdb import InfluxDBClient
from sklearn.ensemble import GradientBoostingRegressor
from sys import exit

# ---------------------------------------------------------------------------- #

def preprocessing(client: InfluxDBClient, params: list) -> \
                                        (pd.Series, pd.DataFrame, pd.DataFrame):
    """
    Query EmonTx measurements from InfluxDB and prepare the following data:
        * the power load measurement array "y"
        * the feature matrix "X"
        * the horizon feature matrix "Xh" for forecasting power load absorption
    """

    logger = params['LOGGER']
    measurement_ts = params['MEASUREMENT_TS']
    use_temperature = params['USE_TEMPERATURE']
    horizon_length = params['HORIZON_LENGTH']

    # ------------------------------------------------------------------------ #

    # Get timestamp of earlier measurement
    first_timestamp = [i['time'] for i in client.query(
                       f"""SELECT pulse
                           FROM {measurement_ts}
                           LIMIT 1;"""
                       )[(measurement_ts, None)]][0]

    # Get timestamp of latest measurement
    last_timestamp = [i['time'] for i in client.query(
                      f"""SELECT pulse
                         FROM {measurement_ts}
                         ORDER BY time DESC
                         LIMIT 1;"""
                      )[(measurement_ts, None)]][0]

    logger.debug(f'Earliest timestamp in "{measurement_ts}" time series: {first_timestamp}')
    logger.debug(f'Most recent timestamp in "{measurement_ts}" time series: {last_timestamp}')

    # ------------------------------------------------------------------------ #

    # Adjust query if using temperature
    query_temp = ', mean(temp1) as temperature' if use_temperature else ''

    # Query and estimate power consumption from the "pulse" time series
    query = f"""
            SELECT NON_NEGATIVE_DERIVATIVE(MIN(pulse), 1h) as power {query_temp}
            FROM {measurement_ts}
            WHERE time >= '{first_timestamp}'
            GROUP BY time(1m)
            FILL(null);
            """
    logger.debug(f'Querying measurements with: {query}')

    # Insert and process data into Pandas dataframe
    try:
        y = pd.DataFrame(client.query(query)[(measurement_ts, None)]) \
                                                   .set_index('time', drop=True)
        logger.debug(f'Retrieved {y.shape[0]} measurements.')

        # Convert index from string to datetime
        y.index = pd.to_datetime(y.index)

        # Compute hourly mean of power absorption, i.e. energy consumption in kWh
        y = y.resample('1h').mean().dropna()
        logger.debug(f'Head of "y" time series: {y.head()}')

    except KeyError as e:
        logger.error(e)
        exit(0)

    if y.size < 2:
        logger.error(('A larger number of valid measurements is requested for '
                      'computing the power load forecasts.'))
        exit(0)

    # ------------------------------------------------------------------------ #

    # Create feature matrix with "hour", "dayofweek" and eventually "temperature"
    X = []
    X.append(y.index.dayofweek.to_series(name='dayofweek',
                                         index=y.index).astype('category'))
    X.append(y.index.hour.to_series(name='hour',
                                    index=y.index).astype('category'))
    if use_temperature:
        X.append(y['temperature'])
        y = pd.Series(y.drop('temperature', axis=1)['power'])
    else:
        y = y.squeeze()
    X = pd.concat(X, axis=1)
    logger.debug(f'Head of "X" dataframe: {X.head()}')

    # ------------------------------------------------------------------------ #

    # Horizon time series starting from the next hour
    start_time = datetime.now().replace(microsecond=0, second=0, minute=0) + \
                                                              timedelta(hours=1)
    # Initialization of horizon feature matrix
    Xh = pd.DataFrame(index=pd.date_range(start_time,
                                          periods=horizon_length,
                                          freq='H'))
    Xh['dayofweek'] = Xh.index.dayofweek
    Xh['hour'] = Xh.index.hour
    if use_temperature:
        # TODO - Currently just copying last "horizon_length" temperature values
        Xh['temperature'] = X['temperature'][-horizon_length:].values
    logger.debug(f'Head of "Xh" dataframe: {Xh.head()}')

    return y, X, Xh

# ---------------------------------------------------------------------------- #

def forecasting(y: pd.Series, X: pd.DataFrame, Xh: pd.DataFrame,
                params: dict) -> list:
    """
    Predict the power load absorption according to the past measurements, to the
    day of the week and to the hour of the day.
    Return a list of dictionaries in JSON format to be written to InfluxDB.
    """

    logger = params['LOGGER']
    logger.debug('Starting quantile regression...')
    # Predict 1st quartile (alpha = 0.25)
    gbr = GradientBoostingRegressor(loss='quantile',
                                    alpha=0.25,
                                    n_estimators=500,
                                    validation_fraction=0.2,
                                    n_iter_no_change=5,
                                    tol=0.01,
                                    max_features='sqrt')
    gbr.fit(X, y)
    y_1st_q = gbr.predict(Xh)

    # Predict median value (alpha = 0.50)
    gbr.set_params(alpha=0.50)
    gbr.fit(X, y)
    y_median = gbr.predict(Xh)

    # Predict 3rd quartile (alpha = 0.75)
    gbr.set_params(alpha=0.75)
    gbr.fit(X, y)
    y_3rd_q = gbr.predict(Xh)
    logger.debug('Quantile regression completed!')

    # ------------------------------------------------------------------------ #

    # Writing data in JSON format
    forecast_data = []
    for i in range(params['HORIZON_LENGTH']):
        forecast_data.append({'measurement': params['FORECAST_TS'],
                              'time': int(Xh.index[i].timestamp()),
                              'fields': {'power': y_median[i],
                                         '1st_quartile': y_1st_q[i],
                                         '3rd_quartile': y_3rd_q[i]}})

    return forecast_data

# ---------------------------------------------------------------------------- #
