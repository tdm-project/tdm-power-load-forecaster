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

from logging import Logger
import pandas as pd
from datetime import datetime, timedelta, timezone
from influxdb import DataFrameClient
from sklearn.ensemble import GradientBoostingRegressor
from sys import exit

# ---------------------------------------------------------------------------- #

def get_first_timestamp(client: DataFrameClient, field_name: str,
                        measurement_name: str, dt: bool = False) -> str:
    """
    Return the timestamp corresponding to the first data point of "field" from
    "measurement" as a "datetime" or as a "string".
    """
    q = client.query(f"""
                         SELECT "{field_name}"
                         FROM "{measurement_name}"
                         LIMIT 1;
                      """)[measurement_name].index[0]
    if dt:
        return q
    else:
        return str(q)[:19]

# ---------------------------------------------------------------------------- #

def get_last_timestamp(client: DataFrameClient, field_name: str,
                        measurement_name: str, dt: bool = False) -> str:
    """
    Return the timestamp corresponding to the most recent data point of "field"
    from "measurement" as a "datetime" or as a "string".
    """
    q = client.query(f"""
                         SELECT "{field_name}"
                         FROM "{measurement_name}"
                         ORDER BY time DESC
                         LIMIT 1;
                      """)[measurement_name].index[0]
    if dt:
        return q
    else:
        return str(q)[:19]

# ---------------------------------------------------------------------------- #

def download_weather_data(start_time: datetime,
                          end_time: datetime,
                          weather_server_url: str,
                          horizon_length: int,
                          forecast_interval: int,
                          logger: Logger) -> pd.DataFrame:

    # Collect data from web service
    dfr = []
    current_time = end_time
    n_hours = horizon_length
    while current_time >= start_time:
        time_str = current_time.strftime('%Y%m%d%H')
        try:
            dfr.append(pd.read_json(f'{weather_server_url}{time_str}')[:n_hours])
            logger.debug(f'{current_time}: OK!')
            n_hours = forecast_interval
        except Exception as e:
            logger.debug(f'{current_time}: {e}')
            n_hours = n_hours + forecast_interval
            if len(dfr) == 0:
                n_hours = horizon_length
        current_time = current_time - timedelta(hours=6)

    # ------------------------------------------------------------------------ #

    try:
        w_df = pd.concat([df.rename(columns={'T2_BACKUP': 'T2',
                                             'date': 'time'}).set_index('time')
                          for df in reversed(dfr)]).asfreq('H')
    except ValueError as e:
        logger.error(e)
        logger.error('Returning an empty weather dataframe...')
        w_df = pd.DataFrame()

    return w_df

# ---------------------------------------------------------------------------- #

def weather_preprocessing(client: DataFrameClient,
                          params: dict) -> pd.DataFrame:

    logger = params['LOGGER']
    db_name = params['INFLUXDB_DB']
    measurement_ts = params['MEASUREMENT_TS']
    weather_ts = params['WEATHER_TS']
    weather_server_url = params['WEATHER_SERVER_URL']
    weather_start_timestamp = params['WEATHER_START_TIMESTAMP']
    horizon_length = params['HORIZON_LENGTH']
    forecast_interval = params['WEATHER_FORECAST_INTERVAL']

    # ------------------------------------------------------------------------ #

    first_available_weather_timestamp = \
                  datetime.strptime(weather_start_timestamp, '%Y-%m-%d %H:%M:%S'
                                                  ).replace(tzinfo=timezone.utc)

    # Get timestamp of earlier pulse measurement
    first_pulse_timestamp = get_first_timestamp(client=client,
                                                field_name='pulse',
                                                measurement_name=measurement_ts,
                                                dt=True)
    last_pulse_timestamp = get_last_timestamp(client=client,
                                              field_name='pulse',
                                              measurement_name=measurement_ts,
                                              dt=True)
    logger.debug(f'First pulse timestamp: {first_pulse_timestamp}')
    logger.debug(f'Last pulse timestamp: {last_pulse_timestamp}')

    try:
        first_weather_timestamp = get_first_timestamp(client=client,
                                                      field_name='T2_daily_mean',
                                                      measurement_name=weather_ts,
                                                      dt=True)

        last_weather_timestamp = get_last_timestamp(client=client,
                                                    field_name='T2_daily_mean',
                                                    measurement_name=weather_ts,
                                                    dt=True)
    except KeyError:
        logger.debug(f'"{weather_ts}" measurement not in the database yet!')
        first_weather_timestamp = first_available_weather_timestamp
        last_weather_timestamp = None

    first_weather_timestamp = max(first_weather_timestamp,
                                  first_available_weather_timestamp)

    logger.debug(f'First weather timestamp: {first_weather_timestamp}')
    logger.debug(f'Last weather timestamp: {last_weather_timestamp}')

    # Collect weather data from service
    if not last_weather_timestamp:
        # Collect weather data starting from the first available date
        start_time = min(first_pulse_timestamp, first_weather_timestamp)

    else:
        start_time = min(last_pulse_timestamp, last_weather_timestamp)

    start_time = start_time - timedelta(hours=start_time.hour,
                                        minutes=start_time.minute,
                                        seconds=start_time.second)

    end_time = datetime.now().replace(tzinfo=timezone.utc)
    end_time_hour = forecast_interval * (end_time.hour // forecast_interval)
    end_time = datetime(end_time.year, end_time.month, end_time.day,
                        end_time_hour).replace(tzinfo=timezone.utc)

    logger.debug(f'Start time to query weather data: {start_time}')
    logger.debug(f'End time to query weather data: {end_time}')

    # ------------------------------------------------------------------------ #

    # Download weather data from web service
    w_df = download_weather_data(start_time=start_time, end_time=end_time,
                                 weather_server_url=weather_server_url,
                                 horizon_length=horizon_length,
                                 forecast_interval=forecast_interval,
                                 logger=logger)

    # Compute daily mean, minimum and maximum temperature
    w_df.loc[:, 'T2_daily_mean'] = w_df.groupby(w_df.index.date
                                      ).mean().loc[w_df.index.date, 'T2'].values

    w_df.loc[:, 'T2_daily_min'] = w_df.groupby(w_df.index.date
                                       ).min().loc[w_df.index.date, 'T2'].values

    w_df.loc[:, 'T2_daily_max'] = w_df.groupby(w_df.index.date
                                       ).max().loc[w_df.index.date, 'T2'].values

    # Write data to InfluxDB for the current source
    if not w_df.empty:
        logger.debug('Writing weather measurements to database...')
        client.write_points(dataframe=w_df,
                            database=db_name,
                            measurement=weather_ts,
                            protocol='line',
                            time_precision='s')
        logger.debug('Done!')

    # ------------------------------------------------------------------------ #

    # Query temperature from database
    query = f"""SELECT "T2_daily_mean","T2_daily_min","T2_daily_max"
                FROM "{weather_ts}";
             """

    logger.debug('Querying weather measurements...')
    W = client.query(query)[weather_ts]
    logger.debug('Done!')

    return W

# ---------------------------------------------------------------------------- #

def preprocessing(client: DataFrameClient, params: dict, W: pd.DataFrame) -> \
                                        (pd.Series, pd.DataFrame, pd.DataFrame):
    """
    Query EmonTx measurements from InfluxDB and prepare the following data:
        * the power load measurement array "y"
        * the feature matrix "X"
        * the horizon feature matrix "Xh" for forecasting power load absorption
    """

    logger = params['LOGGER']
    db_name = params['INFLUXDB_DB']
    measurement_ts = params['MEASUREMENT_TS']
    processed_ts = params['PROCESSED_TS']
    horizon_length = params['HORIZON_LENGTH']

    # ------------------------------------------------------------------------ #

    # Get timestamp of earlier pulse measurement
    first_timestamp = get_first_timestamp(client=client,
                                          field_name='pulse',
                                          measurement_name=measurement_ts)
    # Get timestamp of latest measurement
    last_timestamp = get_last_timestamp(client=client,
                                        field_name='pulse',
                                        measurement_name=measurement_ts)

    logger.debug(f'Earliest timestamp in "{measurement_ts}" time series: {first_timestamp}')
    logger.debug(f'Most recent timestamp in "{measurement_ts}" time series: {last_timestamp}')

    # ------------------------------------------------------------------------ #

    # Query already processed pulses measurements
    query = f"""
                SELECT power
                FROM "{processed_ts}"
                WHERE time >= '{first_timestamp}';
             """

    logger.debug(f'Querying field "power" from measurement {processed_ts}...')
    p = client.query(query)
    logger.debug('Done!')

    if len(p.keys()) == 0:
        logger.debug(f'Measurement "{processed_ts}" not available in the database')
        last_processed_timestamp = first_timestamp
        p = pd.DataFrame()
    else:
        p = p[processed_ts]
        if p.empty:
            logger.debug(f'Measurement "{processed_ts}" is empty')
            last_processed_timestamp = first_timestamp
        else:
            last_processed_timestamp = str(p.index[-1] - timedelta(hours=1))[:19]
    logger.debug(f'Head of "p" time series: {p.head()}')
    logger.debug(f'Last processed timestamp: {last_processed_timestamp}')

    # ------------------------------------------------------------------------ #

    # Query and estimate power consumption from the "pulse" time series
    query = f"""
            SELECT NON_NEGATIVE_DERIVATIVE(MEDIAN(pulse), 1h) as power
            FROM "{measurement_ts}"
            WHERE time >= '{last_processed_timestamp}'
            GROUP BY time(5m)
            FILL(null);
            """
    logger.debug(f'Querying measurements with: {query}')

    try:
        # Query power consumption data
        y = client.query(query)[measurement_ts]
        logger.debug(f'Retrieved {y.shape[0]} measurements.')

        # Remove negative power values and outliers
        y = y[(y >= 0) & (y < 15000)]

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

    # Write processed data to InfluxDB
    logger.debug('Writing processed power measurements to database...')
    client.write_points(dataframe=y,
                        database=db_name,
                        measurement=processed_ts,
                        protocol='line',
                        time_precision='s')
    logger.debug('Done!')

    # Concatenate previously processed data and new processed data
    logger.debug(f'Number of "p" rows before concatenation: {p.shape[0]}')
    logger.debug(f'Number of "y" rows before concatenation: {y.shape[0]}')
    y = pd.concat([p, y])
    logger.debug(f'Number of "y" rows after concatenation: {y.shape[0]}')
    y = y[~y.index.duplicated(keep='last')]
    logger.debug(f'Number of "y" rows after removing duplicate indices: {y.shape[0]}')

    # ------------------------------------------------------------------------ #

    # Horizon time series starting from the next hour
    start_time = datetime.now().replace(microsecond=0, second=0,
                                                  minute=0) + timedelta(hours=1)
    # Initialization of horizon feature matrix
    Xh = pd.DataFrame(index=pd.date_range(start=start_time,
                                          periods=horizon_length,
                                          tz='UTC', freq='H'))

    # Reformatting of the weather dataframe
    w = pd.concat([pd.DataFrame(index=pd.date_range(start=y.index[0],
                                                    end=Xh.index[-1],
                                                    tz='UTC', freq='H')),
                   W], axis=1).fillna(0)

    # ------------------------------------------------------------------------ #

    # Define "features" dataframe
    X = pd.DataFrame(index=y.index)
    X['dayofweek'] = y.index.dayofweek
    X['hour'] = y.index.hour
    X['week'] = y.index.isocalendar().week
    X['month'] = y.index.month
    X[w.columns] = w.loc[y.index][w.columns].values
    logger.debug(f'Head of "X" dataframe: {X.head()}')
    logger.debug(f'Tail of "X" dataframe: {X.tail()}')

    # ------------------------------------------------------------------------ #

    # Define "horizon" dataframe
    Xh['dayofweek'] = Xh.index.dayofweek
    Xh['hour'] = Xh.index.hour
    Xh['week'] = Xh.index.isocalendar().week
    Xh['month'] = Xh.index.month
    Xh[w.columns] = w.loc[Xh.index][w.columns].values
    logger.debug(f'Head of "Xh" dataframe: {Xh.head()}')
    logger.debug(f'Tail of "Xh" dataframe: {Xh.tail()}')

    return y, X, Xh

# ---------------------------------------------------------------------------- #

def forecasting(y: pd.Series, X: pd.DataFrame, Xh: pd.DataFrame,
                params: dict) -> pd.DataFrame:
    """
    Predict the power load absorption according to the past measurements, to the
    day of the week and to the hour of the day.
    Return a DataFrame containing the columns for the 1st and 3rd quartiles and
    for the median value of the prediction to be written to InfluxDB.
    """

    logger = params['LOGGER']
    logger.debug('Starting quantile regression...')

    # Initialize prediction dataframe
    df_pred = pd.DataFrame(index=Xh.index)

    # Predict 1st quartile (alpha = 0.25)
    gbr = GradientBoostingRegressor(loss='quantile',
                                    alpha=0.25,
                                    n_estimators=25,
                                    validation_fraction=0.2,
                                    n_iter_no_change=5,
                                    tol=0.01)
    gbr.fit(X.values, y['power'].values)
    df_pred['1st_quartile'] = gbr.predict(Xh.values)

    # Predict median value (alpha = 0.50)
    gbr.set_params(alpha=0.50)
    gbr.fit(X.values, y['power'].values)
    df_pred['median'] = gbr.predict(Xh.values)

    # Predict 3rd quartile (alpha = 0.75)
    gbr.set_params(alpha=0.75)
    gbr.fit(X.values, y['power'].values)
    df_pred['3rd_quartile'] = gbr.predict(Xh.values)
    logger.debug('Quantile regression completed!')

    # Set to zero any negative predicted value
    df_pred[df_pred < 0] = 0.0

    return df_pred

# ---------------------------------------------------------------------------- #
