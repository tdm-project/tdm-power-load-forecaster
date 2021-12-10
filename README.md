# TDM Edge Power Load Forecaster

The TDM Edge Power Load Forecaster is an application specifically designed for the [TDM Edge Gateway](http://www.tdm-project.it/en/) devices paired with the EmonTx energy monitors. The application, running in the background, periodically
* collects the pulse measurements from the InfluxDB database to compute the power load consumption;
* collects the temperature historical measurements and forecasts for the Cagliari area;
* trains a Gradient Boosting quantile regression model to forecast the 1st quartile, the median value and the 3rd quartile of power load consumption in the next 72 hours.


## Configurations
Settings are retrieved from both configuration file and command line.
Values are applied in the following order, the last overwriting the previous:

1. configuration file section `GENERAL` for the common options (`logging`, InfluxDB parameters, ...);
2. configuration file section `Power_Load_Forecaster` for both common and specific options;
3. command line options.


### Configuration file

`logging_level`
: threshold level for log messages (default: `20`)

`influxdb_host`
: hostname or address of the influx database (default: `influxdb`)

`influxdb_port`
: port of the influx database (default: `8086`)

`gps_location`
: GPS coordinates of the sensor expressed as latitude and longitude (default: `0.0,0.0`)

`measurement_ts`
: name of the InfluxDB *measurement* storing the EmonTx pulses time series

`processed_ts`
: name of the InfluxDB *measurement* storing the processed power load time series

`forecast_ts`
: name of the InfluxDB *measurement* storing the power load forecast time series

`forecast_interval`
: interval, in seconds, between consecutive power load forecasting runs

`horizon_length`
: length, in hours, of the forecast horizon

`weather_ts`
: name of the InfluxDB *measurement* storing the weather-related time series

`weather_server_url`
: URL of the web service providing the weather-related time series

`weather_start_timestamp`
: date corresponding to the first available weather-related historical measurements

`weather_forecast_interval`
: interval, in hours, between consecutive weather forecasts


#### Options accepted in `GENERAL` section

* `logging_level`
* `influxdb_host`
* `influxdb_port`
* `gps_location`

In the following example of configuration file, the `logging_level` setting is overwritten to `20` only for the `Power_Load_Forecaster` application, while other applications use `10` as specified in the `GENERAL` section:

```ini
[GENERAL]
influxdb_host = influxdb
influxdb_port = 8086
gps_location = 0.0,0.0
logging_level = 10

[Power_Load_Forecaster]
measurement_ts = emontx3
processed_ts = processed
forecast_ts = forecast
forecast_interval = 21600
horizon_length = 72
weather_ts = weather
weather_server_url = http://159.65.112.157/api/gfs/Cagliari/T2?date=
weather_start_timestamp = 2021-03-08 00:00:00
weather_forecast_interval = 6
```


### Command line

`-h`, `--help`
: show this help message and exit

`-c FILE`, `--config-file FILE`
: specify the config file

`-l LOGGING_LEVEL`, `--logging-level LOGGING_LEVEL`
: threshold level for log messages (default: 20)

`--influxdb-host INFLUXDB_HOST`
: hostname or address of the influx database (default: influxdb)

`--influxdb-port INFLUXDB_PORT`
: port of the influx database (default: 8086)

`--gps-location GPS_LOCATION`
: GPS coordinates of the sensor expressed as latitude and longitude (default: 0.0,0.0)

`--measurement-ts MEASUREMENT_TS`
: name of the time series containing the pulse measurements (default: emontx3)

`--processed-ts PROCESSED_TS`
: name of the time series containing the processed pulse measurements (default: processed)

`--forecast-ts FORECAST_TS`
: name of the time series containing the power load forecasts (default: forecast)

`--forecast-interval FORECAST_INTERVAL`
: interval, in seconds, between consecutive power load forecasting runs (default: 21600 seconds)

`--horizon-length HORIZON_LENGTH`
: length, in hours, of the forecast horizon (default: 72 hours)

`--weather-ts WEATHER_TS`
: name of the time series containing the weather historical data and forecasts (default: weather)

`--weather-server-url WEATHER_SERVER_URL`
: URL of the web service to download weather historical data and forecasts (default: http://159.65.112.157/api/gfs/Cagliari/T2?date=)

`--weather-start-timestamp WEATHER_START_TIMESTAMP`
: timestamp corresponding to the availability of data from the web service to download weather historical data and forecasts (default: 2021-03-08 00:00:00)

`--weather-forecast-interval WEATHER_FORECAST_INTERVAL`
: interval, in hours, between consecutive weather forecasts (default: 6 hours)
