#!/usr/bin/env python
#
#  Copyright 2018-2021, CRS4 - Center for Advanced Studies, Research and Development
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

"""
This module tests:
    * that the GENERAL options defined for all the TDM modules are defined and
    work as expected;
    * the specific section overrides the GENERAL one;
    * the specific options work as expected;
    * the command line options override the configuration file.
"""

# ---------------------------------------------------------------------------- #

import logging
import os
import unittest
from unittest.mock import Mock

from power_load_forecaster import configuration_parser
from power_load_forecaster import APPLICATION_NAME
from power_load_forecaster import (INFLUXDB_HOST,
                                   INFLUXDB_PORT,
                                   FORECAST_INTERVAL,
                                   HORIZON_LENGTH)

# ---------------------------------------------------------------------------- #

COMMANDLINE_PARAMETERS = {'logging_level': {'cmdline': '--logging-level',
                                            'default': logging.INFO},
                          'influxdb_host': {'cmdline': '--influxdb-host',
                                            'default': INFLUXDB_HOST},
                          'influxdb_port': {'cmdline': '--influxdb-port',
                                            'default': INFLUXDB_PORT},
                          'forecast_interval': {'cmdline': '--forecast-interval',
                                                'default': FORECAST_INTERVAL},
                          'horizon_length': {'cmdline': '--horizon-length',
                                             'default': HORIZON_LENGTH}}

# ---------------------------------------------------------------------------- #

class TestCommandLineParser(unittest.TestCase):
    """"
    Tests if the command line options override the settings in the
    configuration file.
    """

    def setUp(self):
        self._test_options = Mock()
        self._test_options.influxdb_host = 'influxdb_host_option'
        self._test_options.influxdb_port = INFLUXDB_PORT + 10
        self._test_options.logging_level = logging.INFO + 10
        self._test_options.forecast_interval = FORECAST_INTERVAL + 10
        self._test_options.horizon_length = HORIZON_LENGTH + 10

        self._test_configuration = Mock()
        self._test_configuration.influxdb_host = 'influxdb_host_configuration'
        self._test_configuration.influxdb_port = INFLUXDB_PORT + 20
        self._test_configuration.logging_level = logging.INFO + 20
        self._test_configuration.forecast_interval = FORECAST_INTERVAL + 20
        self._test_configuration.horizon_length = HORIZON_LENGTH + 20

        self._config_file = '/tmp/config.ini'

        _f = open(self._config_file, "w")
        _f.write("[{:s}]\n".format(APPLICATION_NAME))
        _f.write("influxdb_host = {}\n".format(
            self._test_configuration.influxdb_host))
        _f.write("influxdb_port = {}\n".format(
            self._test_configuration.influxdb_port))
        _f.write("logging_level = {}\n".format(
            self._test_configuration.logging_level))
        _f.write("forecast_interval  = {}\n".format(
            self._test_configuration.forecast_interval))
        _f.write("horizon_length  = {}\n".format(
            self._test_configuration.horizon_length))
        _f.close()

    # ------------------------------------------------------------------------ #

    def test_command_line_long(self):
        """"
        Tests if the command line options are parsed.
        """
        _cmd_line = []

        _cmd_line.extend(['--config-file', None])
        _cmd_line.extend(
            ['--influxdb-host', str(self._test_options.influxdb_host)])
        _cmd_line.extend(
            ['--influxdb-port', str(self._test_options.influxdb_port)])
        _cmd_line.extend(
            ['--logging-level', str(self._test_options.logging_level)])
        _cmd_line.extend(
            ['--forecast-interval', str(self._test_options.forecast_interval)])
        _cmd_line.extend(
            ['--horizon-length', str(self._test_options.horizon_length)])

        _args = configuration_parser(_cmd_line)

        self.assertEqual(
            self._test_options.logging_level, _args.logging_level)
        self.assertEqual(
            self._test_options.influxdb_host, _args.influxdb_host)
        self.assertEqual(
            self._test_options.influxdb_port, _args.influxdb_port)
        self.assertEqual(
            self._test_options.forecast_interval, _args.forecast_interval)
        self.assertEqual(
            self._test_options.horizon_length, _args.horizon_length)

    # ------------------------------------------------------------------------ #

    def test_command_line_long_override(self):
        """"
        Tests if the command line options override the settings in the
        configuration file (long options).
        """
        _cmd_line = []

        _cmd_line.extend(
            ['--config-file', str(self._config_file)])
        _cmd_line.extend(
            ['--influxdb-host', str(self._test_options.influxdb_host)])
        _cmd_line.extend(
            ['--influxdb-port', str(self._test_options.influxdb_port)])
        _cmd_line.extend(
            ['--logging-level', str(self._test_options.logging_level)])
        _cmd_line.extend(
            ['--forecast-interval', str(self._test_options.forecast_interval)])
        _cmd_line.extend(
            ['--horizon-length', str(self._test_options.horizon_length)])

        _args = configuration_parser(_cmd_line)

        self.assertEqual(
            self._test_options.logging_level, _args.logging_level)
        self.assertEqual(
            self._test_options.influxdb_host, _args.influxdb_host)
        self.assertEqual(
            self._test_options.influxdb_port, _args.influxdb_port)
        self.assertEqual(
            self._test_options.forecast_interval, _args.forecast_interval)
        self.assertEqual(
            self._test_options.horizon_length, _args.horizon_length)

    # ------------------------------------------------------------------------ #

    def test_command_line_long_partial_override(self):
        """"
        Tests if the command line options override the settings in the
        configuration file (long options).
        """
        for _opt, _par in COMMANDLINE_PARAMETERS.items():
            _cmd_line = ['--config-file', str(self._config_file)]
            _cmd_line.extend([
                _par['cmdline'],
                str(getattr(self._test_options, _opt))])

            _args = configuration_parser(_cmd_line)

            self.assertEqual(
                getattr(_args, _opt),
                getattr(self._test_options, _opt))

            for _cfg, _val in COMMANDLINE_PARAMETERS.items():
                if _cfg == _opt:
                    continue
                self.assertEqual(
                    getattr(_args, _cfg),
                    getattr(self._test_configuration, _cfg))

    # ------------------------------------------------------------------------ #

    def tearDown(self):
        os.remove(self._config_file)

# ---------------------------------------------------------------------------- #

class TestGeneralSectionConfigFileParser(unittest.TestCase):
    """"
    Checks if the GENERAL section options are present in the parser, their
    default values are defined and the GENERAL SECTION of configuration file is
    read and parsed.
    """

    def setUp(self):
        self._default = Mock()
        self._default.influxdb_host = INFLUXDB_HOST
        self._default.influxdb_port = INFLUXDB_PORT
        self._default.logging_level = logging.INFO
        self._default.forecast_interval = FORECAST_INTERVAL
        self._default.horizon_length = HORIZON_LENGTH

        self._test = Mock()
        self._test.influxdb_host = 'influxdb_host_test'
        self._test.influxdb_port = INFLUXDB_PORT + 100
        self._test.logging_level = logging.INFO + 10
        self._test.forecast_interval = FORECAST_INTERVAL + 100
        self._test.horizon_length = HORIZON_LENGTH + 100

        self._override = Mock()
        self._override.influxdb_host = 'influxdb_host_override'
        self._override.influxdb_port = INFLUXDB_PORT + 200
        self._override.logging_level = logging.INFO + 20
        self._override.forecast_interval = FORECAST_INTERVAL + 200
        self._override.horizon_length = HORIZON_LENGTH + 20

        self._config_file = '/tmp/config.ini'
        _f = open(self._config_file, "w")
        _f.write("[GENERAL]\n")
        _f.write("influxdb_host = {}\n".format(self._test.influxdb_host))
        _f.write("influxdb_port = {}\n".format(self._test.influxdb_port))
        _f.write("logging_level = {}\n".format(self._test.logging_level))
        _f.write("forecast_interval = {}\n".format(self._test.forecast_interval))
        _f.write("horizon_length = {}\n".format(self._test.horizon_length))
        _f.close()

    # ------------------------------------------------------------------------ #

    def test_general_arguments(self):
        """
        Checks the presence of the GENERAL section in the parser.
        """
        _cmd_line = []
        _args = configuration_parser(_cmd_line)

        self.assertIn('logging_level', _args)
        self.assertIn('influxdb_host', _args)
        self.assertIn('influxdb_port', _args)
        self.assertIn('forecast_interval', _args)
        self.assertIn('horizon_length', _args)

    # ------------------------------------------------------------------------ #

    def test_general_default(self):
        """
        Checks the defaults of the GENERAL section in the parser.
        """
        _cmd_line = []
        _args = configuration_parser(_cmd_line)

        self.assertEqual(self._default.logging_level, _args.logging_level)
        self.assertEqual(self._default.influxdb_host, _args.influxdb_host)
        self.assertEqual(self._default.influxdb_port, _args.influxdb_port)
        self.assertEqual(self._default.forecast_interval, _args.forecast_interval)
        self.assertEqual(self._default.horizon_length, _args.horizon_length)

    # ------------------------------------------------------------------------ #

    def test_general_options(self):
        """
        Tests the parsing of the options in the GENERAL section.
        """
        _cmd_line = ['-c', self._config_file]
        _args = configuration_parser(_cmd_line)

        self.assertEqual(self._test.logging_level, _args.logging_level)
        self.assertEqual(self._test.influxdb_host, _args.influxdb_host)
        self.assertEqual(self._test.influxdb_port, _args.influxdb_port)

        self.assertNotEqual(_args.logging_level, self._default.logging_level)
        self.assertNotEqual(_args.influxdb_host, self._default.influxdb_host)

    # ------------------------------------------------------------------------ #

    def test_general_override_options(self):
        """
        Tests if the options in the GENERAL section are overridden by the same
        options in the specific section.
        """
        _config_specific_override_file = '/tmp/override_config.ini'

        _f = open(_config_specific_override_file, "w")
        _f.write("[GENERAL]\n")
        _f.write(
            "influxdb_host = {}\n".
            format(self._test.influxdb_host))
        _f.write(
            "influxdb_port = {}\n".
            format(self._test.influxdb_port))
        _f.write(
            "logging_level = {}\n".
            format(self._test.logging_level))
        _f.write(
            "forecast_interval = {}\n".
            format(self._test.forecast_interval))
        _f.write(
            "horizon_length = {}\n".
            format(self._test.horizon_length))
        _f.write("[{:s}]\n".format(APPLICATION_NAME))
        _f.write(
            "influxdb_host = {}\n".
            format(self._override.influxdb_host))
        _f.write(
            "influxdb_port = {}\n".
            format(self._override.influxdb_port))
        _f.write(
            "logging_level = {}\n".
            format(self._override.logging_level))
        _f.write(
            "forecast_interval = {}\n".
            format(self._override.forecast_interval))
        _f.write(
            "horizon_length = {}\n".
            format(self._override.horizon_length))
        _f.close()

        _cmd_line = ['-c', _config_specific_override_file]
        _args = configuration_parser(_cmd_line)

        self.assertEqual(self._override.logging_level, _args.logging_level)
        self.assertEqual(self._override.influxdb_host, _args.influxdb_host)
        self.assertEqual(self._override.influxdb_port, _args.influxdb_port)
        self.assertEqual(self._override.forecast_interval, _args.forecast_interval)
        self.assertEqual(self._override.horizon_length, _args.horizon_length)

        os.remove(_config_specific_override_file)

    # ------------------------------------------------------------------------ #

    def tearDown(self):
        os.remove(self._config_file)

# ---------------------------------------------------------------------------- #

class TestSpecificOptions(unittest.TestCase):
    """
    Checks if the specific options are present in the parser and their
    default values are defined
    """

    def setUp(self):

        self._default_interval = 60*60*6
        self._default_length = 72
        self._test_interval = 10
        self._test_length = 5

        self._config_file = '/tmp/config.ini'
        _f = open(self._config_file, "w")
        _f.write("[{:s}]\n".format(APPLICATION_NAME))
        _f.write("forecast_interval = {}\n".format(self._test_interval))
        _f.write("horizon_length = {}\n".format(self._test_length))
        _f.close()

    # ------------------------------------------------------------------------ #

    def test_specific_arguments(self):
        """
        Checks the presence of the specific options in the parser.
        """
        _args = configuration_parser()

        self.assertIn('forecast_interval', _args)
        self.assertIn('horizon_length', _args)

    # ------------------------------------------------------------------------ #

    def test_specific_default(self):
        """
        Checks the default values of the specific options in the parser.
        """
        _args = configuration_parser()

        self.assertEqual(self._default_interval, _args.forecast_interval)
        self.assertEqual(self._default_length, _args.horizon_length)

    # ------------------------------------------------------------------------ #

    def test_specific_options(self):
        """
        Tests the parsing of the options in the specific section.
        """
        _cmd_line = ['-c', self._config_file]
        _args = configuration_parser(_cmd_line)

        self.assertEqual(self._test_interval, _args.forecast_interval)
        self.assertEqual(self._test_length, _args.horizon_length)

    # ------------------------------------------------------------------------ #

    def tearDown(self):
        os.remove(self._config_file)

# ---------------------------------------------------------------------------- #

if __name__ == '__main__':
    unittest.main()

# ---------------------------------------------------------------------------- #
