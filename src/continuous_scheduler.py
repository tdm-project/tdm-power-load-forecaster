#!/usr/bin/env python
#
#  Copyright 2018-2021, CRS4 - Center for Advanced Studies, Research and Development in Sardinia
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

import sched
from time import sleep, time

# ---------------------------------------------------------------------------- #

class TaskWrapper(object):

    def __init__(self, task, period, priority, scheduler, *args, **kwargs):
        self._task      = task
        self._period    = period
        self._priority  = priority
        self._scheduler = scheduler

        self._args   = args
        self._kwargs = kwargs

    # ------------------------------------------------------------------------ #

    def __call__(self, *args, **kwargs):
        self._task(*self._args, **self._kwargs)
        self._scheduler.enter(self._period, self._priority, self, *self._args,
                              **self._kwargs)

# ---------------------------------------------------------------------------- #

class MainScheduler(object):

    def __init__(self):
        self._scheduler = sched.scheduler(time, sleep)

    # ------------------------------------------------------------------------ #

    def add_task(self, task, delay, period, priority, *args, **kwargs):
        _task = TaskWrapper(task, period, priority, self._scheduler, *args,
                            **kwargs)
        self._scheduler.enter(delay, priority, _task, *args, **kwargs)

    # ------------------------------------------------------------------------ #

    def start(self):
        self._scheduler.run()

# ---------------------------------------------------------------------------- #
