# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
This module contains the Apache Livy sensor.
"""

from airflow.contrib.hooks.livy_hook import LivyHook, TERMINAL_STATES
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class LivySensor(BaseSensorOperator):
    """
    Monitor a Livy sessions for termination.

    :param livy_conn_id: reference to a pre-defined Livy connection
    :type livy_conn_id: str
    :param batch_id: identifier of the monitored batch
    :type batch_id: int, str
    :param timeout: timeout
    :type timeout: int
    """

    @apply_defaults
    def __init__(
        self,
        livy_conn_id='livy_default',
        batch_id=None,
        timeout=24 * 3600,
        *vargs,
        **kwargs
    ):
        super(LivySensor, self).__init__(*vargs, **kwargs)
        self._livy_conn_id = livy_conn_id
        self._batch_id = batch_id
        self._timeout = timeout

        self._livy_hook = LivyHook(livy_conn_id=self._livy_conn_id)

    def poke(self, context):
        batch_id = self._batch_id

        status = self._livy_hook.get_batch_state(batch_id)
        return status in TERMINAL_STATES
