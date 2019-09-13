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
This module contains the Apache Livy operator.
"""

from time import sleep, gmtime, mktime

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.contrib.hooks.livy_hook import LivyHook, BatchState, TERMINAL_STATES


class LivyOperator(BaseOperator):
    """
    :param file: Path of the  file containing the application to execute (required).
    :type file: str
    :param class_name: Application Java/Spark main class string.
    :type class_name: str
    :param args: Command line arguments for the application s.
    :type args: list
    :param jars: jars to be used in this sessions.
    :type jars: list
    :param py_files: Python files to be used in this session.
    :type py_files: list
    :param files: files to be used in this session.
    :type files: list
    :param driver_memory: Amount of memory to use for the driver process  string.
    :type driver_memory: str
    :param driver_cores: Number of cores to use for the driver process int.
    :type driver_cores: str
    :param executor_memory: Amount of memory to use per executor process  string.
    :type executor_memory: str
    :param executor_cores: Number of cores to use for each executor  int.
    :type executor_cores: str
    :param num_executors: Number of executors to launch for this session  int.
    :type num_executors: str
    :param archives: Archives to be used in this session.
    :type archives: list
    :param queue: The name of the YARN queue to which submitted string.
    :type queue: str
    :param name: The name of this session  string.
    :type name: str
    :param conf: Spark configuration properties.
    :type conf: dict
    :param proxy_user: User to impersonate when running the job.
    :type proxy_user: str
    :param livy_conn_id: reference to a pre-defined Livy Connection.
    :type livy_conn_id: str
    :param polling_interval: time in seconds between polling for job completion. Don't poll for values >=0
    :type polling_interval: int
    :param timeout: for a value greater than zero, number of seconds to poll before killing the batch.
    :type timeout: int
    """

    @apply_defaults
    def __init__(
        self,
        file=None,
        args=None,
        conf=None,
        livy_conn_id='livy_default',
        polling_interval=0,
        timeout=24 * 3600,
        *vargs,
        **kwargs
    ):
        super(LivyOperator, self).__init__(*vargs, **kwargs)

        self._spark_params = {
            'file': file,
            'args': args,
            'conf': conf,
        }

        self._spark_params['proxy_user'] = kwargs.get('proxy_user')
        self._spark_params['class_name'] = kwargs.get('class_name')
        self._spark_params['jars'] = kwargs.get('jars')
        self._spark_params['py_files'] = kwargs.get('py_files')
        self._spark_params['files'] = kwargs.get('files')
        self._spark_params['driver_memory'] = kwargs.get('driver_memory')
        self._spark_params['driver_cores'] = kwargs.get('driver_cores')
        self._spark_params['executor_memory'] = kwargs.get('executor_memory')
        self._spark_params['executor_cores'] = kwargs.get('executor_cores')
        self._spark_params['num_executors'] = kwargs.get('num_executors')
        self._spark_params['archives'] = kwargs.get('archives')
        self._spark_params['queue'] = kwargs.get('queue')
        self._spark_params['name'] = kwargs.get('name')

        self._livy_conn_id = livy_conn_id
        self._polling_interval = polling_interval
        self._timeout = timeout

        self._livy_hook = None
        self._batch_id = None
        self._start_ts = None

    def _init_hook(self):
        if self._livy_conn_id:
            if self._livy_hook and isinstance(self._livy_hook, LivyHook):
                self.log.info("livy_conn_id is ignored when Livy hook is already provided")
            else:
                self._livy_hook = LivyHook(livy_conn_id=self._livy_conn_id)

        if not self._livy_hook:
            raise AirflowException("Unable to create LivyHook")

    def execute(self, context):
        self._init_hook()

        self._batch_id = self._livy_hook.post_batch(**self._spark_params)

        self._start_ts = LivyOperator.get_ts()

        if self._polling_interval > 0:
            self.poll_for_termination(self._batch_id)

        return self._batch_id

    def poll_for_termination(self, batch_id):
        """
        Pool Livy for batch termination.

        :param batch_id: id of the batch session to monitor.
        :type batch_id: int
        """
        state = self._livy_hook.get_batch_state(batch_id)
        while state not in TERMINAL_STATES:
            self.log.debug('Batch with id %s is in state: %s', batch_id, state.value)
            if self._timeout > 0 and LivyOperator.get_ts() - self._start_ts > self._timeout:
                # timeout enabled and expired
                self.log.warning("Batch %s execution exceeded timeout. Last known state: %s",
                                 batch_id, state.value)
                self.kill()
                raise AirflowException("Batch timeout reached")
            sleep(self._polling_interval)
            state = self._livy_hook.get_batch_state(batch_id)
        self.log.info("Batch with id %s terminated with state: %s", batch_id, state.value)
        if state != BatchState.SUCCESS:
            raise AirflowException("Batch did not succeed")

    def on_kill(self):
        self.kill()

    def kill(self):
        """
        Delete the current batch session.
        """
        if self._batch_id is not None:
            self._livy_hook.delete_batch(self._batch_id)

    @staticmethod
    def get_ts():
        """
        Get current time.
        """
        return mktime(gmtime())
