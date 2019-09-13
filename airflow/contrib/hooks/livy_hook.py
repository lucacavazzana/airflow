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
This module contains the Apache Livy hook.
"""

import re
from enum import Enum
import json
import requests

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class BatchState(Enum):
    """
    Batch session states
    """
    NOT_STARTED = 'not_started'
    STARTING = 'starting'
    RUNNING = 'running'
    IDLE = 'idle'
    BUSY = 'busy'
    SHUTTING_DOWN = 'shutting_down'
    ERROR = 'error'
    DEAD = 'dead'
    KILLED = 'killed'
    SUCCESS = 'success'


TERMINAL_STATES = {
    BatchState.SUCCESS,
    BatchState.DEAD,
    BatchState.KILLED,
    BatchState.ERROR,
}


class LivyHook(BaseHook, LoggingMixin):
    """
    Hook for Apache Livy through the REST API.

    For more information about the API refer to
    https://livy.apache.org/docs/latest/rest-api.html

    :param livy_conn_id: reference to a pre-defined Livy Connection.
    :type livy_conn_id: str
    """
    def __init__(self, livy_conn_id='livy_default'):
        super(LivyHook, self).__init__(livy_conn_id)
        self._livy_conn_id = livy_conn_id
        self._build_base_url()

    def _build_base_url(self):
        """
        Build connection URL
        """
        params = self.get_connection(self._livy_conn_id)

        base_url = params.host

        if not base_url:
            raise AirflowException("Missing Livy endpoint hostname")

        if '://' not in base_url:
            base_url = '{}://{}'.format('http', base_url)
        if not re.search(r':\d+$', base_url):
            base_url = '{}:{}'.format(base_url, str(params.port or 8998))

        self._base_url = base_url

    def get_conn(self):
        pass

    def post_batch(self, *args, **kwargs):
        """
        Perform request to submit batch
        """

        batch_submit_body = json.dumps(LivyHook.build_post_batch_body(*args, **kwargs))
        headers = {'Content-Type': 'application/json'}

        self.log.info("Submitting job {} to {}".format(batch_submit_body, self._base_url))
        response = requests.post(self._base_url + '/batches', data=batch_submit_body, headers=headers)
        self.log.debug("Got response: {}".format(response.text))

        if response.status_code != 201:
            raise AirflowException("Could not submit batch. Status code: {}".format(response.status_code))

        batch_id = LivyHook._parse_post_response(response.json())
        if batch_id is None:
            raise AirflowException("Unable to parse a batch session id")
        self.log.info("Batch submitted with session id: {}".format(batch_id))

        return batch_id

    def get_batch(self, session_id):
        """
        Fetch info about the specified batch
        :param session_id: identifier of the batch sessions
        :type session_id: int
        """
        LivyHook._validate_session_id(session_id)

        self.log.debug("Fetching info for batch session {}".format(session_id))
        response = requests.get('{}/batches/{}'.format(self._base_url, session_id))

        if response.status_code != 200:
            self.log.warning("Got status code {} for session {}".format(response.status_code, session_id))
            raise AirflowException("Unable to fetch batch with id: {}".format(session_id))

        return response.json()

    def get_batch_state(self, session_id):
        """
        Fetch the state of the specified batch
        :param session_id: identifier of the batch sessions
        :type session_id: int
        """
        LivyHook._validate_session_id(session_id)

        self.log.debug("Fetching info for batch session {}".format(session_id))
        response = requests.get('{}/batches/{}/state'.format(self._base_url, session_id))

        if response.status_code != 200:
            self.log.warning("Got status code {} for session {}".format(response.status_code, session_id))
            raise AirflowException("Unable to fetch state for batch id: {}".format(session_id))

        jresp = response.json()
        if 'state' not in jresp:
            raise AirflowException("Unable to get state for batch with id: {}".format(session_id))
        return BatchState(jresp['state'])

    def delete_batch(self, session_id):
        """
        Delete the specified batch
        :param session_id: identifier of the batch sessions
        :type session_id: int
        """
        LivyHook._validate_session_id(session_id)

        self.log.info("Deleting batch session {}".format(session_id))
        response = requests.delete('{}/batches/{}'.format(self._base_url, session_id))

        if response.status_code != 200:
            self.log.warning("Got status code {} for session {}".format(response.status_code, session_id))
            raise AirflowException("Could not kill the batch with session id: {}".format(session_id))

        return response.json()

    @staticmethod
    def _validate_session_id(session_id):
        try:
            int(session_id)
        except (TypeError, ValueError):
            raise AirflowException("'session_id' must represent an integer")

    @staticmethod
    def _parse_post_response(response):
        """Parse batch response for batch id"""
        return response['id'] if 'id' in response else None

    @staticmethod
    def build_post_batch_body(
        file,
        args=None,
        conf=None,
        **kwargs
    ):
        """
        Build the post batch request body.
        For more information about the format refer to
        See https://livy.apache.org/docs/latest/rest-api.html

        :param file: Path of the file containing the application to execute (required).
        :type file: str
        :param proxy_user: User to impersonate when running the job.
        :type proxy_user: str
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
        """
        body = {'file': file}

        class_name = kwargs.get('class_name')
        jars = kwargs.get('jars')
        py_files = kwargs.get('py_files')
        files = kwargs.get('files')
        archives = kwargs.get('archives')
        name = kwargs.get('name')
        driver_memory = kwargs.get('driver_memory')
        driver_cores = kwargs.get('driver_cores')
        executor_memory = kwargs.get('executor_memory')
        executor_cores = kwargs.get('executor_cores')
        num_executors = kwargs.get('num_executors')
        queue = kwargs.get('queue')
        proxy_user = kwargs.get('proxy_user')

        if proxy_user:
            body['proxyUser'] = proxy_user
        if class_name:
            body['className'] = class_name
        if args and LivyHook._validate_list_of_stringables(args):
            body['args'] = [str(val) for val in args]
        if jars and LivyHook._validate_list_of_stringables(jars):
            body['jars'] = jars
        if py_files and LivyHook._validate_list_of_stringables(py_files):
            body['pyFiles'] = py_files
        if files and LivyHook._validate_list_of_stringables(files):
            body['files'] = files
        if driver_memory and LivyHook._validate_list_of_stringables(driver_memory):
            body['driverMemory'] = driver_memory
        if driver_cores:
            body['driverCores'] = driver_cores
        if executor_memory and LivyHook._validate_size_format(executor_memory):
            body['executorMemory'] = executor_memory
        if executor_cores:
            body['executorCores'] = executor_cores
        if num_executors:
            body['numExecutors'] = num_executors
        if archives and LivyHook._validate_size_format(archives):
            body['archives'] = archives
        if queue:
            body['queue'] = queue
        if name:
            body['name'] = name
        if conf and LivyHook._validate_extra_conf(conf):
            body['conf'] = conf

        return body

    @staticmethod
    def _validate_size_format(size):
        if size and not (isinstance(size, str) and re.match(r'\d+[kmgt]b?', size, re.IGNORECASE)):
            raise AirflowException("Invalid java size format for string'{}'".format(size))
        return True

    @staticmethod
    def _validate_list_of_stringables(vals):
        """Check the value can be converted to strings"""
        if vals and any(1 for val in vals if not isinstance(val, (str, int, float))):
            raise AirflowException("List of strings expected")
        return True

    @staticmethod
    def _validate_extra_conf(conf):
        """Check configuration values are either strings or ints"""
        if conf:
            if not isinstance(conf, dict):
                raise AirflowException("'conf' argument must be a dict")
            if any(True for k, v in conf.items() if not (v and isinstance(v, str) or isinstance(v, int))):
                raise AirflowException("'conf' values must be either strings or ints")
        return True
