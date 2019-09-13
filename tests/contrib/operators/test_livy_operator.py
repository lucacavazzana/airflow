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
#

import unittest
from unittest.mock import MagicMock, patch

from airflow import DAG, AirflowException
from airflow.models import Connection
from airflow.utils import db
from airflow.utils import timezone
from airflow.contrib.operators.livy_operator import LivyOperator, BatchState

DEFAULT_DATE = timezone.datetime(2017, 1, 1)
mock_livy_client = MagicMock()

BATCH_ID = 100


class TestLivyOperator(unittest.TestCase):
    def setUp(self):
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG('test_dag_id', default_args=args)
        db.merge_conn(Connection(
            conn_id='livyunittest', conn_type='livy',
            host='http://localhost:8998'
        ))

    @patch('airflow.contrib.operators.livy_operator.LivyHook.get_batch_state')
    def test_poll_for_termination(self, mock_livy):

        state_list = 2 * [BatchState.RUNNING] + [BatchState.SUCCESS]

        def side_effect(_):
            if state_list:
                return state_list.pop(0)
            # fail if does not stop right before
            raise AssertionError()

        mock_livy.side_effect = side_effect

        task = LivyOperator(
            livy_conn_id='livyunittest',
            file='sparkapp',
            polling_interval=1,
            dag=self.dag,
            task_id='livy_example'
        )
        task._init_hook()
        task._start_ts = LivyOperator.get_ts()
        task.poll_for_termination(BATCH_ID)

        mock_livy.assert_called_with(BATCH_ID)
        self.assertEqual(mock_livy.call_count, 3)

    @patch('airflow.contrib.operators.livy_operator.LivyHook.get_batch_state')
    def test_poll_for_termination_fail(self, mock_livy):

        state_list = 2 * [BatchState.RUNNING] + [BatchState.ERROR]

        def side_effect(_):
            if state_list:
                return state_list.pop(0)
            # fail if does not stop right before
            raise AssertionError()

        mock_livy.side_effect = side_effect

        task = LivyOperator(
            livy_conn_id='livyunittest',
            file='sparkapp',
            polling_interval=1,
            dag=self.dag,
            task_id='livy_example'
        )
        task._init_hook()
        task._start_ts = LivyOperator.get_ts()

        with self.assertRaises(AirflowException):
            task.poll_for_termination(BATCH_ID)

        mock_livy.assert_called_with(BATCH_ID)
        self.assertEqual(mock_livy.call_count, 3)

    @patch('airflow.contrib.operators.livy_operator.LivyHook.get_batch_state',
           return_value=BatchState.SUCCESS)
    @patch('airflow.contrib.operators.livy_operator.LivyHook.post_batch', return_value=BATCH_ID)
    def test_execution(self, mock_post, mock_get):
        task = LivyOperator(
            livy_conn_id='livyunittest',
            file='sparkapp',
            polling_interval=1,
            dag=self.dag,
            task_id='livy_example'
        )
        task.execute(context={})

        call_args = mock_post.call_args[1]
        self.assertIn('file', call_args)
        self.assertEqual(call_args['file'], 'sparkapp')
        mock_get.assert_called_with(BATCH_ID)

    @patch('airflow.contrib.operators.livy_operator.LivyHook.post_batch', return_value=BATCH_ID)
    @patch('airflow.contrib.operators.livy_operator.LivyHook.get_batch_state',
           return_value=BatchState.RUNNING)
    @patch('airflow.contrib.operators.livy_operator.LivyHook.delete_batch', return_value={})
    def test_timeout(self, mock_delete, mock_get, mock_post):

        task = LivyOperator(
            livy_conn_id='livyunittest',
            file='sparkapp',
            dag=self.dag,
            task_id='livy_example',
            polling_interval=1,
            timeout=3,
        )

        with self.assertRaises(AirflowException):
            task.execute(context={})

        mock_post.assert_called_once()
        self.assertGreater(mock_get.call_count, 1)
        mock_delete.assert_called_once_with(BATCH_ID)


if __name__ == '__main__':
    unittest.main()
