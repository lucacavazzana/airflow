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

import unittest
from unittest.mock import patch, MagicMock
import json
from requests.exceptions import RequestException

from airflow import AirflowException
from airflow.models import Connection
from airflow.utils import db

from airflow.contrib.hooks.livy_hook import LivyHook, BatchState

TEST_ID = 100
SAMPLE_GET_RESPONSE = {'id': TEST_ID, 'state': BatchState.SUCCESS.value}


class TestLivyHook(unittest.TestCase):

    def setUp(self):
        db.merge_conn(Connection(conn_id='simple', host='http://host:1234'))
        db.merge_conn(Connection(conn_id='default_port', host='http://host'))
        db.merge_conn(Connection(conn_id='default_protocol', host='host'))
        db.merge_conn(Connection(conn_id='port_set', host='host', port=1234))
        db.merge_conn(Connection(conn_id='host_overrides_port', host='http://host:4321', port=1234))
        db.merge_conn(Connection(conn_id='missing_host', port=1234))
        db.merge_conn(Connection(conn_id='invalid_uri', uri='http://invalid_uri:4321'))

    def test_build_base_url(self):

        connection_url_mapping = {
            # id, expected
            'simple': 'http://host:1234',
            'default_port': 'http://host:8998',
            'default_protocol': 'http://host:8998',
            'port_set': 'http://host:1234',
            'host_overrides_port': 'http://host:4321',
        }

        for conn_id, expected in connection_url_mapping.items():
            with self.subTest(conn_id):
                hook = LivyHook(livy_conn_id=conn_id)

                self.assertEqual(hook._base_url, expected)

    def test_missing_host(self):
        with self.assertRaises(AirflowException):
            LivyHook(livy_conn_id='missing_host')

    def test_build_body(self):
        with self.subTest('minimal request'):
            body = LivyHook.build_post_batch_body(file='appname')

            self.assertEqual(body, {'file': 'appname'})

        with self.subTest('complex request'):
            body = LivyHook.build_post_batch_body(
                file='appname',
                class_name='org.example.livy',
                proxy_user='proxyUser',
                args=['a', '1'],
                jars=['jar1', 'jar2'],
                files=['file1', 'file2'],
                py_files=['py1', 'py2'],
                queue='queue',
                name='name',
                conf={'a': 'b'},
                driver_memory='1M',
                executor_memory='1m',
                executor_cores='1',
                num_executors='10',
            )

            self.assertEqual(body, {
                'file': 'appname',
                'className': 'org.example.livy',
                'proxyUser': 'proxyUser',
                'args': ['a', '1'],
                'jars': ['jar1', 'jar2'],
                'files': ['file1', 'file2'],
                'pyFiles': ['py1', 'py2'],
                'queue': 'queue',
                'name': 'name',
                'conf': {'a': 'b'},
                'driverMemory': '1M',
                'executorMemory': '1m',
                'executorCores': '1',
                'numExecutors': '10'
            })

    def test_parameters_validation(self):
        with self.subTest('not a size'):
            with self.assertRaises(AirflowException):
                LivyHook.build_post_batch_body(file='appname', executor_memory='xxx')

        with self.subTest('list of stringables'):
            self.assertEqual(
                LivyHook.build_post_batch_body(file='appname', args=['a', 1, 0.1])['args'],
                ['a', '1', '0.1']
            )

    def test_validate_size_format(self):
        with self.subTest('lower 1'):
            self.assertTrue(LivyHook._validate_size_format('1m'))

        with self.subTest('lower 2'):
            self.assertTrue(LivyHook._validate_size_format('1mb'))

        with self.subTest('upper 1'):
            self.assertTrue(LivyHook._validate_size_format('1G'))

        with self.subTest('upper 2'):
            self.assertTrue(LivyHook._validate_size_format('1GB'))

        with self.subTest('numeric'):
            with self.assertRaises(AirflowException):
                LivyHook._validate_size_format(1)

        with self.subTest('None'):
            self.assertTrue(LivyHook._validate_size_format(None))

    def test_validate_extra_conf(self):
        with self.subTest('valid'):
            try:
                LivyHook._validate_extra_conf({'k1': 'v1', 'k2': 0})
            except AirflowException:
                self.fail("Exception raised")

        with self.subTest('empty dict'):
            try:
                LivyHook._validate_extra_conf({})
            except AirflowException:
                self.fail("Exception raised")

        with self.subTest('none'):
            try:
                LivyHook._validate_extra_conf(None)
            except AirflowException:
                self.fail("Exception raised")

        with self.subTest('not a dict 1'):
            with self.assertRaises(AirflowException):
                LivyHook._validate_extra_conf('k1=v1')

        with self.subTest('not a dict 2'):
            with self.assertRaises(AirflowException):
                LivyHook._validate_extra_conf([('k1', 'v1'), ('k2', 0)])

        with self.subTest('nested dict'):
            with self.assertRaises(AirflowException):
                LivyHook._validate_extra_conf({'outer': {'inner': 'val'}})

        with self.subTest('empty items'):
            with self.assertRaises(AirflowException):
                LivyHook._validate_extra_conf({'has_val': 'val', 'no_val': None})

        with self.subTest('empty string'):
            with self.assertRaises(AirflowException):
                LivyHook._validate_extra_conf({'has_val': 'val', 'no_val': ''})

    @staticmethod
    def build_mock_response(mock_request, status_code, body):
        """helper method"""
        if not isinstance(mock_request, MagicMock):
            raise ValueError("Mock expected")
        mock_request.return_value.status_code = status_code
        mock_request.return_value.json.return_value = body

    @patch('requests.post')
    def test_post_batch(self, mock_request):

        batch_id = 100

        hook = LivyHook()
        hook._base_url = 'http://localhost:8998'

        with self.subTest('batch submit success'):
            TestLivyHook.build_mock_response(
                mock_request,
                201,
                {'id': batch_id, 'state': BatchState.STARTING, 'log': []}
            )

            resp = hook.post_batch(file='sparkapp')

            request_args = mock_request.call_args[1]

            mock_request.assert_called_once()
            mock_request.assert_called_with(
                'http://localhost:8998/batches',
                data=json.dumps({'file': 'sparkapp'}),
                headers={'Content-Type': 'application/json'}
            )
            self.assertIn('data', request_args)
            self.assertIsInstance(request_args['data'], str)
            self.assertIsInstance(resp, int)
            self.assertEqual(resp, batch_id)

        mock_request.reset_mock()

        with self.subTest('batch submit failed'):
            TestLivyHook.build_mock_response(mock_request, 400, {})

            with self.assertRaises(AirflowException):
                hook.post_batch(file='sparkapp')

            request_args = mock_request.call_args[1]

            mock_request.assert_called_once()
            mock_request.assert_called_with(
                'http://localhost:8998/batches',
                data=json.dumps({'file': 'sparkapp'}),
                headers={'Content-Type': 'application/json'}
            )
            self.assertIn('data', request_args)
            self.assertIsInstance(request_args['data'], str)

    @patch('requests.get')
    def test_get_batch(self, mock_request):

        batch_id = 100

        hook = LivyHook()
        hook._base_url = 'http://localhost:8998'

        with self.subTest('get batch success'):
            TestLivyHook.build_mock_response(mock_request, 200, {'id': batch_id})

            resp = hook.get_batch(batch_id)

            mock_request.assert_called_once()
            mock_request.assert_called_with('http://localhost:8998/batches/{}'.format(batch_id))
            self.assertIsInstance(resp, dict)
            self.assertIn('id', resp)

        mock_request.reset_mock()

        with self.subTest('get batch failed'):
            TestLivyHook.build_mock_response(mock_request, 400, {})

            with self.assertRaises(AirflowException):
                hook.get_batch(batch_id)
            mock_request.assert_called_once()
            mock_request.assert_called_with('http://localhost:8998/batches/{}'.format(batch_id))

    def test_invalid_uri(self):
        hook = LivyHook(livy_conn_id='invalid_uri')
        with self.assertRaises(RequestException):
            hook.post_batch(file='sparkapp')

    @patch('requests.get')
    def test_get_batch_state(self, mock_request):

        batch_id = 100
        running = BatchState.RUNNING

        hook = LivyHook()
        hook._base_url = 'http://localhost:8998'

        with self.subTest('get batch success'):
            TestLivyHook.build_mock_response(mock_request, 200, {'id': batch_id, 'state': running.value})

            state = hook.get_batch_state(batch_id)

            mock_request.assert_called_once()
            mock_request.assert_called_with('http://localhost:8998/batches/{}/state'.format(batch_id))
            self.assertIsInstance(state, BatchState)
            self.assertEqual(state, running)

        mock_request.reset_mock()

        with self.subTest('get batch failed'):
            TestLivyHook.build_mock_response(mock_request, 400, {})

            with self.assertRaises(AirflowException):
                hook.get_batch_state(batch_id)
            mock_request.assert_called_once()
            mock_request.assert_called_with('http://localhost:8998/batches/{}/state'.format(batch_id))

    def test_parse_post_response(self):
        batch_id = 100

        res_id = LivyHook._parse_post_response({'id': batch_id, 'log': []})

        self.assertEqual(batch_id, res_id)

    @patch('requests.delete')
    def test_delete_batch(self, mock_request):

        batch_id = 100

        hook = LivyHook()
        hook._base_url = 'http://localhost:8998'

        with self.subTest('get batch success'):
            TestLivyHook.build_mock_response(mock_request, 200, {'msg': 'deleted'})

            resp = hook.delete_batch(batch_id)

            mock_request.assert_called_once()
            mock_request.assert_called_with('http://localhost:8998/batches/{}'.format(batch_id))
            self.assertEqual(resp, {'msg': 'deleted'})

        mock_request.reset_mock()

        with self.subTest('get batch failed'):
            TestLivyHook.build_mock_response(mock_request, 400, {})

            with self.assertRaises(AirflowException):
                hook.delete_batch(batch_id)
            mock_request.assert_called_once()
            mock_request.assert_called_with('http://localhost:8998/batches/{}'.format(batch_id))

    @patch('requests.post')
    def test_missing_batch_id(self, mock_post):
        hook = LivyHook()
        hook._base_url = 'http://localhost:8998'

        TestLivyHook.build_mock_response(mock_post, 201, {})

        with self.assertRaises(AirflowException):
            hook.post_batch(file='sparkapp')

        mock_post.assert_called_once()
        mock_post.assert_called_with(
            'http://localhost:8998/batches',
            data=json.dumps({'file': 'sparkapp'}),
            headers={'Content-Type': 'application/json'}
        )

    @patch('requests.get')
    def test_get_batch_validation(self, mock_call):
        hook = LivyHook(livy_conn_id='simple')
        hook._base_url = 'http://host:8998'
        TestLivyHook.build_mock_response(mock_call, 200, SAMPLE_GET_RESPONSE)

        with self.subTest('get_batch int'):
            hook.get_batch(TEST_ID)
            mock_call.assert_called_with('{}/batches/{}'.format(hook._base_url, TEST_ID))

        with self.subTest('get_batch str'):
            hook.get_batch(str(TEST_ID))
            mock_call.assert_called_with('{}/batches/{}'.format(hook._base_url, TEST_ID))

        for val in [None, 'one', {'a': 'b'}]:
            with self.subTest('get_batch {}'.format(val)):
                with self.assertRaises(AirflowException):
                    hook.get_batch(val)

    @patch('requests.get')
    def test_get_batch_state_validation(self, mock_call):
        hook = LivyHook(livy_conn_id='simple')
        hook._base_url = 'http://host:8998'
        TestLivyHook.build_mock_response(mock_call, 200, SAMPLE_GET_RESPONSE)

        with self.subTest('get_batch int'):
            hook.get_batch_state(TEST_ID)
            mock_call.assert_called_with('{}/batches/{}/state'.format(hook._base_url, TEST_ID))

        with self.subTest('get_batch str'):
            hook.get_batch_state(str(TEST_ID))
            mock_call.assert_called_with('{}/batches/{}/state'.format(hook._base_url, TEST_ID))

        for val in [None, 'one', {'a': 'b'}]:
            with self.subTest('get_batch {}'.format(val)):
                with self.assertRaises(AirflowException):
                    hook.get_batch_state(val)

    @patch('requests.delete')
    def test_delete_batch_validation(self, mock_call):
        hook = LivyHook(livy_conn_id='simple')
        hook._base_url = 'http://host:8998'
        TestLivyHook.build_mock_response(mock_call, 200, {'id': TEST_ID})

        with self.subTest('get_batch int'):
            hook.delete_batch(TEST_ID)
            mock_call.assert_called_with('{}/batches/{}'.format(hook._base_url, TEST_ID))

        with self.subTest('get_batch str'):
            hook.delete_batch(str(TEST_ID))
            mock_call.assert_called_with('{}/batches/{}'.format(hook._base_url, TEST_ID))

        for val in [None, 'one', {'a': 'b'}]:
            with self.subTest('get_batch {}'.format(val)):
                with self.assertRaises(AirflowException):
                    hook.delete_batch(val)

    def test_check_session_id(self):
        with self.subTest('valid 00'):
            try:
                LivyHook._validate_session_id(100)
            except AirflowException:
                self.fail("")

        with self.subTest('valid 01'):
            try:
                LivyHook._validate_session_id(0)
            except AirflowException:
                self.fail("")

        with self.subTest('None'):
            with self.assertRaises(AirflowException):
                LivyHook._validate_session_id(None)

        with self.subTest('random string'):
            with self.assertRaises(AirflowException):
                LivyHook._validate_session_id('asd')


if __name__ == '__main__':
    unittest.main()
