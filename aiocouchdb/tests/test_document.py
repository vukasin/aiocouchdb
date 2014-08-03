# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import json
import aiocouchdb.client
import aiocouchdb.feeds
import aiocouchdb.document
import aiocouchdb.tests.utils as utils
from aiocouchdb.client import urljoin


class DatabaseTestCase(utils.TestCase):

    def setUp(self):
        super().setUp()
        self.url_doc = urljoin(self.url, 'db', 'docid')
        self.doc = aiocouchdb.document.Document(self.url_doc)

    def test_init_with_url(self):
        self.assertIsInstance(self.doc.resource, aiocouchdb.client.Resource)

    def test_init_with_resource(self):
        res = aiocouchdb.client.Resource(self.url_doc)
        doc = aiocouchdb.document.Document(res)
        self.assertIsInstance(doc.resource, aiocouchdb.client.Resource)
        self.assertEqual(self.url_doc, self.doc.resource.url)

    def test_exists(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.exists())
        self.assert_request_called_with('HEAD', 'db', 'docid')
        self.assertTrue(result)

    def test_exists_rev(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.exists('1-ABC'))
        self.assert_request_called_with('HEAD', 'db', 'docid',
                                        params={'rev': '1-ABC'})
        self.assertTrue(result)

    def test_exists_forbidden(self):
        resp = self.mock_json_response()
        resp.status = 403
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.exists())
        self.assert_request_called_with('HEAD', 'db', 'docid')
        self.assertFalse(result)

    def test_exists_not_found(self):
        resp = self.mock_json_response()
        resp.status = 404
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.exists())
        self.assert_request_called_with('HEAD', 'db', 'docid')
        self.assertFalse(result)

    def test_modified(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.modified('1-ABC'))
        self.assert_request_called_with('HEAD', 'db', 'docid',
                                        headers={'IF-NONE-MATCH': '"1-ABC"'})
        self.assertTrue(result)

    def test_not_modified(self):
        resp = self.mock_json_response()
        resp.status = 304
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.modified('1-ABC'))
        self.assert_request_called_with('HEAD', 'db', 'docid',
                                        headers={'IF-NONE-MATCH': '"1-ABC"'})
        self.assertFalse(result)

    def test_get(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.get())
        self.assert_request_called_with('GET', 'db', 'docid')
        self.assertEqual({}, result)

    def test_get_rev(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.get('1-ABC'))
        self.assert_request_called_with('GET', 'db', 'docid',
                                        params={'rev': '1-ABC'})
        self.assertEqual({}, result)

    def test_get_params(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        all_params = {
            'att_encoding_info': True,
            'attachments': True,
            'atts_since': ['1-ABC'],
            'conflicts': False,
            'deleted_conflicts': True,
            'local_seq': True,
            'meta': False,
            'open_revs': ['1-ABC', '2-CDE'],
            'rev': '1-ABC',
            'revs': True,
            'revs_info': True
        }

        for key, value in all_params.items():
            self.run_loop(self.doc.get(**{key: value}))
            if key in ('atts_since', 'open_revs'):
                value = json.dumps(value)
            self.assert_request_called_with('GET', 'db', 'docid',
                                            params={key: value})

    def test_update(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.update({}))
        self.assert_request_called_with('PUT', 'db', 'docid',
                                        data={})
        self.assertEqual({}, result)

    def test_update_params(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        all_params = {
            'batch': "ok",
            'new_edits': True,
            'rev': '1-ABC'
        }

        for key, value in all_params.items():
            self.run_loop(self.doc.update({}, **{key: value}))
            self.assert_request_called_with('PUT', 'db', 'docid',
                                            data={},
                                            params={key: value})

    def test_update_expect_mapping(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        self.assertRaises(TypeError, self.run_loop, self.doc.update([]))

        class Foo(dict):
            pass

        doc = Foo()
        self.run_loop(self.doc.update(doc))
        self.assert_request_called_with('PUT', 'db', 'docid', data={})

    def test_update_reject_docid_collision(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        self.assertRaises(ValueError,
                          self.run_loop,
                          self.doc.update({'_id': 'foo'}))

    def test_remove(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.remove('1-ABC'))
        self.assert_request_called_with('DELETE', 'db', 'docid',
                                        params={'rev': '1-ABC'})
        self.assertEqual({}, result)

    def test_remove_preserve_content(self):
        resp = self.mock_json_response(data=b'{"_id": "foo", "bar": "baz"}')
        self.request.return_value = self.future(resp)

        self.run_loop(self.doc.remove('1-ABC', preserve_content=True))
        self.assert_request_called_with('PUT', 'db', 'docid',
                                        data={'_id': 'foo',
                                              '_deleted': True,
                                              'bar': 'baz'})

    def test_copy(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.copy('newid'))
        self.assert_request_called_with('COPY', 'db', 'docid',
                                        headers={'DESTINATION': 'newid'})
        self.assertEqual({}, result)