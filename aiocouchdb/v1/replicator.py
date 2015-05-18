# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio

from aiocouchdb.replicator.abc import ISourcePeer, ITargetPeer
from aiocouchdb.replicator.utils import retry_if_failed
from . import database


class Peer(ISourcePeer, ITargetPeer):

    default_retries = 10
    default_timeout = 60
    max_delay = 300

    def __init__(self, info, *,
                 retries: int=None,
                 socket_options=None,
                 timeout: int=None):
        self.db = database.Database(info.url)
        self.db.resource.session.auth = info.auth
        self.retries = retries or self.default_retries
        self.socket_options = socket_options
        self.timeout = timeout or self.default_timeout

    @asyncio.coroutine
    def retry_if_failed(self, coro, *, expected_errors: tuple=()):
        return (yield from retry_if_failed(coro, self.retries,
                                           expected_errors=expected_errors,
                                           max_delay=self.max_delay,
                                           timeout=self.timeout))

    @asyncio.coroutine
    def exists(self):
        return (yield from self.retry_if_failed(self.db.exists()))

    @asyncio.coroutine
    def create(self):
        return (yield from self.db.create())

    @asyncio.coroutine
    def info(self):
        return (yield from self.retry_if_failed(self.db.info()))

    @asyncio.coroutine
    def ensure_full_commit(self):
        info = (yield from self.retry_if_failed(self.db.ensure_full_commit()))
        return info['instance_start_time']

    @asyncio.coroutine
    def open_doc_revs(self, docid: str, open_revs: list, callback_coro, *,
                      atts_since: list=None,
                      latest: bool=None,
                      revs: bool=None):
        doc = yield from self.db.doc(docid)
        reader = yield from doc.get_open_revs(*open_revs,
                                              atts_since=atts_since,
                                              latest=latest,
                                              revs=revs)
        while True:
            doc, atts = yield from reader.next()
            if doc is None:
                break
            yield from callback_coro(doc, None if atts.at_eof() else atts)

    @asyncio.coroutine
    def get_filter_function_code(self, filter_name: str):
        if filter_name is None:
            return
        if filter_name.startswith('_'):
            return
        ddoc_name, func_name = filter_name.split('/', 1)
        ddoc = yield from self.db.ddoc(ddoc_name)
        return (yield from ddoc.doc.get())['filters'][func_name]

    @asyncio.coroutine
    def get_replication_log(self, rep_id: str):
        doc = self.db['_local/' + rep_id]
        if (yield from doc.exists()):
            return (yield from self.retry_if_failed(doc.get()))
        return {}

    @asyncio.coroutine
    def update_replication_log(self,
                               rep_id: str,
                               doc: dict, *,
                               rev: str=None):
        docapi = yield from self.db.doc('_local/' + rep_id)
        info = (yield from self.retry_if_failed(docapi.update(doc, rev=rev)))
        return info['rev']

    @asyncio.coroutine
    def revs_diff(self, id_revs):
        return (yield from self.retry_if_failed(self.db.revs_diff(id_revs)))

    @asyncio.coroutine
    def changes(self, changes_queue, *,
                continuous: bool=False,
                doc_ids: list=None,
                filter: str=None,
                query_params: dict=None,
                since=None,
                view: str=None):
        doc_ids = doc_ids or []
        feed = yield from self.db.changes(
            *doc_ids,
            feed='continuous' if continuous else 'normal',
            filter=filter,
            params=query_params,
            since=since,
            view=view)
        while True:
            event = yield from feed.next()
            if event is None:
                yield from changes_queue.put((feed.last_seq, None))
                break
            yield from changes_queue.put((event['seq'], event))

    @asyncio.coroutine
    def update_doc(self, doc, atts=None):
        docapi = yield from self.db.doc(doc['_id'])
        yield from self.retry_if_failed(docapi.update(doc, atts=atts))

    @asyncio.coroutine
    def update_docs(self, docs):
        return (yield from self.retry_if_failed(self.db.bulk_docs(docs)))
