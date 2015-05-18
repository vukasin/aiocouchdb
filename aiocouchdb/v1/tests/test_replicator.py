# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import aiocouchdb.replicator

from . import utils

from .. import replicator

class ReplicatorTestCase(utils.ServerTestCase):

    _test_target = 'couchdb'
    timeout = 30

    @utils.using_database('source')
    @utils.using_database('target')
    def test_simple_replication(self, source, target):
        yield from utils.populate_database(source, 100)
        rep_task = aiocouchdb.replicator.ReplicationTask(
            source=source.resource.url,
            target=target.resource.url,
            checkpoint_interval=5,
            use_checkpoints=True
        )
        rep = aiocouchdb.replicator.Replication(
            'aiocouchdb', rep_task, replicator.Peer, replicator.Peer)

        state = yield from (yield from rep.start())

        src_info = yield from source.info()
        tgt_info = yield from target.info()
        self.assertEqual(src_info['doc_count'], tgt_info['doc_count'])

    @utils.using_database('source')
    @utils.using_database('target')
    def test_continuous_replication(self, source, target):
        yield from utils.populate_database(source, 100)
        rep_task = aiocouchdb.replicator.ReplicationTask(
            source=source.resource.url,
            target=target.resource.url,
            continuous=True,
            checkpoint_interval=5,
            use_checkpoints=True
        )
        rep = aiocouchdb.replicator.Replication(
            'aiocouchdb', rep_task, replicator.Peer, replicator.Peer)

        rep_task = yield from rep.start()

        for i in range(10):
            doc = yield from source.doc()
            yield from doc.update({'count': 1})
            yield from asyncio.sleep(0.1)

        yield from asyncio.sleep(1)

        src_info = yield from source.info()
        tgt_info = yield from target.info()
        self.assertEqual(src_info['doc_count'], tgt_info['doc_count'])
