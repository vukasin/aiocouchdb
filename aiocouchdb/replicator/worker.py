# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio

from .abc import ISourcePeer, ITargetPeer
from .work_queue import WorkQueue


__all__ = (
    'ReplicationWorker',
)


class ReplicationWorker(object):
    """Replication worker is a base unit that does all the hard work on transfer
    documents from Source peer the Target one.

    :param source: Source peer
    :param target: Target peer
    :param changes_queue: A queue from where new changes events will be fetched
    :param reports_queue: A queue to where worker will send all reports about
                          replication progress
    :param int batch_size: Amount of events to get from `changes_queue`
                           to process
    :param int max_conns: Amount of simultaneous connection to make against
                          peers at the same time
    """

    default_batch_size = 100
    default_max_conns = 4

    def __init__(self,
                 source: ISourcePeer,
                 target: ITargetPeer,
                 changes_queue: WorkQueue,
                 reports_queue: WorkQueue, *,
                 batch_size: int=None,
                 max_conns: int=None):
        self.source = source
        self.target = target
        self.reports_queue = reports_queue
        self.changes_queue = changes_queue
        self.batch_size = batch_size or self.default_batch_size
        self.max_conns = max_conns or self.default_max_conns

    def start(self):
        """Starts Replication worker."""
        return asyncio.async(self.changes_fetch_loop(
            self.changes_queue, self.reports_queue, batch_size=self.batch_size
        ))

    @asyncio.coroutine
    def changes_fetch_loop(self,
                           changes_queue: WorkQueue,
                           reports_queue: WorkQueue, *,
                           batch_size: int):
        # couch_replicator_worker:queue_fetch_loop/5
        while True:
            changes = yield from changes_queue.get(batch_size)

            if changes is changes_queue.CLOSED:
                break

            # Ensure that we report about the highest seq in the batch
            report_seq = sorted(changes, key=lambda i: i['seq'])[-1]['seq']
            # Notify checkpoints_loop that we start work on the batch
            yield from reports_queue.put((False, report_seq))
