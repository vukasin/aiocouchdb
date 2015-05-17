# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio

from itertools import groupby
from operator import itemgetter

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

    # couch_replicator_worker actually uses byte sized buffer for remote source,
    # but that's kind of strange and forces to run useless json encoding.
    # We'll stay with the buffer configuration that it uses for local source.
    # However, both are still not configurable.
    docs_buffer_size = 10

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
            self.changes_queue, self.reports_queue, self.source, self.target,
            batch_size=self.batch_size, max_conns=self.max_conns))

    @asyncio.coroutine
    def changes_fetch_loop(self,
                           changes_queue: WorkQueue,
                           reports_queue: WorkQueue,
                           source: ISourcePeer,
                           target: ITargetPeer, *,
                           batch_size: int,
                           max_conns: int):
        # couch_replicator_worker:queue_fetch_loop/5
        while True:
            seqs_changes = yield from changes_queue.get(batch_size)

            if seqs_changes is changes_queue.CLOSED:
                break

            # Ensure that we report about the highest seq in the batch
            seqs, changes = zip(*sorted(seqs_changes))
            report_seq = seqs[-1]

            # Notify checkpoints_loop that we start work on the batch
            yield from reports_queue.put((False, report_seq))

            idrevs = yield from self.find_missing_revs(target, changes)
            if not idrevs:
                # No difference were found, report about and move on
                yield from reports_queue.put((True, report_seq))
                continue

            yield from self.remote_process_batch(source, target, idrevs,
                                                 max_conns=max_conns)

            yield from reports_queue.put((True, report_seq))

    @asyncio.coroutine
    def readers_loop(self,
                     inbox: asyncio.Queue,
                     outbox: asyncio.Queue,
                     max_conns: int):
        # handle_call({fetch_doc, ...}, From, State)

        # FIXME: there should be a better way to break the loop if one of the
        # readers failed.
        everything_is_broken = asyncio.Event()

        def on_done(reader, event=everything_is_broken):
            readers.discard(reader)
            if reader.exception():
                # handle_info({'EXIT', Pid, Reason}, State)
                event.set()
                semaphore.release()
                raise reader.exception()
            else:
                # handle_info({'EXIT', Pid, normal}, State)
                assert reader.result is not None, 'that should not be'
                outbox.put_nowait(reader.result())
                semaphore.release()

        readers = set()
        semaphore = asyncio.Semaphore(max_conns)
        while True:
            item = yield from inbox.get()

            if item is None:
                # Once we received a stop signal, let's ensure that all
                # the remain readers are done.
                yield from asyncio.wait(readers)
                break

            # So we block here until one of the readers will release
            # a semaphore in done callback.
            yield from semaphore.acquire()

            # Need to avoid spawn another reader when everything is broken
            if everything_is_broken.is_set():
                # And since everything is broken, no need to wait for
                # the remaining readers
                for reader in readers:
                    reader.cancel()
                break

            reader = asyncio.async(self.fetch_doc_open_revs(*item))
            reader.add_done_callback(on_done)
            readers.add(reader)

    @asyncio.coroutine
    def batch_docs_loop(self,
                        inbox: asyncio.Queue,
                        target: ITargetPeer, *,
                        buffer_size: int):
        # handle_call({batch_doc, ...}, From, State)
        # We use separate loop for batching docs in order to make main_loop
        # more responsive for communication with Replication task.
        batch = []
        while True:
            docs = yield from inbox.get()

            if docs is None:
                # Like handle_call({flush, ...}, From, State)
                # but we're already know that all readers are done their work.
                if batch:
                    yield from self.update_docs(target, batch)
                break

            batch.extend(docs)

            if len(batch) >= buffer_size:
                yield from self.update_docs(target, batch)
                batch[:] = []

    @asyncio.coroutine
    def find_missing_revs(self, target: ITargetPeer, changes: list) -> dict:
        # couch_replicator_worker:find_missing/2
        # Unlike couch_replicator we also remove duplicate revs from diff
        # request which may eventually when the same document with the conflicts
        # had updated multiple times within the same batch slice.
        by_id = itemgetter('id')
        seen = set()
        seen_add = seen.add
        revs_diff = yield from target.revs_diff({
            docid: [change['rev']
                    for event in events
                    for change in event['changes']
                    if (docid, change['rev']) not in seen
                    and (seen_add((docid, change['rev'])) or True)]
            for docid, events in groupby(sorted(changes, key=by_id), key=by_id)
        })
        return {docid: (content['missing'],
                        content.get('possible_ancestors', []))
                for docid, content in revs_diff.items()}

    @asyncio.coroutine
    def remote_process_batch(self,
                             source: ISourcePeer,
                             target: ITargetPeer,
                             idrevs: dict, *,
                             max_conns: int):
        # couch_replicator_worker:remote_process_batch/2
        # Well, this isn't true remote_process_batch/2 reimplementation since
        # we don't need to provide here any protection from producing long URL
        # as we don't even know if the target will use HTTP protocol.
        #
        # As the side effect, we request all the conflicts from the source in
        # single API call.
        #
        # Protection of possible long URLs should be done on ISource
        # implementation side.

        batch_docs_queue = asyncio.Queue()
        batch_docs_loop_task = asyncio.async(self.batch_docs_loop(
            batch_docs_queue, target, buffer_size=self.docs_buffer_size))

        readers_inbox = asyncio.Queue()
        readers_loop_task = asyncio.async(self.readers_loop(
            readers_inbox, batch_docs_queue, max_conns))

        for docid, (missing, possible_ancestors) in idrevs.items():
            yield from readers_inbox.put((
                source, target, docid, missing, possible_ancestors))

        # We've done for readers
        yield from readers_inbox.put(None)

        # Ensure that all readers are done
        yield from readers_loop_task

        # Ask to flush all the remain in buffer docs
        yield from batch_docs_queue.put(None)

        # Ensure all docs are flushed
        yield from batch_docs_loop_task

    @asyncio.coroutine
    def fetch_doc_open_revs(self,
                            source: ISourcePeer,
                            target: ITargetPeer,
                            docid: str,
                            revs: list,
                            possible_ancestors: list) -> list:
        # couch_replicator_worker:fetch_doc/4
        acc = []

        @asyncio.coroutine
        def handle_doc_atts(doc: dict, atts):
            # couch_replicator_worker:remote_doc_handler/2
            if atts is None:
                # remote_doc_handler({ok, #doc{atts = []}}, Acc)
                acc.append(doc)
            else:
                # remote_doc_handler({ok, Doc}, Acc)
                yield from self.update_doc(target, doc, atts)

        yield from source.open_doc_revs(docid, revs, handle_doc_atts,
                                        atts_since=possible_ancestors,
                                        latest=True,
                                        revs=True)
        return acc

    @asyncio.coroutine
    def update_doc(self, target: ITargetPeer, doc: dict, atts):
        # couch_replicator_worker:flush_doc/2
        yield from target.update_doc(doc, atts)

    @asyncio.coroutine
    def update_docs(self, target: ITargetPeer, docs: list):
        # couch_replicator_worker:flush_docs/2
        # TODO: error and stats reports
        yield from target.update_docs(docs)
