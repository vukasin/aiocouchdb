# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#


import asyncio
import bisect
import uuid

from .abc import ISourcePeer, ITargetPeer
from .records import ReplicationTask
from .work_queue import WorkQueue
from . import utils


__all__ = (
    'Replication',
)


class Replication(object):
    """Replication job maker."""

    default_checkpoint_interval = 5  # seconds
    default_worker_batch_size = 100
    default_worker_processes = 4
    lowest_seq = 0
    max_history_entries = 50

    def __init__(self,
                 rep_uuid: str,
                 rep_task: ReplicationTask,
                 source_peer_class,
                 target_peer_class, *,
                 protocol_version=3):
        self.rep_uuid = rep_uuid
        self.rep_task = rep_task
        self.protocol_version = protocol_version
        self.source = source_peer_class(rep_task.source)
        self.target = target_peer_class(rep_task.target)

    @asyncio.coroutine
    def changes_reader_loop(self,
                            changes_queue: WorkQueue,
                            source: ISourcePeer,
                            rep_task: ReplicationTask,
                            start_seq):
        # couch_replicator_changes_reader
        feed = yield from source.changes(
            continuous=rep_task.continuous,
            doc_ids=rep_task.doc_ids,
            filter=rep_task.filter,
            query_params=rep_task.query_params,
            since=start_seq,
            view=rep_task.view)
        while True:
            event = yield from feed.next()
            if event is None:
                changes_queue.close()
                break
            yield from changes_queue.put(event)

    @asyncio.coroutine
    def checkpoints_loop(self,
                         rep_id: str,
                         reports_queue: WorkQueue,
                         session_id: str,
                         source: ISourcePeer,
                         target: ITargetPeer,
                         source_info: dict,
                         target_info: dict,
                         source_log: dict,
                         target_log: dict,
                         history: list, *,
                         use_checkpoints: bool=True,
                         checkpoint_interval: int=default_checkpoint_interval):
        def proxy_do_checkpoint(seq):
            # proxy function to reduce things duplication
            return self.do_checkpoint(
                rep_id, session_id, seq,
                source, source_info, source_log,
                target, target_info, target_log,
                history)

        committed_seq = self.lowest_seq
        current_thought_seq = self.lowest_seq
        highest_seq_done = self.lowest_seq
        seqs_in_progress = []  # we need ordset here

        timer = asyncio.async(asyncio.sleep(checkpoint_interval))

        # local optimization: gather all the reports from queue
        # In order to reduce context switches between asyncio tasks, we gather
        # all the available reports with single call.
        get_reports = asyncio.async(reports_queue.get_all())
        while True:
            # timer and reports awaiter should be run concurrently and do not
            # block each other.
            yield from asyncio.wait([timer, get_reports],
                                    return_when=asyncio.FIRST_COMPLETED)
            if get_reports.done():
                reports = get_reports.result()

                if reports is reports_queue.CLOSED:
                    timer.cancel()
                    break

                for is_done, report_seq in reports:
                    if is_done:
                        # handle_call({report_seq_done, Seq, ...}, From, State)
                        highest_seq_done = max(highest_seq_done, report_seq)
                        # Here is a problem that solved: assume 3 workers are
                        # processing changes feed. First worker handles changes
                        # with seq 0-100, second - 101-200, third - 201-300.
                        # First hanged, third is done, after a while second
                        # is done. What's the sequence number we should record
                        # in checkpoint? Should we make a checkpoint either
                        # if first worker in the end will get crashed?
                        if not seqs_in_progress:
                            # dummy branch, see below
                            pass
                        elif seqs_in_progress[0] == report_seq:
                            current_thought_seq = seqs_in_progress.pop(0)
                        else:
                            seqs_in_progress.pop(bisect.bisect_left(
                                seqs_in_progress, report_seq))

                        if not seqs_in_progress:
                            # No more seqs in progress, make sure that we make
                            # checkpoint with the highest seq that done
                            current_thought_seq = max(current_thought_seq,
                                                      highest_seq_done)

                    else:
                        # handle_call({report_seq, Seq, ...}, From, State)
                        # Is append and sort better than insert with bisect?
                        seqs_in_progress.append(report_seq)
                        seqs_in_progress.sort()

                get_reports = asyncio.async(reports_queue.get_all())

            if timer.done():
                timer = asyncio.async(asyncio.sleep(checkpoint_interval))

                if not use_checkpoints:
                    # We don't use checkpoints. Could we not use timer as well?
                    continue

                if committed_seq == current_thought_seq:
                    # Nothing was changed, no need to make a checkpoint.
                    continue

                committed_seq = yield from proxy_do_checkpoint(
                    current_thought_seq)

        # We are going to do the last checkpoint while having some sequences
        # in progress. What's wrong?
        assert not seqs_in_progress, seqs_in_progress

        if committed_seq != highest_seq_done:
            # Do the last checkpoint
            committed_seq = yield from proxy_do_checkpoint(highest_seq_done)

    @asyncio.coroutine
    def start(self):
        """Starts a replication."""
        # couch_replicator:do_init/1
        # couch_replicator:init_state/1
        rep_task, source, target = self.rep_task, self.source, self.target

        # we'll need source and target info later
        source_info, target_info = yield from self.verify_peers(
            source, target, rep_task.create_target)

        rep_id = yield from self.generate_replication_id(
            rep_task, source, self.rep_uuid, self.protocol_version)

        source_log, target_log = yield from self.find_replication_logs(
            rep_id, source, target)
        found_seq, history = self.compare_replication_logs(
            source_log, target_log)
        start_seq = rep_task.since_seq or found_seq

        num_workers = (rep_task.worker_processes
                       or self.default_worker_processes)
        batch_size = (rep_task.worker_batch_size
                      or self.default_worker_batch_size)
        max_items = num_workers * batch_size * 2

        # we don't support changes queue limitation by byte size while we relay
        # on asyncio.Queue which only limits items by their amount.
        # max_size = 100 * 1024 * num_workers

        changes_queue = WorkQueue(maxsize=max_items)
        changes_reader_task = asyncio.async(self.changes_reader_loop(
            changes_queue, source, rep_task, start_seq))

        session_id = uuid.uuid4().hex
        reports_queue = WorkQueue()
        if rep_task.checkpoint_interval:
            checkpoint_interval = rep_task.checkpoint_interval
        else:
            checkpoint_interval = self.default_checkpoint_interval
        checkpoints_loop_task = asyncio.async(self.checkpoints_loop(
            # TODO use rep_state
            rep_id, reports_queue, session_id,
            source, source_info, source_log,
            target, target_info,  target_log, history,
            use_checkpoints=rep_task.use_checkpoints,
            checkpoint_interval=checkpoint_interval))

        raise NotImplementedError

    @asyncio.coroutine
    def verify_peers(self, source: ISourcePeer, target: ITargetPeer,
                     create_target: bool=False) -> tuple:
        """Verifies that source and target databases are exists and accessible.

        If target is not exists (HTTP 404) it may be created in case when
        :attr:`ReplicationTask.create_target` is set as ``True``.

        Raises :exc:`aiocouchdb.error.HttpErrorException` exception depending
        from the HTTP error of initial peers requests.
        """
        source_info = yield from source.info()

        if not (yield from target.exists()):
            if create_target:
                yield from target.create()
        target_info = yield from target.info()

        return source_info, target_info

    @asyncio.coroutine
    def generate_replication_id(self,
                                rep_task: ReplicationTask,
                                source: ISourcePeer,
                                rep_uuid: str,
                                protocol_version: int) -> str:
        """Generates replication ID for the protocol version `3` which is
        actual for CouchDB 1.2+.

        If non builtin filter function was specified in replication task,
        their source code will be fetched using CouchDB Document API.

        :rtype: str
        """
        if protocol_version != 3:
            raise RuntimeError('Only protocol version 3 is supported')

        func_code = yield from source.get_filter_function_code(rep_task.filter)

        return utils.replication_id_v3(
            rep_uuid,
            rep_task.source,
            rep_task.target,
            continuous=rep_task.continuous,
            create_target=rep_task.create_target,
            doc_ids=rep_task.doc_ids,
            filter=func_code.strip() if func_code else None,
            query_params=rep_task.query_params)

    @asyncio.coroutine
    def find_replication_logs(self,
                              rep_id: str,
                              source: ISourcePeer,
                              target: ITargetPeer) -> (dict, dict):
        """Searches for Replication logs on both source and target peers."""
        source_doc = yield from source.get_replication_log(rep_id)
        target_doc = yield from target.get_replication_log(rep_id)

        return source_doc, target_doc

    def compare_replication_logs(self, source: dict, target: dict) -> tuple:
        """Compares Replication logs in order to find the common history and
        the last sequence number for the Replication to start from."""
        # couch_replicator:compare_replication_logs/2

        if not source or not target:
            return self.lowest_seq, []

        source_session_id = source.get('session_id')
        target_session_id = target.get('session_id')
        if source_session_id == target_session_id:
            # Last recorded session ID for both Source and Target matches.
            # Hooray! We found it!
            return (source.get('source_last_seq', self.lowest_seq),
                    source.get('history', []))
        else:
            return self.compare_replication_history(source.get('history', []),
                                                    target.get('history', []))

    def compare_replication_history(self, source: list, target: list) -> tuple:
        # couch_replicator:compare_rep_history/2

        if not source or not target:
            return self.lowest_seq, []

        source_id = source[0].get('session_id')
        if any(item.get('session_id') == source_id for item in target):
            return source[0].get('recorded_seq', self.lowest_seq), source[1:]

        target_id = target[0].get('session_id')
        if any(item.get('session_id') == target_id for item in source[1:]):
            return target[0].get('recorded_seq', self.lowest_seq), target[1:]

        return self.compare_replication_history(source[1:], target[1:])

    @asyncio.coroutine
    def do_checkpoint(self,
                      rep_id: str,
                      session_id: str,
                      seq,
                      source: ISourcePeer,
                      source_info: dict,
                      source_log: dict,
                      target: ITargetPeer,
                      target_info: dict,
                      target_log: dict,
                      history: list):
        # couch_replicator:do_checkpoint/1

        yield from self.ensure_full_commit(source, source_info,
                                           target, target_info)
        return (yield from self.record_checkpoint(rep_id, session_id, seq,
                                                  source, source_log,
                                                  target, target_log,
                                                  history))

    @asyncio.coroutine
    def ensure_full_commit(self,
                           source: ISourcePeer,
                           source_info: dict,
                           target: ITargetPeer,
                           target_info: dict):
        """Ask Source and Target peers to ensure that all changes that made
        are flushed on disk or other persistent storage.

        Terminates a Replication if Source or Target changed their start time
        value."""
        # Why we need to ensure_full_commit on source? Only just for start time?
        source_start_time = yield from source.ensure_full_commit()
        target_start_time = yield from target.ensure_full_commit()

        if source_start_time != source_info['instance_start_time']:
            raise RuntimeError('source start time was changed')

        if target_start_time != target_info['instance_start_time']:
            raise RuntimeError('target start time was changed')

    @asyncio.coroutine
    def record_checkpoint(self,
                          rep_id: str,
                          session_id: str,
                          seq,
                          source: ISourcePeer,
                          source_log: dict,
                          target: ITargetPeer,
                          target_log: dict,
                          history: list):
        """Records Checkpoint on the both Peers and returns recorded sequence
        back."""

        new_history = ([self.new_history_entry(session_id, seq)]
                       + history[:self.max_history_entries - 1])

        source_log['history'] = new_history
        target_log['history'] = new_history

        source_rev = yield from source.update_replication_log(
            rep_id, source_log)
        target_rev = yield from target.update_replication_log(
            rep_id, target_log)

        # TODO: get rid dependency on mutable things
        source_log['_rev'] = source_rev
        target_log['_rev'] = target_rev
        history[:] = new_history

        return seq

    def new_history_entry(self, session_id: str, recorded_seq) -> dict:
        """Returns a new replication history entry suitable to be added to
        replication log."""
        # TODO: add more fields according CouchDB structure
        # but these ones are required.
        return {
            'session_id': session_id,
            'recorded_seq': recorded_seq,
        }
