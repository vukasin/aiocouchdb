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
import datetime
import functools
import itertools
import uuid

from .abc import ISourcePeer, ITargetPeer
from .records import ReplicationTask, ReplicationState, TsSeq
from .work_queue import WorkQueue
from .worker import ReplicationWorker
from . import utils


__all__ = (
    'Replication',
)


class Replication(object):
    """Replication job maker."""

    default_checkpoint_interval = 5  # seconds
    default_worker_processes = 4
    lowest_seq = 0
    max_history_entries = 50
    worker_class = ReplicationWorker

    def __init__(self,
                 rep_uuid: str,
                 rep_task: ReplicationTask,
                 source_peer_class,
                 target_peer_class, *,
                 protocol_version=3):
        self.source = source_peer_class(rep_task.source,
                                        retries=rep_task.retries_per_request,
                                        socket_options=rep_task.socket_options,
                                        timeout=rep_task.connection_timeout)
        self.target = target_peer_class(rep_task.target,
                                        retries=rep_task.retries_per_request,
                                        socket_options=rep_task.socket_options,
                                        timeout=rep_task.connection_timeout)
        self.state = ReplicationState(
            rep_task,
            rep_uuid=rep_uuid,
            protocol_version=protocol_version
        )

    @asyncio.coroutine
    def main_loop(self,
                  changes_reader_loop_task: asyncio.Task,
                  checkpoints_loop_task: asyncio.Task,
                  reports_queue: WorkQueue,
                  workers_tasks: list) -> ReplicationState:
        # Basically implementation of
        # couch_replicator:handle_info({'EXIT', Pid, _}, State)
        # monitor all the subtasks for their exit status and terminate
        # replication if things goes wrong

        pending = [changes_reader_loop_task,
                   checkpoints_loop_task] + workers_tasks
        workers_tasks_set = set(workers_tasks)
        while True:
            done, pending = yield from asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED)

            if changes_reader_loop_task.done():
                if changes_reader_loop_task.exception():
                    # Changes reader died, that's a critical situation.
                    for task in pending:
                        task.cancel()
                    raise changes_reader_loop_task.exception()

            if checkpoints_loop_task.done():
                # Checkpoint loop should not be done here.
                for task in pending:
                    task.cancel()
                if checkpoints_loop_task.exception():
                    raise checkpoints_loop_task.exception()
                else:
                    raise RuntimeError('checkpoint loop unexpectedly stopped')

            for worker in workers_tasks:
                if worker in done:
                    if worker.exception():
                        # Could we check if it still possible to make
                        # a checkpoint before completely crash?
                        for task in pending:
                            task.cancel()
                        raise worker.exception()

            if not (workers_tasks_set & pending):
                # All done, ask to do the last checkpoint
                reports_queue.close()
                break

        assert changes_reader_loop_task.done(), \
            'Why changes reader is still active when all workers are done?'

        # Waiting for the last checkpoint
        yield from checkpoints_loop_task
        return self.state

    @asyncio.coroutine
    def changes_reader_loop(self,
                            changes_queue: WorkQueue,
                            reports_queue: WorkQueue,
                            source: ISourcePeer,
                            rep_task: ReplicationTask,
                            start_seq: TsSeq):
        # couch_replicator_changes_reader

        inbox = asyncio.Queue(maxsize=changes_queue.maxsize)
        changes_task = asyncio.async(source.changes(
            inbox,
            continuous=rep_task.continuous,
            doc_ids=rep_task.doc_ids,
            filter=rep_task.filter,
            query_params=rep_task.query_params,
            since=start_seq.id,
            view=rep_task.view))
        # couch_replicator uses couch_replication:changes_manager_loop_open/4
        # to mark requested _batches_ with ordered numbers. Here we use
        # different approach to avoid having own changes_manager_loop (once
        # there was the one) as it makes solution more complicated by marking
        # all changes.
        #
        # Counter starts with greater than start_seq TS value in order to avoid
        # comparison default lowest seq value with the first received one.
        for ts in itertools.count(start_seq.ts + 1):
            inbox_get = asyncio.async(inbox.get())
            if not changes_task.done():
                yield from asyncio.wait([changes_task, inbox_get],
                                        return_when=asyncio.FIRST_COMPLETED)
            if changes_task.done():
                if changes_task.exception():
                    # Assume that ISource.changes implementation had done
                    # everything to fix the problem, but failed.
                    # So we have too.
                    raise changes_task.exception()
            seq, event = yield from inbox_get
            if event is None:
                # Report about the last seq regardless if it is actually
                # processed by any worker - checkpoints_loop will keep working
                # until all workers will finish their job.
                # We need such report for case when changes feed is filtered:
                # workers may not proceed the last seq, but we actually read
                # until it. No need read it and seqs before it again when we
                # restart the same replication.
                #
                # couch_replicator_changes_reader uses own TS counter  for
                # the last_seq which always lower than those what reported by
                # workers. Not sure if it's bug or not.
                yield from reports_queue.put((True, TsSeq(ts, seq)))
                changes_queue.close()
                break
            # We form TsSeq here in order to isolate workers from knowledge
            # about TsSeq thing.
            yield from changes_queue.put((TsSeq(ts, seq), event))

    @asyncio.coroutine
    def checkpoints_loop(self,
                         reports_queue: WorkQueue,
                         source: ISourcePeer,
                         target: ITargetPeer,
                         rep_state: ReplicationState,
                         use_checkpoints: bool=True,
                         checkpoint_interval: int=default_checkpoint_interval):

        current_through_seq = rep_state.committed_seq
        highest_seq_done = rep_state.committed_seq
        seqs_in_progress = []  # we need ordset here

        do_checkpoint = functools.partial(self.do_checkpoint,
                                          rep_state=rep_state,
                                          source=source,
                                          target=target)
        timer = asyncio.async(asyncio.sleep(checkpoint_interval))

        # local optimization: gather all the reports from queue
        # In order to reduce context switches between asyncio tasks, we gather
        # all the available reports with single call.
        get_reports = asyncio.async(reports_queue.get_all())
        while True:
            self.state = rep_state = rep_state.update(
                current_through_seq=current_through_seq,
                highest_seq_done=highest_seq_done,
                seqs_in_progress=tuple(seqs_in_progress)
            )

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
                            current_through_seq = seqs_in_progress.pop(0)
                        else:
                            index = bisect.bisect_left(seqs_in_progress,
                                                       report_seq)
                            # We don't want to get out of array boundaries
                            # for the last_seq report
                            if index < len(seqs_in_progress):
                                seqs_in_progress.pop(index)

                        if not seqs_in_progress:
                            # No more seqs in progress, make sure that we make
                            # checkpoint with the highest seq that done
                            current_through_seq = max(current_through_seq,
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

                if rep_state.committed_seq == current_through_seq:
                    # Nothing was changed, no need to make a checkpoint.
                    continue

                self.state = rep_state = yield from do_checkpoint(
                    seq=current_through_seq)

        # We are going to do the last checkpoint while having some sequences
        # in progress. What's wrong?
        assert not seqs_in_progress, seqs_in_progress

        if rep_state.committed_seq != highest_seq_done:
            # Do the last checkpoint
            self.state = rep_state = yield from do_checkpoint(
                seq=highest_seq_done)

    @asyncio.coroutine
    def start(self):
        """Starts a replication."""
        # couch_replicator:do_init/1
        # couch_replicator:init_state/1
        rep_task, source, target = self.state.rep_task, self.source, self.target

        # we'll need source and target info later
        source_info, target_info = yield from self.verify_peers(
            source, target, rep_task.create_target)

        rep_id = yield from self.generate_replication_id(
            rep_task, source, self.state.rep_uuid, self.state.protocol_version)

        source_log, target_log = yield from self.find_replication_logs(
            rep_id, source, target)
        found_seq, history = self.compare_replication_logs(
            source_log, target_log)
        start_seq = TsSeq(0, rep_task.since_seq or found_seq)

        num_workers = (rep_task.worker_processes
                       or self.default_worker_processes)
        batch_size = (rep_task.worker_batch_size
                      or self.worker_class.default_batch_size)
        max_items = num_workers * batch_size * 2

        # we don't support changes queue limitation by byte size while we relay
        # on asyncio.Queue which only limits items by their amount.
        # max_size = 100 * 1024 * num_workers

        changes_queue = WorkQueue(maxsize=max_items)
        reports_queue = WorkQueue()
        changes_reader_task = asyncio.async(self.changes_reader_loop(
            changes_queue, reports_queue, source, rep_task, start_seq))

        self.state = state = self.state.update(
            rep_id=rep_id,
            session_id=uuid.uuid4().hex,

            source_seq=source_info['update_seq'],
            start_seq=start_seq,
            committed_seq=start_seq,
            current_through_seq=start_seq,
            highest_seq_done=start_seq,
            seqs_in_progress=tuple(),

            replication_start_time=datetime.datetime.utcnow(),
            source_start_time=source_info['instance_start_time'],
            target_start_time=target_info['instance_start_time'],

            source_log_rev=source_log.get('_rev'),
            target_log_rev=target_log.get('_rev'),
            history=tuple(history),
        )

        if rep_task.checkpoint_interval:
            checkpoint_interval = rep_task.checkpoint_interval
        else:
            checkpoint_interval = self.default_checkpoint_interval
        checkpoints_loop_task = asyncio.async(self.checkpoints_loop(
            reports_queue, source, target, state,
            use_checkpoints=rep_task.use_checkpoints,
            checkpoint_interval=checkpoint_interval))

        workers = [
            self.worker_class(source, target, changes_queue, reports_queue,
                              batch_size=batch_size,
                              max_conns=rep_task.http_connections)
            for _ in range(num_workers)
        ]

        workers_tasks = [worker.start() for worker in workers]

        return asyncio.async(self.main_loop(
            changes_reader_task,
            checkpoints_loop_task,
            reports_queue,
            workers_tasks))

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
                      rep_state: ReplicationState,
                      seq: TsSeq,
                      source: ISourcePeer,
                      target: ITargetPeer):
        # couch_replicator:do_checkpoint/1

        yield from self.ensure_full_commit(source, rep_state.source_start_time,
                                           target, rep_state.target_start_time)
        return (yield from self.record_checkpoint(source, target,
                                                  seq, rep_state))

    @asyncio.coroutine
    def ensure_full_commit(self,
                           source: ISourcePeer,
                           source_start_time: str,
                           target: ITargetPeer,
                           target_start_time: str):
        """Ask Source and Target peers to ensure that all changes that made
        are flushed on disk or other persistent storage.

        Terminates a Replication if Source or Target changed their start time
        value."""
        # Why we need to ensure_full_commit on source? Only just for start time?
        source_start_time_2 = yield from source.ensure_full_commit()
        target_start_time_2 = yield from target.ensure_full_commit()

        if source_start_time != source_start_time_2:
            raise RuntimeError('source start time was changed')

        if target_start_time != target_start_time_2:
            raise RuntimeError('target start time was changed')

    @asyncio.coroutine
    def record_checkpoint(self,
                          source: ISourcePeer,
                          target: ITargetPeer,
                          seq: TsSeq,
                          rep_state: ReplicationState):
        """Records Checkpoint on the both Peers and returns recorded sequence
        back."""

        source_log = self.new_replication_log(rep_state, seq)
        target_log = self.new_replication_log(rep_state, seq)

        source_rev = yield from source.update_replication_log(
            rep_state.rep_id, source_log, rev=rep_state.source_log_rev)
        target_rev = yield from target.update_replication_log(
            rep_state.rep_id, target_log, rev=rep_state.target_log_rev)

        return rep_state.update(
            committed_seq=seq,
            history=source_log['history'],
            last_checkpoint_made_time=datetime.datetime.utcnow(),
            source_log_rev=source_rev,
            target_log_rev=target_rev
        )

    def new_replication_log(self,
                            rep_state: ReplicationState,
                            seq: TsSeq) -> dict:
        prev_history = rep_state.history[:self.max_history_entries - 1]
        return {
            'history': (self.new_history_entry(rep_state, seq),) + prev_history,
            'replication_id_version': rep_state.protocol_version,
            'session_id': rep_state.session_id,
            'source_last_seq': seq.id
        }

    def new_history_entry(self,
                          rep_state: ReplicationState,
                          seq: TsSeq) -> dict:
        """Returns a new replication history entry suitable to be added to
        replication log."""
        return {
            # required
            'session_id': rep_state.session_id,
            'recorded_seq': seq.id,
            # misc
            'start_time': self.format_time(rep_state.replication_start_time),
            'end_time': self.format_time(datetime.datetime.utcnow()),
            'start_last_seq': rep_state.committed_seq.id,
            'end_last_seq': seq.id,
            # TODO: add stats
        }

    def format_time(self, utcdt: datetime.datetime) -> str:
        """Formats a time into RFC 1123 with GMT tz."""
        weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep',
                  'Oct', 'Nov', 'Dec']
        return '{weekday}, {dt:%d} {month} {dt:%Y %H:%M:%S} GMT'.format(
            weekday=weekdays[utcdt.weekday()], month=months[utcdt.month - 1],
            dt=utcdt)
