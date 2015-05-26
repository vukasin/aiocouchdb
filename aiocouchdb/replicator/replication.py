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
from .records import ReplicationTask
from . import utils


__all__ = (
    'Replication',
)


class Replication(object):
    """Replication job maker."""

    lowest_seq = 0

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
        # start_seq will be needed for the further step
        start_seq = rep_task.since_seq or found_seq

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
