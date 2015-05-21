# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import uuid

from .abc import ISourcePeer, ITargetPeer
from .records import ReplicationTask
from .replication import Replication


__all__ = (
    'Replicator',
)


REPLICATOR_UUID = uuid.uuid4().hex


class Replicator(object):
    """Replications manager."""

    replication_job_class = Replication

    #: Replication protocol version
    protocol_version = 3

    def __init__(self,
                 source_peer_class: ISourcePeer,
                 target_peer_class: ITargetPeer, *,
                 replication_job_class=None,
                 rep_uuid: str=None):
        self.source_peer_class = source_peer_class
        self.target_peer_class = target_peer_class
        if replication_job_class is not None:
            self.replication_job_class = replication_job_class
        self.rep_uuid = rep_uuid or REPLICATOR_UUID
        self.registry = {}

    def __getitem__(self, rep_id: str):
        return self.registry[rep_id]

    @asyncio.coroutine
    def start_replication(self, rep_task: ReplicationTask, *,
                          source_peer_class=None,
                          target_peer_class=None) -> str:
        """Starts a new Replication process."""

        replication = self.replication_job_class(
            rep_task,
            source_peer_class or self.source_peer_class,
            target_peer_class or self.target_peer_class,
            rep_uuid=self.rep_uuid,
            protocol_version=self.protocol_version)

        task = yield from replication.start()
        self.registry[replication.state.rep_id] = task
        return replication.state.rep_id

    @asyncio.coroutine
    def cancel_replication(self, rep_id: str):
        self.registry[rep_id].cancel()
