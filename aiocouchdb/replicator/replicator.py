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


__all__ = (
    'Replicator',
)


class Replicator(object):
    """Replications manager."""

    def __init__(self,
                 source_peer_class: ISourcePeer,
                 target_peer_class: ITargetPeer):
        self.source_peer_class = source_peer_class
        self.target_peer_class = target_peer_class

    def __getitem__(self, repid):
        raise NotImplementedError

    @asyncio.coroutine
    def start_replication(self, task: ReplicationTask, *,
                          source_peer_class: ISourcePeer=None,
                          target_peer_class: ITargetPeer=None):
        """Starts a new Replication process."""
        raise NotImplementedError

    @asyncio.coroutine
    def cancel_replication(self, repid):
        raise NotImplementedError
