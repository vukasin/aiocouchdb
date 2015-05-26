# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

from collections import namedtuple, Sequence

from aiohttp.multidict import CIMultiDict

from aiocouchdb.authn import NoAuthProvider

from . import utils


__all__ = (
    'TsSeq',
    'PeerInfo',
    'ReplicationTask',
    'ReplicationState',
)


class TsSeq(namedtuple('TsSeq', ['ts', 'id'])):
    """Timestamped (actually not) Sequence ID."""
    __slots__ = ()


class PeerInfo(namedtuple('Peer', ['url', 'headers', 'auth'])):
    """Represents replication peer with it URL, headers to apply on request and
    authentication provider to use."""

    __slots__ = ()

    def __new__(cls, info: (str, dict)):
        """Constructs a peer instance from URL string or dict object.

        :raises: :exc:`RuntimeError` in case of invalid configuration
        """

        if isinstance(info, str):
            url, auth = utils.maybe_extract_credentials(info)
            headers = CIMultiDict()
            auth = auth or NoAuthProvider()

        elif isinstance(info, dict):
            url, auth = utils.maybe_extract_credentials(info['url'])

            headers = CIMultiDict(info.get('headers', {}))

            basic_auth = utils.maybe_extract_basic_auth(headers)
            proxy_auth = utils.maybe_extract_proxy_auth(headers)
            oauth_auth = utils.maybe_extract_oauth_auth(info)

            auths = list(filter(None,
                                [auth, basic_auth, proxy_auth, oauth_auth]))
            if len(auths) > 1:
                raise RuntimeError('Authentication conflict: {}'.format(auths))

            auth = auths[0] if auths else NoAuthProvider()

        else:
            raise TypeError('Invalid type of info: {} ; expected str or dict'
                            ''.format(type(info)))

        return super().__new__(cls, url, headers, auth)


class ReplicationTask(namedtuple('ReplicationTask', [
    'source',
    'target',
    'repid',
    'cancel',
    'continuous',
    'create_target',
    'doc_ids',
    'filter',
    'proxy',
    'query_params',
    'since_seq',
    'user_ctx',
    'view',
    'checkpoint_interval',
    'connection_timeout',
    'http_connections',
    'retries_per_request',
    'socket_options',
    'use_checkpoints',
    'worker_batch_size',
    'worker_processes'
])):
    """ReplicationTask is a special object that mimics CouchDB Replication
    document in the way to describe replication process task."""

    __slots__ = ()

    def __new__(cls, source, target, *,
                repid: str=None,
                cancel: bool=None,
                continuous: bool=None,
                create_target: bool=None,
                doc_ids: Sequence=None,
                filter: str=None,
                proxy: str=None,
                query_params: dict=None,
                since_seq: (int, str)=None,
                user_ctx: dict=None,
                view: str=None,
                checkpoint_interval: int=None,
                connection_timeout: int=None,
                http_connections: int=None,
                retries_per_request: int=None,
                socket_options: str=None,
                use_checkpoints: bool=None,
                worker_batch_size: int=None,
                worker_processes: int=None):
        """Creates a new replication task object based on provided parameters.

        :param source: Source database URL str or dict object
        :param target: Target database URL str or dict object

        :param str repid: Replication ID
        :param bool cancel: Whenever need to cancel active replication
        :param bool continuous: Runs continuous replication
        :param bool create_target: Creates target database if it not exists
        :param list doc_ids: List of specific document ids to replicate,
                             requires `filter` argument to be set as
                             ``_doc_ids``
        :param str filter: Filter function name
        :param str proxy: Proxy server URL
        :param dict query_params: Custom query parameters for filter function
        :param since_seq: Start replication from specified sequence number
        :param dict user_ctx:
        :param str view: View function as filter, requires `filter` argument
                         to be set as ``_view``

        :param int checkpoint_interval: Minimal time in seconds before the
            next checkpoint could be made
        :param int connection_timeout: Peer request connection timeout
            in seconds
        :param int http_connections: Amount of connections to run against
            the Source peer
        :param int retries_per_request: Amount of retries for non-fatal errors
            before give up and crash
        :param socket_options: Custom socket options (not used)
        :param bool use_checkpoints: Allows to record checkpoints if ``True``
        :param int worker_batch_size: Maximum amount of changes feed events
            to process by worker with the single iteration
        :param int worker_processes: Amount of workers to spawn

        :returns: Returns a `Replicator Task` namedtuple instance.
        :rtype: ReplicationTask
        """

        source = PeerInfo(source)
        target = PeerInfo(target)

        params = locals()

        for param, value in params.items():
            if value is None:
                continue

            if param not in ReplicationTask.__new__.__annotations__:
                continue

            typespec = ReplicationTask.__new__.__annotations__[param]
            if isinstance(value, typespec):
                continue

            raise TypeError('{} expected to be {}, got {}'.format(
                param, typespec, type(value)))

        if doc_ids:
            if filter is None:
                params['filter'] = '_doc_ids'
            elif filter != '_doc_ids':
                raise ValueError('If doc_ids argument specified the filter one'
                                 ' should be set as "_doc_ids"')

        if view:
            if filter is None:
                params['filter'] = '_view'
            elif filter != '_view':
                raise ValueError('If view argument specified the filter one'
                                 ' should be set as "_view"')

        if filter and not filter.startswith('_') and '/' not in filter:
            raise ValueError('Invalid filter.'
                             ' Must match `ddocname/filtername` or `_.*`'
                             ' pattern')

        return super().__new__(cls, *(params[key] for key in cls._fields))

    def __repr__(self):
        return '<{}.{}({})>'.format(
            self.__module__,
            self.__class__.__qualname__,
            ', '.join('='.join((key, str(value)))
                      for key, value in zip(self._fields, self)
                      if value is not None))


class ReplicationState(namedtuple('ReplicationState', [
    'rep_task',

    'rep_id',
    'rep_uuid',
    'protocol_version',
    'session_id',

    'source_seq',
    'start_seq',
    'committed_seq',
    'current_through_seq',
    'highest_seq_done',
    'seqs_in_progress',

    'replication_start_time',
    'source_start_time',
    'target_start_time',
    'last_checkpoint_made_time',

    'source_log_rev',
    'target_log_rev',
    'history',
])):
    """Replication state pretty good describes itself by it name - it's a state
    that Replication process carries on during their lifetime.

    The closes analogy in `couch_replicator` implementation is a `#rep_state{}`_
    record. However, it has no intention to hold exact the same data.

    Replication state is immutable (almost) object in order to make it easy
    for sharing while replication is going on without worry about side effects
    from state update.

    .. _#rep_state{}: https://github.com/apache/couchdb-couch-replicator/blob/master/src/couch_replicator.erl#L47-L81
    """

    __slots__ = ()

    def __new__(cls, rep_task: ReplicationTask, *,
                rep_id: str=None,
                rep_uuid: str=None,
                protocol_version: int=None,
                session_id: str=None,

                source_seq: TsSeq=None,
                start_seq: TsSeq=None,
                committed_seq: TsSeq=None,
                current_through_seq: TsSeq=None,
                highest_seq_done: TsSeq=None,
                seqs_in_progress: frozenset=None,

                replication_start_time: float=None,
                source_start_time: str=None,
                target_start_time: str=None,
                last_checkpoint_made_time: float=None,

                source_log_rev: str=None,
                target_log_rev: str=None,
                history: tuple=None):
        """Creates a new replication state namedtuple object based on provided
        :class:`~aiocouchdb.replicator.records.ReplicationTask` and the other
        parameters.

        :param aiocouchdb.records.records.ReplicationTask rep_task:
            Replication Task
        :param str rep_id: Replication ID
        :param str rep_uuid: Replicator UUID
        :param int protocol_version: Replication protocol version

        :param source_seq: Source Sequence ID at the moment of Replication start
        :param start_seq: Replication start Sequence ID
        :param committed_seq: Source Sequence ID which is committed to Target
        :param current_through_seq: Currently processed Sequence ID
        :param highest_seq_done: Highest Sequence ID that is done by workers
        :param tuple seqs_in_progress: Sequence IDs that are in progress

        :param float replication_start_time: Replication start timestamp
        :param str source_start_time: Source peer start time
        :param str target_start_time: Target peer start time
        :param float last_checkpoint_made_time: Last checkpoint made timestamp

        :param str source_log_rev: Replication log revision on Source side
        :param str target_log_rev: Replication log revision on Target side
        :param tuple history: Replication history

        :rtype: ReplicationState
        """
        params = locals()
        return super().__new__(cls, *(params[key] for key in cls._fields))

    def update(self, **kwargs):
        """Returns a new instance of state with the requested changes."""
        return self._replace(**kwargs)
