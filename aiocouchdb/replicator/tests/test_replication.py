# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

from unittest.mock import Mock, MagicMock

import asyncio

from aiocouchdb.tests import utils
from .. import abc
from .. import records
from .. import replication
from .. import work_queue


class ReplicationTestCase(utils.TestCase):

    def setUp(self):
        super().setUp()

        self.source = MagicMock(name='source', spec=abc.ISourcePeer)
        self.target = MagicMock(name='target', spec=abc.ITargetPeer)
        self.repl = replication.Replication(
            rep_uuid='',
            rep_task=records.ReplicationTask('http://localhost:5984/source',
                                             'http://localhost:5984/target'),
            source_peer_class=Mock(),
            target_peer_class=Mock())

    def test_verify_peers(self):
        yield from self.repl.verify_peers(self.source, self.target, False)
        self.assertTrue(self.source.info.called)
        self.assertTrue(self.target.exists.called)
        self.assertFalse(self.target.create.called)
        self.assertTrue(self.target.info.called)

    def test_verify_peers_create_target(self):
        self.target.exists.return_value = self.future(False)

        yield from self.repl.verify_peers(self.source, self.target, True)
        self.assertTrue(self.source.info.called)
        self.assertTrue(self.target.exists.called)
        self.assertTrue(self.target.create.called)
        self.assertTrue(self.target.info.called)

    def test_generate_replication_id_without_filter_function(self):
        yield from self.repl.generate_replication_id(
            rep_task=self.repl.rep_task,
            source=self.source,
            rep_uuid='',
            protocol_version=3)
        self.assertTrue(self.source.get_filter_function_code.called)

    def test_generate_replication_id_with_filter_function(self):
        rep_task = self.repl.rep_task._replace(filter='test/passed')
        self.source.get_filter_function_code.return_value = self.future('test')

        yield from self.repl.generate_replication_id(
            rep_task=rep_task,
            source=self.source,
            rep_uuid='',
            protocol_version=3)
        self.assertTrue(self.source.get_filter_function_code.called)

    def test_generate_replication_id_bad_version(self):
        with self.assertRaises(RuntimeError):
            yield from self.repl.generate_replication_id(
                rep_task=self.repl.rep_task,
                source=self.source,
                rep_uuid='',
                protocol_version=10)

    def test_find_replication_logs(self):
        rep_id = 'replication-id'
        yield from self.repl.find_replication_logs(
            rep_id, self.source, self.target)

        self.assertTrue(self.source.get_replication_log)
        self.assertEqual(((rep_id,), {}),
                         self.source.get_replication_log.call_args)

        self.assertTrue(self.target.get_replication_log)
        self.assertEqual(((rep_id,), {}),
                         self.target.get_replication_log.call_args)

    def test_compare_replication_logs(self):
        self.assertEqual((self.repl.lowest_seq, []),
                         self.repl.compare_replication_logs({}, {}))

    def test_compare_empty(self):
        self.assertEqual((self.repl.lowest_seq, []),
                         self.repl.compare_replication_logs({}, {}))
        self.assertEqual((self.repl.lowest_seq, []),
                         self.repl.compare_replication_logs(
                             {}, {'history': [], 'session_id': 'test'}))
        self.assertEqual((self.repl.lowest_seq, []),
                         self.repl.compare_replication_logs(
                             {'history': [], 'session_id': 'test'}, {}))

    def test_session_id_match(self):
        source = {'history': [{}], 'session_id': 'test', 'source_last_seq': 42}
        target = {'history': [], 'session_id': 'test', 'source_last_seq': 24}
        self.assertEqual((42, [{}]),
                         self.repl.compare_replication_logs(source, target))

    def test_compare_replication_history_empty(self):
        source = {'history': [], 'session_id': 'foo'}
        target = {'history': [], 'session_id': 'bar'}
        self.assertEqual((self.repl.lowest_seq, []),
                         self.repl.compare_replication_logs(source, target))

    def test_compare_replication_history_no_match(self):
        source = {'history': [{'session_id': 'foo', 'recorded_seq': 42}],
                  'session_id': 'foo'}
        target = {'history': [{'session_id': 'bar', 'recorded_seq': 24}],
                  'session_id': 'bar'}
        self.assertEqual((self.repl.lowest_seq, []),
                         self.repl.compare_replication_logs(source, target))

    def test_compare_replication_history_has_match(self):
        source = {'history': [{'session_id': 'bao', 'recorded_seq': 42},
                              {'session_id': 'foo', 'recorded_seq': 24}],
                  'session_id': 'foo'}
        target = {'history': [{'session_id': 'zao', 'recorded_seq': 84},
                              {'session_id': 'foo', 'recorded_seq': 34}],
                  'session_id': 'bar'}
        self.assertEqual((24, []),
                         self.repl.compare_replication_logs(source, target))

    def test_compare_replication_history_match_on_target(self):
        source = {'history': [{'session_id': 'bao', 'recorded_seq': 42},
                              {'session_id': 'foo', 'recorded_seq': 24}],
                  'session_id': 'foo'}
        target = {'history': [{'session_id': 'foo', 'recorded_seq': 34},
                              {'session_id': 'zao', 'recorded_seq': 14}],
                  'session_id': 'bar'}
        self.assertEqual((34, [{'session_id': 'zao', 'recorded_seq': 14}]),
                         self.repl.compare_replication_logs(source, target))

    def test_changes_reader_loop(self):
        class Changes(object):
            def __init__(self, items: list):
                self.items = list(reversed(items))

            @asyncio.coroutine
            def next(self):
                if not self.items:
                    return None
                return self.items.pop()

        self.source.changes.return_value = self.future(Changes([1, 2, 3, 4, 5]))

        changes_queue = work_queue.WorkQueue()

        changes_reader = asyncio.async(self.repl.changes_reader_loop(
            changes_queue=changes_queue,
            source=self.source,
            rep_task=self.repl.rep_task,
            start_seq=21))

        yield from asyncio.sleep(0.1)  # context switch

        self.assertTrue(self.source.changes.called)

        items = yield from changes_queue.get(3)
        self.assertEqual([1, 2, 3], items)

        items = yield from changes_queue.get(3)
        self.assertEqual([4, 5], items)

        items = yield from changes_queue.get(20)
        self.assertEqual(changes_queue.CLOSED, items)

    def test_checkpoints_loop_no_reports(self):
        reports_queue = work_queue.WorkQueue()
        self.repl.do_checkpoint = MagicMock()

        checkpoints_loop = asyncio.async(self.new_checkpoints_loop(
            reports_queue, checkpoint_interval=0.1))

        yield from asyncio.sleep(0.5)

        self.assertFalse(self.repl.do_checkpoint.called)

    def test_checkpoints_loop_no_checkpoints(self):
        reports_queue = work_queue.WorkQueue()
        self.repl.do_checkpoint = MagicMock()

        checkpoints_loop = asyncio.async(self.new_checkpoints_loop(
            reports_queue, checkpoint_interval=0.1, use_checkpoints=False))

        yield from reports_queue.put((True, 1))
        yield from asyncio.sleep(0.5)

        self.assertFalse(self.repl.do_checkpoint.called)

    def test_checkpoints_loop_wait_for_lowerest_seq_done(self):
        reports_queue = work_queue.WorkQueue()
        self.repl.do_checkpoint = MagicMock()

        checkpoints_loop = asyncio.async(self.new_checkpoints_loop(
            reports_queue, checkpoint_interval=0.1))

        yield from reports_queue.put((False, 1))
        yield from reports_queue.put((False, 2))
        yield from reports_queue.put((True, 2))

        yield from asyncio.sleep(0.5)

        self.assertFalse(self.repl.do_checkpoint.called)

        if checkpoints_loop.done():
            raise checkpoints_loop.exception()

        yield from reports_queue.put((True, 1))
        yield from asyncio.sleep(1)

        self.assertTrue(self.repl.do_checkpoint.called)

        self.assertEqual(2, self.repl.do_checkpoint.call_args[0][2])

    def test_checkpoints_loop_do_the_last_checkpoint_on_close_if_possible(self):
        reports_queue = work_queue.WorkQueue()
        self.repl.do_checkpoint = MagicMock()

        checkpoints_loop = asyncio.async(self.new_checkpoints_loop(
            reports_queue, checkpoint_interval=10))

        yield from reports_queue.put((True, 2))

        reports_queue.close()

        yield from asyncio.sleep(0.01)

        self.assertEqual(2, self.repl.do_checkpoint.call_args[0][2])

    def test_ensure_full_commit(self):
        self.source.ensure_full_commit.return_value = self.future(42)
        self.target.ensure_full_commit.return_value = self.future(42)

        yield from self.repl.ensure_full_commit(
            self.source, {'instance_start_time': 42},
            self.target, {'instance_start_time': 42})

        self.assertTrue(self.source.ensure_full_commit.called)
        self.assertTrue(self.target.ensure_full_commit.called)

    def test_ensure_full_commit_source_start_time_out_of_sync(self):
        self.source.ensure_full_commit.return_value = self.future(24)
        self.target.ensure_full_commit.return_value = self.future(42)

        with self.assertRaises(RuntimeError):
            yield from self.repl.ensure_full_commit(
                self.source, {'instance_start_time': 42},
                self.target, {'instance_start_time': 42})

        self.assertTrue(self.source.ensure_full_commit.called)
        self.assertTrue(self.target.ensure_full_commit.called)

    def test_ensure_full_commit_target_start_time_out_of_sync(self):
        self.source.ensure_full_commit.return_value = self.future(42)
        self.target.ensure_full_commit.return_value = self.future(24)

        with self.assertRaises(RuntimeError):
            yield from self.repl.ensure_full_commit(
                self.source, {'instance_start_time': 42},
                self.target, {'instance_start_time': 42})

        self.assertTrue(self.source.ensure_full_commit.called)
        self.assertTrue(self.target.ensure_full_commit.called)

    def test_record_checkpoint(self):
        self.source.update_replication_log.return_value = self.future('1-abc')
        self.target.update_replication_log.return_value = self.future('1-cde')
        history = []
        source_log = {}
        target_log = {}

        committed_seq = yield from self.repl.record_checkpoint(
            'rep_id', 'ssid', 42, self.source, source_log,
            self.target, target_log, history)

        self.assertEqual(1, len(history))
        self.assertEqual('ssid', history[0]['session_id'])
        self.assertEqual(42, history[0]['recorded_seq'])
        self.assertEqual(42, committed_seq)

        self.assertEqual(history, source_log['history'])
        self.assertEqual(history, target_log['history'])

        self.assertTrue(self.source.update_replication_log.called)
        self.assertTrue(self.target.update_replication_log.called)

        self.assertEqual('1-abc', source_log['_rev'])
        self.assertEqual('1-cde', target_log['_rev'])

    def new_checkpoints_loop(self,
                             reports_queue, *,
                             checkpoint_interval=5,
                             use_checkpoints=True):
        return self.repl.checkpoints_loop(
            reports_queue=reports_queue,
            checkpoint_interval=checkpoint_interval,
            use_checkpoints=use_checkpoints,
            # these are irrelevant
            rep_id=None,
            session_id=None,
            source=None,
            target=None,
            source_info=None,
            target_info=None,
            source_log=None,
            target_log=None,
            history=None)
