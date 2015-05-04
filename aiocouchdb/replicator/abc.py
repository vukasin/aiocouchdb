# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import abc
import asyncio


class ISourcePeer(object, metaclass=abc.ABCMeta):
    """Source peer interface."""

    def __init__(self, peer_info):
        pass

    @abc.abstractmethod
    @asyncio.coroutine
    def exists(self) -> bool:
        """Checks if Database is exists and available for further queries.

        :rtype: bool
        """

    @abc.abstractmethod
    @asyncio.coroutine
    def info(self) -> dict:
        """Returns information about Database. This dict object MUST contains
        the following fields:

        - **instance_start_time** (`str`) - timestamp when the Database was
          opened, expressed in microseconds since the epoch.

        - **update_seq** - current database Sequence ID.

        :rtype: dict
        """

    @abc.abstractmethod
    @asyncio.coroutine
    def get_filter_function_code(self, filter_name: str) -> str:
        """Returns filter function code that would be applied on changes feed.

        :param str filter_name: Filter function name

        :rtype: str
        """
        # We do abstract from knowledge about design documents and the place
        # where filters are defined there.


class ITargetPeer(object, metaclass=abc.ABCMeta):
    """Target peer interface."""

    def __init__(self, peer_info):
        pass

    @abc.abstractmethod
    @asyncio.coroutine
    def exists(self) -> bool:
        """Checks if database is exists and available for further queries.

        :rtype: bool
        """

    @abc.abstractmethod
    @asyncio.coroutine
    def info(self) -> dict:
        """Returns information about Database. This dict object MUST contains
        the following fields:

        - **instance_start_time** (`str`) - timestamp when the Database was
          opened, expressed in microseconds since the epoch.

        - **update_seq** - current database Sequence ID.

        :rtype: dict
        """

    @abc.abstractmethod
    @asyncio.coroutine
    def create(self):
        """Creates target database."""
