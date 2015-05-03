# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import abc


class ISourcePeer(object, metaclass=abc.ABCMeta):
    """Source peer interface."""

    def __init__(self, peer_info):
        pass


class ITargetPeer(object, metaclass=abc.ABCMeta):
    """Target peer interface."""

    def __init__(self, peer_info):
        pass
