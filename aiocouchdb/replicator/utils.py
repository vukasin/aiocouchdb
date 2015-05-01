# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import base64

from aiohttp.multidict import CIMultiDict

from aiocouchdb.authn import (
    BasicAuthProvider,
    ProxyAuthProvider,
    OAuthProvider
)
from aiocouchdb.hdrs import (
    AUTHORIZATION,
    X_AUTH_COUCHDB_USERNAME,
    X_AUTH_COUCHDB_ROLES,
    X_AUTH_COUCHDB_TOKEN
)
from aiocouchdb.client import extract_credentials


def maybe_extract_credentials(url: str):
    url, credentials = extract_credentials(url)
    auth = BasicAuthProvider(*credentials) if credentials else None
    return url, auth


def maybe_extract_basic_auth(headers: CIMultiDict):
    if AUTHORIZATION not in headers:
        return

    if not headers[AUTHORIZATION].startswith('Basic '):
        return

    authhdr = headers.pop(AUTHORIZATION)

    token = authhdr.split('Basic ', 1)[-1].encode()
    credentials = base64.b64decode(token).decode().split(':')
    return BasicAuthProvider(*credentials)


def maybe_extract_proxy_auth(headers: CIMultiDict):
    if X_AUTH_COUCHDB_USERNAME not in headers:
        return

    user = headers.pop(X_AUTH_COUCHDB_USERNAME)
    roles = headers.pop(X_AUTH_COUCHDB_ROLES, None)
    token = headers.pop(X_AUTH_COUCHDB_TOKEN, None)
    return ProxyAuthProvider(user, roles, token)


def maybe_extract_oauth_auth(peer: dict):
    if 'auth' not in peer:
        return

    if 'oauth' not in peer['auth']:
        return

    return OAuthProvider(
        consumer_key=peer['auth']['oauth']['consumer_key'],
        consumer_secret=peer['auth']['oauth']['consumer_secret'],
        resource_key=peer['auth']['oauth']['token'],
        resource_secret=peer['auth']['oauth']['token_secret'])
