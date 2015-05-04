# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import base64
import hashlib

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

from . import erlterm


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


def replication_id_v3(uuid: str, source, target, *,
                      continuous: bool=None,
                      create_target: bool=None,
                      doc_ids: list=None,
                      filter: str=None,
                      query_params: list=None) -> str:
    """Generates replication id for protocol version 3."""
    repid = [uuid.encode('utf-8'),
             get_peer_endpoint(source),
             get_peer_endpoint(target)]
    maybe_append_filter_info(repid,
                             doc_ids=doc_ids,
                             filter=filter,
                             query_params=query_params)
    repid = hashlib.md5(erlterm.encode(repid)).hexdigest()
    return maybe_append_options(repid, [('continuous', continuous),
                                        ('create_target', create_target)])


def get_peer_endpoint(peer) -> tuple:
    url = maybe_append_trailing_slash(peer.url)
    headers = sorted(peer.headers.items())
    if url.startswith('http'):
        return (erlterm.Atom(b'remote'), url, headers)
    else:
        raise RuntimeError('local peers are not supported')


def maybe_append_trailing_slash(url: str) -> str:
    if not url.startswith('http'):
        return url
    if url.endswith('/'):
        return url
    return url + '/'


def maybe_append_filter_info(repid: list, *,
                             doc_ids: list=None,
                             filter: str=None,
                             query_params: list=None):
    if filter is None:
        if doc_ids:
            repid.append([idx.encode('utf-8') for idx in doc_ids])
    else:
        if isinstance(query_params, dict):
            query_params = sorted((key.encode('utf-8'), value.encode('utf-8'))
                                  for key, value in query_params.items())
        elif query_params:
            query_params = [(key.encode('utf-8'), value.encode('utf-8'))
                            for key, value in query_params]
        else:
            query_params = []
        repid.extend([filter.strip().encode('utf-8'), (query_params,)])


def maybe_append_options(repid: str, options: list) -> str:
    for key, value in options:
        if value:
            repid += '+' + key
    return repid
