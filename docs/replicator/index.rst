.. _replicator:

=====================
aiocouchdb Replicator
=====================

:status: **alpha**

.. contents::


The Story About an Implementation
=================================

Replication Task
----------------

So, aiocouchdb is going to have it own implementation of the `couch_replicator`_
- a special service that replicates documents between two databases by using
:ref:`CouchDB Replication Protocol <1.x:replication/protocol>`.
Where should we start?

At first, we need to figure out *how* we'll describe a command to start a
replication. The good idea is to reuse CouchDB `replicator document` structure
as the task definition. It may be a bit specifics, but the most of options we
can utilize. This will give us compatibility on the data level and help to
define additional feature set for the implementation.

That's how :class:`~aiocouchdb.replicator.records.ReplicationTask` has been
created. `couch_replicator` doesn't has a direct analog of this object, thought
`#rep record`_ contains the base options from it.

Replicator API
==============

.. toctree::
  api


.. _couch_replicator: https://github.com/apache/couchdb-couch-replicator
.. _#rep record: https://github.com/apache/couchdb-couch-replicator/blob/master/src/couch_replicator.hrl#L15-L24
