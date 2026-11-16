.. index::
   single: Document search
   pair: Configuration; Qdrant

.. highlight:: rst

.. _qdrant:

Document search
===============

EOS can index the text of replica documents into an external `Qdrant
<https://qdrant.tech>`_ service and search that index for a caller. The MGM
is the only search endpoint. It embeds the query, asks Qdrant for similar
chunks, and returns a hit only after the caller's identity is allowed to read
the file. Qdrant does not see the caller, the path, or the ACL.

An empty ``mgmofs.qdrant_endpoint`` leaves the feature off. Indexing runs only
when that endpoint is set and ``space.qdrant.index`` is enabled on the
``default`` space. Search stays available after indexing is switched off, as
long as the Qdrant endpoint and the embedding endpoint are both set.

MGM configuration
-----------------

Add the endpoint to ``/etc/xrd.cf.mgm``. Changing these keys requires an MGM
restart. The API key is not written to the log and is not returned to clients.

.. code-block:: text

   mgmofs.qdrant_endpoint http://qdrant.example:6333
   mgmofs.qdrant_apikey <secret>
   mgmofs.qdrant_collection eos
   mgmofs.embed_endpoint http://embedder.example:8080
   mgmofs.embed_model text-embedding
   mgmofs.qdrant_overfetch 5
   mgmofs.qdrant_maxbytes 32M

.. epigraph::

   ======================== ================================================================
   Key                      Meaning
   ======================== ================================================================
   ``qdrant_endpoint``      Qdrant REST endpoint. Required. A missing scheme becomes
                            ``http://``.
   ``qdrant_apikey``        Optional Qdrant ``api-key``. It is not sent to the embedder.
   ``qdrant_collection``    Collection name. Default ``eos``. Created on the first upsert.
   ``embed_endpoint``       OpenAI-compatible embedding service. Required for search and
                            indexing. A missing scheme becomes ``http://``. If the value
                            does not contain ``embeddings``, ``/v1/embeddings`` is appended.
   ``embed_model``          Model name sent to the embedder. Default ``text-embedding``.
   ``qdrant_overfetch``     Candidates requested per result slot. Default ``5``, at least 1.
   ``qdrant_maxbytes``      Largest file accepted for indexing. Default ``32M``.
   ======================== ================================================================

A new embedding model needs a new collection. Set ``mgmofs.qdrant_collection``
to that name and restart the MGM. The catalog records the file checksum only,
so changing ``mgmofs.embed_model`` without a new collection does not rebuild
points that are already stored.

Indexing
--------

Indexing is a space setting and applies only to the ``default`` space. It
takes effect without a restart:

.. code-block:: bash

   eos space config default space.qdrant.index=1
   eos space config default space.qdrant.index=0

Accepted values are ``1``, ``true``, ``0``, and ``false``. The default is off.
Other spaces and other values are rejected.

A successful write close of an indexable replica queues that file
immediately. The FST indexer re-reads it, extracts the text, and upserts
Qdrant without waiting for the disk scan. The scan still runs, by default
every four hours, and queues any document the close did not. Erasure-coded
files are not indexed. The file name must end in one of: ``pdf``, ``docx``,
``doc``, ``odt``, ``rtf``, ``pptx``, ``ppt``, ``odp``, ``md``, ``markdown``,
``txt``, ``text``, ``tex``, ``latex``, ``ltx``, ``html``, ``htm``, ``rst``.
The comparison is case-insensitive. A file larger than ``qdrant_maxbytes`` is
skipped.

Search
------

The console command takes a quoted query. ``--limit`` defaults to 8 and cannot
exceed 50.

.. code-block:: bash

   eos file search "quarterly report" --limit 8

The same command is available on the gRPC file interface. Authenticated HTTP
clients use the MGM HTTP port:

.. code-block:: bash

   curl -sS -X POST https://mgm.example.org:8443/v1/search \
     -H 'Content-Type: application/json' \
     -d '{"query":"quarterly report","limit":8}'

The response is ``{"results":[{"fileid","path","chunk","score","text"}]}``.
``text`` is the matching chunk and is returned only after the ACL check. A
missing collection is an empty result list. A query that is empty or longer
than 4096 characters is rejected.
