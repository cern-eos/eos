.. index::
   single: MCP
   pair: HTTP; MCP
   pair: gRPC; MCP

.. highlight:: rst

.. _mcp:

Read-only admin MCP
===================

The MGM serves a read-only `Model Context Protocol <https://modelcontextprotocol.io>`_
(MCP) endpoint for administration. An AI assistant or any other MCP client can
use it to inspect the instance status, run read-only admin commands such as
``ns stat``, ``space ls``, ``node ls`` or ``fs status``, and read log files. No
tool changes configuration, writes data, or reads an arbitrary path.

The endpoint is available on the MGM HTTPS port as ``POST /mcp`` and on the
Eos gRPC service as ``Eos.Mcp``. Both accept the same JSON-RPC documents.

The caller must be an MGM admin or an *MCP reader*. An MCP reader is an
ordinary, non-admin user that may use only this endpoint. Its credential gives
no admin rights on the console or on any other interface. Inside the endpoint
an allowlisted read-only command runs with the reader's identity plus the
admin group (``adm``), and the MGM log records each such call.

The steps below set up a reader called ``mcpbot`` with an EOS token.

Step 1: Check the prerequisites
-------------------------------

* The MGM HTTPS port is enabled (XrdHttp, by default ``8443``) for ``POST /mcp``,
  or the gRPC server is enabled (``EOS_MGM_GRPC_PORT``, by default ``50051``)
  for ``Eos.Mcp``.
* Token issuing is enabled, which means ``token.generation`` is not 0:

  .. code-block:: bash

     eos space status default | grep token.generation
     # if it is 0 or missing:
     eos space config default space.token.generation=1

* ``vid tokensudo`` lets a token set its owner. ``never`` blocks the
  setup. ``always`` (the default), ``encrypted`` and ``strong`` all work for
  HTTPS and gRPC:

  .. code-block:: bash

     eos vid ls -s | grep tokensudo

Step 2: Create the local user on the MGM nodes
----------------------------------------------

The MGM resolves ``mcpbot`` with the local password database. Create the user
on every MGM node, including standby MGMs, with the **same uid** everywhere,
because the membership is stored by numeric uid in the shared configuration.
The account does not need a home directory or a login shell:

.. code-block:: bash

   # on every MGM node, as root
   groupadd --system --gid 950 mcpbot
   useradd --system --uid 950 --gid mcpbot --no-create-home \
     --home-dir / --shell /sbin/nologin --comment "EOS MCP reader" mcpbot
   id mcpbot

Pick a uid and gid that are free on all MGM nodes. Do not add the user to
``adm`` or any other admin group, because that would make it a full admin.

Step 3: Mark the user as MCP reader
-----------------------------------

.. code-block:: bash

   eos vid set membership mcpbot +mcp
   eos vid ls -s

The output contains a line like ``mcp => uids(mcpbot)``. The membership is
stored in the MGM configuration and survives restarts.

Step 4: Issue a token for the MCP client
----------------------------------------

Issue an EOS token for the exact path ``/mcp`` with ``mcpbot`` as owner and
group. The token cannot be used to access files, because its path is not a
namespace path. Keep it short-lived and store it with restricted permissions:

.. code-block:: bash

   eos token --path /mcp --permission rx --owner mcpbot --group mcpbot \
     --expires $(date -d '+30 days' +%s) > /root/mcpbot.token
   chmod 600 /root/mcpbot.token

To inspect a token:

.. code-block:: bash

   eos token --token $(cat /root/mcpbot.token)

Step 5: Test the endpoint
-------------------------

The token is sent as ``Authorization: Bearer <token>``. The request needs
``Content-Type: application/json``.

.. code-block:: bash

   TOKEN=$(cat /root/mcpbot.token)
   MCP=https://mgm.example.org:8443/mcp

   # handshake
   curl -sS -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -H "Accept: application/json, text/event-stream" \
     $MCP -d '{"jsonrpc":"2.0","id":1,"method":"initialize",
               "params":{"protocolVersion":"2025-06-18"}}'

   # list the tools
   curl -sS -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     $MCP -d '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'

   # run 'eos ns stat --summary'
   curl -sS -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     $MCP -d '{"jsonrpc":"2.0","id":3,"method":"tools/call",
               "params":{"name":"eos_ns_stat",
                         "arguments":{"params":{"Summary":true}}}}'

   # run 'eos space ls -m'
   curl -sS -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     $MCP -d '{"jsonrpc":"2.0","id":4,"method":"tools/call",
               "params":{"name":"eos_space_ls",
                         "arguments":{"params":{"outformat":"MONITORING"}}}}'

A tool result arrives in ``result.content[0].text``. The MGM log shows each
command run by the reader:

.. code-block:: bash

   grep 'mcp reader runs admin command' /var/log/eos/mgm/xrdlog.mgm

Step 6: Configure the MCP client
--------------------------------

An MCP client that supports the Streamable HTTP transport needs only the URL
and the header. For example:

.. code-block:: json

   {
     "mcpServers": {
       "eos": {
         "url": "https://mgm.example.org:8443/mcp",
         "headers": { "Authorization": "Bearer <token>" }
       }
     }
   }

The server does not open a session stream. ``GET`` and ``DELETE`` are answered
with HTTP 405, and notifications with HTTP 202 and an empty body.

See :ref:`mcp-clients` for client-specific configuration.

Step 7 (optional): Use gRPC
---------------------------

``Eos.Mcp`` takes the credential in ``McpRequest.authkey`` and the JSON-RPC
document as a string in ``McpRequest.body``. The same token works:

.. code-block:: bash

   grpcurl -plaintext \
     -import-path /path/to/eos/common/grpc-proto -proto Rpc.proto \
     -d "{\"authkey\":\"$TOKEN\",
          \"body\":\"{\\\"jsonrpc\\\":\\\"2.0\\\",\\\"id\\\":1,\\\"method\\\":\\\"tools/list\\\"}\"}" \
     mgm.example.org:50051 eos.rpc.Eos/Mcp

Drop ``-plaintext`` when the gRPC server runs with TLS
(``EOS_MGM_GRPC_SSL_CERT``, ``EOS_MGM_GRPC_SSL_KEY``, ``EOS_MGM_GRPC_SSL_CA``).

A plain gRPC key (``eos vid set map -grpc key:<secret> vuid:mcpbot vgid:mcpbot``)
is only honoured when the client host is registered with
``eos vid add gateway <host> grpc``. A registered gateway that sends no key
is mapped to ``root``. Prefer the token, which needs no gateway entry.

.. _mcp-clients:

MCP clients
-----------

Any MCP client that supports the Streamable HTTP transport and lets you set a
custom ``Authorization`` header can use the endpoint. That covers most current
IDE and CLI clients. Clients that only speak stdio can connect through a small
open-source bridge.

The endpoint implements the basic form of the transport: plain JSON-RPC over
``POST`` with JSON responses, no streaming and no session id. ``GET`` returns
405, which the specification allows for servers that do not stream. The
protocol versions ``2024-11-05``, ``2025-03-26`` and ``2025-06-18`` are
accepted, and notifications are answered with 202. There is no OAuth: a
missing or wrong token gets 403, so clients do not start an OAuth login flow.

Clients that work directly
^^^^^^^^^^^^^^^^^^^^^^^^^^

These clients take the URL and a header in their configuration:

* **Cursor**: the ``mcpServers`` JSON with ``url`` and ``headers`` from
  Step 6 works as is.
* **Claude Code**:

  .. code-block:: bash

     claude mcp add --transport http eos https://mgm.example.org:8443/mcp \
       --header "Authorization: Bearer $TOKEN"

* **VS Code with Copilot**: ``.vscode/mcp.json`` with ``"type": "http"``,
  ``url`` and ``headers``.
* **Open-source clients**:

  * `Gemini CLI <https://github.com/google-gemini/gemini-cli>`_ uses
    ``httpUrl`` and ``headers`` in ``settings.json``.
  * `OpenAI Codex CLI <https://github.com/openai/codex>`_ has a Streamable
    HTTP server entry with a bearer-token setting.
  * `Goose <https://github.com/block/goose>`_,
    `Cline <https://github.com/cline/cline>`_ and
    `Continue <https://github.com/continuedev/continue>`_ all support remote
    HTTP servers with headers. The exact configuration keys depend on the
    version.

Clients that need a bridge
^^^^^^^^^^^^^^^^^^^^^^^^^^

Claude Desktop and other stdio-only clients need a bridge. The built-in remote
connectors of Claude Desktop expect OAuth, so a fixed Bearer token does not fit
there. The open-source `mcp-remote <https://github.com/geelen/mcp-remote>`_
bridge runs locally and forwards to the endpoint:

.. code-block:: json

   {
     "mcpServers": {
       "eos": {
         "command": "npx",
         "args": ["mcp-remote", "https://mgm.example.org:8443/mcp",
                  "--header", "Authorization:${EOS_MCP_AUTH}"],
         "env": { "EOS_MCP_AUTH": "Bearer <token>" }
       }
     }
   }

Testing and scripting
^^^^^^^^^^^^^^^^^^^^^

`MCP Inspector <https://github.com/modelcontextprotocol/inspector>`_
(``npx @modelcontextprotocol/inspector``) is the official open-source debugging
UI. Choose *Streamable HTTP*, enter the URL and set the Bearer token. You can
then browse the tools and resources and call them by hand. It is the best first
check after the ``curl`` tests of Step 5.

The official `Python SDK <https://github.com/modelcontextprotocol/python-sdk>`_
and `TypeScript SDK <https://github.com/modelcontextprotocol/typescript-sdk>`_
both have Streamable HTTP clients for monitoring scripts or bots. A minimal
Python example:

.. code-block:: python

   import asyncio, os
   from mcp import ClientSession
   from mcp.client.streamable_http import streamablehttp_client

   async def main():
       headers = {"Authorization": f"Bearer {os.environ['EOS_MCP_TOKEN']}"}
       async with streamablehttp_client("https://mgm.example.org:8443/mcp",
                                        headers=headers) as (r, w, _):
           async with ClientSession(r, w) as s:
               await s.initialize()
               res = await s.call_tool("eos_ns_stat", {"params": {"Summary": True}})
               print(res.content[0].text)

   asyncio.run(main())

Client pitfalls
^^^^^^^^^^^^^^^

* **TLS certificate of the MGM**: if it is signed by a CA the client does not
  trust (such as the CERN Grid CA), Node-based clients (MCP Inspector,
  mcp-remote, Gemini CLI) need ``NODE_EXTRA_CA_CERTS=/path/to/ca.pem``. Python
  clients need ``SSL_CERT_FILE``.
* **Old HTTP+SSE-only clients**: clients from before March 2025 that only speak
  the older two-endpoint SSE transport do not work, even though the
  ``2024-11-05`` version string is accepted.
* **gRPC**: ``Eos.Mcp`` is EOS-specific, so no standard MCP client can use it.
  It is meant for your own gRPC tools or ``grpcurl``.

The endpoint has not yet been tested against these clients. A good order is
MCP Inspector first, then Cursor or Claude Code.

Revoking access
---------------

.. code-block:: bash

   # stop treating mcpbot as MCP reader, effective immediately
   eos vid set membership mcpbot -mcp

   # remove the local user on every MGM node
   userdel mcpbot

Without the membership the token maps to an ordinary user and the endpoint
answers HTTP 403. A token also stops working when it expires. Increasing
``space.token.generation`` revokes **all** tokens of the instance.

Troubleshooting
---------------

``403`` with ``admin or mcp reader identity required`` (gRPC: ``PERMISSION_DENIED``)
   The credential did not map to ``mcpbot``. Check that the token has not
   expired, that its path is exactly ``/mcp``, that ``vid tokensudo`` is not
   ``never``, that ``mcpbot`` exists on the MGM that answered, with the same
   uid as on the others, and that ``eos vid ls -s`` lists it as reader.

``415`` or ``406``
   ``Content-Type`` must be ``application/json``. If ``Accept`` is sent, it must
   include ``application/json`` or ``*/*``.

``413``
   The request body is larger than 64 KiB.

JSON-RPC error ``-32602`` such as ``parameter is not allowed: Reset``
   The arguments contain a field that is not in the tool's input schema.
   Run ``tools/list`` to see the accepted fields.

Tool result with ``retc=...`` from the command
   The command itself failed, for example because a space or filesystem does
   not exist. The text is the command's own error output.

Reference
---------

Tools
^^^^^

``eos_status``
   Node, space, group and filesystem status from the filesystem view. Argument
   ``scope``: ``all``, ``nodes``, ``spaces``, ``groups`` or ``filesystems``.

``eos_log_list``, ``eos_log_read``
   List and read the allowlisted logs. ``eos_log_read`` takes ``name``,
   ``lines`` (1 to 2000, default 200) and, for ``memory``, ``level``. The names
   are ``memory`` (the MGM in-memory log), ``mgm``, ``fst``, ``mq``, ``logbook``,
   ``eosxd-stacktraces`` and ``eosxd-logtraces``. Only the last 1 MiB of a file
   is read.

``eos_<command>``
   Read-only admin commands. They run the same implementation as the console:

   * ``ns``: ``eos_ns_stat`` (without reset), ``eos_ns_master_log``,
     ``eos_ns_drain_list``, ``eos_ns_tracker_list``, ``eos_ns_behaviour_list``,
     ``eos_ns_behaviour_get``
   * ``fs``: ``eos_fs_ls``, ``eos_fs_status``, ``eos_fs_dumpmd``,
     ``eos_fs_compare``
   * ``node``, ``group``, ``df``: ``eos_node_ls``, ``eos_node_status``,
     ``eos_group_ls``, ``eos_df``
   * ``space``: ``eos_space_ls``, ``eos_space_status``, ``eos_space_node_get``,
     ``eos_space_tracker``, ``eos_space_inspector`` (monitoring format only),
     ``eos_space_groupbalancer_status``, ``eos_space_groupdrainer_status``
   * ``io``: ``eos_io_stat``, ``eos_io_ns``, ``eos_io_shaping_list``,
     ``eos_io_shaping_policy_list``, ``eos_io_shaping_config_list``,
     ``eos_io_shaping_pressure_list``
   * ``fsck``: ``eos_fsck_stat``, ``eos_fsck_report``
   * others: ``eos_devices_ls`` (without refresh), ``eos_monit_config_ls``,
     ``eos_config_ls``, ``eos_config_dump`` (secrets masked), ``eos_access_ls``,
     ``eos_debug_get``, ``eos_convert_list``, ``eos_convert_config``,
     ``eos_sched_ls``, ``eos_sched_show``, ``eos_sched_disabled_ls``,
     ``eos_route_list``, ``eos_recycle_ls``

Command arguments
^^^^^^^^^^^^^^^^^

Command tools take ``{"params": {...}, "json": true|false}``. ``params`` uses
the field names of the command message in ``eos-protobuf-spec``, for example
``{"Summary": true}`` for ``ns stat`` or ``{"outformat": "MONITORING"}`` for
``space ls``. ``json`` requests JSON output, like ``eos --json``.

Only the fields listed in a tool's input schema are accepted. Fields that
select a write operation are rejected, for example ``Reset`` for ``ns stat`` or
``op`` for ``ns tracker``. Unknown fields are rejected too. Output larger than
4 MiB is truncated.

Not exposed
^^^^^^^^^^^

``config changelog`` (shows unmasked values), ``quota ls`` (changes in-memory
state), ``io report`` (reads a caller-given path), every subcommand that
changes state, and the commands that are not protobuf commands, such as
``vid ls``, ``who`` and ``version``.

Resources
^^^^^^^^^

``eos://status`` returns the same data as ``eos_status``, and
``eos://logs/<name>`` returns the tail of one allowlisted log.
