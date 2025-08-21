.. highlight:: rst

.. index::
   single: Namespace routing System

Routing System
==============

The EOS route system provides a method to redirect paths within an existing namespace to an external namespace.
It can be thought of as symbolic links that allow clients to connect to another EOS instance.

This can be used to create a parent MGM that contains references to other EOS instances in a tree like structure,
or to connect EOS namespaces together in a mesh like manner.

`vid` policy and other access control still applies as if a user were connecting directly.

A route is defined as a path, that maps to a remote hostname and port, that is the MGM of a remote EOS namespace.
Access to this path is via a redirect at the HTTP or xrootd level, and will incur some latency.

When the latency penalty of the redirect is not desired, it's better to cache the redirect or use an autofs(8)
or similar automounting solution for the paths.

The link always links to the root of the connected namespace.

As an example we can define three routes:

.. epigraph::
    
   ====================================== =======================
    Path                                   Destination
   ====================================== =======================
   /eos/test-namespace-1                   test-mgm-1:1094:8000
   /eos/test-namespace-2                   test-mgm-2:1094:8000
   /eos/test-namespace-1/test-namespace-3  test-mgm-3:1094:8000
   ====================================== =======================

Changing directory to `/eos/test-namespace-1`, would be akin to connecting directly to the mgm at `test-mgm-1:1094`.

.. code-block:: bash

    EOS Console [root://localhost] |/> route link /eos/test-namespace-1 test-mgm-1:1094:8000
    EOS Console [root://localhost] |/> route link /eos/test-namespace-2 test-mgm-2:1094:8000
    EOS Console [root://localhost] |/> route link /eos/test-namespace-1/test-namespace-3 test-mgm-3:1094:8000
    EOS Console [root://localhost] |/> route ls
    /eos/test-namespace-1/ => test-mgm-1:1094:8000
    /eos/test-namespace-1/test-namespace-3/ => test-mgm-3:1094:8000
    /eos/test-namespace-2/ => test-mgm-2:1094:8000


The above configuration defines defines the path configuration in the example above.

If a port combination is not specified, the route assumes a xrootd port of 1094, and a http port of 8000.

Creating a link
---------------

A link is created using the `route link` command. It takes the option of a path and a destination host. Optional
specification includes the MGM's xrootd port, and the MGM's http port. Unspecified, they default to 1094 and 8000
respectively.

.. code-block:: bash

    EOS Console [root://localhost] |/> route link /eos/test-path eosdevsrv2:1094:8000
    EOS Console [root://localhost] |/> route ls
    /eos/test-path/ => eosdevsrv2:1094:8000


Removing a link
---------------

A link is removed using the `route unlink` command. Only a path needs to be specified.

.. code-block:: bash

    EOS Console [root://localhost] |/> route unlink /eos/test-namespace-1


Displaying current links
------------------------

The `route ls` command shows current active links. An asterix is displayed in
front of the MGM node which acts as a master for that particular mapping.

.. code-block:: bash

    EOS Console [root://localhost] |/> route ls
    /eos/test-namespace-1/ => *test-mgm-1:1094:8000
    /eos/test-namespace-1/test-namespace-3/ => *test-mgm-3:1094:8000
    /eos/test-namespace-2/ => *test-mgm-2:1094:8000


Making links visible to clients
-------------------------------

EOS will not display the link in a directory listing. This means it's possible to have an invisible link, and
stat or fileinfo commands will fail against the link path.

Creating a directory for the path will make it visible to clients, but accounting information will not be accurate until
a client changes into the path and queries again.

Connecting clients
------------------

HTTP and xrootd clients can effectively connect to the top level MGM and will automatically follow redirects. It is
recommended for performance reasons to connect FUSE clients via an automounter directly to each MGM, and use either
path mounting or bind mounts to replicate the tree structure.

Automounting
------------

It is possible to convert the output of `route ls` and place it into a map for an automounter or other process to use.
