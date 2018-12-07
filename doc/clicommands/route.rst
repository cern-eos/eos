route
-----

.. code-block:: text

  route [ls|link|unlink]
    namespace routing to redirect clients to external instances
.. code-block:: text

    route ls [<path>]
    list all routings or the one matching for the given path
    route link <path> <dst_host>[:<xrd_port>[:<http_port>]],...
    create routing from <path> to destination host. If the xrd_port
    is ommited the default 1094 is used, if the http_port is ommited
    the default 8000 is used. Several dst_hosts can be specified by
    separating them with ",". The redirection will go to the MGM
    from the specified list
    e.g route /eos/dummy/ foo.bar:1094:8000
    route unlink <path>
    remove routing matching path
