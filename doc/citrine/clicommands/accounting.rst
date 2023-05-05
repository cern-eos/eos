accounting
----------

.. code-block:: text

  usage: accounting report [-f]                          : prints accounting report in JSON, data is served from cache if possible
    -f : forces a synchronous report instead of using the cache (only use this if the cached data is too old)
    accounting config -e [<expired>] -i [<invalid>] : configure caching behaviour
    -e : expiry time in minutes, after this time frame asynchronous update happens, default is 10 minutes
    -i : invalidity time in minutes, after this time frame synchronous update happens, must be greater than expiry time, default is never
