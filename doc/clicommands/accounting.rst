accounting
---

.. code-block:: text

  '[eos] accounting report [-f]' report accounting information of quota nodes in standard JSON format
  Options:
    -f : issue a forced update bypassing the cache (use only if cached data is too old and you can't wait for the update)

  '[eos] accounting config [-e <expired> | -i <invalid>]' configure accounting caching behaviour
  Options:
    -e : set new expiry time in minutes. After this, data is served from cache instantly (you most likely get the cached data) and asynchronous update is issued. 10 min is default.
    -i : set new invalidity time in minutes. After this, the data is no longer served from cache, the client will wait for a synchronous update. Never by default.
         Must be greater than expiry to take effect.

.. code-block:: text

  Report:
    This command uses caching to avoid frequent re-computation of data. See the config command how to configure its behaviour.