chmod
-----

.. code-block:: text

   usage: chmod [-r] <mode> <path>                             : set mode for <path> (-r recursive)
      <mode> can be only numerical like 755, 644, 700
      <mode> are automatically changed to 2755, 2644, 2700 respectivly
      <mode> to disable attribute inheritance use 4755, 4644, 4700 ...
