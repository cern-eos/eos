fileinfo
--------

.. code-block:: text

  fileinfo <identifier> [--path] [--fid] [--fxid] [--size] [--checksum] [--fullpath] [--proxy] [-m] [--env] [-s|--silent]
    Prints file information for specified <identifier>
    <identifier> = <path>|fid:<fid-dec>|fxid:<fid-hex>|inode:<inode-dec>
  Options:
    --path                        :  filters output to show path field
    --fid                         :  filters output to show fid field
    --fxid                        :  filters output to show fxid field
    --size                        :  filters output to show size field
    --checksum                    :  filters output to show checksum field
    --fullpath                    :  adds physical path information to the output
    --proxy                       :  adds proxy information to the output
    -m                            :  prints single-line information in monitoring format
    --env                         :  prints information in OucEnv format
    -s | --silent                      :  silent - used to run as internal command
.. code-block:: text

  Remarks:
    Filters stack up and apply only to normal display mode.
    Command also supports JSON output.
