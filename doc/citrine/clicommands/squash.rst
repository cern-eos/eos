squash
------

.. code-block:: text

  usage: squash new <path>                                                  : create a new squashfs under <path>
    squash pack [-f] <path>                                            : pack a squashfs image
    -f will recreate the package but keeps the symbolic link locally
    squash unpack [-f] <path>                                          : unpack a squashfs image for modification
    -f will atomically update the local package
    squash info <path>                                                 : squashfs information about <path>
    squash rm <path>                                                   : delete a squashfs attached image and its smart link
    squash relabel <path>                                              : relable a squashfs image link e.g. after an image move in the namespace
    squash roll <path>                                                 : will create a squash package from the EOS directory pointed by <path
    squash unroll <path>                                               : will store the squash package contents unpacked into the EOS package directory
    squash install --curl=https://<package>.tgz|.tar.gz <path>         : create a squashfs package from a web archive under <path>
