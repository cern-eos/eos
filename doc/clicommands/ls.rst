ls
--

.. code-block:: text

   usage: ls [-lani] <path>                                                  :  list directory <path>
      -l : show long listing
      -a : show hidden files
      -i : add inode information
      -n : show numerical user/group ids
      -s : checks only if the directory exists without listing
      path=file:... : list on a local file system
      path=root:... : list on a plain XRootD server (does not work on native XRootD clusters
      path=...      : all other paths are considered to be EOS paths!
