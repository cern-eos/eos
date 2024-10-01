ls
--

.. code-block:: text

  usage: ls [-laniyFN] [--no-globbing] <path>                                     :  list directory <path>
    -l : show long listing
    -y : show long listing with backend(tape) status
    -lh: show long listing with readable sizes
    -a : show hidden files
    -i : add inode information
    -n : show numerical user/group ids
    -F : append indicator '/' to directories
    -s : checks only if the directory exists without listing
    --no-globbing|-N : disables path globbing feature (e.g: list a file containing '[]' characters)
    path=file:... : list on a local file system
    path=root:... : list on a plain XRootD server (does not work on native XRootD clusters
    path=...      : all other paths are considered to be EOS paths!
