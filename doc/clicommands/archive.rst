archive
-------

.. code-block:: text

   usage: archive <subcmd> 
      create <path>                          : create archive file
      put [--retry] <path>                   : copy files from EOS to archive location
      get [--retry] <path>                   : recall archive back to EOS
      purge[--retry] <path>                  : purge files on disk
      transfers [all|put|get|purge|job_uuid] : show status of running jobs
      list [<path>]                          : show status of archived directories in subtree
      kill <job_uuid>                        : kill transfer
      help [--help|-h]                       : display help message
