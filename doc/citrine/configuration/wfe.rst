.. highlight:: wfe

.. index::
   single:: WFE - Work Flow Engine

WFE Engine
==========
The workflow engine is a versatile event triggered storage process chain. Currently all events are created by file operations.
The policy to emit events is described as extended attributes of a parent directory. Each workflow is named. The default workflow
is named 'default' and used if no workflow name is provided in an URL as `?eos.workflow=default`.

The workflow engine allows to create chained workflows: E.g. one workflow can trigger an event emission to run the next workflow in the chain and so on...

.. epigraph::

   ==================== ==================================================================================================
   Event                Description
   ==================== ==================================================================================================
   sync::create         event is triggered at the MGM when a file is being created (synchronous event)
   open                 event is triggered at the MGM when a 'file open'
                        - if the return of an open call is ENONET a workflow defined stall time is returned
   sync::prepare        event is triggered at the MGM when a 'prepare' is issued (synchronous event)
   sync::abort_prepare  event is triggered at the MGM when xrdfs prepare -f issued (synchronous event)
   sync::offline        event is triggered at the MGM when a 'file open' is issued against an offline file (synchronous
                        event)
   retrieve_failed      event is triggered with an error message at the MGM when the retrieval of a file has failed
   archive_failed       event is triggered with an error message at the MGM when the archival of a file has failed
   closer               event is triggered via the MGM when a read-open file is closed on an FST.
   sync::closew         event is triggered via the FST when a write-open file is closed (it has priority over the asynchronous one)
   closew               event is triggered via the MGM when a write-open file is closed on an FST
   sync::delete         event is triggered at the MGM when a file has been deleted (synchronous event)
   ==================== ==================================================================================================

Currently the workflow engine implements two action targets. The **bash:shell** target is a powerful target.
It allows you to execute any shell command as a workflow. This target provides a large set of template parameters
which EOS can give as input arguments to the called shell command. This is described later. The **mail** target
allows to send an email notification to a specified recipient and mostly used for demonstration.

.. epigraph::

   ============= =============================================================================================
   Action Target Description
   ============= =============================================================================================
   bash:shell    run an arbitrary shell command line with template argument substitution
   mail          send an email notification to a provided recipient when such an event is triggered
   ============= =============================================================================================

Configuration
-------------

Engine
++++++
The WFE engine has to be enabled/disabled in the default space only:

.. code-block:: bash

   # enable
   eos space config default space.wfe=on  
   # disable
   eos space config default space.wfe=off

The current status of the WFE can be seen via:

.. code-block:: bash

   eos -b space status default
   # ------------------------------------------------------------------------------------
   # Space Variables
   # ....................................................................................
   ...
   wfe                            := off
   wfe.interval                   := 10
   ...

The interval in which the WFE engine is running is defined by the **wfe.interval**
space variable. The default is 10 seconds if unspecified.

.. code-block:: bash

   # run the LRU scan once a week
   eos space config default space.wfe.interval=10

The thread-pool size of concurrently running workflows is defined by the **wfe.ntx** space variable.
The default is to run all workflow jobs sequentially with a single thread.

.. code-block:: bash

   # configure a thread pool of 16 workflow jobs in parallel
   eos space config default space.wfe.ntx=10

Workflows are stored in a virtual queue system. The queues display the status of each workflow. By default workflows older than 7 days are cleaned up.
This setting can be changed by the **wfe.keeptime** space variable. That is the time in seconds how long workflows are kept in the virtual queue system before
they get deleted.

.. code-block:: bash

   # keep workflows for 1 week
   eos space config default space.wfe.keeptime=604800

Workflow Configuration
++++++++++++++++++++++++++++++++

The **mail** workflow
`````````````````````
As an example we want to send an email to a mailing list, whenever a file is deposited. This workflow can be specified like this:

.. code-block:: bash

   # define a workflow to send when a file is written
   eos attr set sys.workflow.closew.default="mail:eos-project.cern.ch: a file has been written!" /eos/dev/mail/

   # place a new file
   eos cp /etc/passwd /eos/dev/mail/passwd

   # eos-project.cern.ch will receive an Email with a subject like: eosdev ( eosdev1.cern.ch ) event=closew fid=000004f7 )
   # and the text in the body : a file has been written!


The **bash:shell** workflow
``````````````````````````````````````````````````

Most people want to run a command whenever a file is placed, read or deleted. To invoke a shell command one configures the **bash:shell** workflow.
As an example consider this simple echo command, which prints the path when a **closew** event is triggered: 

.. code-block:: bash

   # define a workflow to echo the full path when a file is written
   eos attr set "sys.workflow.closew.default=sys.workflow.closew.default="bash:shell:mylog echo <eos::wfe::path>" /eos/dev/echo/

The template parameters ``<eos::wfe::path>`` is replaced with the full logical path of the file, which was written. The third parameters ``mylog`` in **bash:shell:mylog** specifies the name of 
the log file for this workflow which is found on the MGM under ``/var/log/eos/wfe/mylog.log`` 

Once one uploads a file into the ``echo`` directory, the following log entry is created in ``/var/log/eos/wfe/mylog.log``

.. code-block:: bash

   ----------------------------------------------------------------------------------------------------------------------
   1466173303 Fri Jun 17 16:21:43 CEST 2016 shell echo /eos/dev/echo/passwd
   /eos/dev/echo/passwd
   retc=0

The full list of static template arguments is given here:

.. epigraph::

   =========================== =============================================================================================
   Template                    Description
   =========================== =============================================================================================
   <eos::wfe::uid>             user id of the file owner
   <eos::wfe::gid>             group id of the file owner
   <eos::wfe::username>        user name of the file owner
   <eos::wfe::groupname>       group name of the file owner
   <eos::wfe::ruid>            user id invoking the workflow
   <eos::wfe::rgid>            group id invoking the workflow
   <eos::wfe::rusername>       user name invoking the workflow
   <eos::wfe::rgroupname>      group name invoking the workflow
   <eos::wfe::path>            full absolute file path which has triggered the workflow
   <eos::wfe::base64:path>     base64 encoded full absolute file path which has triggered the workflow
   <eos::wfe::turl>            XRootD transfer URL providing access by file id e.g. root://myeos.cern.ch//mydir/myfile?eos.lfn=fxid:00001aaa
   <eos::wfe::host>            client host name triggering the workflow
   <eos::wfe::sec.app>         client application triggering the workflow (this is defined externally via the CGI ``?eos.app=myapp``)
   <eos::wfe::sec.name>        client security credential name triggering the workflow
   <eos::wfe::sec.prot>        client security protocol triggering the workflow
   <eos::wfe::sec.grps>        client security groups triggering the workflow
   <eos::wfe::instance>        EOS instance name
   <eos::wfe::ctime.s>         file creation time seconds
   <eos::wfe::ctime.ns>        file creation time nanoseconds
   <eos::wfe::mtime.s>         file modification time seconds
   <eos::wfe::mtime.ns>        file modification time nanoseconds
   <eos::wfe::size>            file size
   <eos::wfe::cid>             parent container id
   <eos::wfe::fid>             file id (decimal)
   <eos::wfe::fxid>            file id (hexacdecimal)
   <eos::wfe::name>            basename of the file
   <eos::wfe::base64:name>     base64 encoded basename of the file
   <eos::wfe::link>            resolved symlink path if the original file path is a symbolic link to a file
   <eos::wfe::base64:link>     base64 encoded resolved symlink path if the original file path is a symbolic link to a file
   <eos::wfe::checksum>        checksum string
   <eos::wfe::checksumtype>    checksum type string
   <eos::wfe::event>           event name triggering this workflow (e.g. closew)
   <eos::wfe::queue>           queue name triggering this workflow (e.g. can be 'q' or 'e')
   <eos::wfe::workflow>        workflow name triggering this workflow (e.g. default)
   <eos::wfe::now>             current unix timestamp when running this workflow
   <eos::wfe::when>            scheduling unix timestamp when to run this workflow
   <eos::wfe::base64:metadata> a full base64 encoded meta data blop with all file metadata and parent metadata including extended attributes
   <eos::wfe::vpath>           the path of the workflow file in the virtual workflow directory when the workflow is executed
                               - you can use this to attach messages/log as an extended attribute to a workflow if desired
   =========================== =============================================================================================


Extended attributes of a file and it's parent container can be read with dynamic template arguments:

.. epigraph::

   ================================ ========================================================================================
   Template                         Description
   ================================ ========================================================================================
   <eos::wfe::fxattr:<key>>         Retrieves the value of the extended attribute of the triggering file with name <key>
                                    - sets UNDEF if not existing
   <eos::wfe::fxattr:base64:<key>>  Retrieves the base64 encoded value of the extended attribute of the triggering file with name <key>
                                    - sets UNDEF if not existing
   <eos::wfe::cxattr:<key>>         Retrieves the value of the extended attribute of parent directory of the triggering file
                                    - sets UNDEF if not existing
   ================================ ========================================================================================



Here is an  example for a dynamic attribute:

.. code-block:: bash

   # define a workflow to echo the meta blob and the acls of the parent directory when a file is written
   eos attr set "sys.workflow.closew.default=sys.workflow.closew.default="bash:shell:mylog echo <eos::wfe::base64:metadata> <eos::wfe::cxattr:sys.acl>" /eos/dev/echo/


Configuring retry policies for  **bash:shell** workflows
````````````````````````````````````````````````````````

If a **bash:shell** workflow failes e.g. the command returns rc!=0 and no retry policy is defined, the workflow job ends up in the **failed** queue. For each 
workflow the number of retries and the delay for retry can be defined via extended attributes. To reschedule a workflow after a failure the shell command has to return **EAGAIN** e.g. ``exit(11)``.
The number of retries for a failing workflow can be defined as:

.. code-block:: bash

   # define a workflow to return EAGAIN to be retried
   eos attr set "sys.workflow.closew.default=sys.workflow.closew.default="bash:shell:fail '(exit 11)'" /eos/dev/echo/

   # set the maximum number of retries
   eos attr set "sys.workflow.closew.default.retry.max=3" /eos/dev/echo/

The previous workflow will be scheduled three times without delay. If you want to schedule a retry at a later point in time, you can define the delay for retry for a particular workflow like:

.. code-block:: bash

   # configure a workflow retry after 1 hour
   eos attr set "sys.workflow.closew.default.retry.delay=3600" /eos/dev/echo/


Returning result attributes 
````````````````````````````

if a **bash::shell** workflow is used, the STDERR of the command is parsed for return attribute tags, which are either tagged on the triggering file (path) or the virtual workflow entry (vpath):

.. epigraph::

   ============================================== =====================================================================================
   Syntax                                         Resulting Action
   ============================================== =====================================================================================
   <eos::wfe::path::fxattr:<key>>=base64:<value>  set a file attribute <key> on <eos::wfe::path> to the base64 decoded value of <value>
   <eos::wfe::path::fxattr:<key>>=<value>         set a file attribute <key> on <eos::wfe::path> to <value> (value can not contain space)
   <eos::wfe::vpath::fxattr:<key>>=base64:<value> set a file attribute <key> on <eos::wfe::vpath> to the base64 decoded value of <value>
   <eos::wfe::vpath::fxattr:<key>>=:<value>       set a file attribute <key> on <eos::wfe::vpath> to <value> (value can not contain space)
   ============================================== =====================================================================================

Virtual /proc Workflow queue directories
++++++++++++++++++++++++++++++++++++++++++++

The virtual directory structure for triggered workflows can be found under ``/eos/<instance>/proc/workflow``. 

Here is an example:

.. code-block:: bash

   EOS Console [root://localhost] |/eos/dev/> eos find /eos/dev/proc/workflow/
   /eos/dev/proc/workflow/20160617/d/
   /eos/dev/proc/workflow/20160617/d/default/
   /eos/dev/proc/workflow/20160617/d/default/1466171933:000004f7:closew
   /eos/dev/proc/workflow/20160617/d/default/1466173303:000004fd:closew
   /eos/dev/proc/workflow/20160617/f/
   /eos/dev/proc/workflow/20160617/f/default/
   /eos/dev/proc/workflow/20160617/f/default/1466171873:000004f4:closew
   /eos/dev/proc/workflow/20160617/f/default/1466173183:000004fa:closew
   /eos/dev/proc/workflow/20160617/q/
   /eos/dev/proc/workflow/20160617/q/default/1466173283:000004fb:closew

The virtual tree is organized with entries like ``<proc>/workflow/<year-month-day>/<queue>/<workflow>/<unix-timestamp>:<fid>:<event>``.
Workflows are scheduled only from the **q** and **e** queues. All other entries describe a ``finale state`` and will be expired as configured by the cleanup policy described in the beginning.

The existing queues are described here:

.. epigraph::

   =========================== ========================================================================================
   Queue                       Description
   =========================== ========================================================================================
   ../q/..                     all triggered asynchronous workflows appear first in this queue
   ../s/..                     scheduled asynchronous workflows and triggered synchronous workflows appear in this queue
   ../r/..                     running workflows appear in this queue
   ../e/..                     failed workflows with retry policy appear here
   ../f/..                     failed workflows without retry appear here
   ../g/..                     workflows with 'gone' files or some global misconfiguration appear here
   ../d/..                     successful workflows with 0 return code
   =========================== ========================================================================================


Synchronous workflows
``````````````````````

The **deletion** and **prepare** workflow are synchronous workflows which are executed in-line. They are stored and tracked as asynchronous workflows in the proc filesystem. The emitted event on deletion is **sync::delete**, the emitted event on prepare is **sync::prepare**. 

Workflow log and return codes
-----------------------------

The return codes and log information is tagged on the virtual directory entries in the proc filesystem as extended attributes:

.. code-block:: bash

   sys.wfe.retc=<return code value>
   sys.wfe.log=<message describing the result of running the workflow>





