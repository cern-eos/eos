.. highlight:: rst

.. index::
   single: dynamicec;

.. _systemd:

DynamicEc
=========

The DynamicEc will dynamically remove excessstipes from the raid coded files in the system, if the system becomes too loaded with data.
When the DynamicEc starts to cleanup, it has a thread scanning the system for potential files to be reduced. Another thread will then look at the potential files and reduce them if possible.
When looking if the file will be reduced, there is both a minimum size and minimum age for the files to be reduced and they are configurable.

Threshold
---------

The max threshold is for how full the system will have to be in order to start the DynamicEc. It is as default 98% of the maximum capacity of the system.

To set the max threshold percentage:

.. code-block:: bash

	eos space config default space.dynamicec.maxthreshold=<threshold percentage>

To show the max threshold percentage:

.. code-block:: bash
	
	eos space config default space.dynamicec.maxthreshold=show 
	

It is also possible to set a min threshold, this is for how much the system will try to lower the storede size to in percentage. This can be set with:

.. code-block:: bash

	eos space config default space.dynamicec.maxthreshold=<threshold percentage> 

To show the min threshold percentage:

.. code-block:: bash

	eos space config default space.dynamicec.minthreshold=show
	
The default minimum threshold is 95%.

File age
--------

This is the age for how old in seconds the files will have to be in order to be reduced.

The function to change the age is:

.. code-block:: bash

	eos space config default space.dynamicec.agefromwhentodelete=<new age in seconds>
	
To see the current age the command is:

.. code-block:: bash	

	eos space config default space.dynamicec.agefromwhentodelete=show

The default age is 3600 seconds.

File size
---------

The file size is the minimum size of the files wich will be reduced. The default size is 1048576 bytes, and it can be set from the command line:

.. code-block:: bash

	eos space config default space.dynamicec.minsize=<minsize in bytes>
	
To show what it currently is use the command:

.. code-block:: bash

	eos space config default space.dynamicec.minsize=show

Wait time
---------

The wait time is used varius places in the code. It is used after it has booted to give the system more load time. When the cleanup has had a cycle and waiting to take another run. It is also an extra wait time aften the scan has done a cycle.

The wait time can be set with the command: 

.. code-block:: bash

	eos space config default space.dynamicec.waittime=<new wait time in seconds>

To show what it currently is use the command:

.. code-block:: bash

	eos space config default space.dynamicec.waittime=show
	
The default wait time is 30 seconds.

Map max size and sleep time
---------------------------

This is the max size that can be in the map after the files have been scanned. If the files in the map exceed the limit, the scanner will sleep and check again. The default max size for the map is 10 TB.
If this happens then the sleep time will by default be 600 seconds, if the scan runs though all the files, it will wait for 28800 seconds as default.

The map max size can be set with:

.. code-block:: bash

	eos space config default space.dynamicec.sizeformapmax=<max size for map in bytes>
	
To show what it currently is use the command:

.. code-block:: bash
	
	eos space config default space.dynamicec.sizeformapmax=show
	

The sleep when full can be set with:

.. code-block:: bash

	eos space config default space.dynamicec.sleepwhenfull=<sleep when map is full in seconds>

To show what it currently is use the command:

.. code-block:: bash

	eos space config default space.dynamicec.sleepwhenfull=show

The sleep when done can be set with:

.. code-block:: bash

	eos space config default space.dynamicec.sleepwhendone=<sleep when done in seconds>

To show what it currently is use the command:

.. code-block:: bash
	
	eos space config default space.dynamicec.sleepwhendone=show


Turn off/on
-----------

The DynamicEc can be turned off if the feature is not needed. The commands to turn off and on the DynamicEc is:

.. code-block:: bash

	eos space config default space.dynamicec=off
	
	eos space config default space.dynamicec=on

Test
----

There is a function to speed up the system and schould only be used by the test of the system, the commands to turn this on and off is:

.. code-block:: bash

	eos space config default space.dynamicec.test=on

	eos space config default space.dynamicec.test=off

Restartscan
-----------

This will restart the scan, it is used in the tests, but it can also be usefull for other purposes.
To restart the scan use the command:

.. code-block:: bash

	eos space config default space.dynamicec.restartscan=on

Print status
-----------

This command will print the status for the dynamicEc to the log of DynamicEc:

.. code-block:: bash

	eos space config default space.dynamicec.printall=on

Unchangeable variables
----------------------

The OnWork set in the constructor as default true, but is set to false in unit tests. This can only be set in the constructor and it is used in the unittests, where there is no test environment.





































