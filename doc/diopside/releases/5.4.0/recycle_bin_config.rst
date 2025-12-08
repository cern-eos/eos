Recycle Bin Configuration Update
================================

.. important::
   **Configuration Migration Required**

   The recycle bin configuration has moved from extended attributes on the recycle directory to the global EOS configuration.

New Configuration Commands
--------------------------

Use the ``eos recycle config`` command to configure the recycle bin policy.

**Enable/Disable:**

.. code-block:: bash

   # Enable the recycle bin globally
   eos recycle config --enable on

   # Disable the recycle bin globally
   eos recycle config --enable off


**Set Policy Parameters:**

.. code-block:: bash

   # Set keep time in seconds (e.g., 1 day)
   eos recycle config --lifetime 86400

   # Set space keep ratio (0.0 - 1.0)
   eos recycle config --ratio 0.8

   # Set collection interval in seconds
   eos recycle config --collect-interval 300


Cleanup Legacy Configuration
----------------------------

Old extended attributes on the recycle directory (typically ``/eos/<instance>/proc/recycle/`` or configured path) are **NO LONGER USED** for policy enforcement and should be removed to avoid confusion.

**Attributes to Remove:**

* ``sys.recycle.keeptime``
* ``sys.recycle.keepratio``
* ``sys.recycle.collectinterval``
* ``sys.recycle.removeinterval``

**How to Remove:**

Use the ``eos attr rm`` command on your recycle bin directory (check your specific path, commonly ``/eos/<instance>/proc/recycle/``):

.. code-block:: bash

   eos attr rm sys.recycle.keeptime /eos/<instance>/proc/recycle/
   eos attr rm sys.recycle.keepratio /eos/<instance>/proc/recycle/
   eos attr rm sys.recycle.collectinterval /eos/<instance>/proc/recycle/
   eos attr rm sys.recycle.removeinterval /eos/<instance>/proc/recycle/

.. note::
   The ``sys.recycle`` attribute on individual directories is **STILL SUPPORTED** to explicitly mark a subtree for recycling even if global enforcement is off. You do not need to remove ``sys.recycle`` from user directories.

Space Policy Notice
-------------------

The ``eos space config ... policy.recycle=on`` command is **DEPRECATED** and has been removed. Use ``eos recycle config --enable on`` instead.
