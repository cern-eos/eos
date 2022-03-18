.. highlight:: rst

.. index::
   single: Configuration

Configuration
=============

Enable XRootD HTTP support on the MGM
-------------------------------------

Enable XRootD HTTP support by following the :doc:`../configuration/http_tpc`

xrd.cf.mgm configuration
------------------------

On the ``/etc/xrd.cf.mgm`` configuration file, the following parameters must be set:

.. code-block:: text

    mgmofs.tapeenabled true
    taperestapi.sitename cern-cta-xxxx

The ``taperestapi.sitename`` parameter corresponds to the targeted metadata identifier that will be used by the user
to pass metadata to CTA for each file staged.

REST API activation/deactivation
--------------------------------

Activation:

.. code-block:: bash

    eos space config default taperestapi=on
    success: Tape REST API enabled

Deactivation:

.. code-block:: bash

    eos space config default taperestapi=off
    success: Tape REST API disabled

Warning: the tape REST API can only be enabled/disabled on the default space. An error message will be displayed
in the case one tries to enable it on a different space:

.. code-block:: bash

    eos space config OtherSpace taperestapi=on
    error: the tape REST API can only be enabled on the default space