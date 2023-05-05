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

    eos space config default taperestapi.status=on
    success: Tape REST API enabled

Deactivation:

.. code-block:: bash

    eos space config default taperestapi.status=off
    success: Tape REST API disabled

Warning: the tape REST API can only be enabled/disabled on the default space. An error message will be displayed
in the case one tries to enable it on a different space:

.. code-block:: bash

    eos space config OtherSpace taperestapi.status=on
    error: the tape REST API STAGE resource can only be enabled or disabled on the default space

REST API STAGE resource activation/deactivation
--------------------------------

By default, the STAGE resource of the tape REST API is not activated. If one deactivates the tape REST API and activates it again,
the STAGE resource will be deactivated by default.

Activation:

.. code-block:: bash

    eos space config default taperestapi.stage=on
    success: Tape REST API STAGE resource enabled

Deactivation:

.. code-block:: bash

    eos space config default taperestapi.stage=off
    success: success: Tape REST API STAGE resource disabled