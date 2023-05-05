.. highlight:: rst

.. index::
   single: Group Drainer

Group Drainer
=============

The group drainer uses the doc:`converter` mechanism to drain files from groups to target groups.
Failed transfers are retried a configurable number of times before finally reaching either a
drained or drainfail status for a group. It uses an architecture similar to GroupBalancer with a
special Drainer Engine which only looks for groups marked as *drain* as source groups. The target
groups are by default chosen as a threshold below the total group fillness average. Similar to
converter and groupbalancer this is enabled/disabled at a space level.


Configuration
-------------

.. code-block:: bash

   # enable/disable
   eos space config space.groupbalancer = <on/off>

   # force a group to drain
   eos group set <groupname> drain



   # The list of various configuration flags supported in the eos cli
   space config <space-name> space.groupdrainer=on|off                   : enable/disable the group drainer [ default=on ]
   space config <space-name> space.groupdrainer.threshold=<threshold>    : configure the threshold(%) for picking target groups
   space config <space-name> space.groupdrainer.group_refresh_interval   : configure time in seconds for refreshing cached groups info [default=300]
   space config <space-name> space.groupdrainer.retry_interval           : configure time in seconds for retrying failed drains [default=4*3600]
   space config <space-name> space.groupdrainer.retry_count              : configure the amount of retries for failed drains [default=5]
   space config <space-name> space.groupdrainer.ntx                      : configure the max file transfer queue size [default=10000]


The `threshold` param by default is a percent threshold below the total computed average of all group fillness. If you want to ignore this and target
every available group, then threshold=0 will do that.
The `group refresh interval` determines how often we refresh the list of groups in the system, since this is not expected to change that often by
default we only do it every 5 minutes (or when any groupdrainer config sees a change)
The `ntx` is the maximum amount of transfers we keep as active, it is okay to set this value higher than converter's ntx so that a healthy queue is maintained
and the converter is kept busy. However if you want to reduce throughput, reducing the ntx will essentially throttle the files we schedule for transfers
The `retry_interval` and `retry_count` determine the amount of retries we do for a failed transfer. By default we try upto 5 times before giving up and
eventually marking the FS as drainfailed. This will need manual intervention similar to handling regular FS drains.

Status
------

Currently a very minimal status command is implemented, which only informs about
the total transfers in queue and failed being tracked currently, in addition to
the count of groups in drain state and target groups. This is expected to change
in the future with more information about the progress of the drain.

This command can be accessed via

.. code-block:: bash

   eos space groupdrainer status <spacename>


Recommendations
---------------

It is recommended not to drain FS individually within the groups that are marked as in drain state
as the groupdrainer may target the same files targeted by the regular drainer and similarly they
may compete on drain complete statuses.

GroupBalancer only targets groups that are not in drain state, so in groups in drain state will not
be picked as either source or target groups by the GroupBalancer. However if no threshold is configured
then we might end up in scenarios where a file is being targeted by GroupDrainer to a group that is
relatively full eventually forcing the GroupBalancer to also balance. To avoid this it is recommended to
set the threshold so that only groups below average are targeted by GroupDrainer.


Completion
----------

In a groupdrain scenario:
An individual FS is marked as either drained/drainfailed
- When all the files in the FS are converted ie. transferred to other groups (`drained`)
- There are some files which even after `retry_count` attempts were failing transfer (`drainfailed`)


A groupdrain is marked as complete when all the FSes in a group are in drained or drainfailed mode.
In this scenario the group status is set as `drained` or `drainfailed`, which should be visible in the
`eos group ls` command.
