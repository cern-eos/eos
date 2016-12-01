  acl
  ---

  .. code-block:: text

    Usage: eos acl [-l|--list] [-R|--recursive]
    [--sys|--user] <rule> <path>
	--help           Print help
    -R, --recursive      Apply on directories recursively
    -l, --lists          List ACL rules
	--user           Set usr.acl rules on directory
	--sys            Set sys.acl rules on directory

    <rule> is created based on chmod rules.
    Every rule begins with [u|g|egroup] followed by : and identifier.

    Afterwards can be:
     = for setting new permission
     : for modification of existing permission

    This is followed by rule definition.
    Every ACL flag can be added with + or removed with -, or in case
    of setting new ACL permission just entered ACL flag.
