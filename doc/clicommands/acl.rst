acl
---

.. code-block:: text

  eos acl [-l|--list] [-R|--recursive] [--sys|--user] <rule> <path>
    atomically set and modify ACLs for the given directory path
.. code-block:: text

    -h, --help      : print help message
    -R, --recursive : apply to directories recursively
    -l, --list      : list ACL rules
    --user      : handle/list user.acl rules on directory
    --sys       : handle/list sys.acl rules on directory
    <rule> is created similarly to chmod rules. Every rule begins with
    [u|g|egroup] followed by ":" or "=" and an identifier.
    ":" is used to for modifying permissions while
    "=" is used for setting/overwriting permissions.
    When modifying permissions every ACL flag can be added with
    "+" or removed with "-".
  Examples:
    acl --user u:1001=rwx /eos/dev/
    Set ACLs for user id 1001 to rwx
    acl --user u:1001:-w /eos/dev
    Remove 'w' flag for user id 1001
    acl --user u:1001:+m /eos/dev
    Add change mode permission flag for user id 1001
    acl --user u:1010= /eos/dev
    Remove all ACls for user id 1001
