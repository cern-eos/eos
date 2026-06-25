token
-----

.. code-block:: text

  token --token  <token> | --path <path> --expires <expires> [--permission <perm>] [--owner <owner>] [--group <group>] [--tree] [--scope <scope1> [--scope <scope2>] ...] [--origin <origin1> [--origin <origin2>] ...]]
    get or show a token
.. code-block:: text

    token --token <token>
    : provide a JSON dump of a token - independent of validity
    --path <path>                 : define the namespace restriction - if ending with '/' this is a directory or tree, otherwise it references a file
    --permission <perm>           : define the token bearer permissions e.g 'rx' 'rwx' 'rwx!d' 'rwxq' - see acl command for permissions
    --owner <owner>               : identify the bearer with as user <owner>
    --group <group>               : identify the bearer with a group <group>
    --tree                        : request a subtree token granting permissions for the whole tree under <path>
    --scope <scope>               : restrict gRPC command usage, e.g. grpc.exec.quota.get; multiple scope parameters can be provided
    --origin <origin>            : restrict token usage to <origin> - multiple origin parameters can be provided
    <origin> := <regexp:hostname>:<regex:username>:<regex:protocol>
    - described by three regular extended expressions matching the
    bearers hostname, possible authenticated name and protocol
    - default is *.*.*
  Examples:
    eos token --path /eos/ --permission rx --tree
    : token with browse permission for the whole /eos/ tree
    eos token --path /eos/project/ --permission rx --tree --scope grpc.exec.quota.get
    : token limited to read-only gRPC quota lookups under /eos/project/
    eos token --path /eos/file --permission rwx --owner foo --group bar
    : token granting write permission for /eos/file as user foo:bar
    eos token --token zteos64:...
    : dump the given token
