config
------

.. code-block:: text

  config changelog|dump|export|load|ls|reset|save [OPTIONS]
  '[eos] config' provides the configuration interface to EOS.
.. code-block:: text

  Subcommands:
  config changelog [-#lines]
    show the last <#> lines from the changelog - default is 10
  config dump [-cfgpqmsv] [<name>]
    dump configuration with name <name> or current one by default
    -c|--comments : dump only comment config
    -f|--fs       : dump only file system config
    -g|--global   : dump only global config
    -p|--policy   : dump only policy config
    -q|--quota    : dump only quota config
    -m|--map      : dump only mapping config
    -r|--route    : dump only routing config
    -s|--geosched : dump only geosched config
    -v|--vid      : dump only virtual id config
  config export [-f] [<name>]
    export a configuration stored on file to QuarkDB - you need to specify the full path
    -f : overwrite existing config name and create a timestamped backup
  config load <name>
    load config (optionally with name)
  config ls [-b|--backup]
    list existing configurations
    --backup|-b : show also backup & autosave files
  config reset
    reset all configuration to empty state
  config save [-f] [<name>] [-c|--comment "<comment>"]
    save config (optionally under name)
    -f : overwrite existing config name and create a timestamped backup
    If no name is specified the current config file is overwritten.
    -c : add a comment entry to the config
    Extended option will also add the entry to the logbook.
