config
------

.. code-block:: text

  config autosave|changelog|diff|dump|export|load|ls|reset|save [OPTIONS]
  '[eos] config' provides the configuration interface to EOS.
.. code-block:: text

  Subcommands:
  config autosave [on|off]
    without on/off just prints the state otherwise set's autosave to on or off
  config changelog [-#lines]
    show the last <#> lines from the changelog - default is 10
  config diff
    show changes since last load/save operation
  config dump [-cfgpqmsv] [<name>]
    dump configuration with name <name> or current one by default
    -c|--comment  : dump only comment config
    -f|--fs       : dump only file system config
    -g|--global   : dump only global config
    -p|--policy   : dump only policy config
    -q|--quota    : dump only quota config
    -m|--map      : dump only mapping config
    -s|--geosched : dump only geosched config
    -v|--vid      : dump only virtual id config
  config export [-f] [<name>]
    export a configuration stored on file to Redis
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
