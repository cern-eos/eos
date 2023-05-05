config
------

.. code-block:: text

   usage:
  config changelog|dump|export|load|ls|reset|save [OPTIONS]
  '[eos] config' provides the configuration interface to EOS.
  Subcommands:
  config changelog [-#lines] : show the last #lines from the changelog - default is 10
  config dump [<name>] : dump configuration with name <name> or current one by default
  config export <name> [-f] : export a configuration stored on file to QuarkDB (you need to specify the full path!)
  	 -f : overwrite existing config name and create a timestamped backup
  config load <name> : load <name> config
  config ls [-b|--backup] : list existing configurations
  	 -b : show also backup & autosave files
  config reset : reset all configuration to empty state
  config save <name> [-f] [-c|--comment "<comment>"] : save config under <name>
  	 -f : overwrite existing config name and create a timestamped backup
  	 -c : add a comment entry to the config
