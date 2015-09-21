config
------

.. code-block:: text

   config ls|dump|load|save|diff|changelog|reset|autosave [OPTIONS]
   '[eos] config' provides the configuration interface to EOS.
.. code-block:: text

   Options:
   config ls   [--backup|-b] :
      list existing configurations
      --backup|-b : show also backup & autosave files
   config dump [--fs|-f] [--vid|-v] [--quota|-q] [--policy|-p] [--comment|-c] [--global|-g] [--access|-a] [<name>] [--map|-m]] : 
      dump current configuration or configuration with name <name>
      -f : dump only file system config
      -v : dump only virtual id config
      -q : dump only quota config
      -p : dump only policy config
      -g : dump only global config
      -a : dump only access config
      -m : dump only mapping config
   config save [-f] [<name>] [--comment|-c "<comment>"] ] :
      save config (optionally under name)
      -f : overwrite existing config name and create a timestamped backup
   =>   if no name is specified the current config file is overwritten
   config load <name> :
      load config (optionally with name)
   config diff :
      show changes since last load/save operation
   config changelog [-#lines] :
      show the last <#> lines from the changelog - default is -10
   config reset :
      reset all configuration to empty state
   config autosave [on|off] :
      without on/off just prints the state otherwise set's autosave to on or off
