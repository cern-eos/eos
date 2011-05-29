#!/bin/bash
###########################
# create the EOS man pages
###########################
rm -rf man1 >& /dev/null
mkdir man1
# create eos command include file
./create_eos_cmds.pl > eos.cmds
help2man --include eos.cmds --no-discard-stderr --help-option="-h " --no-info eos > man1/eos.1
gzip man1/eos.1
for name in `eos -b help | awk '{print$1}' |grep -v '?' | grep -v '.q' | grep -v 'test' `; do 
    help2man --name "eos $name" --no-discard-stderr --help-option="$name -h " --no-info eos > man1/eos::$name.1
    gzip man1/eos::$name.1
done
unlink eos.cmds

