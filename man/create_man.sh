#!/bin/bash
###########################
# create the EOS man pages
###########################
export EOS_DISABLE_PIPEMODE=1
rm -rf man1 >& /dev/null
mkdir man1
# create eos command include file
./create_eos_cmds.pl > eos.cmds
help2man --include eos.cmds --no-discard-stderr --help-option="-h " --no-info eos > man1/eos.1
gzip man1/eos.1
for name in `eos -b help | awk '{print$1}' |grep -v '?' | grep -v '.q' | grep -v 'test' | grep -v 'exit' | grep -v 'motd' | grep -v 'quit' | grep -v 'timing' | grep -v 'silent' | grep -v 'version' | grep -v 'whoami'`; do
    echo "Processing command ${name} ..."
    help2man --name "eos $name" --no-discard-stderr --help-option="$name -h " --no-info "eos " > man1/eos-$name.1
    gzip man1/eos-$name.1
done
unlink eos.cmds

