#!/bin/bash

###########################
# create the EOS man pages
###########################
export EOS_DISABLE_PIPEMODE=1
rm -rf man1 >& /dev/null
mkdir man1

# commands excluded from man page generation
excluded=(
        "exit"
        "help"
        "json"
        "motd"
        "quit"
        "silent"
        "test"
        "timing"
        "whoami"
        ".q"
        "?"
    )

# create eos command include file
./create_eos_cmds.pl ${excluded[*]} > eos.cmds
help2man --include eos.cmds --no-discard-stderr --help-option="-h " --no-info eos > man1/eos.1
gzip man1/eos.1

excluded[${#excluded[@]} - 1]="\?"
excluded_string=$(IFS=\| ; echo "${excluded[*]}")

for name in `eos -b help | awk '{print $1}' | grep -w -v -E "${excluded_string}"`; do
    echo "Processing command ${name} ..."
    help2man --name "eos $name" --no-discard-stderr --help-option="$name -h " --no-info "eos " > man1/eos-$name.1
    gzip man1/eos-$name.1
done

unlink eos.cmds
