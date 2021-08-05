#!/bin/bash

###########################
# create the EOS man pages
###########################

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

export EOS_DISABLE_PIPEMODE=1
rm -rf man1 >& /dev/null
mkdir man1

# create eos command include file
wdir=$(dirname $0)
excluded=( "help" "json" "silent" "timing" "quit" "status" "exit" "test" ".q" "")
${wdir}/create_eos_cmds.pl ${excluded[*]} > eos.cmds
help2man --include=eos.cmds --help-option="-h " --no-info --no-discard-stderr eos > man1/eos.1
gzip man1/eos.1
unlink eos.cmds

# prepare array of command names
excluded[${#excluded[@]} - 1]="\?"
excluded_string=$(IFS=\| ; echo "${excluded[*]}")
commands=(`eos -b help | awk '{print $1}' | grep -w -v -E "${excluded_string}" | cut -d\  -f1`)

# generate man page for each command asynchronously
echo "Generating man pages:"
for (( i=0; i<${#commands[@]}; i++ )); do
    name="${commands[$i]}"
    printf "  [%2s/%2s] Processing command %s\n" $((${i} + 1)) ${#commands[@]} ${name}
    help2man --name="eos $name" --help-option="$name -h " --no-info --no-discard-stderr eos > man1/eos-${name}.1 \
        && gzip man1/eos-${name}.1 &
done

# wait at most 5 seconds for all man pages to be created
COUNT=0
while [[ `ls man1/ | wc -l` -ne $((${#commands[@]} + 1)) ]]; do
    COUNT=$((COUNT + 1))
    [[ ${COUNT} -eq 6 ]] && break
    sleep 1
done
