#!/bin/bash

########################################################################
# On the SLC6 help2man version there is no "--no-discard-stderr" option.
#
# This script is a workaround for this. EOS commands will be called
# through this script, which will redirect stderr output to stdout.
#
# The workaround should be replaced by the help2man
# "--no-discard-stderr" option once SLC6 is no longer supported.
########################################################################

eos $@ 2>&1
