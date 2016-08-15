#!/bin/bash
#-------------------------------------------------------------------------------
# File: enable-hooks.sh
# Author: Elvin-Alin Sindrilaru <esindril@cern.ch>
#-------------------------------------------------------------------------------
#
#/************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2016 CERN/Switzerland                                  *
# *                                                                      *
# * This program is free software: you can redistribute it and/or modify *
# * it under the terms of the GNU General Public License as published by *
# * the Free Software Foundation, either version 3 of the License, or    *
# * (at your option) any later version.                                  *
# *                                                                      *
# * This program is distributed in the hope that it will be useful,      *
# * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
# * GNU General Public License for more details.                         *
# *                                                                      *
# * You should have received a copy of the GNU General Public License    *
# * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
# ************************************************************************/

#-------------------------------------------------------------------------------
# Description: Convenience script which enables all the available hooks located
# in ${GIT_ROOT}/git/hooks/
#-------------------------------------------------------------------------------

# Exit if there is any error
set -e

HOOK_NAMES="pre-commit"
GIT_ROOT=$(git rev-parse --show-toplevel)
DEFAULT_HOOKS_DIR=${GIT_ROOT}/.git/hooks
CUSTOM_HOOKS_DIR=${GIT_ROOT}/git/hooks

for HOOK in ${HOOK_NAMES}; do
    # If custom hook exists and is executable then create symlink
    if [ -e ${CUSTOM_HOOKS_DIR}/${HOOK}.sh ] && [ -x ${CUSTOM_HOOKS_DIR}/${HOOK}.sh ]
    then
	echo "Enable hook: ${CUSTOM_HOOKS_DIR}/${HOOK}.sh"
	ln -s -f ../../git/hooks/${HOOK}.sh ${DEFAULT_HOOKS_DIR}/${HOOK}
    fi
done
