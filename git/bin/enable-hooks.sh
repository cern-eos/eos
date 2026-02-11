#!/bin/bash
#-------------------------------------------------------------------------------
# File: enable-hooks.sh
# Author: Elvin-Alin Sindrilaru <esindril@cern.ch>
# Author: Luis Antonio Obis Aparicio <luis.obis@cern.ch>
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
# Description: Convenience script which enables all the pre-commit hooks for the current repository. It also checks if the required dependencies are installed and provides instructions on how to install them if they are missing.
#-------------------------------------------------------------------------------

REQUIRED_DEPS="clang-format pre-commit"
HAS_ERROR=0

# Check dependencies
for DEP in ${REQUIRED_DEPS}; do
    if ! command -v "${DEP}" > /dev/null 2>&1; then
        echo "[!] ${DEP} is not installed."

        # Determine the specific install command
        if [[ "$OSTYPE" == "darwin"* ]]; then
            INSTALL_CMD="brew install ${DEP}"
        elif command -v dnf > /dev/null 2>&1; then
            if [[ "${DEP}" == "clang-format" ]]; then
                INSTALL_CMD="sudo dnf install clang-tools-extra"
            else
                INSTALL_CMD="sudo dnf install pre-commit"
            fi
        else
            INSTALL_CMD="(Use your system package manager to install '${DEP}')"
        fi

        echo "    To install, run: '${INSTALL_CMD}'"
        HAS_ERROR=1
    fi
done

# Exit if any dependencies were missing
if [ $HAS_ERROR -ne 0 ]; then
    exit 1
fi

echo "[+] All dependencies found!"

# Locate the git root directory
REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null)

if [ -z "$REPO_ROOT" ]; then
    echo "[!] Error: This directory is not part of a git repository." >&2
    exit 1
fi

# Move to root and install pre-commit
echo "[*] Navigating to repo root: $REPO_ROOT"
cd "$REPO_ROOT" || exit 1

echo "[*] Installing pre-commit hooks..."
pre-commit install

echo "[+] Setup complete! To uninstall hooks run: 'pre-commit uninstall'"
