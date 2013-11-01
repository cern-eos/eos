#######################################################################
# EOS - the CERN Disk Storage System
# Copyright (C) 2013 CERN/Switzerland
#
# Author: Joaquim Rocha - CERN
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#######################################################################

import subprocess
import sys
import os

EOS_COMMAND = 'eos'
EXCLUDE_COMMANDS = ['console', '?', '.q', 'license', 'version', 'motd', 'pwd', 'silent',
                    'whoami', 'json', 'exit', 'timing', 'help', 'quit']

CLI_COMMANDS_BASE_NAME = 'clicommands'
CLI_COMMANDS_DIR = os.path.dirname(__file__) + '/' + CLI_COMMANDS_BASE_NAME + '/'
CLI_COMMANDS_SECTION_FILE_PATH = os.path.dirname(__file__) + '/' + CLI_COMMANDS_BASE_NAME + '.rst'
CLI_COMMANDS_SECTION = \
'''.. _clientcommands:

Client Commands
================

.. toctree::
   :maxdepth: 2

'''

HELP_OUTPUT = {}

def get_commands_and_desc():
    '''
    Gets the commands and descriptions by parsing the output of 'eos help'.
    Returns a dictionary with the command name as key and the command
    description as value.
    '''
    sys.stdout.flush()
    output = subprocess.Popen([EOS_COMMAND, "help"], stdout=subprocess.PIPE).communicate()[0]
    sys.stdout.flush()
    lines = output.splitlines()
    commands = {}
    # we exclude the 1st line because it's just "help" and it has
    # some weirdly encoded chars
    for line in lines[1:]:
        words = line.split(' ')
        if not words:
            continue
        command = words[0]
        commands[command] = ''
        if len(words) > 1:
            commands[command] = ' '.join(words[1:]).strip()
    return commands

def get_help_for_command(command):
    '''
    Returns the help for a command and, if they exist, all the subcommands.
    '''
    output = subprocess.Popen([EOS_COMMAND, command, "-h"], stdout=subprocess.PIPE).communicate()[0]
    if '--help-all' in output:
        sys.stdout.flush()
        output = subprocess.Popen([EOS_COMMAND, command, "-H"], stdout=subprocess.PIPE).communicate()[0]
    sys.stdout.flush()
    # usually the first line of the output is the very same command we
    # executed, so we remove it
    if output.startswith(command):
        output = output[len(command) + 4:]
    return output

def command_help_to_rst(command):
    '''
    Returns the ReStructuredText output for a command by formatting
    the help output of a command, or, if it is an "excluded" command,
    it formats the command's description instead.
    '''
    if command in EXCLUDE_COMMANDS:
        rst = '.. code-block:: text\n\n'
        rst += '   ' + command
        description = HELP_OUTPUT[command]
        if not description:
            return rst
        # uncapitalize the first letter of the description for consistency
        # with commands whose description doesn't come from this dict
        description = description[0].lower() + description[1:]
        rst += ' : ' + description
        return rst

    help_str = get_help_for_command(command)
    rst = ''
    blocks = help_str.split('\n\n')
    # this variable is used to indicate if a block of
    # text is a subcommand or not, so it doesn't separate
    # blocks of text that might be examples of usage.
    inside_subcommand_help = True
    for block in blocks:
        if inside_subcommand_help:
            inside_subcommand_help = False
            rst += '.. code-block:: text\n\n'
        for line in block.splitlines():
            USAGE_TEXT = 'Usage: '
            if line.startswith(USAGE_TEXT):
                inside_subcommand_help = True
                line = line[len(USAGE_TEXT):]
            if line.startswith('  '):
                line = '   ' + line.strip()
            rst += '   ' + line + '\n'
    return rst

def generate_command_section_help(command):
    rst_title = command + '\n' + ('-' * len(command)) + '\n\n'
    rst = rst_title + command_help_to_rst(command)
    return rst

def make_file_name(command):
    file_name = command
    file_name = file_name.replace(' ', '-')
    file_name = file_name.replace('?', 'question')
    file_name = file_name.replace('.', 'point')
    return file_name

def create_index_file(commands):
    commands_toc = ''
    file_contents = CLI_COMMANDS_SECTION
    for command in commands:
        file_contents += '   ' + CLI_COMMANDS_BASE_NAME + '/' + make_file_name(command) + '\n'
    file_obj = open(CLI_COMMANDS_SECTION_FILE_PATH, 'w')
    file_obj.write(file_contents)
    file_obj.close()

def create_command_file(command):
    command_file_path = CLI_COMMANDS_DIR + make_file_name(command) + '.rst'
    file_obj = open(command_file_path, 'w')
    file_obj.write(generate_command_section_help(command))
    file_obj.close()

if __name__ == '__main__':
    HELP_OUTPUT = get_commands_and_desc()
    sorted_commands = sorted(HELP_OUTPUT.keys())
    create_index_file(sorted_commands)
    if not os.path.exists(CLI_COMMANDS_DIR):
        os.makedirs(CLI_COMMANDS_DIR)
    for command in sorted_commands:
        create_command_file(command)
