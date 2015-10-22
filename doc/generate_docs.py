#!/usr/bin/python
# ------------------------------------------------------------------------------
# File: generate_docs.py
# Author: Joaquim Rocha <jrocha@cern.ch>
#         Elvin-Alin Sindrilaru <esindril@cern.ch>
# ------------------------------------------------------------------------------
#
# ******************************************************************************
# EOS - the CERN Disk Storage System
# Copyright (C) 2015 CERN/Switzerland
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
# ******************************************************************************

""" This script generates the Sphinx documentation located in /eos/doc/. First
    it generates the documentation of the CLI and then it runs Sphinx on the
    rest of the documentation. It should be run like this:

    #> python generate_docs.py

    In principle you don't need to run this explicitly. You generate the docu-
    mentation after running cmake using:

    #> make doc
"""

from __future__ import unicode_literals
from __future__ import print_function
from errno import EIO
import sys
import os
import subprocess
import shutil

SPHINX_BUILD = "/usr/bin/sphinx-build"
EOS_EXE = "/usr/bin/eos"
CMD_NAME_CONVERT = {".q": "pointq", "?": "question"}

def get_dict_cmd_info():
    """ Return a dictionary of eos commands and their description """
    if not os.path.exists(EOS_EXE):
	raise OSError("could not find \"eos\" executable")

    cmds_out, __ = subprocess.Popen(["{0} help".format(EOS_EXE)],
				    stdout=subprocess.PIPE,
				    stderr=subprocess.PIPE,
				    shell=True).communicate()
    cmd_dict = {}


    for line in cmds_out.splitlines()[1:]:
	[cmd, desc] = line.split(' ',1 )
	cmd_dict[cmd] = desc

    return cmd_dict

def generate_summary_rst(file_path, cmd_lst):
    """ Generate the clicommands.rst containing the list of all the commands
    for which a description is provided.

    Args:
	file_path (string): Absolute path to file
	cmd_lst  (lst): List of all the commands
    """
    header ='\n'.join([".. _clientcommands:",
		       "",
		       "Client Commands",
		       "================",
		       "",
		       ".. toctree::",
		       "  :maxdepth: 2",
		       "", ""])

    with open(file_path, 'w') as f:
	f.write(header)

	for cmd in cmd_lst:
	    if cmd in CMD_NAME_CONVERT:
		fn_cmd = CMD_NAME_CONVERT[cmd]
	    else:
		fn_cmd = cmd

	    f.write("  clicommands/{0}\n".format(fn_cmd))

def format_cmd_help(cmd):
    """ Format the help output of the command to be written int the .rst file.

    Args:
	cmd (string): EOS command

    Return:
	Contents of the rst file for this command
    """
    out, __ = subprocess.Popen([EOS_EXE, cmd, "-h"],
			       stdout=subprocess.PIPE,
			       stderr=subprocess.PIPE).communicate()
    if '--help-all' in out:
	sys.stdout.flush()
	out, __ = subprocess.Popen([EOS_EXE, cmd, "-H"],
				   stdout=subprocess.PIPE,
				   stderr=subprocess.PIPE).communicate()
    sys.stdout.flush()

    # The first line usually contains the command, so we remove it
    if out.startswith(cmd):
	out = out[len(cmd) + 4:]

    blocks = out.split("\n\n")
    out_rst = ''.join([cmd, '\n', ('-' * len(cmd)), '\n\n'])
    # Decide if the current block of text is a subcommand or not
    inside_subcmd = True

    for block in blocks:
	if inside_subcmd:
	    inside_subcmd = False
	    out_rst += ".. code-block:: text\n\n"

	for line in block.splitlines():
	    usage_tag = "Usage: "
	    space_tag = "  "

	    if line.startswith(usage_tag):
		inside_subcmd = True
		line = line[len(usage_tag):]

	    if line.startswith(space_tag):
		line = ''.join([space_tag, line.strip()])

	    out_rst += ''.join([space_tag, line, '\n'])

    return out_rst

def format_cmd_desc(cmd, desc):
    """ Format the description output of the command to be written to the .rst
	file.

    Args:
	cmd (string): EOS command
	desc (string): Command description

    Return:
	Contents of the .rst file for this command.
    """
    space_tag = "  "
    # Uncapitalize first letter of description
    desc = desc[0].lower() + desc[1:]
    title_rst = ''.join([cmd, '\n', ('-' * len(cmd)), '\n\n'])
    out_rst = ''.join([title_rst, ".. code-block:: text\n\n", space_tag, cmd,
		       " : ", desc])
    return out_rst

def generate_cmds_rst(root_dir, cmd_dict):
    """ Generate .rst description files for the individual commands

    Args:
	root_dir (string): Absolute path where file are saved
	cmd_dict (dict): Dictionary with commands and their description
    """
    # List of commands that don't have "-h" flag, so we just put their
    # description in the .rst file.
    desc_only_lst = ['console', '?', '.q', 'license', 'version', 'motd', 'pwd',
		     'silent', 'touch', 'whoami', 'json', 'exit', 'timing', 'help',
		     'quit', 'member', 'rtlog']

    for (cmd, desc) in cmd_dict.iteritems():
	print("Processing command {0}".format(cmd), file=sys.stdout)

	if cmd in desc_only_lst:
	    out_rst = format_cmd_desc(cmd, desc)
	else:
	    out_rst = format_cmd_help(cmd)

	# For strange commands rename the .rst filenames
	if cmd in CMD_NAME_CONVERT:
	    fn_cmd = CMD_NAME_CONVERT[cmd]
	else:
	    fn_cmd = cmd

	fpath_rst = ''.join([root_dir, "/", fn_cmd, ".rst"])
	with open(fpath_rst, 'w') as f:
	    f.write(out_rst)

def main():
    """ Main function """
    try:
	cmd_dict = get_dict_cmd_info()
    except OSError as e:
	print("ERROR: {0}".format(e))
	sys.exit(EIO)

    # Create summary file clicommands.rst and directory containing the description
    # of the individual commands
    cwd = os.getcwd() # this should be ~/eos/doc
    doc_dest = ''.join([cwd, "/html"])
    cli_cmd_dir = ''.join([cwd, "/clicommands"])
    cli_cmd_file = ''.join([cli_cmd_dir, ".rst"])

    # Clean up old documentation
    if (os.path.exists(doc_dest) or os.path.exists(cli_cmd_file)
	or os.path.exists(cli_cmd_dir)):

	print("INFO: Clean up old documentation", file=sys.stdout)

	for entry in [doc_dest, cli_cmd_dir, cli_cmd_file]:
	    try:
		if os.path.isdir(entry):
		    shutil.rmtree(entry)
		else:
		    os.remove(entry)
	    except OSError as __:
		pass # entry already deleted

    try:
	os.makedirs(cli_cmd_dir)
    except OSError as e:
	print("ERROR: {0}".format(e), file=sys.stderr)
	sys.exit(EIO)

    try:
	os.mknod(cli_cmd_file)
    except Exception as e:
	print("ERROR: {0}".format(e), file=sys.stderr)
	sys.exit(EIO)

    # Generate the CLI commands documentation
    cmd_lst = cmd_dict.keys()
    generate_summary_rst(cli_cmd_file, cmd_lst)
    generate_cmds_rst(cli_cmd_dir, cmd_dict)

    # Generate the rest of the documentation using Sphinx
    print("Generating Sphinx documentation ...")
    sphinx_proc = subprocess.Popen([SPHINX_BUILD, "-b", "html", cwd, doc_dest],
				   stdout=subprocess.PIPE,
				   stderr=subprocess.PIPE)
    out, err = sphinx_proc.communicate()

    if sphinx_proc.returncode:
	print("ERROR: sphinx-build failed, msg={0}".format(err))
	sys.exit(sphinx_proc.returncode)

if __name__ == '__main__':
    main()
