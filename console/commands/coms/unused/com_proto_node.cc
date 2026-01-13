//------------------------------------------------------------------------------
// @file: com_proto_node.cc
// @author: Fabio Luchetti - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your token) any later version.                                   *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "console/ConsoleMain.hh"
#include "console/commands/helpers/NodeHelper.hh"

extern int com_node(char*);
void com_node_help();

//------------------------------------------------------------------------------
// Node command entry point
//------------------------------------------------------------------------------
int com_protonode(char* arg)
{
  if (wants_help(arg)) {
    com_node_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  NodeHelper node(gGlobalOpts);

  if (!node.ParseCommand(arg)) {
    com_node_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = node.Execute();
  return global_retc;
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_node_help()
{
  std::ostringstream oss;
  oss
      << " usage:\n"
      << "node ls [-s] [-b|--brief] [-m|-l|--sys|--io|--fsck] [<node>] : list all nodes or only <node>. <node> is a substring match and can be a comma seperated list\n"
      << "\t      -s : silent mode\n"
      << "\t      -b : display host names without domain names\n"
      << "\t      -m : monitoring key=value output format\n"
      << "\t      -l : long output - list also file systems after each node\n"
      << "\t    --io : print IO statistics\n"
      << "\t   --sys : print SYS statistics (memory + threads)\n"
      << "\t  --fsck : print filesystem check statistcis\n"
      << std::endl
      << "node config <host:port> <key>=<value : configure file system parameters for each filesystem of this node\n"
      << "\t    <key> : error.simulation=io_read|io_write|xs_read|xs_write|fmd_open|fake_write|close|unresponsive\n"
      << "\t            If offset is given then the error will get triggered for requests past the given value.\n"
      << "\t            Accepted format for offset: 8B, 10M, 20G etc.\n"
      << "\t            fmd_open            : simulate a file metadata mismatch when opening a file\n"
      << "\t            open_delay[_<sec>]  : add by default 120 sec delay per open operation\n"
      << "\t            read_delay[_<sec>]  : add by default 10 sec delay per read operation\n"
      << "\t            io_read[_<offset>]  : simulate read errors\n"
      << "\t            io_write[_<offset>] : simulate write errors\n"
      << "\t            xs_read             : simulate checksum errors when reading a file\n"
      << "\t            xs_write[_<sec>]    : simulate checksum errors on write with an optional delay, default 0\n"
      << "\t            fake_write          : do not really write data to disk\n"
      << "\t            close               : return an error on close\n"
      << "\t            close_commit_mgm    : simulate error during close commit to MGM\n"
      << "\t            unresponsive        : emulate a write/close request taking 2 minutes\n"
      << "\t            <none>              : disable error simulation (any value other than the previous ones is fine!)\n"
      << "\t    <key> : publish.interval=<sec> - set the filesystem state publication interval to <sec> seconds\n"
      << "\t    <key> : debug.level=<level>    - set the node into debug level <level> [default=notice] -> see debug --help for available levels\n"
      << "\t    <key> : stripexs=on|off        - enable/disable synchronously stripe checksum computation\n"
      << "\t    <key> : for other keys see help of 'fs config' for details\n"
      << std::endl
      << "node set <queue-name>|<host:port> on|off                 : activate/deactivate node\n"
      << std::endl
      << "node rm  <queue-name>|<host:port>                        : remove a node\n"
      << std::endl
      << "node txgw <queue-name>|<host:port> <on|off> : enable (on) or disable (off) node as a transfer gateway\n"
      << std::endl
      << "node proxygroupadd <group-name> <queue-name>|<host:port> : add a node to a proxy group\n"
      << std::endl
      << "node proxygrouprm <group-name> <queue-name>|<host:port> : rm a node from a proxy group\n"
      << std::endl
      << "node proxygroupclear <queue-name>|<host:port> : clear the list of groups a node belongs to\n"
      << std::endl
      << "node status <queue-name>|<host:port> : print's all defined variables for a node\n"
      << std::endl;
  std::cerr << oss.str() << std::endl;
}
