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
      << "\t    <key> : gw.rate=<mb/s> - set the transfer speed per gateway transfer\n"
      << "\t    <key> : gw.ntx=<#>     - set the number of concurrent transfers for a gateway node\n"
      << "\t    <key> : error.simulation=io_read|io_write|xs_read|xs_write|fmd_open\n"
      << "\t            If offset is given the the error will get triggered for request past the given value.\n"
      << "\t            Accepted format for offset: 8B, 10M, 20G etc.\n"
      << "\t            io_read[_<offset>]  : simulate read  errors\n"
      << "\t            io_write[_<offset>] : simulate write errors\n"
      << "\t            xs_read             : simulate checksum errors when reading a file\n"
      << "\t            xs_write            : simulate checksum errors when writing a file\n"
      << "\t            fmd_open            : simulate a file metadata mismatch when opening a file\n"
      << "\t            <none>              : disable error simulation (every value than the previous ones are fine!)\n"
      << "\t    <key> : publish.interval=<sec> - set the filesystem state publication interval to <sec> seconds\n"
      << "\t    <key> : debug.level=<level> - set the node into debug level <level> [default=notice] -> see debug --help for available levels\n"
      << "\t    <key> : for other keys see help of 'fs config' for details\n"
      << std::endl
      << "node set <queue-name>|<host:port> on|off                 : activate/deactivate node\n"
      << std::endl
      << "node rm  <queue-name>|<host:port>                        : remove a node\n"
      << std::endl
      << "node register <host:port|*> <path2register> <space2register> [--force] [--root] : register filesystems on node <host:port>\n"
      << "\t      <path2register> is used as match for the filesystems to register e.g. /data matches filesystems /data01 /data02 etc. ... /data/ registers all subdirectories in /data/\n"
      << "\t      <space2register> is formed as <space>:<n> where <space> is the space name and <n> must be equal to the number of filesystems which are matched by <path2register> e.g. data:4 or spare:22 ...\n"
      << "\t      --force : removes any existing filesystem label and re-registers\n"
      << "\t      --root  : allows to register paths on the root partition\n"
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
