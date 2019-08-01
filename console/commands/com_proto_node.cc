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
#include "common/StringTokenizer.hh"
#include "console/commands/ICmdHelper.hh"

extern int com_node(char*);
void com_node_help();

//------------------------------------------------------------------------------
//! Class NodeHelper
//------------------------------------------------------------------------------
class NodeHelper : public ICmdHelper
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  NodeHelper()
  {
    mHighlight = true;
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~NodeHelper() override = default;

  //----------------------------------------------------------------------------
  //! Parse command line input
  //!
  //! @param arg input
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseCommand(const char* arg) override;
};

//------------------------------------------------------------------------------
// Parse command line input
//------------------------------------------------------------------------------
bool NodeHelper::ParseCommand(const char* arg)
{
  eos::console::NodeProto* node = mReq.mutable_node();
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();
  std::string token;

  if (!tokenizer.NextToken(token)) {
    return false;
  }

  // one of { ls, set, status, txgw, proxygroupadd|proxygrouprm|proxygroupclear, rm, config, register }
  if (token == "ls") {
    eos::console::NodeProto_LsProto* ls = node->mutable_ls();

    while (tokenizer.NextToken(token)) {
      if (token == "-s") {
        mIsSilent = true;
      } else if (token == "-b" || token == "--brief") {
        ls->set_outhost(true);
      } else if (token == "-m") {
        mHighlight = false;
        ls->set_outformat(eos::console::NodeProto_LsProto::MONITORING);
      } else if (token == "-l") {
        ls->set_outformat(eos::console::NodeProto_LsProto::LISTING);
      } else if (token == "--io") {
        ls->set_outformat(eos::console::NodeProto_LsProto::IO);
      } else if (token == "--sys") {
        ls->set_outformat(eos::console::NodeProto_LsProto::SYS);
      } else if (token == "--fsck") {
        ls->set_outformat(eos::console::NodeProto_LsProto::FSCK);
      } else if ((token.find('-') != 0)) { // does not begin with "-"
        ls->set_selection(token);
      } else {
        return false;
      }
    }
  } else if (token == "rm") {
    if (!tokenizer.NextToken(token)) {
      return false;
    }

    eos::console::NodeProto_RmProto* rm = node->mutable_rm();
    rm->set_node(token);
  } else if (token == "status") {
    if (!tokenizer.NextToken(token)) {
      return false;
    }

    eos::console::NodeProto_StatusProto* status = node->mutable_status();
    status->set_node(token);
  } else if (token == "set") {
    if (!tokenizer.NextToken(token)) {
      return false;
    }

    eos::console::NodeProto_SetProto* set = node->mutable_set();
    set->set_node(token);

    if (!tokenizer.NextToken(token)) {
      return false;
    }

    if (token == "on" || token == "off") {
      set->set_node_state_switch(token);
    } else {
      return false;
    }
  } else if (token == "txgw") {
    if (!tokenizer.NextToken(token)) {
      return false;
    }

    eos::console::NodeProto_TxgwProto* txgw = node->mutable_txgw();
    txgw->set_node(token);

    if (!tokenizer.NextToken(token)) {
      return false;
    }

    if (token == "on" || token == "off") {
      txgw->set_node_txgw_switch(token);
    } else {
      return false;
    }
  } else if (token == "config") {
    if (!tokenizer.NextToken(token)) {
      return false;
    }

    eos::console::NodeProto_ConfigProto* config = node->mutable_config();
    config->set_node_name(token);

    if (!tokenizer.NextToken(token)) {
      return false;
    }

    std::string::size_type pos = token.find('=');

    if (pos != std::string::npos &&
        count(token.begin(), token.end(),
              '=') == 1) {  // contains 1 and only 1 '='. It expects a token like <key>=<value>
      config->set_node_key(token.substr(0, pos));
      config->set_node_value(token.substr(pos + 1, token.length() - 1));
    } else {
      return false;
    }
  } else if (token == "register") {
    if (!tokenizer.NextToken(token)) {
      return false;
    }

    eos::console::NodeProto_RegisterProto* registerx = node->mutable_registerx();
    registerx->set_node_name(token);

    if (!tokenizer.NextToken(token)) {
      return false;
    }

    registerx->set_node_path2register(token);

    if (!tokenizer.NextToken(token)) {
      return false;
    }

    registerx->set_node_space2register(token);

    // repeats twice to (eventually) parse both flags.
    if (tokenizer.NextToken(token)) {
      if (token == "--force") {
        registerx->set_node_force(true);
      } else if (token == "--root") {
        registerx->set_node_root(true);
      } else {
        return false;
      }
    }

    if (tokenizer.NextToken(token)) {
      if (token == "--force") {
        registerx->set_node_force(true);
      } else if (token == "--root") {
        registerx->set_node_root(true);
      } else {
        return false;
      }
    }
  } else if (token == "proxygroupadd" || token == "proxygrouprm" ||
             token == "proxygroupclear") {
    eos::console::NodeProto_ProxygroupProto* proxygroup =
      node->mutable_proxygroup();

    if (token == "proxygroupadd") {
      proxygroup->set_node_action(eos::console::NodeProto_ProxygroupProto::ADD);
    } else if (token == "proxygrouprm") {
      proxygroup->set_node_action(eos::console::NodeProto_ProxygroupProto::RM);
    } else if (token == "proxygroupclear") {
      proxygroup->set_node_action(eos::console::NodeProto_ProxygroupProto::CLEAR);
    }

    if (token == "proxygroupclear") {
      if (tokenizer.NextToken(token)) {
        proxygroup->set_node(token);
      } else {
        return false;
      }
    } else {
      if (tokenizer.NextToken(token)) {
        proxygroup->set_node_proxygroup(token);

        if (tokenizer.NextToken(token)) {
          proxygroup->set_node(token);
        } else {
          return false;
        }
      } else {
        return false;
      }
    }
  } else { // no proper subcommand
    return false;
  }

  return true;
}

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

  NodeHelper node;

  if (!node.ParseCommand(arg)) {
    com_node_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = node.Execute(false);

  // Provide compatibility in case the server does not support the protobuf
  // implementation ie. < 4.5.0
  if (global_retc) {
    if (node.GetError().find("Cannot allocate memory") != std::string::npos) {
      global_retc = com_node(arg);
    } else {
      std::cerr << node.GetError();
    }
  }

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
