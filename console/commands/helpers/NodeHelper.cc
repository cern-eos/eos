//------------------------------------------------------------------------------
//! @file NodeHelper.cc
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "console/commands/helpers/NodeHelper.hh"
#include "common/StringTokenizer.hh"

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
