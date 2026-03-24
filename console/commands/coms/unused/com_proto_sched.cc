// ----------------------------------------------------------------------
// File: com_proto_sched.cc
// Author: Abhishek Lekshmanan - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                           *
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

#include "common/StringTokenizer.hh"
#include "common/StringConversion.hh"
#include "common/StringUtils.hh"
#include "common/ParseUtils.hh"

#include "console/ConsoleMain.hh"
#include "console/commands/ICmdHelper.hh"

void com_sched_help();

//------------------------------------------------------------------------------
//! Class SchedHelper
//------------------------------------------------------------------------------
struct SchedHelper: public ICmdHelper
{
  SchedHelper(const GlobalOptions& opts):
    ICmdHelper(opts)
  {
    mIsAdmin = true;
  }

  ~SchedHelper() = default;

  bool ParseCommand(const char* arg) override;
};

bool SchedHelper::ParseCommand(const char* arg)
{
  eos::console::SchedProto* sched = mReq.mutable_sched();
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();
  std::string token;

  if (!tokenizer.NextToken(token)) {
    return false;
  }

  if (token == "configure" || token == "config") {
    eos::console::SchedProto_ConfigureProto* config = sched->mutable_config();

    if (!tokenizer.NextToken(token)) {
      return false;
    }

    if (token == "type") {
      eos::console::SchedProto_TypeProto* type = config->mutable_type();

      if (!tokenizer.NextToken(token)) {
        return false;
      }

      type->set_schedtype(token);
    } else if (token == "weight") {
      eos::console::SchedProto_WeightProto* weight_prot = config->mutable_weight();

      std::string space, id_str, weight_str;
      bool status = tokenizer.NextToken(space) && tokenizer.NextToken(id_str) && tokenizer.NextToken(weight_str);
      if (!status) {
        return false;
      }

      int32_t item_id;
      uint8_t weight;
      eos::common::StringToNumeric(id_str, item_id);
      eos::common::StringToNumeric(weight_str, weight);

      weight_prot->set_id(item_id);
      weight_prot->set_weight(weight);
      weight_prot->set_spacename(space);
    } else if (token == "show") {
      eos::console::SchedProto_ShowProto* show_prot = config->mutable_show();
      if (!tokenizer.NextToken(token)) {
        return false;
      }

      if (token == "type") {
        show_prot->set_option(eos::console::SchedProto_ShowProto::TYPE);
        if (tokenizer.NextToken(token)) {
          show_prot->set_spacename(token);
        }
      }
    } else if (token == "forcerefresh") {
      // TODO: implement a space level refresh command; however it requires a
      // deep copy of the internal spacemap ptrs of all other spaces other than
      // the asked space probably easier to just do a full refresh
      config->mutable_refresh();
    }

  } else if (token == "ls") {
    // Implement me!
    eos::console::SchedProto_LsProto* ls = sched->mutable_ls();

    if (!tokenizer.NextToken(token)) {
      return false;
    }
    ls->set_spacename(token);

    if (!tokenizer.NextToken(token)) {
      return false;
    }

    if (token == "bucket") {
      ls->set_option(eos::console::SchedProto_LsProto::BUCKET);
    } else if (token == "disk") {
      ls->set_option(eos::console::SchedProto_LsProto::DISK);
    } else {
      ls->set_option(eos::console::SchedProto_LsProto::ALL);
    }
  }

  return true;
}

int com_proto_sched(char* arg)
{
  if (wants_help(arg)) {
    com_sched_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  SchedHelper sched(gGlobalOpts);

  if (!sched.ParseCommand(arg)) {
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = sched.Execute();
  return global_retc;
}

void com_sched_help()
{
  std::ostringstream oss;
  oss << " Usage:\n"
      << " sched configure type <schedtype>\n"
      << "\t <schedtype> is one of roundrobin,weightedrr,tlrr,random,weightedrandom,geo\n"
      << "\t if configured via space; space takes precedence\n"
      << " sched configure weight <space> <fsid> <weight>\n"
      << "\t configure weight for a given fsid in the given space\n"
      << " sched configure show type [spacename]\n"
      << "\t show existing configured scheduler; optionally for space\n"
      << " sched configure forcerefresh [spacename]\n"
      << "\t Force refresh scheduler internal state\n"
      << " ls <spacename> <bucket|disk|all>\n"
      << std::endl;
  std::cerr << oss.str();
}
