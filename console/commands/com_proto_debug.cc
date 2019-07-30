//------------------------------------------------------------------------------
// File: com_proto_debug.cc
// Author: Fabio Luchetti - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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
#include "common/Logging.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/ICmdHelper.hh"

void com_debug_help();

//------------------------------------------------------------------------------
//! Class DebugHelper
//------------------------------------------------------------------------------
class DebugHelper : public ICmdHelper
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  DebugHelper() = default;

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~DebugHelper() = default;

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
bool DebugHelper::ParseCommand(const char* arg)
{
  eos::console::DebugProto* debugproto = mReq.mutable_debug();
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();
  std::string token;

  if (!tokenizer.NextToken(token)) {
    return false;
  }

  if ((token == "-h") || (token == "--help")) {
    return false;
  }

  if (token == "get") {
    eos::console::DebugProto_GetProto* get = debugproto->mutable_get();
    get->set_placeholder(true);
  } else if (token == "this") {
    debug = !debug;
    fprintf(stdout, "info: toggling shell debugmode to debug=%d\n", debug);
    eos::common::Logging& g_logging = eos::common::Logging::GetInstance();

    if (debug) {
      g_logging.SetLogPriority(LOG_DEBUG);
    } else {
      g_logging.SetLogPriority(LOG_NOTICE);
    }

    mIsLocal = true;
  } else {
    // token must be one of [debug info warning notice err crit alert emerg]
    eos::console::DebugProto_SetProto* set = debugproto->mutable_set();
    set->set_debuglevel(token);

    if (tokenizer.NextToken(token)) {
      if (token == "--filter") {
        if (!tokenizer.NextToken(token)) {
          return false;
        }

        set->set_filter(token);
      } else {
        set->set_nodename(token);

        if (tokenizer.NextToken(token)) {
          if (token != "--filter") {
            return false;
          } else {
            if (!tokenizer.NextToken(token)) {
              return false;
            } else {
              set->set_filter(token);
            }
          }
        }
      }
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Debug CLI
//------------------------------------------------------------------------------
int
com_protodebug(char* arg)
{
  if (wants_help(arg)) {
    com_debug_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  DebugHelper debug;

  if (!debug.ParseCommand(arg)) {
    com_debug_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = debug.Execute();
  return global_retc;
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_debug_help()
{
  std::ostringstream oss;
  oss
      << "Usage: debug get|this|<level> [node-queue] [--filter <unitlist>]"
      << std::endl
      << "'[eos] debug ...' allows to get or set the verbosity of the EOS log files in MGM and FST services."
      << std::endl
      << std::endl
      << "Options: "
      << std::endl
      << std::endl
      << "debug get"
      << std::endl
      << "\t : retrieve the current log level for the mgm and fsts node-queue"
      << std::endl
      << std::endl
      << "debug this"
      << std::endl
      << "\t : toggle EOS shell debug mode"
      << std::endl
      << std::endl
      << "debug  <level> [--filter <unitlist>]"
      << std::endl
      << "\t : set the MGM where the console is connected to into debug level <level>"
      << std::endl
      << std::endl
      << "debug  <level> <node-queue> [--filter <unitlist>]"
      << std::endl
      << "\t : set the <node-queue> into debug level <level>."
      << std::endl
      << "\t - <node-queue> are internal EOS names e.g. '/eos/<hostname>:<port>/fst'"
      << std::endl
      << "\t - <unitlist> is a comma separated list of strings of software units which should be filtered out in the message log!"
      << std::endl
      << "\t   The default filter list is: 'Process,AddQuota,Update,UpdateHint,UpdateQuotaStatus,SetConfigValue,Deletion,GetQuota,PrintOut,RegisterNode,SharedHash,listenFsChange,"
      << std::endl
      << "\t   placeNewReplicas,placeNewReplicasOneGroup,accessReplicas,accessReplicasOneGroup,accessHeadReplicaMultipleGroup,updateTreeInfo,updateAtomicPenalties,updateFastStructures,work'."
      << std::endl
      << std::endl
      << "The allowed debug levels are: debug info warning notice err crit alert emerg"
      << std::endl
      << std::endl
      << "Examples:" << std::endl
      << "  debug info *                         set MGM & all FSTs into debug mode 'info'"
      << std::endl
      << std::endl
      << "  debug err /eos/*/fst                 set all FSTs into debug mode 'info'"
      << std::endl
      << std::endl
      << "  debug crit /eos/*/mgm                set MGM into debug mode 'crit'" <<
      std::endl
      << std::endl
      << "  debug debug --filter MgmOfsMessage   set MGM into debug mode 'debug' and filter only messages coming from unit 'MgmOfsMessage'."
      << std::endl;
  std::cerr << oss.str() << std::endl;
}
