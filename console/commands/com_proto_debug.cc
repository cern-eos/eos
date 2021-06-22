//------------------------------------------------------------------------------
// @file: com_proto_debug.cc
// @author: Fabio Luchetti - CERN
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
#include "console/ConsoleMain.hh"
#include "console/commands/ICmdHelper.hh"

extern int com_debug(char*);
void com_debug_help();

//------------------------------------------------------------------------------
//! Class DebugHelper
//------------------------------------------------------------------------------
class DebugHelper : public ICmdHelper
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param opts global options
  //----------------------------------------------------------------------------
  DebugHelper(const GlobalOptions& opts):
    ICmdHelper(opts)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~DebugHelper() override = default;

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

  if (token == "get") {
    eos::console::DebugProto_GetProto* get = debugproto->mutable_get();
    get->set_placeholder(true);
  } else if (token == "this") {
    global_debug = !global_debug;
    gGlobalOpts.mDebug = global_debug;
    fprintf(stdout, "info: toggling shell debugmode to debug=%d\n", global_debug);
    mIsLocal = true;
  } else {
    // token should be one of [debug info warning notice err crit alert emerg]
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
            }

            set->set_filter(token);
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

  DebugHelper debug(gGlobalOpts);

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
      << " usage:\n"
      << "debug get|this|<level> [node-queue] [--filter <unitlist>]\n"
      << "'[eos] debug ...' allows to get or set the verbosity of the EOS log files in MGM and FST services.\n"
      << std::endl
      << "Options:\n"
      << std::endl
      << "debug get : retrieve the current log level for the mgm and fsts node-queue\n"
      << std::endl
      << "debug this : toggle EOS shell debug mode\n"
      << std::endl
      << "debug  <level> [--filter <unitlist>] : set the MGM where the console is connected to into debug level <level>\n"
      << std::endl
      << "debug  <level> <node-queue> [--filter <unitlist>] : set the <node-queue> into debug level <level>.\n"
      << "\t - <node-queue> are internal EOS names e.g. '/eos/<hostname>:<port>/fst'\n"
      << "\t - <unitlist> is a comma separated list of strings of software units which should be filtered out in the message log!\n"
      << std::endl
      << "The default filter list is:\n"
      << "'Process,AddQuota,Update,UpdateHint,UpdateQuotaStatus,SetConfigValue,Deletion,GetQuota,PrintOut,RegisterNode,SharedHash,listenFsChange,placeNewReplicas,"
      << "placeNewReplicasOneGroup,accessReplicas,accessReplicasOneGroup,accessHeadReplicaMultipleGroup,updateTreeInfo,updateAtomicPenalties,updateFastStructures,work'.\n"
      << std::endl
      << "The allowed debug levels are:\n"
      << "debug,info,warning,notice,err,crit,alert,emerg\n"
      << std::endl
      << "Examples:\n"
      << "\t debug info *                         set MGM & all FSTs into debug mode 'info'\n"
      << std::endl
      << "\t debug err /eos/*/fst                 set all FSTs into debug mode 'info'\n"
      << std::endl
      << "\t debug crit /eos/*/mgm                set MGM into debug mode 'crit'\n"
      << std::endl
      << "\t debug debug --filter MgmOfsMessage   set MGM into debug mode 'debug' and filter only messages coming from unit 'MgmOfsMessage'.\n"
      << std::endl;
  std::cerr << oss.str() << std::endl;
}
