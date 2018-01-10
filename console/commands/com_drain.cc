// ----------------------------------------------------------------------
// File: com_drain.cc
// Author: Andrea Manzi - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

/*----------------------------------------------------------------------------*/
#include "console/commands/ICmdHelper.hh"
#include "console/ConsoleMain.hh"
#include "common/Drain.pb.h"
#include "common/StringConversion.hh"
#include "common/StringTokenizer.hh"
/*----------------------------------------------------------------------------*/

using eos::console::DrainProto;

void com_drain_help();

//------------------------------------------------------------------------------
//! Class DrainHelper
//------------------------------------------------------------------------------
class DrainHelper: public ICmdHelper
{
public:
  DrainHelper()
  {
    mIsAdmin = true;
  }
  //----------------------------------------------------------------------------
  //! Parse command line input
  //!
  //! @param arg input
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseCommand(const char* arg);

};

//------------------------------------------------------------------------------
// Parse command line input
//------------------------------------------------------------------------------
bool
DrainHelper::ParseCommand(const char* arg)
{
  const char* option;
  eos::console::DrainProto* drain = mReq.mutable_drain();
  // split subcommands
  eos::common::StringTokenizer subtokenizer(arg);
  subtokenizer.GetLine();
  option = subtokenizer.GetToken();
  std::string subcommand = (option ? option : "");

  if (subcommand == "start") {
      drain->set_op(DrainProto::START);
  }
  if (subcommand == "stop") {
      drain->set_op(DrainProto::STOP);
  }
  if (subcommand == "clear") {
      drain->set_op(DrainProto::CLEAR);
  }
  if (subcommand == "status") {
      drain->set_op(DrainProto::STATUS);
  }
  const char* fsid = subtokenizer.GetToken();
  const char* targetFsId = subtokenizer.GetToken();

  if (fsid) {
      int ifsid = atoi(fsid);

      if (ifsid == 0 ) {
        return false;
      }
  } else if (subcommand != "status") {
        return false;
  } else {
    fsid="0";
  }

  if (subcommand == "start") {
    if (targetFsId) {
      int ifsid = atoi(targetFsId);

      if (ifsid == 0 ) {
        return false;
      }
    } else {
      targetFsId="0";
    }
  } else {
    targetFsId="0";
  }

  drain->set_fsid(fsid);
  drain->set_targetfsid(targetFsId);
  return true;
}


//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_drain_help()
{
  fprintf(stdout, "'[eos] drain ..' provides the drain interface of EOS.\n");
  fprintf(stdout,
          "Usage: drain start|stop|status [OPTIONS]\n");
  fprintf(stdout, "Options:\n");
  fprintf(stdout, "drain start <fsid> [<targetFsId>]: \n");
  fprintf(stdout,
          "                                                  start the draining of the given fsid. If a targetFsId is specified, the drain process will move the replica to that fs\n\n");
  fprintf(stdout, "drain stop <fsid> : \n");
  fprintf(stdout,
          "                                                  stop the draining of the given fsid.\n\n");
  fprintf(stdout, "drain clear <fsid> : \n");
  fprintf(stdout,
          "                                                  clear the draining info for the given fsid.\n\n");
  fprintf(stdout, "drain status [fsid] :\n");
  fprintf(stdout,
          "                                                  show the status of the drain activities on the system. If the fsid is specified shows detailed info about that fs drain\n");
  fprintf(stdout, "Report bugs to eos-dev@cern.ch.\n");
  global_retc = EINVAL;
}


//------------------------------------------------------------------------------
//  entrypoint
//------------------------------------------------------------------------------
int com_drain(char* arg)
{
  if (wants_help(arg)) {
    com_drain_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  DrainHelper drain;

  if (!drain.ParseCommand(arg)) {
    com_drain_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = drain.Execute();
  return global_retc;
}
