//------------------------------------------------------------------------------
// File: IoCmd.cc
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

#include "IoCmd.hh"
#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Iostat.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Method implementing the specific behavior of the command executed
//------------------------------------------------------------------------------
eos::console::ReplyProto
IoCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  eos::console::IoProto io = mReqProto.io();
  eos::console::IoProto::SubcmdCase subcmd = io.subcmd_case();

  switch (subcmd) {
  case eos::console::IoProto::kStat:
    StatSubcmd(io.stat(), reply);
    break;

  case eos::console::IoProto::kEnable:
    EnableSubcmd(io.enable(), reply);
    break;

  case eos::console::IoProto::kReport:
    ReportSubcmd(io.report(), reply);
    break;

  case eos::console::IoProto::kNs:
    NsSubcmd(io.ns(), reply);
    break;

  default:
    reply.set_retc(EINVAL);
    reply.set_std_err("error: not supported");
  }

  return reply;
}

//------------------------------------------------------------------------------
// Execute stat subcommand
//------------------------------------------------------------------------------
void IoCmd::StatSubcmd(const eos::console::IoProto_StatProto& stat,
                       eos::console::ReplyProto& reply)
{
  XrdOucString out = "";

  // If nothing is selected, we show the summary information
  if (!(stat.apps() || stat.domain() || stat.top() || stat.details())) {
    gOFS->IoStats->PrintOut(out, true, stat.details(),
                            stat.monitoring(), stat.numerical(),
                            stat.top(), stat.domain(), stat.apps());
  } else {
    gOFS->IoStats->PrintOut(out, stat.summary(), stat.details(),
                            stat.monitoring(), stat.numerical(),
                            stat.top(), stat.domain(), stat.apps());
  }

  reply.set_std_out(out.c_str());
  reply.set_retc(0);
}

//------------------------------------------------------------------------------
// Execute enable subcommand
//------------------------------------------------------------------------------
void IoCmd::EnableSubcmd(const eos::console::IoProto_EnableProto& enable,
                         eos::console::ReplyProto& reply)
{
  int ret_c {0};
  std::string out, err;

  if (enable.switchx()) {
    if ((!enable.reports()) && (!enable.namespacex())) {
      if (enable.upd_address().length()) {
        if (gOFS->IoStats->AddUdpTarget(enable.upd_address().c_str())) {
          out += ("success: enabled IO udp target " + enable.upd_address()).c_str();
        } else {
          err += ("error: IO udp target was not configured " +
                  enable.upd_address()).c_str();
          ret_c = EINVAL;
        }
      } else {
        if (enable.popularity()) {
          // Always enable collection otherwise we don't get anything for
          // popularity reporting
          gOFS->IoStats->Start();

          if (gOFS->IoStats->StartPopularity()) {
            out += "success: enabled IO popularity collection";
          } else {
            err += "error: IO popularity collection already enabled";
            ret_c = EINVAL;
          }
        } else {
          if (gOFS->IoStats->StartCollection()) {
            out += "success: enabled IO report collection";
          } else {
            err += "error: IO report collection already enabled";
            ret_c = EINVAL;
          }
        }
      }
    } else {
      if (enable.reports()) {
        if (gOFS->IoStats->StartReport()) {
          out += "success: enabled IO report store";
        } else {
          err += "error: IO report store already enabled";
          ret_c = EINVAL;
        }
      }

      if (enable.namespacex()) {
        if (gOFS->IoStats->StartReportNamespace()) {
          out += "success: enabled IO report namespace";
        } else {
          err += "error: IO report namespace already enabled";
          ret_c = EINVAL;
        }
      }
    }
  } else {
    if ((!enable.reports()) && (!enable.namespacex())) {
      if (enable.upd_address().length()) {
        if (gOFS->IoStats->RemoveUdpTarget(enable.upd_address().c_str())) {
          out += ("success: disabled IO udp target " + enable.upd_address()).c_str();
        } else {
          err += ("error: IO udp target was not configured " +
                  enable.upd_address()).c_str();
          ret_c = EINVAL;
        }
      } else {
        if (enable.popularity()) {
          if (gOFS->IoStats->StopPopularity()) {
            out += "success: disabled IO popularity collection";
          } else {
            err += "error: IO popularity collection already disabled";
            ret_c = EINVAL;
          }
        } else {
          if (gOFS->IoStats->StopCollection()) {
            out += "success: disabled IO report collection";
          } else {
            err += "error: IO report collection already disabled";
            ret_c = EINVAL;
          }
        }
      }
    } else {
      if (enable.reports()) {
        if (gOFS->IoStats->StopReport()) {
          out += "success: disabled IO report store";
        } else {
          err += "error: IO report store already enabled";
          ret_c = EINVAL;
        }
      }

      if (enable.namespacex()) {
        if (gOFS->IoStats->StopReportNamespace()) {
          out += "success: disabled IO report namespace";
        } else {
          err += "error: IO report namespace already disabled";
          ret_c = EINVAL;
        }
      }
    }
  }

  if (ret_c) {
    reply.set_std_err(err.c_str());
  } else {
    reply.set_std_out(out.c_str());
  }

  reply.set_retc(ret_c);
}

//------------------------------------------------------------------------------
// Execute report subcommand
//------------------------------------------------------------------------------
void IoCmd::ReportSubcmd(const eos::console::IoProto_ReportProto& report,
                         eos::console::ReplyProto& reply)
{
  XrdOucString out = "";
  XrdOucString err = "";;

  if (mVid.uid == 0) {
    (void) Iostat::NamespaceReport(report.path().c_str(), out, err);
  }

  reply.set_std_out(out.c_str());
  reply.set_std_err(err.c_str());
  reply.set_retc(0);
}

//------------------------------------------------------------------------------
// Execute ns subcommand
//------------------------------------------------------------------------------
void IoCmd::NsSubcmd(const eos::console::IoProto_NsProto& ns,
                     eos::console::ReplyProto& reply)
{
  std::string option;

  if (ns.monitoring()) {
    option += "-m";
  }

  if (ns.rank_by_byte()) {
    option += "-b";
  }

  if (ns.rank_by_access()) {
    option += "-n";
  }

  if (ns.last_week()) {
    option += "-w";
  }

  if (ns.hotfiles()) {
    option += "-f";
  }

  switch (ns.count()) {
  case eos::console::IoProto_NsProto::ONEHUNDRED:
    option += "-100";
    break;

  case eos::console::IoProto_NsProto::ONETHOUSAND:
    option += "-1000";
    break;

  case eos::console::IoProto_NsProto::TENTHOUSAND:
    option += "-10000";
    break;

  case eos::console::IoProto_NsProto::ALL:
    option += "-a";
    break;

  default : // NONE
    break;
  }

  XrdOucString out = "";
  gOFS->IoStats->PrintNs(out, option.c_str());
  reply.set_std_out(out.c_str());
  reply.set_retc(0);
}

EOSMGMNAMESPACE_END
