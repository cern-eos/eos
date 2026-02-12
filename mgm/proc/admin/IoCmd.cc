//------------------------------------------------------------------------------
// @file: IoCmd.cc
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

#include "IoCmd.hh"
#include "mgm/iostat/Iostat.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/proc/ProcInterface.hh"
#include "zmq.hpp"
#include <algorithm>
#include <iomanip>
#include <string>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Method implementing the specific behavior of the command executed
//------------------------------------------------------------------------------
eos::console::ReplyProto
IoCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  const eos::console::IoProto io = mReqProto.io();

  switch (mReqProto.io().subcmd_case()) {
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

  case eos::console::IoProto::kMonitor:
    MonitorSubcommand(io.monitor(), reply);
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
void
IoCmd::StatSubcmd(const eos::console::IoProto_StatProto& stat, eos::console::ReplyProto& reply)
{
  XrdOucString out = "";
  bool monitoring = stat.monitoring() || WantsJsonOutput();

  // If nothing is selected, we show the summary information
  if (!(stat.apps() || stat.domain() || stat.top() || stat.details())) {
    gOFS->mIoStats->PrintOut(out,
                             true,
                             stat.details(),
                             monitoring,
                             stat.numerical(),
                             stat.top(),
                             stat.domain(),
                             stat.apps(),
                             stat.sample_stat(),
                             stat.time_ago(),
                             stat.time_interval());
  } else {
    gOFS->mIoStats->PrintOut(out,
                             stat.summary(),
                             stat.details(),
                             monitoring,
                             stat.numerical(),
                             stat.top(),
                             stat.domain(),
                             stat.apps(),
                             stat.sample_stat(),
                             stat.time_ago(),
                             stat.time_interval());
  }

  if (WantsJsonOutput()) {
    out = ResponseToJsonString(out.c_str()).c_str();
  }

  reply.set_std_out(out.c_str());
  reply.set_retc(0);
}

//------------------------------------------------------------------------------
// Execute enable subcommand
//------------------------------------------------------------------------------
void
IoCmd::EnableSubcmd(const eos::console::IoProto_EnableProto& enable, eos::console::ReplyProto& reply)
{
  std::ostringstream out, err;
  int ret_c = 0;

  if (enable.switchx()) {
    // enable
    if ((!enable.reports()) && (!enable.namespacex())) {
      if (enable.upd_address().length()) {
        if (gOFS->mIoStats->AddUdpTarget(enable.upd_address().c_str())) {
          out << "success: enabled IO udp target " << enable.upd_address();
        } else {
          err << "error: IO udp target was not configured " << enable.upd_address();
          ret_c = EINVAL;
        }
      } else {
        if (enable.popularity()) {
          // Always enable collection otherwise we don't get anything for
          // popularity reporting
          gOFS->mIoStats->StartCollection();

          if (gOFS->mIoStats->StartPopularity()) {
            out << "success: enabled IO popularity collection";
          } else {
            err << "error: IO popularity collection already enabled";
            ret_c = EINVAL;
          }
        } else {
          if (gOFS->mIoStats->StartCollection()) {
            out << "success: enabled IO report collection";
          } else {
            err << "error: IO report collection already enabled";
            ret_c = EINVAL;
          }
        }
      }
    } else {
      // disable
      if (enable.reports()) {
        if (gOFS->mIoStats->StartReport()) {
          out << "success: enabled IO report store";
        } else {
          err << "error: IO report store already enabled";
          ret_c = EINVAL;
        }
      }

      if (enable.namespacex()) {
        if (gOFS->mIoStats->StartReportNamespace()) {
          out << "success: enabled IO report namespace";
        } else {
          err << "error: IO report namespace already enabled";
          ret_c = EINVAL;
        }
      }
    }
  } else {
    if ((!enable.reports()) && (!enable.namespacex())) {
      if (enable.upd_address().length()) {
        if (gOFS->mIoStats->RemoveUdpTarget(enable.upd_address().c_str())) {
          out << "success: disabled IO udp target " << enable.upd_address();
        } else {
          err << "error: IO udp target was not configured " << enable.upd_address();
          ret_c = EINVAL;
        }
      } else {
        if (enable.popularity()) {
          if (gOFS->mIoStats->StopPopularity()) {
            out << "success: disabled IO popularity collection";
          } else {
            err << "error: IO popularity collection already disabled";
            ret_c = EINVAL;
          }
        } else {
          if (gOFS->mIoStats->StopCollection()) {
            out << "success: disabled IO report collection";
          } else {
            err << "error: IO report collection already disabled";
            ret_c = EINVAL;
          }
        }
      }
    } else {
      if (enable.reports()) {
        if (gOFS->mIoStats->StopReport()) {
          out << "success: disabled IO report store";
        } else {
          err << "error: IO report store already disabled";
          ret_c = EINVAL;
        }
      }

      if (enable.namespacex()) {
        if (gOFS->mIoStats->StopReportNamespace()) {
          out << "success: disabled IO report namespace";
        } else {
          err << "error: IO report namespace already disabled";
          ret_c = EINVAL;
        }
      }
    }
  }

  reply.set_std_out(out.str());
  reply.set_std_err(err.str());
  reply.set_retc(ret_c);
}

//------------------------------------------------------------------------------
// Execute report subcommand
//------------------------------------------------------------------------------
void
IoCmd::ReportSubcmd(const eos::console::IoProto_ReportProto& report, eos::console::ReplyProto& reply)
{
  XrdOucString out{""};
  XrdOucString err{""};

  if (mVid.uid != 0) {
    reply.set_std_err("error: you have to take role 'root' to execute this command");
    reply.set_retc(EPERM);
    return;
  }

  if (gOFS->mIoStats) {
    gOFS->mIoStats->PrintNsReport(report.path().c_str(), out);
  }

  reply.set_std_out(out.c_str());
  reply.set_std_err(err.c_str());
  reply.set_retc(0);
}

//------------------------------------------------------------------------------
// Execute ns subcommand
//------------------------------------------------------------------------------
void
IoCmd::NsSubcmd(const eos::console::IoProto_NsProto& ns, eos::console::ReplyProto& reply)
{
  std::string option;

  if (ns.monitoring() || WantsJsonOutput()) {
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

  default: // NONE
    break;
  }

  XrdOucString out = "";
  gOFS->mIoStats->PrintNsPopularity(out, option.c_str());

  if (WantsJsonOutput()) {
    out = ResponseToJsonString(out.c_str()).c_str();
  }

  reply.set_std_out(out.c_str());
  reply.set_retc(0);
}

//----------------------------------------------------------------------------
//! Find the number of the '-' char to display
//----------------------------------------------------------------------------
static size_t
findSize(IoBuffer::Summaries& sums, size_t winTime)
{
  size_t size = 0;

  IoBuffer::Data data(sums.aggregated().at(winTime));
  for (auto app : data.apps()) {
    if (app.first.length() > size) {
      size = app.first.length();
    }
  }
  for (auto uid : data.uids()) {
    if (std::to_string(uid.first).length() > size) {
      size = std::to_string(uid.first).length();
    }
  }
  for (auto gid : data.gids()) {
    if (std::to_string(gid.first).length() > size) {
      size = std::to_string(gid.first).length();
    }
  }

  return size + 8;
}

//----------------------------------------------------------------------------
//! Find the number of the '-' char to display
//----------------------------------------------------------------------------
static size_t
findSize(Limiter& limit)
{
  size_t size = 0;

  for (auto app : limit.rApps) {
    if (app.first.length() > size) {
      size = app.first.length();
    }
  }
  for (auto app : limit.wApps) {
    if (app.first.length() > size) {
      size = app.first.length();
    }
  }
  for (auto uid : limit.rUids) {
    if (std::to_string(uid.first).length() > size) {
      size = std::to_string(uid.first).length();
    }
  }
  for (auto uid : limit.wUids) {
    if (std::to_string(uid.first).length() > size) {
      size = std::to_string(uid.first).length();
    }
  }
  for (auto gid : limit.rGids) {
    if (std::to_string(gid.first).length() > size) {
      size = std::to_string(gid.first).length();
    }
  }
  for (auto gid : limit.wGids) {
    if (std::to_string(gid.first).length() > size) {
      size = std::to_string(gid.first).length();
    }
  }

  return size + 7;
}

//----------------------------------------------------------------------------
//! change the number of byte to MB/s (also GB/s|TB/s) string
//----------------------------------------------------------------------------
static std::string
toMega(size_t byte, float iops = 1, size_t precision = 3, bool print = true, bool isTrivial = false)
{
  std::ostringstream os;

  if (byte == 0) {
    return "0 MB/s";
  }

  float finalByte = (static_cast<float>(byte) / 1000000) * iops;

  os << std::fixed << std::setprecision(precision);
  if (isTrivial) {
    os << "*";
  }

  if (print && finalByte >= 1000) {
    finalByte /= 1000;
    if (finalByte >= 1000) {
      finalByte /= 1000;
      os << std::setprecision(2) << finalByte;
      os << " TB/s";
    } else {
      os << std::setprecision(2) << finalByte;
      os << " GB/s";
    }
  } else if (print) {
    os << finalByte;
    os << " MB/s";
  } else {
    os << finalByte;
  }

  return os.str();
}

//----------------------------------------------------------------------------
//! Monitor command to display the bandwidth
//----------------------------------------------------------------------------
void
MonitorRatesShow(const eos::console::IoProto_MonitorProto::QueryRates& monitor_show, eos::console::ReplyProto& reply)
{
  std::stringstream ss;

  bool printApps = monitor_show.apps_only();
  bool printUids = monitor_show.users_only();
  bool printGids = monitor_show.groups_only();
  if (!printApps && !printUids && !printGids) {
    printApps = printUids = printGids = true;
  }

  bool jsonOutput = monitor_show.json();
  bool printStd = false;
  bool printSize = false;

  // TODO: get limits or reservations

  IoBuffer::Summaries sums(gOFS->mIoShaper.getShaping());
  size_t winTime = !sums.aggregated().empty() ? std::min_element(sums.aggregated().begin(),
                                                                 sums.aggregated().end(),
                                                                 [](auto& a, auto& b) { return a.first < b.first; })
                                                    ->first
                                              : 0;

  /// Handle errors
  if (sums.aggregated().empty()) {
    if (jsonOutput == true) {
      return reply.set_std_out("{}");
    }
    return reply.set_std_out("(empty)\n");
  }

  if (sums.aggregated().find(winTime) == sums.aggregated().end()) {
    reply.set_std_err("no matching window found " + std::to_string(winTime));
    return;
  }

  if (!printApps && !printUids && !printGids) {
    reply.set_std_err("you cannot select multiple targets");
    return;
  }

  const size_t W1 = findSize(sums, winTime);
  constexpr size_t W2 = 14;
  constexpr size_t W3 = 7;
  constexpr size_t W4 = 13;

  ss << "Window := " << winTime << " s" << "\n\n";
  ss << std::left << std::setw(W1) << "who" << std::right << std::setw(W2) << "read BW" << std::setw(W3) << "limit";
  if (printStd) {
    ss << std::setw(W4) << "std";
  }
  if (printSize) {
    ss << std::setw(W3) << "size";
  }
  ss << std::setw(W2) << "write BW" << std::setw(W3) << "limit";
  if (printStd) {
    ss << std::setw(W4) << "std";
  }
  if (printSize) {
    ss << std::setw(W3) << "size";
  }
  ss << std::setw(W2) << "read IOPS" << std::setw(W2) << "write IOPS";
  ss << '\n';

  size_t size = ((W2 * 5) + W1) + (printSize ? W3 * 2 : 0) + (printStd ? W4 * 2 : 0);
  while (size--) {
    ss << "─";
  }
  ss << '\n' << std::setprecision(3) << std::fixed;

  IoBuffer::Data data(sums.aggregated().at(winTime));
  Shaping::Scaler scaler(gOFS->mIoShaper.getScaler());

  if (jsonOutput) {
    google::protobuf::util::JsonPrintOptions jsonOption;
    std::string out;

    jsonOption.add_whitespace = true;
    absl::Status absl;
    if (!(printApps && printUids && printGids)) {
      if (printApps) {
        data.clear_uids();
        data.clear_gids();
      } else if (printUids) {
        data.clear_apps();
        data.clear_gids();
      } else if (printGids) {
        data.clear_apps();
        data.clear_uids();
      }
    }
    absl = google::protobuf::json::MessageToJsonString(data, &out, jsonOption);
    if (!absl.ok()) {
      reply.set_std_err("failed to parse data protobuf object");
      return;
    }
    ss.str("");
    ss << out;
  } else {
    if (printApps) {
      for (auto app : data.apps()) {
        auto& appScaler = scaler.apps();
        ss << std::left << std::setw(W1) << "app (" + app.first + ")" << std::right << std::setw(W2)
           << toMega(app.second.ravrg(), app.second.riops());
        /// Read apps
        if (appScaler.read().find(app.first) != appScaler.read().end()) {
          ss << std::setprecision(3) << std::setw(2) << (appScaler.read().at(app.first).istrivial() ? " *" : "  ")
             << std::setw(5) << appScaler.read().at(app.first).limit();
        } else {
          ss << std::setw(8) << 1.00;
        }

        if (printStd) {
          ss << std::setw(W4) << toMega(app.second.rstd(), app.second.riops());
        }
        if (printSize) {
          ss << std::setw(W3) << app.second.rsize();
        }

        ss << std::setw(W2) << toMega(app.second.wavrg(), app.second.wiops()) << std::setw(W3);

        /// Write apps
        if (appScaler.write().find(app.first) != appScaler.write().end()) {
          ss << std::setprecision(3) << std::setw(2) << (appScaler.write().at(app.first).istrivial() ? " *" : "  ")
             << std::setw(5) << appScaler.write().at(app.first).limit();
        } else {
          ss << std::setw(8) << 1.00;
        }

        if (printStd) {
          ss << std::setw(W4) << toMega(app.second.wstd(), app.second.wiops());
        }
        if (printSize) {
          ss << std::setw(W3) << app.second.wsize();
        }

        ss << std::setw(W2) << app.second.riops() << std::setw(W2) << app.second.wiops() << "\n";
      }
    }

    if (printUids) {
      for (const auto& uid : data.uids()) {
        auto& uidScaler = scaler.uids();
        ss << std::left << std::setw(W1) << "uid (" + std::to_string(uid.first) + ")" << std::right << std::setw(W2)
           << toMega(uid.second.ravrg(), uid.second.riops());
        /// Read uid
        if (uidScaler.read().find(uid.first) != uidScaler.read().end()) {
          ss << std::setprecision(3) << std::setw(2) << (uidScaler.read().at(uid.first).istrivial() ? " *" : "  ")
             << std::setw(5) << uidScaler.read().at(uid.first).limit();
        } else {
          ss << std::setw(8) << 1.00;
        }

        if (printStd) {
          ss << std::setw(W4) << toMega(uid.second.rstd(), uid.second.riops());
        }
        if (printSize) {
          ss << std::setw(W3) << uid.second.rsize();
        }

        ss << std::setw(W2) << toMega(uid.second.wavrg(), uid.second.wiops());

        /// Write uid
        if (uidScaler.write().find(uid.first) != uidScaler.write().end()) {
          ss << std::setprecision(3) << std::setw(2) << (uidScaler.write().at(uid.first).istrivial() ? " *" : "  ")
             << std::setw(5) << uidScaler.write().at(uid.first).limit();
        } else {
          ss << std::setw(8) << 1.00;
        }

        if (printStd) {
          ss << std::setw(W4) << toMega(uid.second.wstd(), uid.second.wiops());
        }
        if (printSize) {
          ss << std::setw(W3) << uid.second.wsize();
        }

        ss << std::setw(W2) << uid.second.riops() << std::setw(W2) << uid.second.wiops() << "\n";
      }
    }

    if (printGids) {
      for (auto gid : data.gids()) {
        auto& gidScaler = scaler.gids();
        ss << std::left << std::setw(W1) << "gid (" + std::to_string(gid.first) + ")" << std::right << std::setw(W2)
           << toMega(gid.second.ravrg(), gid.second.riops());

        /// Read gid
        if (gidScaler.read().find(gid.first) != gidScaler.read().end()) {
          ss << std::setprecision(3) << std::setw(2) << (gidScaler.read().at(gid.first).istrivial() ? " *" : "  ")
             << std::setw(5) << std::right << gidScaler.read().at(gid.first).limit();
        } else {
          ss << std::setw(8) << 1.00;
        }

        if (printStd) {
          ss << std::setw(W4) << toMega(gid.second.rstd(), gid.second.riops());
        }
        if (printSize) {
          ss << std::setw(W3) << gid.second.rsize();
        }

        ss << std::setw(W2) << toMega(gid.second.wavrg(), gid.second.wiops());

        /// Write gid
        if (gidScaler.write().find(gid.first) != gidScaler.write().end()) {
          ss << std::setprecision(3) << std::setw(2) << (gidScaler.write().at(gid.first).istrivial() ? " *" : "  ")
             << std::setw(5) << gidScaler.write().at(gid.first).limit();
        } else {
          ss << std::setw(8) << 1.00;
        }

        if (printStd) {
          ss << std::setw(W4) << toMega(gid.second.wstd(), gid.second.wiops());
        }
        if (printSize) {
          ss << std::setw(W3) << gid.second.wsize();
        }

        ss << std::setw(W2) << gid.second.riops() << std::setw(W2) << gid.second.wiops() << "\n";
      }
    }
  }

  reply.set_std_out(ss.str().c_str());
}

void
MonitorThrottleShow(const eos::console::IoProto_MonitorProto::ThrottleProto::ListAction& monitor_throttle,
                    eos::console::ReplyProto& reply)
{
  Limiter limit(gOFS->mIoShaper.getLimiter());
  std::stringstream std_out;

  size_t W1 = findSize(limit);
  const size_t W2 = 16;
  const size_t W3 = 8;

  bool printApps = monitor_throttle.apps_only();
  bool printUids = monitor_throttle.users_only();
  bool printGids = monitor_throttle.groups_only();
  if (!printApps && !printUids && !printGids) {
    printApps = printUids = printGids = true;
  }

  // TODO: jsonOutput is unused for now
  const bool jsonOutput = monitor_throttle.json();

  /// Display layout
  std_out << std::left << std::setw(W1) << "who" << std::right << std::setw(W2) << "read limit" << std::right
          << std::setw(W3) << "status" << std::right << std::setw(W2) << "write limit" << std::right << std::setw(W3)
          << "status";
  std_out << '\n';

  size_t size = W1 + (W2 * 2) + (W3 * 2);
  while (size--) {
    std_out << "─";
  }
  std_out << '\n' << std::setprecision(3);
  /// Handle errors
  if (!printApps && !printUids && !printGids) {
    reply.set_std_err("you cannot select multiple targets");
    return;
  }

  size_t fullSize = limit.rApps.size() + limit.wApps.size() + limit.rGids.size() + limit.wGids.size() +
                    limit.rUids.size() + limit.wUids.size();

  if (!fullSize) {
    return reply.set_std_out("(empty)\n");
  }

  /// Read apps
  if (printApps) {
    for (auto app : limit.rApps) {
      std_out << std::left << std::setw(W1) << ("app (" + app.first + ")") << std::right << std::setw(W2)
              << (app.second.limit == 0 ? "N/A" : toMega(app.second.limit, 1, 0, true, app.second.isTrivial))
              << std::right << std::setw(W3) << (app.second.isEnable == 0 ? "off" : "on");
      if (limit.wApps.find(app.first) != limit.wApps.end()) {
        auto write = limit.wApps.find(app.first);
        std_out << std::right << std::setw(W2)
                << (write->second.limit == 0 ? "N/A" : toMega(write->second.limit, 1, 0, true, app.second.isTrivial))
                << std::setw(W3) << (write->second.isEnable == 0 ? "off" : "on");
      } else {
        std_out << std::right << std::setw(W2) << "N/A" << std::setw(W3) << "off";
      }
      std_out << '\n';
    }
    /// Write apps
    for (auto app : limit.wApps) {
      if (limit.rApps.find(app.first) == limit.rApps.end()) {
        std_out << std::left << std::setw(W1) << ("app (" + app.first + ")") << std::right << std::setw(W2)
                << "N/A MB/s" << std::setw(W3) << "off";
        std_out << std::right << std::setw(W2)
                << (app.second.limit == 0 ? "N/A" : toMega(app.second.limit, 1, 0, true, app.second.isTrivial))
                << std::setw(W3) << (app.second.isEnable == 0 ? "off" : "on");
        std_out << '\n';
      }
    }
  }

  if (printUids) {
    /// Read uids
    for (auto uid : limit.rUids) {
      std_out << std::left << std::setw(W1) << ("uid (" + std::to_string(uid.first) + ")") << std::right
              << std::setw(W2)
              << (uid.second.limit == 0 ? "N/A" : toMega(uid.second.limit, 1, 0, true, uid.second.isTrivial))
              << std::right << std::setw(W3) << (uid.second.isEnable == 0 ? "off" : "on");
      if (limit.wUids.find(uid.first) != limit.wUids.end()) {
        auto write = limit.wUids.find(uid.first);
        std_out << std::right << std::setw(W2)
                << (write->second.limit == 0 ? "N/A" : toMega(write->second.limit, 1, 0, true, uid.second.isTrivial))
                << std::setw(W3) << (write->second.isEnable == 0 ? "off" : "on");
      } else {
        std_out << std::right << std::setw(W2) << "N/A" << std::setw(W3) << "off";
      }
      std_out << '\n';
    }
    /// Write uids
    for (auto uid : limit.wUids) {
      if (limit.rUids.find(uid.first) == limit.rUids.end()) {
        std_out << std::left << std::setw(W1) << ("uid (" + std::to_string(uid.first) + ")") << std::right
                << std::setw(W2) << "N/A" << std::setw(W3) << "off";
        std_out << std::right << std::setw(W2)
                << (uid.second.limit == 0 ? "N/A" : toMega(uid.second.limit, 1, 0, true, uid.second.isTrivial))
                << std::setw(W3) << (uid.second.isEnable == 0 ? "off" : "on");
        std_out << '\n';
      }
    }
  }

  if (printGids) {
    /// Read gids
    for (auto gid : limit.rGids) {
      std_out << std::left << std::setw(W1) << ("gid (" + std::to_string(gid.first) + ")") << std::right
              << std::setw(W2)
              << (gid.second.limit == 0 ? "N/A" : toMega(gid.second.limit, 1, 0, true, gid.second.isTrivial))
              << std::right << std::setw(W3) << (gid.second.isEnable == 0 ? "off" : "on");
      if (limit.wGids.find(gid.first) != limit.wGids.end()) {
        auto write = limit.wGids.find(gid.first);
        std_out << std::right << std::setw(W2)
                << (write->second.limit == 0 ? "N/A" : toMega(write->second.limit, 1, 0, true, gid.second.isTrivial))
                << std::setw(W3) << (write->second.isEnable == 0 ? "off" : "on");
      } else {
        std_out << std::right << std::setw(W2) << "N/A" << std::setw(W3) << "off";
      }
      std_out << '\n';
    }
    /// Write gids
    for (auto gid : limit.wGids) {
      if (limit.rGids.find(gid.first) == limit.rGids.end()) {
        std_out << std::left << std::setw(W1) << ("gid (" + std::to_string(gid.first) + ")") << std::right
                << std::setw(W2) << "N/A" << std::setw(W3) << (gid.second.isEnable == 0 ? "off" : "on");
        std_out << std::right << std::setw(W2)
                << (gid.second.limit == 0 ? "N/A" : toMega(gid.second.limit, 1, 0, true, gid.second.isTrivial))
                << std::setw(W3) << (gid.second.isEnable == 0 ? "off" : "on");
        std_out << '\n';
      }
    }
  }

  reply.set_std_out(std_out.str());
}

void
MonitorThrottleRemove(const eos::console::IoProto_MonitorProto::ThrottleProto::RemoveAction& monitor_throttle,
                      eos::console::ReplyProto& reply)
{
  Limiter limit(gOFS->mIoShaper.getLimiter());
  std::stringstream std_out;

  // TODO: this should differently for limits / reservations
  const bool is_read = monitor_throttle.is_read();
  const std::string read_or_write = is_read ? "read" : "write";

  switch (monitor_throttle.target_case()) {
  case eos::console::IoProto_MonitorProto::ThrottleProto::RemoveAction::kApp: {
    const auto& app = monitor_throttle.app();
    if (gOFS->mIoShaper.rmLimit(app, read_or_write)) {
      std_out << "App " << app << " set successfully removed\n";
    } else {
      std_out << "Remove app " << app << " failed\n";
    }
    break;
  }

  case eos::console::IoProto_MonitorProto::ThrottleProto::RemoveAction::kUser: {
    const uid_t user = monitor_throttle.user();
    if (gOFS->mIoShaper.rmLimit(io::TYPE::UID, user, read_or_write)) {
      std_out << "User " << user << " set successfully removed\n";
    } else {
      std_out << "Remove user " << user << " failed\n";
    }
    break;
  }

  case eos::console::IoProto_MonitorProto::ThrottleProto::RemoveAction::kGroup: {
    const gid_t group = monitor_throttle.group();
    if (gOFS->mIoShaper.rmLimit(io::TYPE::GID, group, read_or_write)) {
      std_out << "Group " << group << " set successfully removed\n";
    } else {
      std_out << "Remove group " << group << " failed\n";
    }
    break;
  }

  default:
    // It is good practice to handle the 'TARGET_NOT_SET' or unknown cases
    std_out << "Error: Target not set or unknown remove action.\n";
    break;
  }

  reply.set_std_out(std_out.str());
}

void
MonitorThrottleSet(const eos::console::IoProto_MonitorProto::ThrottleProto::SetAction& monitor_throttle,
                   eos::console::ReplyProto& reply)
{
  Limiter limit(gOFS->mIoShaper.getLimiter());

  std::stringstream std_out;

  const bool is_read = monitor_throttle.is_read();
  const auto rate = monitor_throttle.rate_megabytes_per_sec();

  bool is_enable_or_disable = false;
  bool is_enable = monitor_throttle.enable(); // false if disabled

  switch (monitor_throttle.update_case()) {
  case eos::console::IoProto_MonitorProto::ThrottleProto::SetAction::kRateMegabytesPerSec: {
    if (rate == 0) {
      std_out << "Invalid rate specified. Rate must be greater than 0.\n";
      reply.set_std_err(std_out.str());
      return;
    }
    is_enable_or_disable = false;
    break;
  }

  case eos::console::IoProto_MonitorProto::ThrottleProto::SetAction::kEnable: {
    is_enable_or_disable = true;
    break;
  }

  case eos::console::IoProto_MonitorProto::ThrottleProto::SetAction::UPDATE_NOT_SET:
  default:
    std_out << "Invalid update field specified. You must specify a rate using '--rate' or enable toggle via '--enable' "
               "or '-disable'.\n";
    reply.set_std_err(std_out.str());
    return;
  }

  const auto type = monitor_throttle.type();

  const bool is_limit = type == eos::console::IoProto_MonitorProto_ThrottleProto_LimitOrReservation_LIMIT;

  if (const bool is_reservation =
          type == eos::console::IoProto_MonitorProto_ThrottleProto_LimitOrReservation_RESERVATION;
      !is_limit && !is_reservation) {
    std_out << "Invalid type specified. Valid types are '--limit' and '--reservation'.\n";
    reply.set_std_err(std_out.str());
    return;
  }

  if (!is_limit) {
    std_out << "Reservations are not supported yet.\n";
    reply.set_std_err(std_out.str());
    return;
  }
  // TODO: this should differently for limits / reservations

  const std::string read_or_write = is_read ? "read" : "write";
  bool status = false;
  switch (monitor_throttle.target_case()) {
  case eos::console::IoProto_MonitorProto::ThrottleProto::SetAction::kApp: {
    const auto& app = monitor_throttle.app();
    if (is_enable_or_disable) {
      status = gOFS->mIoShaper.setStatus(app, read_or_write, is_enable);
      if (status) {
        std_out << "App " << app << " " << read_or_write << " limit successfully "
                << (is_enable ? "enabled" : "disabled") << "\n";
      } else {
        std_out << (is_enable ? "Enable" : "Disable") << " app " << app << " " << read_or_write << " limit failed\n";
      }
    } else {
      status = gOFS->mIoShaper.setLimiter(app, rate, read_or_write);
      if (status) {
        std_out << "App " << app << " " << read_or_write << " limit successfully set to " << rate << " MB/s\n";
      } else {
        std_out << "Set app " << app << " " << read_or_write << " limit failed\n";
      }
    }
    break;
  }

  case eos::console::IoProto_MonitorProto::ThrottleProto::SetAction::kUser: {
    const uid_t user = monitor_throttle.user();
    if (is_enable_or_disable) {
      status = gOFS->mIoShaper.setStatus(io::TYPE::UID, user, read_or_write, is_enable);
      if (status) {
        std_out << "User " << user << " " << read_or_write << " limit successfully "
                << (is_enable ? "enabled" : "disabled") << "\n";
      } else {
        std_out << (is_enable ? "Enable" : "Disable") << " user " << user << " " << read_or_write << " limit failed\n";
      }
    } else {
      status = gOFS->mIoShaper.setLimiter(io::TYPE::UID, user, rate, read_or_write);
      if (status) {
        std_out << "User " << user << " " << read_or_write << " limit successfully set to " << rate << " MB/s\n";
      } else {
        std_out << "Set user " << user << " " << read_or_write << " limit failed\n";
      }
    }
    break;
  }

  case eos::console::IoProto_MonitorProto::ThrottleProto::SetAction::kGroup: {
    const gid_t group = monitor_throttle.group();
    if (is_enable_or_disable) {
      status = gOFS->mIoShaper.setStatus(io::TYPE::UID, group, read_or_write, is_enable);
      if (status) {
        std_out << "Group " << group << " " << read_or_write << " limit successfully "
                << (is_enable ? "enabled" : "disabled") << "\n";
      } else {
        std_out << (is_enable ? "Enable" : "Disable") << " group " << group << " " << read_or_write
                << " limit failed\n";
      }
    } else {
      status = gOFS->mIoShaper.setLimiter(io::TYPE::GID, group, rate, read_or_write);
      if (status) {
        std_out << "Group " << group << " " << read_or_write << " limit successfully set to " << rate << " MB/s\n";
      } else {
        std_out << "Set group " << group << " " << read_or_write << " limit failed\n";
      }
    }
    break;
  }

  case eos::console::IoProto_MonitorProto::ThrottleProto::SetAction::TARGET_NOT_SET:
  default:
    reply.set_std_err(std_out.str());
    return;
    break;
  }

  reply.set_std_out(std_out.str());
}

void
MonitorWindow(const eos::console::IoProto::MonitorProto::WindowProto& win, eos::console::ReplyProto& reply)
{
  std::stringstream std_out;
  bool actions_taken = false;

  for (const auto& w : win.rm()) {
    actions_taken = true;
    if (gOFS->mIoShaper.rm(w)) {
      std_out << "Window " << w << " successfully removed" << std::endl;
    } else {
      std_out << "Remove window " << w << " failed" << std::endl;
    }
  }

  for (const auto& w : win.add()) {
    actions_taken = true;
    if (gOFS->mIoShaper.addWindow(w)) {
      std_out << "Window " << w << " successfully added" << std::endl;
    } else {
      std_out << "Add window " << w << " failed" << std::endl;
    }
  }

  if (win.ls()) {
    actions_taken = true;

    // Retrieve summaries for FST windows
    IoBuffer::Summaries sums(gOFS->mIoShaper.getShaping());

    std_out << "[FST's Available windows] :=";
    if (sums.aggregated().empty()) {
      std_out << " (empty)";
    } else {
      // Iterate over the map (key is the window time)
      for (const auto& [time, data] : sums.aggregated()) {
        std_out << " " << time;
      }
    }

    std_out << "\n[MGM set windows]         :=";

    // Retrieve scaler for MGM windows
    const auto scaler(gOFS->mIoShaper.getScaler());

    if (scaler.windows().empty()) {
      std_out << " (empty)\n";
    } else {
      for (const auto& window : scaler.windows()) {
        std_out << " " << window;
      }
      std_out << "\n";
    }
  }

  // If no specific window action was requested (shouldn't happen if client is correct)
  if (!actions_taken) {
    // Optional: handle empty request or default to ls
  }

  reply.set_std_out(std_out.str());
}

//----------------------------------------------------------------------------
//! Manage monitor subcommand to manage all the commands of io monitor
//----------------------------------------------------------------------------
void
IoCmd::MonitorSubcommand(const eos::console::IoProto_MonitorProto& monitor, eos::console::ReplyProto& reply)
{
  eos::common::RWMutexWriteLock wr_lock(FsView::gFsView.ViewMutex);

  switch (monitor.subcmd_case()) {

  case eos::console::IoProto_MonitorProto::kShow: {
    MonitorRatesShow(monitor.show(), reply); // Implement this new method
    break;
  }

  case eos::console::IoProto_MonitorProto::kThrottle: {
    switch (monitor.throttle().action_case()) {
    case eos::console::IoProto_MonitorProto_ThrottleProto::kShow:
      MonitorThrottleShow(monitor.throttle().show(), reply);
      break;

    case eos::console::IoProto_MonitorProto_ThrottleProto::kSet:
      MonitorThrottleSet(monitor.throttle().set(), reply);
      break;

    case eos::console::IoProto_MonitorProto_ThrottleProto::kRemove:
      MonitorThrottleRemove(monitor.throttle().remove(), reply);
      break;

    default:
      reply.set_std_err("Monitor throttle: invalid subcommand");
      break;
    }
    break;
  }

  case eos::console::IoProto_MonitorProto::kWindow: {
    MonitorWindow(monitor.window(), reply); // Implement this new method
    break;
  }

  case eos::console::IoProto_MonitorProto::SUBCMD_NOT_SET:
  default:
    reply.set_std_err("Monitor command: sub-command (show/throttle/window) not specified");
    break;
  }
}

EOSMGMNAMESPACE_END
