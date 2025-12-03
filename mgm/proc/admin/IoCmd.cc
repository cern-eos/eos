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
#include "mgm/proc/ProcInterface.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/iostat/Iostat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Iostat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Iostat.hh"
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
  eos::console::IoProto io = mReqProto.io();

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
    MonitorSubcmd(io.monitor(), reply);
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
  bool monitoring = stat.monitoring() || WantsJsonOutput();

  // If nothing is selected, we show the summary information
  if (!(stat.apps() || stat.domain() || stat.top() || stat.details())) {
    gOFS->mIoStats->PrintOut(out, true, stat.details(),
                             monitoring, stat.numerical(),
                             stat.top(), stat.domain(), stat.apps(),
                             stat.sample_stat(), stat.time_ago(),
                             stat.time_interval());
  } else {
    gOFS->mIoStats->PrintOut(out, stat.summary(), stat.details(),
                             monitoring, stat.numerical(),
                             stat.top(), stat.domain(), stat.apps(),
                             stat.sample_stat(), stat.time_ago(),
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
void IoCmd::EnableSubcmd(const eos::console::IoProto_EnableProto& enable,
                         eos::console::ReplyProto& reply)
{
  std::ostringstream out, err;
  int ret_c = 0;

  if (enable.switchx()) { // enable
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
    } else { // disable
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
void IoCmd::ReportSubcmd(const eos::console::IoProto_ReportProto& report,
                         eos::console::ReplyProto& reply)
{
  XrdOucString out {""};
  XrdOucString err {""};

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
void IoCmd::NsSubcmd(const eos::console::IoProto_NsProto& ns,
                     eos::console::ReplyProto& reply)
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

  default : // NONE
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

static size_t findSize(IoBuffer::summarys &sums, size_t winTime){
	size_t size = 0;

	IoBuffer::data data(sums.aggregated().at(winTime));
	for (auto app : data.apps())
		if (app.first.length() > size)
			size = app.first.length();
	for (auto uid : data.uids())
		if (std::to_string(uid.first).length() > size)
			size = std::to_string(uid.first).length();
	for (auto gid : data.gids())
		if (std::to_string(gid.first).length() > size)
			size = std::to_string(gid.first).length();

	return size + 8;
}

static std::string toMega(google::uint32 byte){
	std::ostringstream os;
	if (byte == 0)
		return "0  MB/s";

	float finalByte = static_cast<float>(byte) / 1000000;
	os << std::fixed << std::setprecision(3) << finalByte << " MB/s";
	return os.str();
}

void IoCmd::MonitorLs(const eos::console::IoProto_MonitorProto& mn,
                     eos::console::ReplyProto& reply){
	size_t W1 = 20;
	const size_t W2 = 14;
	const size_t W3 = 7;
	const size_t W4 = 9;
	std::stringstream options(mn.options());
	std::string cmdOptions;
	std::stringstream std_out;
	
	bool printApps = true;
	bool printUids = true;
	bool printGids = true;
	bool jsonOutput = false;
	bool printStd = false;
	bool printSize = false;

	IoBuffer::summarys sums(gOFS->mIoShaper.getShaping());
	size_t winTime = !sums.aggregated().empty() ?
		std::min_element(sums.aggregated().begin(), sums.aggregated().end(),
				   [](auto &a, auto &b){return a.first < b.first;})->first : 0;

	if (mn.options() == "-w"){
		std::string windows("[Available window]:");
		for (auto it : sums.aggregated())
			windows += " " + std::to_string(it.first);
		if (sums.aggregated().empty())
			windows += " (empty)";
		reply.set_std_out(windows.c_str());
		return ;
	}

	if (!mn.options().empty()){
		while (options >> cmdOptions){
			if (cmdOptions == "-a"){
				printUids = false;
				printGids = false;
			}
			else if (cmdOptions == "-u"){
				printApps = false;
				printGids = false;
			}
			else if (cmdOptions == "-g"){
				printApps = false;
				printUids = false;
			}
			else if (cmdOptions == "-j")
				jsonOutput = true;
			else if (cmdOptions == "-s")
				printSize = true;
			else if (cmdOptions == "-std")
				printStd = true;
			else {
				char *delemiter = NULL;
				winTime = std::strtol(cmdOptions.c_str(), &delemiter, 10);

				if (*delemiter || !options.eof()){
					reply.set_std_err(mn.cmd() + " " + mn.options() + ": Monitor:  " + "Command not found");
					return ;
				}
			} 
		}
	}

	/// Handle errors
	if (sums.aggregated().empty()){
		std_out << "(empty)" << std::endl;
		reply.set_std_out(std_out.str());
		return ;
	}

	if (sums.aggregated().find(winTime) == sums.aggregated().end()){
		reply.set_std_err("No matching found for window " + std::to_string(winTime));
		return ;
	}

	if (!printApps && !printUids & !printGids){
		reply.set_std_err("You cannot select multiple targets");
		return ;
	}

	W1 = findSize(sums, winTime);
	std_out << "Window := " << winTime << "\n\n";
	std_out << std::left << std::setw(W1) << "who"
		  << std::right << std::setw(W2) << "read BW" << std::setw(W3) << "limit";
	if (printStd)
		std_out << std::setw(W4) << "std";
	if (printSize)
		std_out << std::setw(W3) << "size";
	std_out << std::setw(W2) << "write BW" << std::setw(W3) << "limit";
	if (printStd)
		std_out << std::setw(W4) << "std";
	if (printSize)
		std_out << std::setw(W3) << "size";
	std_out << std::setw(W2) << "read IOPS" << std::setw(W2) << "write IOPS";
	std_out << '\n';

	size_t size = ((W2 * 5) + W1) + (printSize ? W3 * 2 : 0) + (printStd ? W4 * 2 : 0);
	while (size--)
		std_out << "─";
	std_out << '\n' << std::setprecision(3);

	IoBuffer::data data(sums.aggregated().at(winTime));
	Shaping::Scaler scaler(gOFS->mIoShaper.getScaler());

	if (jsonOutput){
		google::protobuf::util::JsonPrintOptions jsonOption;
		std::string out;

		jsonOption.add_whitespace = true;
		absl::Status absl;
		if (!(printApps && printUids && printGids)){
			if (printApps){
				data.clear_uids();
				data.clear_gids();
			}
			else if (printUids){
				data.clear_apps();
				data.clear_gids();
			}
			else if (printGids){
				data.clear_apps();
				data.clear_uids();
			}
		}
		absl = google::protobuf::json::MessageToJsonString(data, &out, jsonOption);
		if (!absl.ok()){
			reply.set_std_err("Failed to parse data protobuf object");
			return ;
		}
		std_out.str("");
		std_out << out;
	} else{
		if (printApps){
			for (auto app : data.apps()){
				std_out << std::left << std::setw(W1) << "app (" + app.first + ")"
					<< std::right << std::setw(W2) << toMega(app.second.ravrg())
					<< std::setw(W3) << 1;

				if (printStd)
					std_out << std::setw(W4) << app.second.rstd();
				if (printSize)
					std_out << std::setw(W3) << app.second.rsize();

				std_out << std::setw(W2) << toMega(app.second.wavrg())
					<< std::setw(W3)<< 1;

				if (printStd)
					std_out << std::setw(W4) << app.second.wstd();
				if (printSize)
					std_out << std::setw(W3) << app.second.wsize();

				std_out << std::setw(W2) << app.second.riops() << std::setw(W2) << app.second.wiops() << "\n";
			}
		}
		if (printUids){
			for (auto uid : data.uids()){
				std_out << std::left << std::setw(W1) << "uid (" + std::to_string(uid.first) + ")"
					<< std::right << std::setw(W2) << toMega(uid.second.ravrg())
					<< std::setw(W3) << 1;

				if (printStd)
					std_out << std::setw(W4) << uid.second.rstd();
				if (printSize)
					std_out << std::setw(W3) << uid.second.rsize();

				std_out << std::setw(W2) << toMega(uid.second.wavrg())
					<< std::setw(W3) << 1;
				
				if (printStd)
					std_out << std::setw(W4) << uid.second.wstd();
				if (printSize)
					std_out << std::setw(W3) << uid.second.wsize();
				
				std_out << std::setw(W2) << uid.second.riops()
					<< std::setw(W2) << uid.second.wiops() << "\n";
			}
		}
		if (printGids){
			for (auto gid : data.gids()){
				std_out << std::left << std::setw(W1) << "gid (" + std::to_string(gid.first) + ")"
					<< std::right << std::setw(W2) << toMega(gid.second.ravrg())
					<< std::setw(W3) << 1;
				
				if (printStd)
					std_out << std::setw(W4) << gid.second.rstd();
				if (printSize)
					std_out << std::setw(W3) << gid.second.rsize();
				
				std_out << std::setw(W2) << toMega(gid.second.wavrg())
					<< std::setw(W3) << 1;
				
				if (printStd)
					std_out << std::setw(W4) << gid.second.wstd();
				if (printSize)
					std_out << std::setw(W3) << gid.second.wsize();
				
				std_out << std::setw(W2) << gid.second.riops()
					<< std::setw(W2) << gid.second.wiops() << "\n";
			}
		}
	}

	reply.set_std_out(std_out.str().c_str());
	return ;
}

void IoCmd::MonitorSubcmd(const eos::console::IoProto_MonitorProto& mn,
                     eos::console::ReplyProto& reply)
{
	eos::common::RWMutexWriteLock wr_lock(FsView::gFsView.ViewMutex);

	std::string cmd(mn.cmd());
	std::stringstream options(mn.options());

	if (cmd == "ls"){
		MonitorLs(mn, reply);
	}else{
		for (auto it = FsView::gFsView.mNodeView.begin(); it != FsView::gFsView.mNodeView.end(); it++){
			if (it->second->GetStatus() == "online"){
				it->second->SetConfigMember("stat.monitor", (cmd + " " + options.str()).c_str(), true);
				reply.set_std_out(it->first + " : " + cmd + " " + options.str());
				reply.set_retc(0);
			}
		}
	}
}

EOSMGMNAMESPACE_END
