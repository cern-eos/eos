//------------------------------------------------------------------------------
// @file: IoMonitorCmd.cc
// @author: Luis Antonio Obis Aparicio - CERN
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
#include "mgm/shaping/TrafficShaping.hh"
#include "zmq.hpp"
#include <algorithm>
#include <iomanip>
#include <string>

EOSMGMNAMESPACE_BEGIN

//----------------------------------------------------------------------------
//! change the number of byte to MB/s (also GB/s|TB/s) string
//----------------------------------------------------------------------------
static std::string
toMega(size_t byte, float iops = 1, size_t precision = 3, bool print = true,
       bool isTrivial = false)
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
MonitorRatesShow(const eos::console::IoProto_MonitorProto::QueryRates& monitor_show,
                 eos::console::ReplyProto& reply)
{
  std::stringstream std_out;

  bool printApps = monitor_show.apps_only();
  bool printUids = monitor_show.users_only();
  bool printGids = monitor_show.groups_only();
  if (!printApps && !printUids && !printGids) {
    printApps = printUids = printGids = true;
  }

  bool jsonOutput = monitor_show.json();
  bool printStd = false;
  bool printSize = false;

  // TODO: redo the printing, include json
}

void
MonitorThrottleShow(
    const eos::console::IoProto_MonitorProto::ThrottleProto::ListAction& monitor_throttle,
    eos::console::ReplyProto& reply)
{
  std::stringstream std_out;

  bool printApps = monitor_throttle.apps_only();
  bool printUids = monitor_throttle.users_only();
  bool printGids = monitor_throttle.groups_only();
  if (!printApps && !printUids && !printGids) {
    printApps = printUids = printGids = true;
  }

  // TODO: jsonOutput is unused for now
  const bool jsonOutput = monitor_throttle.json();

  reply.set_std_out(std_out.str());
}

void
MonitorThrottleRemove(
    const eos::console::IoProto_MonitorProto::ThrottleProto::RemoveAction&
        monitor_throttle,
    eos::console::ReplyProto& reply)
{
  std::stringstream std_out;

  // TODO: this should differently for limits / reservations
  const bool is_read = monitor_throttle.is_read();
  const std::string read_or_write = is_read ? "read" : "write";

  switch (monitor_throttle.target_case()) {
  case eos::console::IoProto_MonitorProto::ThrottleProto::RemoveAction::kApp: {
    const auto& app = monitor_throttle.app();
    // remove
    break;
  }

  case eos::console::IoProto_MonitorProto::ThrottleProto::RemoveAction::kUser: {
    const uid_t user = monitor_throttle.user();
    // remove
    break;
  }

  case eos::console::IoProto_MonitorProto::ThrottleProto::RemoveAction::kGroup: {
    const gid_t group = monitor_throttle.group();
    // remove
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
MonitorThrottleSet(
    const eos::console::IoProto_MonitorProto::ThrottleProto::SetAction& monitor_throttle,
    eos::console::ReplyProto& reply)
{
  std::stringstream std_out;

  const bool is_read = monitor_throttle.is_read();
  const auto rate = monitor_throttle.rate_megabytes_per_sec();

  bool is_enable_or_disable = false;
  bool is_enable = monitor_throttle.enable(); // false if disabled

  switch (monitor_throttle.update_case()) {
  case eos::console::IoProto_MonitorProto::ThrottleProto::SetAction::
      kRateMegabytesPerSec: {
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
    std_out << "Invalid update field specified. You must specify a rate using '--rate' "
               "or enable toggle via '--enable' "
               "or '-disable'.\n";
    reply.set_std_err(std_out.str());
    return;
  }

  const auto type = monitor_throttle.type();

  const bool is_limit =
      type == eos::console::IoProto_MonitorProto_ThrottleProto_LimitOrReservation_LIMIT;

  if (const bool is_reservation =
          type ==
          eos::console::IoProto_MonitorProto_ThrottleProto_LimitOrReservation_RESERVATION;
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
  auto& engine = gOFS->mTrafficShapingEngine;
  switch (monitor_throttle.target_case()) {
  case eos::console::IoProto_MonitorProto::ThrottleProto::SetAction::kApp: {
    const auto& app = monitor_throttle.app();
    auto policy = TrafficShapingPolicy();
    if (const auto existing_policy = engine.GetAppPolicy(app);
        existing_policy.has_value()) {
      policy = existing_policy.value();
    }
    const auto policy_before = policy;

    if (is_enable_or_disable) {
      policy.is_enabled = is_enable;
      std_out << (is_enable ? "Enabling" : "Disabling") << " app " << app << " "
              << read_or_write << " limit\n";
    } else {
      if (read_or_write == "read") {
        policy.limit_read_bytes_per_sec = rate * 1000000; // Convert MB/s to bytes/s
      } else {
        policy.limit_write_bytes_per_sec = rate * 1000000; // Convert MB/s to bytes/s
      }
      std_out << "Setting app " << app << " " << read_or_write << " limit to " << rate
              << " MB/s\n";
    }

    if (policy_before != policy) {
      engine.SetAppPolicy(app, policy);
    }
    break;
  }

  case eos::console::IoProto_MonitorProto::ThrottleProto::SetAction::kUser: {
    const auto& user = monitor_throttle.user();
    auto policy = TrafficShapingPolicy();
    if (const auto existing_policy = engine.GetUidPolicy(user);
        existing_policy.has_value()) {
      policy = existing_policy.value();
    }
    const auto policy_before = policy;

    if (is_enable_or_disable) {
      policy.is_enabled = is_enable;
      std_out << (is_enable ? "Enabling" : "Disabling") << " user " << user << " "
              << read_or_write << " limit\n";
    } else {
      if (read_or_write == "read") {
        policy.limit_read_bytes_per_sec = rate * 1000000; // Convert MB/s to bytes/s
      } else {
        policy.limit_write_bytes_per_sec = rate * 1000000; // Convert MB/s to bytes/s
      }
      std_out << "Setting user " << user << " " << read_or_write << " limit to " << rate
              << " MB/s\n";
    }

    if (policy_before != policy) {
      engine.SetUidPolicy(user, policy);
    }
    break;
  }

  case eos::console::IoProto_MonitorProto::ThrottleProto::SetAction::kGroup: {
    const auto& group = monitor_throttle.group();
    auto policy = TrafficShapingPolicy();
    if (const auto existing_policy = engine.GetGidPolicy(group);
        existing_policy.has_value()) {
      policy = existing_policy.value();
    }
    const auto policy_before = policy;

    if (is_enable_or_disable) {
      policy.is_enabled = is_enable;
      std_out << (is_enable ? "Enabling" : "Disabling") << " group " << group << " "
              << read_or_write << " limit\n";
    } else {
      if (read_or_write == "read") {
        policy.limit_read_bytes_per_sec = rate * 1000000; // Convert MB/s to bytes/s
      } else {
        policy.limit_write_bytes_per_sec = rate * 1000000; // Convert MB/s to bytes/s
      }
      std_out << "Setting group " << group << " " << read_or_write << " limit to " << rate
              << " MB/s\n";
    }

    if (policy_before != policy) {
      engine.SetGidPolicy(group, policy);
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

//----------------------------------------------------------------------------
//! Manage monitor subcommand to manage all the commands of io monitor
//----------------------------------------------------------------------------
void
IoCmd::MonitorSubcommand(const eos::console::IoProto_MonitorProto& monitor,
                         eos::console::ReplyProto& reply)
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

  case eos::console::IoProto_MonitorProto::SUBCMD_NOT_SET:
  default:
    reply.set_std_err(
        "Monitor command: sub-command (show/throttle/window) not specified");
    break;
  }
}

EOSMGMNAMESPACE_END
