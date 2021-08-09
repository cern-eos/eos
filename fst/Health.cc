//------------------------------------------------------------------------------
// File: Health.cc
// Author: Paul Hermann Lensing <paul.lensing@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "Health.hh"
#include "fst/storage/FileSystem.hh"
#include "common/ShellCmd.hh"
#include <unordered_set>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//                        **** Class DiskHealth ****
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Get health information about a certain device
//------------------------------------------------------------------------------
std::map<std::string, std::string>
DiskHealth::getHealth(const std::string& devpath)
{
  std::string dev = Load::DevMap(devpath);

  if (dev.empty()) {
    return std::map<std::string, std::string>();
  }

  // RAID setups; (device-mapper) multipath will be looked up as dm-XX
  if (dev[0] == 'm') {
    return parse_mdstat(dev);
  }

  // Remove partition digits, we need the actual device name for smartctl...
  // only for /dev/sdaX devices, NOT /dev/dm-13 and comparables, 
  // where the trailing number is not a partition indicator
  // edge case would be /dev/sdm1 where dev == "sdm1"
  if (dev.find("dm-") != 0) {
    while (isdigit(dev.back())) {
      dev.pop_back();
    }
  }

  std::lock_guard<std::mutex> lock(mMutex);
  return smartctl_results[dev];
}

//------------------------------------------------------------------------------
// Update health information for all the registered devices
//------------------------------------------------------------------------------
void
DiskHealth::Measure()
{
  std::unordered_set<std::string> dev_names;
  std::map<std::string, std::map<std::string, std::string>> tmp;
  {
    // Collect the name of the devices
    std::lock_guard<std::mutex> lock(mMutex);

    for (const auto& elem : smartctl_results) {
      dev_names.insert(elem.first);
    }
  }

  for (const auto& elem : dev_names) {
    tmp[elem]["summary"] = smartctl(elem.c_str());
  }

  std::lock_guard<std::mutex> lock(mMutex);
  std::swap(smartctl_results, tmp);
}

//------------------------------------------------------------------------------
// Parse /proc/mdstat to obtain raid health
//------------------------------------------------------------------------------
std::map<std::string, std::string>
DiskHealth::parse_mdstat(const std::string& device,
                         const std::string& mdstat_path)
{
  std::string line, buffer;
  std::map<std::string, std::string> health;
  std::ifstream file(mdstat_path.c_str());
  health["summary"] = "no mdstat";

  while (std::getline(file, line)) {
    auto pos = line.find(device + " : ");

    if (pos == std::string::npos) {
      continue;
    }

    buffer = line;

    // Read in also the next lines until empty
    while (std::getline(file, line)) {
      // Trim whitespaces
      while (!line.empty() && line.back() == ' ') {
        line.pop_back();
      }

      if (line.empty()) {
        break;
      }

      buffer += "\n";
      buffer += line;
    }

    int redundancy_factor;
    pos = buffer.find("raid", pos);

    // Skip line if not holding raid info
    if (pos == std::string::npos) {
      continue;
    }

    auto c = buffer[pos + 4];

    switch (c) {
    case '0' :
      health["redundancy_factor"] = "0";
      redundancy_factor = 0;
      break;

    case '1' :
    case '5' :
      health["redundancy_factor"] = "1";
      redundancy_factor = 1;
      break;

    case '6' :
      health["redundancy_factor"] = "2";
      redundancy_factor = 2;
      break;

    default:
      health["summary"] = "unknown raid";
      return health;
    }

    pos = buffer.find("blocks", pos);

    if (pos == std::string::npos) {
      break;
    }

    pos = buffer.find("[", pos);

    if (pos == std::string::npos) {
      break;
    }

    health["drives_total"] = buffer.substr(pos + 1,
                                           buffer.find("/", pos) - pos - 1);
    auto drives_total = strtoll(health["drives_total"].c_str(), 0, 10);
    pos = buffer.find("/", pos);
    health["drives_healthy"] = buffer.substr(pos + 1, buffer.find("]",
                               pos) - pos - 1);
    auto drives_healthy = strtoll(health["drives_healthy"].c_str(), 0, 10);
    auto drives_failed = drives_total - drives_healthy;
    health["drives_failed"] = std::to_string((long long int) drives_failed);
    auto end = buffer.find("md", pos);
    pos = buffer.find("recovery", pos);
    health["indicator"] = pos < end ? "1" : "0";
    // Build summary string
    std::string summary;

    if (health["indicator"] == "1") {
      summary += "! ";
    }

    summary += health["drives_healthy"] + "/" + health["drives_total"];
    summary += " (";

    if (redundancy_factor >= drives_failed) {
      summary += "+";
    }

    summary += std::to_string((long long int) redundancy_factor -
                              (drives_total - drives_healthy));
    summary += ")";
    health["summary"] = summary;
    break;
  }

  return health;
}

//------------------------------------------------------------------------------
// Obtain health of a single locally attached storage device by evaluating
// S.M.A.R.T values.
//------------------------------------------------------------------------------
std::string DiskHealth::smartctl(const char* device)
{
  std::string command("smartctl -q silent -a /dev/");

  // dev name starts with mpath, it's scsi multipath from linux device mapper,
  // i.e. /dev/dm-XY
  if(std::string(device).find("dm-") == 0) {
    command = std::string("smartctl -q silent --device=scsi -a /dev/");
  }

  command += device;
  eos::common::ShellCmd scmd(command.c_str());
  eos::common::cmd_status rc = scmd.wait(5);

  if (rc.exit_code == 0) {
    return "OK";
  }

  if (rc.exit_code == 127) {
    return "no smartctl";
  }

  int mask = 1;

  for (int i = 0; i < 8; i++) {
    if (rc.exit_code & mask) {
      switch (i) {
      case 0:
      case 1:
      case 2:
        return "N/A";

      case 3:
        return "FAILING";

      case 4:
        return "Check";

      // Bit 5: SMART status check returned "DISK OK" but we found that
      // some (usage or prefail) Attributes have been <= threshold at some
      // time in the past.
      // -> once bit 5 is set, this is game over for the rest of the disk
      // life again just like bit 6.
      //
      // Bit 6: The device error log contains records of errors.
      // -> some disks have 1 error log entry that was created upon first
      // start (powertime hour 0 day 0) and this cannot be deleted. The
      // disk is stuck in Check state for its entire life...
      case 5:
      case 6:
        return "OK";

      case 7:
        return "Check";
      }
    }

    mask = mask << 1;
  }

  return "invalid";
}

//------------------------------------------------------------------------------
//                        **** Class Health ****
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Health::Health(unsigned int ival_minutes):
  mSkip(false), mIntervalMin(ival_minutes)
{
  if (mIntervalMin == 0) {
    mIntervalMin = 1;
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
Health::~Health()
{
}

//------------------------------------------------------------------------------
// Method starting the health monitoring thread
//------------------------------------------------------------------------------
void
Health::Monitor()
{
  monitoringThread.reset(&Health::Measure, this);
  monitoringThread.setName("Health-Monitor");
}

//------------------------------------------------------------------------------
// Loop run by the monitoring thread to keep updated the disk health info.
//------------------------------------------------------------------------------
void
Health::Measure(ThreadAssistant& assistant)
{
  while (!assistant.terminationRequested()) {
    mDiskHealth.Measure();

    for (unsigned int i = 0; i < mIntervalMin; i++) {
      if (assistant.terminationRequested()) {
        return;
      }

      assistant.wait_for(std::chrono::seconds(60));

      if (mSkip) {
        mSkip = false;
        break;
      }
    }
  }
}

//------------------------------------------------------------------------------
// Get disk health information for a specific device. If no measurements are
// available then trigger the monitoring thread to do an update.
//------------------------------------------------------------------------------
std::map<std::string, std::string>
Health::getDiskHealth(const std::string& devpath)
{
  auto result = mDiskHealth.getHealth(devpath);

  // If we don't have any result, don't wait for interval timeout to measure
  if (result.empty()) {
    mSkip = true; // this doesn't need a mutex, worst case we wait 1 more min.
  }

  return result;
}

EOSFSTNAMESPACE_END
