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
#include "common/ShellCmd.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//                        **** Class DiskHealth ****
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Get health information about a certain device
//------------------------------------------------------------------------------
std::map<std::string, std::string> DiskHealth::getHealth(const char* devpath)
{
  std::string dev = Load::DevMap(devpath);

  if (dev[0] == 'm') {
    return parse_mdstat(dev.c_str());
  }

  // Chunck partition digits, we need the actual device name for smartctl...
  // no string::back or pop_back support in gcc 4.4 so it looks a bit funny.
  while (isdigit(dev.at(dev.length() - 1))) {
    dev.resize(dev.length() - 1);
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
  std::map<std::string, std::map<std::string, std::string>> tmp;

  for (auto it = smartctl_results.begin(); it != smartctl_results.end(); it++) {
    tmp[it->first]["summary"] = smartctl(it->first.c_str());
  }

  std::lock_guard<std::mutex> lock(mMutex);
  smartctl_results = tmp;
}

//------------------------------------------------------------------------------
// Parse /proc/mdstat to obtain raid health
//------------------------------------------------------------------------------
std::map<std::string, std::string> DiskHealth::parse_mdstat(const char* device)
{
  std::map<std::string, std::string> health;
  std::ifstream file("/proc/mdstat");
  file.rdbuf()->pubsetbuf(0, 0);
  std::stringstream buffer;
  buffer << file.rdbuf();

  if (buffer.str().empty()) {
    health["summary"] = "no mdstat";
    return health;
  }

  std::string output = buffer.str();
  auto pos = output.find(device);
  int redundancy_factor;
  pos = output.find("raid", pos);
  auto c = output[pos + 4];

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

  pos = output.find("blocks", pos);
  pos = output.find("[", pos);
  health["drives_total"] = output.substr(pos + 1, output.find("/",
                                         pos) - pos - 1);
  auto drives_total = strtoll(health["drives_total"].c_str(), 0, 10);
  pos = output.find("/", pos);
  health["drives_healthy"] = output.substr(pos + 1, output.find("]",
                             pos) - pos - 1);
  auto drives_healthy = strtoll(health["drives_healthy"].c_str(), 0, 10);
  auto drives_failed = drives_total - drives_healthy;
  health["drives_failed"] = std::to_string((long long int) drives_failed);
  auto end = output.find("md", pos);
  pos = output.find("recovery", pos);
  health["indicator"] = pos < end ? "1" : "0";
  /* Build summary string. */
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
  return health;
}

//------------------------------------------------------------------------------
// Obtain health of a single locally attached storage device by evaluating
// S.M.A.R.T values.
//------------------------------------------------------------------------------
std::string DiskHealth::smartctl(const char* device)
{
  std::string command("smartctl -q silent -a /dev/");
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
      case 5:
      case 6:
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
// Static helper function for starting the health monitoring thread
//------------------------------------------------------------------------------
void*
Health::StartHealthThread(void* pp)
{
  Health* health = (Health*) pp;
  health->Measure();
  return 0;
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Health::Health(unsigned int ival_minutes):
  mSkip(false), mTid(0), mIntervalMin(ival_minutes)
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
  if (mTid) {
    XrdSysThread::Cancel(mTid);
    XrdSysThread::Join(mTid, 0);
    mTid = 0;
  }
}

//------------------------------------------------------------------------------
// Method starting the health monitoring thread
//------------------------------------------------------------------------------
bool
Health::Monitor()
{
  if (XrdSysThread::Run(&mTid, Health::StartHealthThread,
                        static_cast<void*>(this),
                        XRDSYSTHREAD_HOLD, "Health-Monitor")) {
    return false;
  } else {
    return true;
  }
}

//------------------------------------------------------------------------------
// Loop run by the monitoring thread to keep updated the disk health info.
//------------------------------------------------------------------------------
void
Health::Measure()
{
  while (1) {
    XrdSysThread::SetCancelOff();
    mDiskHealth.Measure();
    XrdSysThread::SetCancelOn();

    for (unsigned int i = 0; i < mIntervalMin; i++) {
      sleep(60);

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
Health::getDiskHealth(const char* devpath)
{
  auto result = mDiskHealth.getHealth(devpath);

  // If we don't have any result, don't wait for interval timeout to measure
  if (result.empty()) {
    mSkip = true; // this doesn't need a mutex, worst case we wait 1 more min.
  }

  return result;
}

EOSFSTNAMESPACE_END
