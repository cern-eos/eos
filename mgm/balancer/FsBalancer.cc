//------------------------------------------------------------------------------
//! @file FsBalancer.cc
//! @author Elvin Sindrilaru <esindril@cern.ch>
//-----------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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

#include "mgm/balancer/FsBalancer.hh"
#include "mgm/FsView.hh"
#include "mgm/XrdMgmOfs.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Update balancer config based on the info registered at the space
//------------------------------------------------------------------------------
void
FsBalancer::ConfigUpdate()
{
  if (!mDoConfigUpdate) {
    return;
  }

  mDoConfigUpdate = false;
  // Collect all the relevant info from the parent space
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex,
                                          __FUNCTION__, __LINE__, __FILE__);
  auto it_space = FsView::gFsView.mSpaceView.find(mSpaceName);

  // Space no longer exist, just disable the balancer
  if (it_space == FsView::gFsView.mSpaceView.end()) {
    mIsEnabled = false;
    return;
  }

  auto* space = it_space->second;

  // Check if balancer is enabled
  if (space->GetConfigMember("balancer") != "on") {
    mIsEnabled = false;
    return;
  }

  mIsEnabled = true;
  // Update other balancer related parameters
  std::string svalue = space->GetConfigMember("balancer.threshold");

  if (svalue.empty()) {
    eos_static_err("msg=\"balancer threshold missing, use default value\" value=%f",
                   mThreshold);
  } else {
    try {
      mThreshold = std::stod(svalue);
    } catch (...) {
      eos_static_err("msg=\"balancer threshold invalid format\" input=\"%s\"",
                     svalue.c_str());
    }
  }

  svalue = space->GetConfigMember("balancer.node.tx");

  if (svalue.empty()) {
    eos_static_err("msg=\"balancer node tx missing, use default value\" value=%f",
                   mTxNumPerNode);
  } else {
    try {
      mTxNumPerNode = std::stoul(svalue);
    } catch (...) {
      eos_static_err("msg=\"balancer node tx invalid format\" input=\"%s\"",
                     svalue.c_str());
    }
  }

  svalue = space->GetConfigMember("balancer.node.rate");

  if (svalue.empty()) {
    eos_static_err("msg=\"balancer node rate missing, use default value\" value=%f",
                   mTxRatePerNode);
  } else {
    try {
      mTxRatePerNode = std::stoul(svalue);
    } catch (...) {
      eos_static_err("msg=\"balancer node rate invalid format\" input=\"%s\"",
                     svalue.c_str());
    }
  }

  return;
}

//------------------------------------------------------------------------------
// Loop handling balancing jobs
//------------------------------------------------------------------------------
void
FsBalancer::Balance(ThreadAssistant& assistant) noexcept
{
  static constexpr std::chrono::seconds enable_refresh_delay {10};

  if (gOFS) {
    gOFS->WaitUntilNamespaceIsBooted(assistant);
  }

  eos_static_info("%s", "msg=\"starting file system balancer thread\"");

  while (!assistant.terminationRequested()) {
    ConfigUpdate();

    if (!mIsEnabled) {
      assistant.wait_for(enable_refresh_delay);
      continue;
    }
  }
}
EOSMGMNAMESPACE_END
