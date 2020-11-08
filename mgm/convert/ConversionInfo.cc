//------------------------------------------------------------------------------
// File: ConversionInfo.cc
// Author: Mihai Patrascoiu - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "mgm/convert/ConversionInfo.hh"
#include "common/Logging.hh"

EOSMGMNAMESPACE_BEGIN

//----------------------------------------------------------------------------
// Constructor
//----------------------------------------------------------------------------
ConversionInfo::ConversionInfo(const eos::common::FileId::fileid_t fid,
                               const eos::common::LayoutId::layoutid_t lid,
                               const eos::common::GroupLocator& location,
                               const std::string& plct_policy,
                               const bool update_ctime) :
  mFid(fid), mLid(lid), mLocation(location), mPlctPolicy(plct_policy),
  mUpdateCtime(update_ctime)
{
  std::ostringstream conversion;
  conversion << std::hex << std::setfill('0')
             << std::setw(16) << mFid           // <fid(016hex)>
             << ":" << mLocation.getSpace()     // :<space>
             << "." << mLocation.getIndex()     // .<group>
             << "#" << std::setw(8) << mLid;     // #<layoutid(08hex)>

  if (!mPlctPolicy.empty()) {
    conversion << "~" << mPlctPolicy;          // ~<placement_policy>
  }

  if (mUpdateCtime) {
    conversion << UPDATE_CTIME;
  }

  mConversionString = conversion.str();
}

//----------------------------------------------------------------------------
// Parse a conversion string representation into a conversion info object
//
// A conversion string has the following format:
// <fid(016hex)>:<space.group>#<layoutid(08hex)>[~<placement_policy>][!]
//----------------------------------------------------------------------------
std::shared_ptr<ConversionInfo> ConversionInfo::parseConversionString(
  std::string sconversion)
{
  using eos::common::FileId;
  using eos::common::LayoutId;
  using eos::common::GroupLocator;
  const char* errmsg = "unable to parse conversion string";
  FileId::fileid_t fid = 0;
  LayoutId::layoutid_t lid = 0;
  GroupLocator location;
  std::string policy;
  bool update_ctime {false};

  if (sconversion.empty()) {
    eos_static_err("%s", "msg=\"conversion string is empty\"");
    return nullptr;
  }

  // Check if ctime needs to be updated
  if (*sconversion.rbegin() == UPDATE_CTIME) {
    update_ctime = true;
    sconversion.erase(sconversion.end() - 1);
  }

  // Parse file id
  size_t pos = sconversion.find(":");

  if ((pos == std::string::npos) || (pos != 16)) {
    eos_static_err("msg=\"%s\" conversion_string=%s "
                   "reason=\"invalid fxid\"", errmsg, sconversion.c_str());
    return nullptr;
  } else {
    std::string hexfid = sconversion.substr(0, pos).c_str();

    try {
      fid = std::stoull(hexfid, 0, 16);
    } catch (...) {}
  }

  // Parse space/group location
  sconversion.erase(0, pos + 1);
  pos = sconversion.find("#");

  if (pos == std::string::npos) {
    eos_static_err("msg=\"%s\" conversion_string=%s "
                   "reason=\"invalid space\"", errmsg, sconversion.c_str());
    return nullptr;
  } else {
    std::string spacegroup = sconversion.substr(0, pos);
    GroupLocator::parseGroup(spacegroup, location);

    if (location.getSpace().empty()) {
      return nullptr;
    }
  }

  // Parse layout id
  sconversion.erase(0, pos + 1);
  pos = sconversion.find("~");
  std::string hexlid = sconversion.c_str();

  if (pos != std::string::npos) {
    hexlid = sconversion.substr(0, pos).c_str();
  }

  try {
    lid = std::stoll(hexlid, 0, 16);
  } catch (...) {}

  // Parse placement policy
  if (pos != std::string::npos) {
    policy = sconversion.substr(pos + 1);
  }

  if (!fid || !lid) {
    eos_static_err("msg=\"%s\" conversion_string=%s "
                   "reason=\"invalid fid or lid\"", errmsg, sconversion.c_str());
    return nullptr;
  }

  return std::make_shared<ConversionInfo>(fid, lid, location, policy,
                                          update_ctime);
}

EOSMGMNAMESPACE_END
