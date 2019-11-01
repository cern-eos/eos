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

EOSMGMNAMESPACE_BEGIN

//----------------------------------------------------------------------------
// Constructor
//----------------------------------------------------------------------------
ConversionInfo::ConversionInfo(const eos::common::FileId::fileid_t fid,
                               const eos::common::LayoutId::layoutid_t lid,
                               const eos::common::GroupLocator& location,
                               const std::string& plct_policy) :
  fid(fid), lid(lid), location(location), plct_policy(plct_policy)
{
  std::ostringstream conversion;

  conversion << std::hex << std::setfill('0')
             << std::setw(16) << fid           // <fid(016hex)>
             << ":" << location.getSpace()     // :<space>
             << "." << location.getIndex()     // .<group>
             << "#" << std::setw(8) << lid;    // #<layoutid(08hex)>

  if (!plct_policy.empty()) {
    conversion << "~" << plct_policy;          // ~<placement_policy>
  }

  conversion_string = conversion.str();
}

//----------------------------------------------------------------------------
// Parse a conversion string representation into a conversion info object.
//
// A conversion string has the following format:
// <fid(016hex)>:<space.group>#<layoutid(08hex)>[~<placement_policy>]
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

  // Parse file id
  size_t pos = sconversion.find(":");

  if ((pos == std::string::npos) || (pos != 16)) {
    eos_static_err("msg=\"%s\" conversion_string=%s "
                   "reason=\"invalid fxid\"", errmsg, sconversion.c_str());
    return nullptr;
  } else {
    const char* hexfid = sconversion.substr(0, pos).c_str();
    fid = strtoll(hexfid, 0, 16);
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

    if (!GroupLocator::parseGroup(spacegroup, location)) {
      return nullptr;
    }
  }

  // Parse layout id
  sconversion.erase(0, pos + 1);
  pos = sconversion.find("~");

  const char* hexlid = sconversion.c_str();

  if (pos == std::string::npos) {
    hexlid = sconversion.substr(0, pos).c_str();
  }

  lid = strtoll(hexlid, 0, 16);

  // Parse placement policy
  if (pos != std::string::npos) {
    policy = sconversion.substr(pos + 1);
  }

  if (!fid || ! lid) {
    eos_static_err("msg=\"%s\" conversion_string=%s "
                   "reason=\"invalid fid or lid\"", errmsg, sconversion.c_str());
    return nullptr;
  }

  return std::make_shared<ConversionInfo>(fid, lid, location, policy);
}

EOSMGMNAMESPACE_END
