//------------------------------------------------------------------------------
// File: Constants.hh
// Author: Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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

#pragma once
#include <string>

namespace eos::mgm
{

// ATTR constants
// We are using static std::string as most use for these are in map.find, wherein we can
// save a potential allocation. A future move would be to a constexpr string/view where
// maps can handle transparent comparision and hence completely avoid allocation in find apis
static const std::string EOS_ATOMIC = "eos.atomic";
static const std::string EOS_INJECTION = "eos.injection";

static const std::string SYS_HARD_LINK = "sys.eos.mdino";
static const std::string SYS_NUM_LINK = "sys.eos.nlink";
static const std::string SYS_OWNER_AUTH = "sys.owner.auth";
static const std::string SYS_VERSIONING = "sys.versioning";
static const std::string SYS_ALTCHECKSUMS = "sys.altxs";
static const std::string SYS_FORCED_ATOMIC = "sys.forced.atomic";
static const std::string SYS_REDIRECT_ENOENT = "sys.redirect.enoent";
static const std::string SYS_FORCED_MINSIZE = "sys.forced.minsize";
static const std::string SYS_FORCED_MAXSIZE = "sys.forced.maxsize";
static const std::string SYS_FORCED_STALLTIME = "sys.forced.stalltime";
static const std::string SYS_FORCED_SPACE = "sys.forced.space";
static const std::string SYS_FORCED_GROUP = "sys.forced.group";
static const std::string SYS_FORCED_LAYOUT = "sys.forced.layout";
static const std::string SYS_FORCED_CHECKSUM = "sys.forced.checksum";
static const std::string SYS_FORCED_BLOCKSIZE = "sys.forced.blocksize";
static const std::string SYS_FORCED_BLOCKCHECKSUM = "sys.forced.blockchecksum";
static const std::string SYS_FORCED_NSTRIPES = "sys.forced.nstripes";

static const std::string USER_VERSIONING = "user.versioning";
static const std::string USER_FORCED_ATOMIC = "user.forced.atomic";
static const std::string USER_STALL_UNAVAILABLE = "user.stall.unavailable";
static const std::string USER_TAG = "user.tag";
static const std::string USER_FORCED_LAYOUT = "user.forced.layout";
static const std::string USER_FORCED_CHECKSUM = "user.forced.checksum";
static const std::string USER_FORCED_BLOCKSIZE = "user.forced.blocksize";
static const std::string USER_FORCED_BLOCKCHECKSUM =
  "user.forced.blockchecksum";
static const std::string USER_FORCED_NSTRIPES = "user.forced.nstripes";

// Policy related strings
static const std::string POLICY_BANDWIDTH = "policy.bandwidth";
static const std::string POLICY_IOPRIORITY = "policy.iopriority";
static const std::string POLICY_IOTYPE = "policy.iotype";
static const std::string POLICY_SCHEDULE = "policy.schedule";


} // eos::mgm
