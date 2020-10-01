//------------------------------------------------------------------------------
//! @file RouteEndpoint.cc
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

#include "mgm/RouteEndpoint.hh"
#include "common/StringConversion.hh"
#include "common/ParseUtils.hh"
#include "XrdCl/XrdClURL.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include <sstream>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Move assignment operator
//------------------------------------------------------------------------------
RouteEndpoint&
RouteEndpoint::operator =(RouteEndpoint&& other) noexcept
{
  if (this != &other) {
    std::swap(mFqdn, other.mFqdn);
    mXrdPort = other.mXrdPort;
    mHttpPort = other.mHttpPort;
    mIsOnline.store(other.mIsOnline.load());
    mIsMaster.store(other.mIsMaster.load());
  }

  return *this;
}

//------------------------------------------------------------------------------
// Parse route endpoint specification from string
//------------------------------------------------------------------------------
bool
RouteEndpoint::ParseFromString(const std::string& input)
{
  std::vector<std::string> tokens;
  eos::common::StringConversion::Tokenize(input, tokens, ":");

  if (tokens.size() != 3) {
    return false;
  }

  mFqdn = tokens[0];

  try {
    mXrdPort = std::stoul(tokens[1]);
    mHttpPort = std::stoul(tokens[2]);
  } catch (const std::exception& e) {
    return false;
  }

  return eos::common::ValidHostnameOrIP(mFqdn);
}

//------------------------------------------------------------------------------
// Get string representation of the endpoint
//------------------------------------------------------------------------------
std::string
RouteEndpoint::ToString() const
{
  std::ostringstream oss;
  oss << mFqdn << ":" << mXrdPort << ":" << mHttpPort;
  return oss.str();
}

//------------------------------------------------------------------------------
// Update master status
//------------------------------------------------------------------------------
void
RouteEndpoint::UpdateStatus()
{
  std::ostringstream oss;
  oss << "root://" << mFqdn << ":" << mXrdPort << "//dummy?xrd.wantprot=sss,unix";
  XrdCl::URL url(oss.str());

  if (!url.IsValid()) {
    mIsOnline = false;
    mIsMaster = false;
    eos_static_crit("invalid url host='%s'", mFqdn.c_str());
    return;
  }

  // Check if node is online
  XrdCl::FileSystem fs(url);
  XrdCl::XRootDStatus st = fs.Ping(1);

  if (!st.IsOK()) {
    mIsOnline = false;
    mIsMaster = false;
    eos_static_debug("failed to ping host='%s'", mFqdn.c_str());
    return;
  }

  mIsOnline.store(true);
  /* TODO: review if we want to have this policy by hostname or not ... currently disabled
  // If the host names is not starting with eos, we assume that is just a plain XrootD service
  if (mFqdn.substr(0,3) != "eos") {
    mIsMaster = true;
    eos_static_debug("disabling EOS master check host='%s' - assuming standard "
                     "XRootD service (hostname does not start with eos...)",
                     mFqdn.c_str());
    return ;
  }
  */
  // Check if node is master
  XrdCl::Buffer* response {nullptr};
  XrdCl::Buffer request;
  request.FromString("/?mgm.pcmd=is_master");

  if (!fs.Query(XrdCl::QueryCode::OpaqueFile, request, response).IsOK()) {
    eos_static_debug("host='%s' is running as 'master'", mFqdn.c_str());
    mIsMaster = false;
  } else {
    eos_static_debug("host='%s' is NOT running as 'master'", mFqdn.c_str());
    mIsMaster = true;
  }

  delete response;
}

EOSMGMNAMESPACE_END
