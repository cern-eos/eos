//------------------------------------------------------------------------------
//! @file RouteEndpoint.hh
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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#pragma once
#include "mgm/Namespace.hh"
#include <string>
#include <stdint.h>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class RouteEndpoint
//------------------------------------------------------------------------------
class RouteEndpoint
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  RouteEndpoint() = default;

  //----------------------------------------------------------------------------
  //! Constructor with parameters
  //!
  //! @param host redirection host
  //! @param xrd_port xrootd redirection port
  //! @param http_port http redirection port
  //----------------------------------------------------------------------------
  RouteEndpoint(const std::string& fqdn, uint32_t xrd_port, uint32_t http_port):
    mFqdn(fqdn), mXrdPort(xrd_port), mHttpPort(http_port), mIsMaster(false)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RouteEndpoint() = default;

  //----------------------------------------------------------------------------
  //! Parse route endpoint specification from string
  //!
  //! @param string route endpoint specification in the form of:
  //!        <host_fqdn>:<xrd_port>:<http_port>
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseFromString(const std::string& input);

  //----------------------------------------------------------------------------
  //! Get string representation in the form of:
  //! <host_fqdn>:<xrd_port>:<http_port>
  //----------------------------------------------------------------------------
  std::string ToString() const;

  //----------------------------------------------------------------------------
  //! Get redirection host
  //----------------------------------------------------------------------------
  inline std::string GetHostname() const
  {
    return mFqdn;
  }

  //----------------------------------------------------------------------------
  //! Get Xrd redirection port
  //----------------------------------------------------------------------------
  inline int GetXrdPort() const
  {
    return mXrdPort;
  }

  //----------------------------------------------------------------------------
  //! Get http redirection port
  //----------------------------------------------------------------------------
  inline int GetHttpPort() const
  {
    return mHttpPort;
  }

  //----------------------------------------------------------------------------
  //! Set is master route
  //!
  //! @param is_master true if this is master route, otherwise false
  //----------------------------------------------------------------------------
  inline void SetMaster(bool is_master)
  {
    mIsMaster = is_master;
  }

  //----------------------------------------------------------------------------
  //! Check if this is a master route
  //!
  //! @return true if master route, otherwise false
  //----------------------------------------------------------------------------
  inline bool IsMaster() const
  {
    return mIsMaster;
  }

  //----------------------------------------------------------------------------
  //! Operator ==
  //----------------------------------------------------------------------------
  bool operator ==(const RouteEndpoint& rhs) const
  {
    return ((mFqdn == rhs.mFqdn) &&
            (mXrdPort == rhs.mXrdPort) &&
            (mHttpPort == rhs.mHttpPort));
  }

  //----------------------------------------------------------------------------
  //! Operator !=
  //----------------------------------------------------------------------------
  bool operator !=(const RouteEndpoint& rhs) const
  {
    return !(*this == rhs);
  }

private:
  std::string mFqdn; ///< Redirection host fqdn
  uint32_t mXrdPort; ///< Redirection xrootd port
  uint32_t mHttpPort; ///< Redirectoin http port
  bool mIsMaster; ///< Mark master route
};

EOSMGMNAMESPACE_END
