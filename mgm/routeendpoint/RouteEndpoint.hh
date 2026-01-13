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
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#pragma once
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include <string>
#include <stdint.h>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class RouteEndpoint
//------------------------------------------------------------------------------
class RouteEndpoint: public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  RouteEndpoint():
    mIsOnline(false), mIsMaster(false), mXrdPort(0), mHttpPort(0)
  {}

  //----------------------------------------------------------------------------
  //! Constructor with parameters
  //!
  //! @param host redirection host
  //! @param xrd_port xrootd redirection port
  //! @param http_port http redirection port
  //----------------------------------------------------------------------------
  RouteEndpoint(const std::string& fqdn, uint32_t xrd_port, uint32_t http_port):
    mIsOnline(false), mIsMaster(false), mFqdn(fqdn), mXrdPort(xrd_port),
    mHttpPort(http_port)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RouteEndpoint() = default;

  //----------------------------------------------------------------------------
  //! Move assignment operator
  //----------------------------------------------------------------------------
  RouteEndpoint& operator =(RouteEndpoint&& other) noexcept;

  //----------------------------------------------------------------------------
  //! Move constructor
  //----------------------------------------------------------------------------
  RouteEndpoint(RouteEndpoint&& other) noexcept
  {
    *this = std::move(other);
  }

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
  //! Update status - both the online and master status
  //------------------------------------------------------------------------------
  void UpdateStatus();

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

  std::atomic<bool> mIsOnline; ///< Mark if node is online
  std::atomic<bool> mIsMaster; ///< Mark if node is master

private:
  std::string mFqdn; ///< Redirection host fqdn
  uint32_t mXrdPort; ///< Redirection xrootd port
  uint32_t mHttpPort; ///< Redirection http port
};

EOSMGMNAMESPACE_END
