//------------------------------------------------------------------------------
// File: XrdConnPool.hh
// Author: Elvin-Alin Sindrilaru - CERN
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

#pragma once
#include "common/Namespace.hh"
#include "common/Logging.hh"
#include "XrdCl/XrdClURL.hh"
#include <mutex>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class XrdConnPool help in creating a pool of xrootd connections that can
//! be reused and allocate the least congested connection to a new request.
//------------------------------------------------------------------------------
class XrdConnPool: public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param is_enabled if true connection pool is enabled
  //! @param max_size default max_size
  //----------------------------------------------------------------------------
  XrdConnPool(bool is_enabled = false, uint32_t max_size = 1024);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~XrdConnPool() = default;

  //----------------------------------------------------------------------------
  //! Assign new connection from the pool to the given URL. What this actually
  //! means is updating the username used in the URL when connecting to the
  //! XRootD server.
  //!
  //! @param url given url
  //!
  //! @return 0 if no connection id assigned, otherwise the value of the id
  //----------------------------------------------------------------------------
  uint32_t AssignConnection(XrdCl::URL& url);

  //----------------------------------------------------------------------------
  //! Release a connection and update the status of the pool
  //!
  //! @param url given url
  //----------------------------------------------------------------------------
  void ReleaseConnection(const XrdCl::URL& url);

  //----------------------------------------------------------------------------
  //! Dump the status of the connection pool to the given string
  //!
  //! @param out string containing the result
  //----------------------------------------------------------------------------
  void Dump(std::string& out) const;

private:
  bool mIsEnabled; ///< Mark if connection pool is enabled
  uint32_t mMaxSize; ///< Maximum size of the connection pool
  std::map<std::string, std::map<uint32_t, uint32_t>> mConnPool;
  std::mutex mPoolMutex; ///< Mutex protecting access to the pool
};

//------------------------------------------------------------------------------
//! Class XrdConnIdHelper RAAI helper to automatically assign and release
//! connection ids to the pool.
//! @note Needs to have the same lifetime as the XrdCl::File object that uses
//! the url.
//------------------------------------------------------------------------------
class XrdConnIdHelper final
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdConnIdHelper(XrdConnPool& pool, XrdCl::URL& url):
    mPool(pool)
  {
    mConnId = mPool.AssignConnection(url);
    mUrl = url;
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~XrdConnIdHelper()
  {
    if (mConnId) {
      mPool.ReleaseConnection(mUrl);
    }
  }

  //----------------------------------------------------------------------------
  //! Check if new connection allocated to URL
  //----------------------------------------------------------------------------
  bool HasNewConnection() const
  {
    return (mConnId != 0ull);
  }

  //----------------------------------------------------------------------------
  //! Get allocated connection id
  //----------------------------------------------------------------------------
  uint32_t GetId() const
  {
    return mConnId;
  }

private:
  uint32_t mConnId; ///< Allocated connection id, 0 if none allocated
  XrdConnPool& mPool; ///< Reference to connection pool
  XrdCl::URL mUrl; ///< URL corresponding to the connection id
};

EOSCOMMONNAMESPACE_END
