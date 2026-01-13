//------------------------------------------------------------------------------
//! @file PathRouting.hh
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
#include "common/Mapping.hh"
#include "common/Logging.hh"
#include "common/AssistedThread.hh"
#include "mgm/routeendpoint/RouteEndpoint.hh"
#include <map>
#include <list>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class PathRouting
//------------------------------------------------------------------------------
class PathRouting: public eos::common::LogId
{
public:

  //! Reroute response type
  enum class Status {
    REROUTE,   ///! Route was found and available
    NOROUTING, ///! No route found
    STALL      ///! Route found but no endpoint available
  };

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param upd_timeout async thread update interval
  //----------------------------------------------------------------------------
  PathRouting(std::chrono::seconds upd_timeout = std::chrono::seconds(5)):
    mTimeout(upd_timeout)
  {
    if (mTimeout.count()) {
      mThread.reset(&PathRouting::UpdateEndpointsStatus, this);
    }
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~PathRouting();

  //----------------------------------------------------------------------------
  //! @brief Route a path according to the configured routing table. This
  //! function does the path translation according to the configured routing
  //! table. It applies the 'longest' matching rule.
  //!
  //! @param inpath path to route
  //! @param ininfo opaque information
  //! @param vid user virtual identity
  //! @param host redirection host
  //! @param port redirection port
  //! @param stat_info stat info string to be aggregated by MgmStats
  //!
  //! @return Status enum representing the state of the routing
  //----------------------------------------------------------------------------
  Status Reroute(const char* inpath, const char* ininfo,
                 eos::common::VirtualIdentity& vid,
                 std::string& host, int& port, std::string& stat_info);

  //----------------------------------------------------------------------------
  //! Add a source/target pair to the path routing table
  //!
  //! @param path prefix path to route
  //! @param endpoint endpoint for the routing
  //!
  //! @return true if route added, otherwise false
  //----------------------------------------------------------------------------
  bool Add(const std::string& path, RouteEndpoint&& endpoint);

  //----------------------------------------------------------------------------
  //! Remove routing for the corresponding path
  //!
  //! @param path routing path to be removed
  //!
  //! @return true if successfully removed, otherwise false
  //----------------------------------------------------------------------------
  bool Remove(const std::string& path);

  //----------------------------------------------------------------------------
  //! Clear all the stored entries in the path routing table
  //----------------------------------------------------------------------------
  void Clear();

  //----------------------------------------------------------------------------
  //! Get routes listing
  //!
  //! @param path get listing for a particular path, if empty then all routes
  //!        will be returned
  //! @param out output string
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool GetListing(const std::string& path, std::string& out) const;

private:

  //----------------------------------------------------------------------------
  //! Method executed by an async thread which is updating the current master
  //! endpoint for each routing
  //!
  //! @param assistant thread executing the method
  //----------------------------------------------------------------------------
  void UpdateEndpointsStatus(ThreadAssistant& assistant) noexcept;

  std::map<std::string, std::list<RouteEndpoint>> mPathRoute;
  mutable eos::common::RWMutex mPathRouteMutex;
  AssistedThread mThread; ///< Thread updating the master endpoints
  std::chrono::seconds mTimeout; ///< Update timeout
};

EOSMGMNAMESPACE_END
