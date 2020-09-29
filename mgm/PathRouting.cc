//------------------------------------------------------------------------------
//! @file PathRouting.cc
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

#include "mgm/PathRouting.hh"
#include "common/Path.hh"
#include "common/StringConversion.hh"
#include "XrdCl/XrdClURL.hh"
#include <sstream>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
PathRouting::~PathRouting()
{
  mThread.join();
}

//------------------------------------------------------------------------------
// Clear the routing table
//------------------------------------------------------------------------------
void
PathRouting::Clear()
{
  eos::common::RWMutexWriteLock lock(mPathRouteMutex);
  mPathRoute.clear();
}

//------------------------------------------------------------------------------
// Add path endpoint pair to the routing table
//------------------------------------------------------------------------------
bool
PathRouting::Add(const std::string& path, RouteEndpoint&& endpoint)
{
  std::string string_rep = endpoint.ToString();
  eos::common::RWMutexWriteLock route_wr_lock(mPathRouteMutex);
  auto it = mPathRoute.find(path);

  if (it == mPathRoute.end()) {
    auto it_emplace = mPathRoute.emplace(path, std::list<RouteEndpoint>());
    it_emplace.first->second.emplace_back(std::move(endpoint));
  } else {
    bool found = false;

    for (const auto& ep : it->second) {
      if (ep == endpoint) {
        found = true;
        break;
      }
    }

    if (found) {
      return false;
    }

    it->second.emplace_back(std::move(endpoint));
  }

  eos_debug("added route %s => %s", path.c_str(), string_rep.c_str());
  return true;
}

//------------------------------------------------------------------------------
// Remove routing for the corresponding path
//------------------------------------------------------------------------------
bool
PathRouting::Remove(const std::string& path)
{
  eos::common::RWMutexWriteLock route_wr_lock(mPathRouteMutex);
  auto it = mPathRoute.find(path);

  if (path.empty() || (it == mPathRoute.end())) {
    return false;
  }

  mPathRoute.erase(it);
  return true;
}

//------------------------------------------------------------------------------
// Route a path according to the configured routing table
//------------------------------------------------------------------------------
PathRouting::Status
PathRouting::Reroute(const char* inpath, const char* ininfo,
                     eos::common::VirtualIdentity& vid,
                     std::string& host, int& port, std::string& stat_info)
{
  // Process and extract the path for which we need to do the routing
  std::string path = (inpath ? inpath : "");
  std::string surl = path;

  if (ininfo) {
    surl += "?";
    surl += ininfo;
  }

  XrdCl::URL url(surl);
  XrdCl::URL::ParamsMap param = url.GetParams();

  // If there is a routing tag in the CGI, we use that one to map
  if (param["eos.route"].length()) {
    path = param["eos.route"];
  } else if (param["mgm.path"].length()) {
    path = param["mgm.path"];
  } else if (param["mgm.quota.space"].length()) {
    path = param["mgm.quota.space"];
  }

  // Make sure path is not empty and is '/' terminated
  if (path.empty()) {
    eos_debug("input path is empty");
    return Status::NOROUTING;
  }

  path = eos::common::StringConversion::curl_unescaped(path.c_str()).c_str();
  eos::common::Path cPath(path.c_str());
  path = cPath.GetPath();

  if (path.back() != '/') {
    path += '/';
  }

  eos_debug("path=%s map_route_size=%d", path.c_str(), mPathRoute.size());
  eos::common::RWMutexReadLock route_rd_lock(mPathRouteMutex);

  if (mPathRoute.empty()) {
    eos_debug("no routes defined");
    return Status::NOROUTING;
  }

  auto it = mPathRoute.find(path);

  if (it == mPathRoute.end()) {
    // Try to find the longest possible match
    if (!cPath.GetSubPathSize()) {
      eos_debug("path=%s has no subpath", path.c_str());
      return Status::NOROUTING;
    }

    for (size_t i = cPath.GetSubPathSize() - 1; i > 0; i--) {
      eos_debug("[route] %s => %s\n", path.c_str(), cPath.GetSubPath(i));
      it = mPathRoute.find(cPath.GetSubPath(i));

      if (it != mPathRoute.end()) {
        break;
      }
    }

    // If no match found then return
    if (it == mPathRoute.end()) {
      return Status::NOROUTING;
    }
  }

  std::ostringstream oss;
  oss << "Rt:";
  // Try to find the master endpoint, if none exists then just redirect to the
  // first endpoint in the list if reachable
  auto* master_ep = &it->second.front();

  for (auto& endpoint : it->second) {
    if (endpoint.mIsOnline.load() && endpoint.mIsMaster.load()) {
      master_ep = &endpoint;
      break;
    }
  }

  if (!master_ep->mIsOnline.load()) {
    eos_warning("no online endpoints for route path=%s", it->first.c_str());
    return Status::STALL;
  }

  // Http redirection
  if (vid.prot == "http" || vid.prot == "https") {
    port = master_ep->GetHttpPort();
    oss << vid.prot.c_str();
  } else {
    // XRootD redirection
    port = master_ep->GetXrdPort();
    oss << "xrd";
  }

  host = master_ep->GetHostname();
  oss << ":" << host;
  stat_info = oss.str();
  eos_debug("re-routing path=%s using match_path=%s to host=%s port=%d",
            path.c_str(), it->first.c_str(), host.c_str(), port);
  return Status::REROUTE;
}

//------------------------------------------------------------------------------
// Get routes listing
//------------------------------------------------------------------------------
bool
PathRouting::GetListing(const std::string& path, std::string& out) const
{
  std::ostringstream oss;
  eos::common::RWMutexReadLock route_rd_lock(mPathRouteMutex);
  auto printRoute =
  [&oss](const std::pair<const std::string, std::list<RouteEndpoint>>& route) {
    bool first = true;
    oss << route.first << " => ";

    for (const auto& endpoint : route.second) {
      if (!first) {
        oss << ",";
      }

      if (!endpoint.mIsOnline.load()) {
        oss << "_";
      } else if (endpoint.mIsMaster.load()) {
        oss << "*";
      }

      oss << endpoint.ToString();
      first = false;
    }

    oss << std::endl;
  };

  // List all paths
  if (path.empty()) {
    for (const auto& elem : mPathRoute) {
      printRoute(elem);
    }
  } else {
    auto it = mPathRoute.find(path);

    if (it == mPathRoute.end()) {
      return false;
    }

    printRoute(*it);
  }

  out = oss.str();
  return true;
}

//------------------------------------------------------------------------------
// Method executed by an async thread which is updating the current master
// endpoint for each routing
//------------------------------------------------------------------------------
void
PathRouting::UpdateEndpointsStatus(ThreadAssistant& assistant) noexcept
{
  while (!assistant.terminationRequested()) {
    {
      eos::common::RWMutexReadLock route_rd_lock(mPathRouteMutex);

      for (auto& route : mPathRoute) {
        int num_masters = 0;
        eos_debug("checking route='%s'", route.first.c_str());

        for (auto& endpoint : route.second) {
          endpoint.UpdateStatus();

          if (endpoint.mIsOnline.load() && endpoint.mIsMaster.load()) {
            ++num_masters;
          }
        }

        // There is smth awfully wrong if we have more than two masters ...
        if (num_masters >= 2) {
          eos_warning("there is more than one master for route path=%s",
                      route.first.c_str());

          // Mark them all as offline so that we stall the clients
          for (auto& endpoint : route.second) {
            endpoint.mIsOnline.store(false);
            endpoint.mIsMaster.store(false);
          }
        }
      }
    }
    assistant.wait_for(mTimeout);
  }
}


EOSMGMNAMESPACE_END
