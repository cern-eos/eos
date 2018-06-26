//------------------------------------------------------------------------------
// File: PathRoute.cc
// Author: Andreas-Joachim Peters - CERN
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

//------------------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Clear the routing table
//------------------------------------------------------------------------------
void
XrdMgmOfs::ClearPathRoutes()
{
  eos::common::RWMutexWriteLock lock(mPathRouteMutex);
  mPathRoute.clear();
}

//------------------------------------------------------------------------------
// Add path endpoint pair to the routing table
//------------------------------------------------------------------------------
bool
XrdMgmOfs::AddPathRoute(const std::string& path, RouteEndpoint&& endpoint)
{
  std::string string_rep = endpoint.ToString();
  eos::common::RWMutexWriteLock route_wr_lock(mPathRouteMutex);
  auto it = mPathRoute.find(path);

  if (it == mPathRoute.end()) {
    mPathRoute.emplace(path, std::list<RouteEndpoint> {std::move(endpoint)});
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
XrdMgmOfs::RemovePathRoute(const std::string& path)
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
bool
XrdMgmOfs::PathReroute(const char* inpath, const char* ininfo,
                       eos::common::Mapping::VirtualIdentity_t& vid,
                       std::string& host, int& port)
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
    return false;
  }

  if (path.back() != '/') {
    path += '/';
    
  XrdOucString sinpath = eos::common::StringConversion::curl_unescaped(path.c_str()).c_str();

  eos::common::Path cPath(sinpath.c_str());

  if (EOS_LOGS_DEBUG) {
    eos_debug("routepath=%s ndir=%d dirlevel=%d", sinpath.c_str(), PathRoute.size(),
              cPath.GetSubPathSize() - 1);
  }

  eos_debug("path=%s map_route_size=%d", sinpath.c_str(), mPathRoute.size());
  eos::common::RWMutexReadLock lock(mPathRouteMutex);

  if (mPathRoute.empty()) {
    eos_debug("no routes defined");
    return false;
  }

  auto it = mPathRoute.find(path);

  if (it == mPathRoute.end()) {
    // Try to find the longest possible match
    eos::common::Path cPath(path.c_str());

    if (!cPath.GetSubPathSize()) {
      eos_debug("path=%s has no subpath", path.c_str());
      return false;
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
      return false;
    }
  }

  std::ostringstream oss;
  oss << "Rt:";
  // @todo (esindril): pick the master
  auto& endpoint = it->second.front();

  // Http redirection
  if (vid.prot == "http" || vid.prot == "https") {
    port = endpoint.GetHttpPort();
    oss << vid.prot.c_str();
  } else {
    // XRootD redirection
    port = endpoint.GetXrdPort();
    oss << "xrd";
  }

  host = endpoint.GetHostname();
  oss << ":" << host;
  MgmStats.Add(oss.str().c_str(), vid.uid, vid.gid, 1);
  eos_debug("re-routing path=%s using match_path=%s to host=%s port=%d",
            path.c_str(), it->first.c_str(), host.c_str(), port);
  return true;
}
