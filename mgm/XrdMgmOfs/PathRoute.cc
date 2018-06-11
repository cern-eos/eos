// ----------------------------------------------------------------------
// File: PathRoute.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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


// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------

/*----------------------------------------------------------------------------*/
void
XrdMgmOfs::ResetPathRoute ()
/*----------------------------------------------------------------------------*/
/*
 * Reset all the stored entries in the path routing table
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::RWMutexWriteLock lock(PathRouteMutex);
  PathRoute.clear();
  Routes.clear();
  RouteXrdPort.clear();
  RouteHttpPort.clear();
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmOfs::AddPathRoute (const char* source,
			 const char* target)
/*----------------------------------------------------------------------------*/
/*
 * Add a source/target pair to the path routing table
 *
 * @param source prefix path to route
 * @param target target route for substitution of prefix
 *
 * This function allows e.g. toroute paths like /eos to external storages like root://...
 * It is used by the Configuration Engin to apply a routing from a configuration
 * file.
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::RWMutexWriteLock lock(PathRouteMutex);
  if (PathRoute.count(source))
  {
    return false;
  }
  else
  {
    std::string starget = target;
    std::vector<std::string> items;
    eos::common::StringConversion::Tokenize(starget, items, ":");
    PathRoute[source] = target;
    Routes[source] = items[0];

    if (items.size()>1)
      RouteXrdPort[source] = atoi(items[1].c_str());
    else
      RouteXrdPort[source] = 1094;

    if (items.size()>2)
      RouteHttpPort[source] = atoi(items[2].c_str());
    else
      RouteHttpPort[source] = 8000;

    ConfEngine->SetConfigValue("route", source, target);
    return true;
  }
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmOfs::PathReroute (const char* inpath,
			const char* ininfo,
			eos::common::Mapping::VirtualIdentity_t &vid, 
			XrdOucString &outhost, 
			int &outport)
/*----------------------------------------------------------------------------*/
/*
 * @brief route a path name according to the configured routing table
 *
 * @param inpath path to route
 * @param outpath rerouted path
 *
 * @return true if there is a routing
 * This function does the path translation according to the configured routing
 * table. It applies the 'longest' matching rule.
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::RWMutexReadLock lock(PathRouteMutex);

  std::string surl = inpath;
  if (ininfo) 
  {
    surl += "?";
    surl += ininfo;
  }
  
  XrdCl::URL url(surl);

  XrdCl::URL::ParamsMap attr = url.GetParams();
  
  // there can be a routing tag in the CGI, in case we use that one to map
  if (attr["eos.route"].length()) {
    inpath = attr["eos.route"].c_str();
  } else if (attr["mgm.path"].length()) {
    inpath = attr["mgm.path"].c_str();
  } else if (attr["mgm.quota.space"].length()) {
    inpath = attr["mgm.quota.space"].c_str();
  }
  
  eos::common::Path cPath(inpath);
  eos_debug("routepath=%s ndir=%d dirlevel=%d", inpath, PathRoute.size(), cPath.GetSubPathSize() - 1);
  
  if (!PathRoute.size()) {
    return false;
  }

  std::string target = "Rt:";

  if (Routes.count(inpath)) {
    if (vid.prot == "http" || vid.prot == "https") {
      // http redirection
      outport = RouteHttpPort[inpath];
      target += vid.prot.c_str();
    } else {
      // xrootd redirection
      outport = RouteXrdPort[inpath];
      target += "xrd";
    }
    
    outhost = Routes[inpath].c_str();
    target += ":";
    target += outhost.c_str();

    eos_debug("re-reouting target=%s port=%d", target.c_str(), outport);
    gOFS->MgmStats.Add(target.c_str(), vid.uid, vid.gid, 1);
    return true;
  }
  
  if (!cPath.GetSubPathSize()) {
    return false;
  }

  for (size_t i = cPath.GetSubPathSize() - 1; i >= 0; i--) {
    if (Routes.count(cPath.GetSubPath(i))) {
      if (vid.prot == "http" || vid.prot == "https") {
	// http redirection
	outport = RouteHttpPort[cPath.GetSubPath(i)];
	target += vid.prot.c_str();
      } else {
	// xrootd redirection
	outport = RouteXrdPort[cPath.GetSubPath(i)];
	target += "xrd";
      }
	
      outhost = Routes[cPath.GetSubPath(i)].c_str();
      target += ":";
      target += outhost.c_str();
      gOFS->MgmStats.Add(target.c_str(), vid.uid, vid.gid, 1);
      eos_debug("re-reouting target=%s port=%d", target.c_str(), outport);
      return true;
    }
  }
  return false;
}
