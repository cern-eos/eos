// ----------------------------------------------------------------------
// File: Path.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

/**
 * @file   Path.hh
 * 
 * @brief  Convenience Class to deal with path names.
 * 
 * 
 */

#ifndef __EOSCOMMON_PATH__
#define __EOSCOMMON_PATH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <vector>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <uuid/uuid.h>

/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
//! Class providing some comfortable functions on path names
/*----------------------------------------------------------------------------*/

class Path {
private:
  XrdOucString fullPath; //< the full path stored
  XrdOucString parentPath; //< path of the parent directory
  XrdOucString lastPath; //< the base name/file name
  XrdOucString atomicPath; //< temporary version of a path e.g. basename => .basename.<uuid>
  std::vector<std::string> subPath; //< a vector with all partial sub-paths

public:
  // ---------------------------------------------------------------------------
  //! Return basename/filename
  // ---------------------------------------------------------------------------

  const char*
  GetName () {
    return lastPath.c_str();
  }

  // ---------------------------------------------------------------------------
  //! Return full path
  // ---------------------------------------------------------------------------

  const char*
  GetPath () {
    return fullPath.c_str();
  }

  // ---------------------------------------------------------------------------
  //! Return path of the parent directory
  // ---------------------------------------------------------------------------

  const char*
  GetParentPath () {
    return parentPath.c_str();
  }

  // ---------------------------------------------------------------------------
  //! Return full path
  // ---------------------------------------------------------------------------

  XrdOucString&
  GetFullPath () {
    return fullPath;
  }

  // ---------------------------------------------------------------------------
  //! Return atomic path
  // ---------------------------------------------------------------------------
  const char*
  GetAtomicPath (bool versioning) {
    if (atomicPath.length())
      return atomicPath.c_str();
    else
      return MakeAtomicPath(versioning);
  }

  // ---------------------------------------------------------------------------
  //! Return a unique atomic version of that path
  // ---------------------------------------------------------------------------
  const char* MakeAtomicPath (bool versioning) {
    // create from <dirname>/<basename> => <dirname>/.[.]<basename>.<uuid>
    char suuid[40];
    uuid_t uuid;
    uuid_generate_time(uuid);
    uuid_unparse(uuid, suuid);
    atomicPath = GetParentPath();
    atomicPath += ".";
    if (versioning)
      atomicPath += ".";
    atomicPath += GetName();
    atomicPath += ".";
    atomicPath += suuid;
    return atomicPath.c_str();
  }

  // ---------------------------------------------------------------------------
  //! Decode an atomic path
  // ---------------------------------------------------------------------------
  const char* DecodeAtomicPath (bool &isVersioning) {
    // create from <dirname>/.[.]<basename>.<uuid> => <dirname>/<basename>
    if (lastPath.beginswith(".") && 
	(lastPath.length() > 37) &&
	(lastPath[lastPath.length()-37] == '.') )
    {
      atomicPath = fullPath;
      lastPath.erase(lastPath.length()-37);
      if (lastPath.beginswith(".."))
      {
	lastPath.erase(0,2);
	isVersioning = true;
      }
      else
      {
	lastPath.erase(0,1);
	isVersioning = false;
      }
      fullPath = parentPath + lastPath;
    }
    return fullPath.c_str();
  }

  // ---------------------------------------------------------------------------
  //! Return sub path with depth i (0 is / 1 is /eos/ aso....)
  // ---------------------------------------------------------------------------

  const char*
  GetSubPath (unsigned int i) {
    if (i < subPath.size()) return subPath[i].c_str();
    else return 0;
  }

  // ---------------------------------------------------------------------------
  //! Return number of sub paths stored
  // ---------------------------------------------------------------------------

  unsigned int
  GetSubPathSize () {
    return subPath.size();
  }

  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------

  Path (const char* path) {
    fullPath = path;
    parentPath = "";
    lastPath = "";
    if ((fullPath == "/") ||
        (fullPath == "/.") ||
        (fullPath == "/..")) {
      fullPath = "/";
      return;
    }

    if (fullPath.endswith('/'))
      fullPath.erase(fullPath.length() - 1);

    // remove  /.$
    if (fullPath.endswith("/.")) {
      fullPath.erase(fullPath.length() - 2);
    }

    // recompute /..$
    if (fullPath.endswith("/..")) {
      int spos = fullPath.rfind("/", fullPath.length()-4);
      if (spos != STR_NPOS) {
        fullPath.erase(spos+1);
      }
    }

    if (!fullPath.beginswith("/")) {
      lastPath = fullPath;
      return;
    }

    int bppos;

    // convert /./
    while ((bppos = fullPath.find("/./")) != STR_NPOS) {
      fullPath.erase(bppos, 2);
    }

    // convert /..
    while ((bppos = fullPath.find("/../")) != STR_NPOS) {
      int spos = fullPath.rfind("/", bppos - 1);
      if (spos != STR_NPOS) {
        fullPath.erase(bppos, 4);
        fullPath.erase(spos + 1, bppos - spos - 1);
      }
    }

    if (!fullPath.length()) {
      fullPath = "/";
    }

    int lastpos = 0;
    int pos = 0;
    do {
      pos = fullPath.find("/", pos);
      std::string subpath;
      if (pos != STR_NPOS) {
        subpath.assign(fullPath.c_str(), pos + 1);
        subPath.push_back(subpath);
        lastpos = pos;
        pos++;
      }
    }
    while (pos != STR_NPOS);
    parentPath.assign(fullPath, 0, lastpos);
    lastPath.assign(fullPath, lastpos + 1);
  }

  // ---------------------------------------------------------------------------
  //! Convenience function to auto-create all needed parent paths for this path object with mode
  // ---------------------------------------------------------------------------

  bool
  MakeParentPath (mode_t mode) {
    int retc = 0;
    struct stat buf;

    if (stat(GetParentPath(), &buf)) {
      for (int i = GetSubPathSize(); i >= 0; i--) {
        // go backwards until the directory exists
        if (!stat(GetSubPath(i), &buf)) {
          // this exists
          for (int j = i + 1; j < (int) GetSubPathSize(); j++) {
            retc |= (mkdir(GetSubPath(j), mode) ? ((errno == EEXIST) ? 0 : -1) : 0);
          }
          break;
        }
      }
    }

    if (retc)
      return false;
    return true;
  }

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------

  ~Path () { };
};
/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

#endif

