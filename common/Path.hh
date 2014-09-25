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
#include "common/http/HttpResponse.hh"
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

#define EOS_COMMON_PATH_VERSION_PREFIX "/.sys.v#."
#define EOS_COMMON_PATH_VERSION_FILE_PREFIX ".sys.v#."
#define EOS_COMMON_PATH_ATOMIC_FILE_PREFIX ".sys.a#."
#define EOS_COMMON_PATH_ATOMIC_FILE_VERSION_PREFIX ".sys.a#.v#"

/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
//! Class providing some comfortable functions on path names
/*----------------------------------------------------------------------------*/

class Path {
protected:
  XrdOucString fullPath; //< the full path stored
  XrdOucString parentPath; //< path of the parent directory
  XrdOucString lastPath; //< the base name/file name
  XrdOucString atomicPath; //< temporary version of a path e.g. basename => .basename.<uuid>
  XrdOucString versionDir; //< directory name storing versions for a file path
  std::vector<std::string> subPath; //< a vector with all partial sub-path

public:



  // ---------------------------------------------------------------------------
  //! Return basename/filename
  // ---------------------------------------------------------------------------

  const char*
  GetName ()
  {
    return lastPath.c_str();
  }

  // ---------------------------------------------------------------------------
  //! Return full path
  // ---------------------------------------------------------------------------

  const char*
  GetPath ()
  {
    return fullPath.c_str();
  }

  // ---------------------------------------------------------------------------
  //! Return path of the parent directory
  // ---------------------------------------------------------------------------

  const char*
  GetParentPath ()
  {
    return parentPath.c_str();
  }

  // ---------------------------------------------------------------------------
  //! Return version directory path
  // ---------------------------------------------------------------------------

  const char*
  GetVersionDirectory ()
  {
    versionDir = GetParentPath();
    versionDir += EOS_COMMON_PATH_VERSION_PREFIX;
    versionDir += GetName();
    versionDir += "/";
    while (versionDir.replace("//", "/"))
    {
    }
    return versionDir.c_str();
  }


  // ---------------------------------------------------------------------------
  //! Return full path
  // ---------------------------------------------------------------------------

  XrdOucString&
  GetFullPath ()
  {
    return fullPath;
  }

  // ---------------------------------------------------------------------------
  //! Return atomic path
  // ---------------------------------------------------------------------------

  const char*
  GetAtomicPath (bool versioning, XrdOucString externuuid = "")
  {
    if (atomicPath.length())
      return atomicPath.c_str();
    else
      return MakeAtomicPath(versioning, externuuid);
  }

  // ---------------------------------------------------------------------------
  //! Return a unique atomic version of that path
  // ---------------------------------------------------------------------------

  const char* MakeAtomicPath (bool versioning, XrdOucString externuuid = "")
  {
    // create from <dirname>/<basename> => <dirname>/<ATOMIC|VERSION_PREFIX><basename>.<uuid>
    char suuid[40];
    uuid_t uuid;
    uuid_generate_time(uuid);
    uuid_unparse(uuid, suuid);
    atomicPath = GetParentPath();
    if (!versioning)
      atomicPath += EOS_COMMON_PATH_ATOMIC_FILE_PREFIX;
    else
      atomicPath += EOS_COMMON_PATH_ATOMIC_FILE_VERSION_PREFIX;
    atomicPath += GetName();
    atomicPath += ".";

    // for chunk paths we have to use the same UUID for all chunks
    if (!externuuid.length())
      atomicPath += suuid;
    else
      atomicPath += externuuid;

    return atomicPath.c_str();
  }

  // ---------------------------------------------------------------------------
  //! Decode an atomic path
  // ---------------------------------------------------------------------------

  const char* DecodeAtomicPath (bool &isVersioning)
  {
    // create from <dirname>/<ATOMIC|VERSION_PREFIX> <basename>.<uuid> => <dirname>/<basename>

    if ( (lastPath.beginswith(EOS_COMMON_PATH_ATOMIC_FILE_PREFIX))  &&
	 (lastPath.length() > 37) &&
	 (lastPath[lastPath.length() -37] == '.') )
    {
      atomicPath = fullPath;
      lastPath.erase(lastPath.length() - 37);
      if (lastPath.beginswith(EOS_COMMON_PATH_ATOMIC_FILE_VERSION_PREFIX))
      {
        lastPath.erase(0,strlen(EOS_COMMON_PATH_ATOMIC_FILE_VERSION_PREFIX));
        isVersioning = true;
      }
      else
      {
	lastPath.erase(0,strlen(EOS_COMMON_PATH_ATOMIC_FILE_PREFIX));
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
  GetSubPath (unsigned int i)
  {
    if (i < subPath.size()) return subPath[i].c_str();
    else return 0;
  }

  // ---------------------------------------------------------------------------
  //! Return number of sub paths stored
  // ---------------------------------------------------------------------------

  unsigned int
  GetSubPathSize ()
  {
    return subPath.size();
  }

  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------

  Path (const char* path)
  {
    Init(path);
  }

  // ---------------------------------------------------------------------------
  //! Initialization
  // ---------------------------------------------------------------------------

  void
  Init (const char* path)
  {
    fullPath = path;

    while (fullPath.replace("//", "/"))
    {
    }

    parentPath = "";
    lastPath = "";

    if ((fullPath == "/") ||
        (fullPath == "/.") ||
        (fullPath == "/.."))
    {
      fullPath = "/";
      return;
    }

    if (fullPath.endswith('/'))
      fullPath.erase(fullPath.length() - 1);

    // remove  /.$
    if (fullPath.endswith("/."))
    {
      fullPath.erase(fullPath.length() - 2);
    }

    // recompute /..$
    if (fullPath.endswith("/.."))
    {
      int spos = fullPath.rfind("/", fullPath.length() - 4);
      if (spos != STR_NPOS)
      {
        fullPath.erase(spos + 1);
      }
    }

    if (!fullPath.beginswith("/"))
    {
      lastPath = fullPath;
      return;
    }

    int bppos;

    // convert /./
    while ((bppos = fullPath.find("/./")) != STR_NPOS)
    {
      fullPath.erase(bppos, 2);
    }

    // convert /..
    while ((bppos = fullPath.find("/../")) != STR_NPOS)
    {
      int spos = fullPath.rfind("/", bppos - 1);
      if (spos != STR_NPOS)
      {
        fullPath.erase(bppos, 4);
        fullPath.erase(spos + 1, bppos - spos - 1);
      }
    }

    if (!fullPath.length())
    {
      fullPath = "/";
    }

    int lastpos = 0;
    int pos = 0;
    do
    {
      pos = fullPath.find("/", pos);
      std::string subpath;
      if (pos != STR_NPOS)
      {
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
  MakeParentPath (mode_t mode)
  {
    int retc = 0;
    struct stat buf;

    if (stat(GetParentPath(), &buf))
    {
      for (int i = GetSubPathSize(); i >= 0; i--)
      {
        // go backwards until the directory exists
        if (!stat(GetSubPath(i), &buf))
        {
          // this exists
          for (int j = i + 1; j < (int) GetSubPathSize(); j++)
          {
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

  ~Path ()
  {
  };
};
/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

#endif

