// ----------------------------------------------------------------------
// File: proc/user/Rm.cc
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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

/*----------------------------------------------------------------------------*/
#include "mgm/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "mgm/Access.hh"
#include "mgm/Quota.hh"
#include "mgm/Recycle.hh"
#include "mgm/Macros.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Rm ()
{
  XrdOucString spath = pOpaque->Get("mgm.path");
  XrdOucString option = pOpaque->Get("mgm.option");
  XrdOucString deep = pOpaque->Get("mgm.deletion");

  const char* inpath = spath.c_str();
  eos::common::Path cPath(inpath);

  XrdOucString filter = "";
  std::set<std::string> rmList;

  NAMESPACEMAP;
  info = 0;
  if (info)info = 0; // for compiler happyness
  PROC_BOUNCE_ILLEGAL_NAMES;
  PROC_BOUNCE_NOT_ALLOWED;

  spath = path;

  if (!spath.length())
  {
    stdErr = "error: you have to give a path name to call 'rm'";
    retc = EINVAL;
  }
  else
  {
    if (spath.find("*") != STR_NPOS)
    {
      // this is wildcard deletion
      eos::common::Path cPath(spath.c_str());
      spath = cPath.GetParentPath();
      filter = cPath.GetName();
    }
    // check if this file exists
    XrdSfsFileExistence file_exists;
    if (gOFS->_exists(spath.c_str(), file_exists, *mError, *pVid, 0))
    {
      stdErr += "error: unable to run exists on path '";
      stdErr += spath.c_str();
      stdErr += "'";
      retc = errno;
      return SFS_OK;
    }

    if (file_exists == XrdSfsFileExistNo)
    {
      stdErr += "error: no such file or directory with path '";
      stdErr += spath.c_str();
      stdErr += "'";
      retc = ENOENT;
      return SFS_OK;
    }

    if (file_exists == XrdSfsFileExistIsFile)
    {
      // if we have rm -r <file> we remove the -r flag
      option = "";
    }

    if ((file_exists == XrdSfsFileExistIsDirectory) && filter.length())
    {
      XrdMgmOfsDirectory dir;
      // list the path and match against filter
      int listrc = dir.open(spath.c_str(), *pVid, (const char*) 0);
      if (!listrc)
      {
        const char* val;
        while ((val = dir.nextEntry()))
        {
          XrdOucString mpath = spath;
          XrdOucString entry = val;
          mpath += val;
          if ((entry == ".") ||
              (entry == ".."))
          {
            continue;
          }
          if (entry.matches(filter.c_str()))
          {
            rmList.insert(mpath.c_str());
          }
        }
      }
      // if we have rm * (whatever wildcard) we remove the -r flag
      option = "";
    }
    else
    {
      rmList.insert(spath.c_str());
    }

    // find everything to be deleted
    if (option == "r")
    {
      std::map<std::string, std::set<std::string> > found;
      std::map<std::string, std::set<std::string> >::const_reverse_iterator rfoundit;
      std::set<std::string>::const_iterator fileit;

      if (((cPath.GetSubPathSize() < 4) && (deep != "deep")) || (gOFS->_find(spath.c_str(), *mError, stdErr, *pVid, found)))
      {
        if ((cPath.GetSubPathSize() < 4) && (deep != "deep"))
        {
          stdErr += "error: deep recursive deletes are forbidden without shell confirmation code!";
          retc = EPERM;
        }
        else
        {
          stdErr += "error: unable to remove file/directory";
          retc = errno;
        }
      }
      else
      {
        eos::IContainerMD::XAttrMap attrmap;

        // check if this path exists at all
        struct stat buf;
        if (!gOFS->_stat(spath.c_str(), &buf, *mError, *pVid, ""))
        {
          // check if this path has a recycle attribute
          if (gOFS->_attr_ls(spath.c_str(), *mError, *pVid, "", attrmap))
          {
            stdErr += "error: unable to get attributes on search path\n";
            retc = errno;
          }
        }

        //.......................................................................
        // see if we have a recycle policy set and if avoid to recycle inside
        // the recycle bin
        //.......................................................................
        if (attrmap.count(Recycle::gRecyclingAttribute) &&
            (!spath.beginswith(Recycle::gRecyclingPrefix.c_str())) &&
            (spath.find("/.sys.v#.") == STR_NPOS))
        {
          //.....................................................................
          // two step deletion via recycle bin
          //.....................................................................
          // delete files in simulation mode
          std::map<uid_t, unsigned long long> user_deletion_size;
          std::map<gid_t, unsigned long long> group_deletion_size;

          for (rfoundit = found.rbegin(); rfoundit != found.rend(); rfoundit++)
          {
            for (fileit = rfoundit->second.begin(); fileit != rfoundit->second.end(); fileit++)
            {
              std::string fspath = rfoundit->first;
              fspath += *fileit;
              if (gOFS->_rem(fspath.c_str(), *mError, *pVid, (const char*) 0, true))
              {
                stdErr += "error: unable to remove file - bulk deletion aborted\n";
                retc = errno;
                return SFS_OK;
              }
            }
          }

          // delete directories in simulation mode
          for (rfoundit = found.rbegin(); rfoundit != found.rend(); rfoundit++)
          {
            // don't even try to delete the root directory
            std::string fspath = rfoundit->first.c_str();
            if (fspath == "/")
              continue;
            if (gOFS->_remdir(rfoundit->first.c_str(), *mError, *pVid, (const char*) 0, true) && (errno != ENOENT))
            {
              stdErr += "error: unable to remove directory - bulk deletion aborted\n";
              retc = errno;
              return SFS_OK;
            }
          }

          struct stat buf;
          if (gOFS->_stat(spath.c_str(), &buf, *mError, *pVid, ""))
          {
            stdErr = "error: failed to stat bulk deletion directory: ";
            stdErr += spath.c_str();
            retc = errno;
            return SFS_OK;
          }
          spath += "/";

          eos::mgm::Recycle lRecycle(spath.c_str(), attrmap[Recycle::gRecyclingAttribute].c_str(), pVid, buf.st_uid, buf.st_gid, (unsigned long long) buf.st_ino);
          int rc = 0;
          if ((rc = lRecycle.ToGarbage("rm-r", *mError)))
          {
            stdErr = "error: failed to recycle path ";
            stdErr += path;
            stdErr += "\n";
            stdErr += mError->getErrText();
            retc = mError->getErrInfo();
            return SFS_OK;
          }
          else
          {
            stdOut += "success: you can recycle this deletion using 'recycle restore ";
            char sp[256];
            snprintf(sp, sizeof (sp) - 1, "%016llx", (unsigned long long) buf.st_ino);
            stdOut += sp;
            stdOut += "'\n";
            retc = 0;
            return SFS_OK;
          }
        }
        else
        {
          //.....................................................................
          // standard way to delete files recursively
          //.....................................................................
          // delete files starting at the deepest level
          for (rfoundit = found.rbegin(); rfoundit != found.rend(); rfoundit++)
          {
            for (fileit = rfoundit->second.begin(); fileit != rfoundit->second.end(); fileit++)
            {
              std::string fspath = rfoundit->first;
              fspath += *fileit;
              if (gOFS->_rem(fspath.c_str(), *mError, *pVid, (const char*) 0))
              {
                stdErr += "error: unable to remove file\n";
                retc = errno;
              }
            }
          }
          // delete directories starting at the deepest level
          for (rfoundit = found.rbegin(); rfoundit != found.rend(); rfoundit++)
          {
            // don't even try to delete the root directory
            std::string fspath = rfoundit->first.c_str();
            if (fspath == "/")
              continue;
            if (gOFS->_remdir(rfoundit->first.c_str(), *mError, *pVid, (const char*) 0))
            {
	      if (errno != ENOENT) {
		stdErr += "error: unable to remove directory";
		retc = errno;
	      }
            }
          }
        }
      }
    }
    else
    {
      for (auto it = rmList.begin(); it != rmList.end(); ++it)
      {
        if (gOFS->_rem(it->c_str(), *mError, *pVid, (const char*) 0) && (errno != ENOENT))
        {
          stdErr += "error: unable to remove file/directory '";
          stdErr += it->c_str();
          stdErr += "'";
          retc |= errno;
        }
      }
    }
  }
  return SFS_OK;
}

EOSMGMNAMESPACE_END
