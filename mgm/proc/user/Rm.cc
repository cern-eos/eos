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
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "mgm/Access.hh"
#include "mgm/Quota.hh"
#include "mgm/Recycle.hh"
#include "mgm/Macros.hh"
#include "common/Path.hh"
#include <regex.h>

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Rm()
{
  XrdOucString spath = "";
  XrdOucString spathid = pOpaque->Get("mgm.file.id");
  XrdOucString scontainerid = pOpaque->Get("mgm.container.id");

  if (spathid.length()) {
    GetPathFromFid(spath, std::strtoull(spathid.c_str(), nullptr, 10), "error: ");
  } else {
    if (scontainerid.length()) {
      GetPathFromCid(spath, std::strtoull(scontainerid.c_str(), nullptr, 10), "error: ");
    } else {
      spath = pOpaque->Get("mgm.path");
    }
  }

  const char* inpath = spath.c_str();
  XrdOucString option = pOpaque->Get("mgm.option");
  XrdOucString deep = pOpaque->Get("mgm.deletion");
  eos::common::Path cPath(inpath);
  bool force = (option.find("f") != STR_NPOS);
  XrdOucString filter = "";
  std::set<std::string> rmList;
  NAMESPACEMAP;
  PROC_BOUNCE_ILLEGAL_NAMES;
  PROC_BOUNCE_NOT_ALLOWED;
  spath = path;

  PROC_TOKEN_SCOPE;

  if (force && (vid.uid)) {
    stdErr = "warning: removing the force flag - this is only allowed for the 'root' role!\n";
    force = false;
  }

  if (!spath.length()) {
    stdErr = "error: you have to give a path name to call 'rm'";
    retc = EINVAL;
  } else {
    if (spath.find("*") != STR_NPOS) {
      // this is wildcard deletion
      eos::common::Path cPath(spath.c_str());
      spath = cPath.GetParentPath();
      filter = cPath.GetName();
    }

    // check if this file exists
    XrdSfsFileExistence file_exists;

    if (gOFS->_exists(spath.c_str(), file_exists, *mError, *pVid, nullptr)) {
      stdErr += "error: unable to run exists on path '";
      stdErr += spath.c_str();
      stdErr += "'";
      retc = errno;
      return SFS_OK;
    }

    if (file_exists == XrdSfsFileExistNo) {
      stdErr += "error: no such file or directory with path '";
      stdErr += spath.c_str();
      stdErr += "'";
      retc = ENOENT;
      return SFS_OK;
    }

    if (file_exists == XrdSfsFileExistIsFile) {
      // if we have rm -r <file> we remove the -r flag
      option.replace("r", "");
    }

    if ((file_exists == XrdSfsFileExistIsDirectory) && filter.length()) {
      regex_t regex_filter;
      // Adding regex anchors for beginning and end of string
      XrdOucString filter_temp = "^";
      // Changing wildcard * into regex syntax
      filter.replace("*", ".*");
      filter_temp += filter;
      filter_temp += "$";
      int reg_rc = regcomp(&(regex_filter), filter_temp.c_str(),
                           REG_EXTENDED | REG_NEWLINE);

      if (reg_rc) {
        stdErr += "error: failed to compile filter regex ";
        stdErr += filter_temp.c_str();
        retc = EINVAL;
        return SFS_OK;
      }

      XrdMgmOfsDirectory dir;
      // list the path and match against filter
      int listrc = dir.open(spath.c_str(), *pVid, nullptr);

      if (!listrc) {
        const char* val;

        while ((val = dir.nextEntry())) {
          XrdOucString mpath = spath;
          XrdOucString entry = val;
          mpath += val;

          if ((entry == ".") ||
              (entry == "..")) {
            continue;
          }

          if (!regexec(&regex_filter, entry.c_str(), 0, nullptr, 0)) {
            rmList.insert(mpath.c_str());
          }
        }
      }

      regfree(&regex_filter);
      // if we have rm * (whatever wildcard) we remove the -r flag
      option.replace("r", "");
    } else {
      rmList.insert(spath.c_str());
    }

    // find everything to be deleted
    if (option.find("r") != STR_NPOS) {
      std::map<std::string, std::set<std::string> > found;
      std::map<std::string, std::set<std::string> >::const_reverse_iterator rfoundit;
      std::set<std::string>::const_iterator fileit;

      if (((cPath.GetSubPathSize() < 4) && (deep != "deep")) ||
          (gOFS->_find(spath.c_str(), *mError, stdErr, *pVid, found))) {
        if ((cPath.GetSubPathSize() < 4) && (deep != "deep")) {
          stdErr += "error: deep recursive deletes are forbidden without shell confirmation code!";
          retc = EPERM;
        } else {
          stdErr += "error: unable to remove file/directory";
          retc = errno;
        }
      } else {
        XrdOucString recyclingAttribute;

        if (!force) {
          // only recycle if there is no '-f' flag
          int rpos;

          if ((rpos = spath.find("/.sys.v#.")) == STR_NPOS) {
            // check if this path has a recycle attribute
            gOFS->_attr_get(spath.c_str(), *mError, *pVid, "",
                            Recycle::gRecyclingAttribute.c_str(), recyclingAttribute);
          } else {
            XrdOucString ppath = spath;
            ppath.erase(rpos);
            // get it from the parent directory for version directories
            gOFS->_attr_get(ppath.c_str(), *mError, *pVid, "",
                            Recycle::gRecyclingAttribute.c_str(), recyclingAttribute);
          }
        }

        //.......................................................................
        // see if we have a recycle policy set
        //.......................................................................
        if (recyclingAttribute.length() &&
            (!spath.beginswith(Recycle::gRecyclingPrefix.c_str()))) {
          //.....................................................................
          // two step deletion via recycle bin
          //.....................................................................
          // delete files in simulation mode
          std::map<uid_t, unsigned long long> user_deletion_size;
          std::map<gid_t, unsigned long long> group_deletion_size;

          for (rfoundit = found.rbegin(); rfoundit != found.rend(); rfoundit++) {
	    int rpos = 0;
	    if ((rpos = rfoundit->first.find("/.sys.v#.")) == STR_NPOS) {
	      // skip to check version files
	      for (fileit = rfoundit->second.begin(); fileit != rfoundit->second.end();
		   fileit++) {
		std::string fspath = rfoundit->first;
		size_t l_pos;
		std::string entry = *fileit;

		if ((l_pos = entry.find(" ->")) != std::string::npos) {
		  entry.erase(l_pos);
		}

		fspath += entry;

		if (gOFS->_rem(fspath.c_str(), *mError, *pVid, nullptr, true)) {
		  stdErr += "error: unable to remove file - bulk deletion aborted\n";
		  retc = errno;
		  return SFS_OK;
		}
	      }
	    }
	  }

          // delete directories in simulation mode
          for (rfoundit = found.rbegin(); rfoundit != found.rend(); rfoundit++) {
            // don't even try to delete the root directory
            std::string fspath = rfoundit->first;

            if (fspath == "/") {
              continue;
            }

            if (gOFS->_remdir(rfoundit->first.c_str(), *mError, *pVid, nullptr,
                              true) && (errno != ENOENT)) {
              stdErr += "error: unable to remove directory - bulk deletion aborted\n";
              retc = errno;
              return SFS_OK;
            }
          }

          struct stat buf;

          if (gOFS->_stat(spath.c_str(), &buf, *mError, *pVid, "")) {
            stdErr = "error: failed to stat bulk deletion directory: ";
            stdErr += spath.c_str();
            retc = errno;
            return SFS_OK;
          }

          spath += "/";
          eos::mgm::Recycle lRecycle(spath.c_str(), recyclingAttribute.c_str(),
                                     pVid, buf.st_uid, buf.st_gid,
                                     (unsigned long long) buf.st_ino);
          int rc = 0;

          if ((rc = lRecycle.ToGarbage("rm-r", *mError))) {
            stdErr = "error: failed to recycle path ";
            stdErr += path;
            stdErr += "\n";
            stdErr += mError->getErrText();
            retc = mError->getErrInfo();
            return SFS_OK;
          } else {
            stdOut += "success: you can recycle this deletion using 'recycle restore ";
            char sp[256];
            snprintf(sp, sizeof(sp) - 1, "%016llx", (unsigned long long) buf.st_ino);
            stdOut += sp;
            stdOut += "'\n";
            retc = 0;
            return SFS_OK;
          }
        } else {
          //.....................................................................
          // standard way to delete files recursively
          //.....................................................................
          // delete files starting at the deepest level
          for (rfoundit = found.rbegin(); rfoundit != found.rend(); rfoundit++) {
            for (fileit = rfoundit->second.begin(); fileit != rfoundit->second.end();
                 fileit++) {
              std::string fspath = rfoundit->first;
              size_t l_pos;
              std::string entry = *fileit;

              if ((l_pos = entry.find(" ->")) != std::string::npos) {
                entry.erase(l_pos);
              }

              fspath += entry;

              if (gOFS->_rem(fspath.c_str(), *mError, *pVid, (const char*) 0, false, false,
                             force)) {
                stdErr += "error: unable to remove file : \n";
                stdErr += fspath.c_str();
                retc = errno;
              }
            }
          }

          // delete directories starting at the deepest level
          for (rfoundit = found.rbegin(); rfoundit != found.rend(); rfoundit++) {
            // don't even try to delete the root directory
            std::string fspath = rfoundit->first;

            if (fspath == "/") {
              continue;
            }

            if (gOFS->_remdir(rfoundit->first.c_str(), *mError, *pVid, nullptr)) {
              if (errno != ENOENT) {
                stdErr += "error: unable to remove directory : ";
                stdErr += rfoundit->first.c_str();
                stdErr += "; reason: ";
                stdErr += mError->getErrText();
                retc = errno;
              }
            }
          }
        }
      }
    } else {
      for (auto it = rmList.begin(); it != rmList.end(); ++it) {
        if (gOFS->_rem(it->c_str(), *mError, *pVid, nullptr, false, false,
                       force) && (errno != ENOENT)) {
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
