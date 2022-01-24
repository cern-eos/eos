// ----------------------------------------------------------------------
// File: proc/user/Ls.cc
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
#include "mgm/Macros.hh"
#include "mgm/Stat.hh"
#include "common/Path.hh"
#include "common/StringUtils.hh"
#include "common/Timing.hh"
#include "common/LayoutId.hh"
#include "namespace/utils/Mode.hh"

EOSMGMNAMESPACE_BEGIN




int
ProcCommand::Ls()
{
  struct result {
    std::string out;
    std::string err;
    int retc;

    std::string getCacheName(uint64_t ino, uint64_t mtime_sec, uint64_t mtime_nsec, std::string options) {
      std::string cacheentry;
      cacheentry = std::to_string(ino);
      cacheentry += ":";
      cacheentry += std::to_string(mtime_sec);
      cacheentry += ".";
      cacheentry += std::to_string(mtime_nsec);
      if (options.length()) {
	cacheentry += ":";
	cacheentry += options;
      }
      return cacheentry;
    }
  };




  static eos::common::LRU::Cache<std::string, struct result> dirCache;
  static bool use_cache = (getenv("EOS_MGM_LISTING_CACHE") && (dirCache.setMaxSize(atoi(getenv("EOS_MGM_LISTING_CACHE")))));
  std::ostringstream oss;
  gOFS->MgmStats.Add("Ls", pVid->uid, pVid->gid, 1);
  XrdOucString spath = pOpaque->Get("mgm.path");
  eos::common::Path cPath(spath.c_str());

  // Check for globbing, we support a maximum depth of 255
  if (cPath.GetSubPathSize() > eos::common::Path::MAX_LEVELS) {
    eos_err("msg=\"path has more than %u levels", eos::common::Path::MAX_LEVELS);
    oss << "error: path has more than " << eos::common::Path::MAX_LEVELS
        << " levels";
    stdErr = oss.str().c_str();
    retc = E2BIG;
    return SFS_OK;
  }

  const char* inpath = cPath.GetPath();
  NAMESPACEMAP;
  PROC_BOUNCE_ILLEGAL_NAMES;
  PROC_BOUNCE_NOT_ALLOWED;

  PROC_TOKEN_SCOPE;

  eos_info("mapped to %s", path);
  spath = path;
  XrdOucString option = pOpaque->Get("mgm.option");
  bool showbackendstatus = false;
  bool longlisting = false;

  if (!spath.length()) {
    stdErr = "error: you have to give a path name to call 'ls'";
    retc = EINVAL;
  } else {
    XrdMgmOfsDirectory dir;
    struct stat buf;
    int listrc = 0;
    XrdOucString filter = "";

    if (spath.find("*") != STR_NPOS) {
      eos::common::Path cPath(spath.c_str());
      spath = cPath.GetParentPath();
      filter = cPath.GetName();
    }

    XrdOucString ls_file;
    std::string uri;

    std::string cacheentry;
    struct result cachedresult;

    if (gOFS->_stat(spath.c_str(), &buf, *mError, *pVid, (const char*) 0, 0, true,
                    &uri)) {
      stdErr = mError->getErrText();
      retc = errno;
    } else {
      // put the resolved uri path
      spath = uri.c_str();

      cacheentry = cachedresult.getCacheName(buf.st_ino, buf.st_mtim.tv_sec, buf.st_mtim.tv_nsec, std::string(option.length()?option.c_str():""));

      if (use_cache && dirCache.tryGet(cacheentry, cachedresult)) {

	if (!gOFS->_access(spath.c_str(), R_OK|X_OK, *mError, *pVid,0,true)) {
	  // return from cache
	  retc = cachedresult.retc;
	  stdOut = cachedresult.out.c_str();
	  stdErr = cachedresult.err.c_str();
	  // reinsert LRU
	  dirCache.insert(cacheentry, cachedresult);
	  return SFS_OK;
	}
	// fall through to report permission errors
      }

      // if this is a directory open it and list
      if (S_ISDIR(buf.st_mode) && ((option.find("d")) == STR_NPOS)) {
        listrc = dir.open(spath.c_str(), *pVid, (const char*) 0);
      } else {
        // if this is a file, open the parent and set the filter
        if (spath.endswith("/")) {
          spath.erase(spath.length() - 1);
        }

        int rpos = spath.rfind("/");

        if (rpos == STR_NPOS) {
          listrc = SFS_ERROR;
          retc = ENOENT;
        } else {
          // this is an 'ls <file>' command which has to return only one entry!
          ls_file.assign(spath, rpos + 1);
          spath.erase(rpos);
          listrc = 0;
        }
      }

      bool translateids = true;

      if ((option.find("n")) != STR_NPOS) {
        translateids = false;
      }

      if ((option.find("s")) != STR_NPOS) {
        // just return '0' if this is a directory
        return SFS_OK;
      }

      if ((option.find("y")) != STR_NPOS) {
        showbackendstatus = true;
        option += "l";
      }

      if ((option.find("l") != STR_NPOS)) {
	longlisting = true;
      }

      if (!listrc) {
        const char* val;

        while ((ls_file.length() && (val = ls_file.c_str())) ||
               (val = dir.nextEntry())) {
          // this return's a single file or a (filtered) directory list
          XrdOucString entryname = val;

          if (((option.find("a")) == STR_NPOS) && entryname.beginswith(".")) {
            // quit if we list a hidden file without 'a' flag
            if (ls_file.length()) {
              break;
            }

            // skip over . .. and hidden files
            continue;
          }

          if ((filter.length()) && (!entryname.matches(filter.c_str()))) {
            // apply filter
            continue;
          }

          if ((((option.find("l")) == STR_NPOS)) && ((option.find("F")) == STR_NPOS)) {
            stdOut += val;
            stdOut += "\n";
          } else {
            std::string backendstatus;
            // return full information
            XrdOucString statpath = spath;
            statpath += "/";
            statpath += val;

            while (statpath.replace("//", "/")) {
            }

            struct stat buf;
	    std::string cks;
            if (gOFS->_stat(statpath.c_str(), &buf, *mError, *pVid, (const char*) 0, 0,
                            false, 0, &cks)) {
	      if (errno != ENOENT) {
		stdErr += "error: unable to stat path ";
		stdErr += statpath;
		stdErr += "\n";
		retc = errno;
	      }
            } else {
              // TODO: convert virtual IDs back
              XrdOucString suid = "";
              suid += (int) buf.st_uid;
              XrdOucString sgid = "";
              sgid += (int) buf.st_gid;
              XrdOucString sizestring = "";
              struct tm* t_tm;
              struct tm t_tm_local;
              t_tm = localtime_r(&buf.st_mtime, &t_tm_local);
              char modestr[11];
              eos::modeToBuffer(buf.st_mode, modestr);

              if (showbackendstatus) {
		std::string rsymbol = eos::common::LayoutId::GetRedundancySymbol(buf.st_mode & EOS_TAPE_MODE_T, buf.st_nlink);
                char sbsts[256];
                snprintf(sbsts, sizeof(sbsts), "%-9s", rsymbol.c_str());
                backendstatus = sbsts;
              }

              if (translateids) {
                {
                  // try to translate with password database
                  int terrc = 0;
                  std::string username = "";
                  username = eos::common::Mapping::UidToUserName(buf.st_uid, terrc);

                  if (!terrc) {
                    char uidlimit[16];
                    snprintf(uidlimit, 12, "%s", username.c_str());
                    suid = uidlimit;
                  }
                }
                {
                  // try to translate with password database
                  std::string groupname = "";
                  int terrc = 0;
                  groupname = eos::common::Mapping::GidToGroupName(buf.st_gid, terrc);

                  if (!terrc) {
                    char gidlimit[16];
                    snprintf(gidlimit, 12, "%s", groupname.c_str());
                    sgid = gidlimit;
                  }
                }
              }

              std::string t_creat = eos::common::Timing::ToLsFormat(t_tm);
              char lsline[4096];
              XrdOucString dirmarker = "";

              if ((option.find("F")) != STR_NPOS) {
                dirmarker = "/";
              }

              if (modestr[0] != 'd') {
                dirmarker = "";
              }

              if ((option.find("i")) != STR_NPOS) {
                // add inode information
                char sinode[16];
                bool isfile = (modestr[0] != 'd');
                snprintf(sinode, 16, "%llu",
                         (unsigned long long)(isfile ? eos::common::FileId::InodeToFid(
                                                buf.st_ino) : buf.st_ino));
                sprintf(lsline, "%-16s", sinode);
                stdOut += lsline;
              }

	      if ((option.find("c")) != STR_NPOS) {
		// add checksum information
		char checksum[36];
		sprintf(checksum, "%-34s",cks.c_str());
		stdOut += checksum;
	      }

              if ((option.find("h")) == STR_NPOS)
                sprintf(lsline, "%s%s %3d %-8.8s %-8.8s %12s %s %s%s", backendstatus.c_str(),
                        modestr,
                        (int) buf.st_nlink,
                        suid.c_str(), sgid.c_str(),
                        eos::common::StringConversion::GetSizeString(sizestring,
                            (unsigned long long) buf.st_size),
                        t_creat.c_str(), val, dirmarker.c_str());
              else
                sprintf(lsline, "%s%s %3d %-8.8s %-8.8s %12s %s %s%s", backendstatus.c_str(),
                        modestr,
                        (int) buf.st_nlink,
                        suid.c_str(), sgid.c_str(),
                        eos::common::StringConversion::GetReadableSizeString(sizestring,
                            (unsigned long long) buf.st_size, ""),
                        t_creat.c_str(), val, dirmarker.c_str());

              if ((option.find("l")) != STR_NPOS) {
                stdOut += lsline;

                if (S_ISLNK(buf.st_mode)) {
                  stdOut += " -> ";
                  XrdOucString link;

                  if (!gOFS->_readlink(statpath.c_str(), *mError, *pVid, link)) {
                    stdOut += link.c_str();
                  } else {
                    stdOut += "( error )\n";
                  }
                }

                stdOut += "\n";
              } else {
                stdOut += val;
                stdOut += dirmarker;
                stdOut += "\n";
              }
            }
          }

          if (stdOut.length() > 1 * 1024 * 1024 * 1024) {
            stdOut += "... (truncated after 1G of output)\n";
            retc = E2BIG;
            stdErr += "warning: list too long - truncated after 1GB of output!\n";
            break;
          }

          if (ls_file.length()) {
            // this was a single file to be listed
            break;
          }
        }

        if (!ls_file.length()) {
          dir.close();
        }
      } else {
        stdErr += "error: unable to open directory";
        retc = errno;
      }
    }
    if (!retc && !showbackendstatus && !longlisting) {
      // we cannot cache listing where people ask for dynamicinformation of children like folder size, Y option ...
      cachedresult.retc = retc;
      cachedresult.out = stdOut.c_str();
      cachedresult.err = stdErr.c_str();
      if (use_cache) {
	dirCache.insert(cacheentry, cachedresult);
      }
    }
  }

  return SFS_OK;
}

EOSMGMNAMESPACE_END
