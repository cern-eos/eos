//------------------------------------------------------------------------------
// File: RmCmd.cc
// Author: Jozsef Makai - CERN
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

#include "RmCmd.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "mgm/Quota.hh"
#include "mgm/Recycle.hh"
#include "mgm/Macros.hh"
#include "mgm/Access.hh"
#include "common/Path.hh"

EOSMGMNAMESPACE_BEGIN

eos::console::ReplyProto
eos::mgm::RmCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  std::ostringstream outStream;
  std::ostringstream errStream;
  XrdOucString m_err {""};
  int ret_c = 0;
  eos::console::RmProto rm = mReqProto.rm();
  auto recursive = rm.recursive();
  auto force = rm.bypassrecycle();
  std::string spath;

  if (rm.path().empty()) {
    std::string full_path;
    std::string msg;

    if (rm.fileid()) {
      GetPathFromFid(full_path, rm.fileid(), msg);
    } else  if (rm.containerid()) {
      GetPathFromCid(full_path, rm.containerid(), msg);
    }

    spath = full_path;

    if (spath.empty() && (rm.fileid() || rm.containerid())) {
      if (vid.uid) {
        reply.set_std_err("warning: removing a pending deletion is allowed "
                          "only for the 'root' role");
        reply.set_retc(EPERM);
      } else {
        // Try to remove a file/container metadata object pending deletion
        // which is blocked in the namespace in a detached state
        bool is_dir = (rm.containerid() != 0ull);

        // If -F is given then we try to force remove the file without waiting
        // for the confirmation from the diskservers
        if (gOFS->RemoveDetached((is_dir ? rm.containerid() : rm.fileid()),
                                 is_dir, force, msg)) {
          reply.set_std_out(msg);
          reply.set_retc(0);
        } else {
          reply.set_std_err(msg);
          reply.set_retc(errno);
        }
      }

      return reply;
    }
  } else {
    spath = rm.path();
  }

  eos::mgm::NamespaceMap(spath, nullptr, mVid);
  std::string err_check;
  int errno_check = 0;
  const char* path = spath.c_str();
  PROC_MVID_TOKEN_SCOPE;

  // Enforce path checks and identity access rights
  if (IsOperationForbidden(spath, mVid, err_check, errno_check)) {
    eos_err("msg=\"operation forbidden\" path=\"%s\" serr_msg=\"%s\" errno=%i",
            spath.c_str(), err_check.c_str(), errno_check);
    reply.set_std_err(err_check);
    reply.set_retc(errno_check);
    return reply;
  }

  eos::common::Path cPath(spath.c_str());
  XrdOucString filter = "";
  std::set<std::string> rmList;

  if (force && (vid.uid)) {
    errStream << "warning: removing the force flag - this is only allowed "
              << "for the 'root' role!" << std::endl;
    force = false;
  }

  if (spath.empty()) {
    errStream << "error: you have to give a path name to call 'rm'";
    ret_c = EINVAL;
  } else {
    if (spath.find('*') != std::string::npos) {
      // this is wildcard deletion
      eos::common::Path objPath(spath.c_str());
      spath = objPath.GetParentPath();
      filter = objPath.GetName();
    }

    // Check file existence
    XrdSfsFileExistence file_exists;
    XrdOucErrInfo errInfo;

    if (gOFS->_exists(spath.c_str(), file_exists, errInfo, mVid, nullptr)) {
      errStream << "error: unable to run exists on path '" << spath << "'";
      reply.set_std_err(errStream.str());
      reply.set_retc(errno);
      return reply;
    }

    if (file_exists == XrdSfsFileExistNo) {
      errStream << "error: no such file or directory with path '" << spath << "'";
      reply.set_std_err(errStream.str());
      reply.set_retc(ENOENT);
      return reply;
    }

    if (file_exists == XrdSfsFileExistIsFile) {
      // if we have rm -r <file> we remove the -r flag
      recursive = false;
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
        errStream << "error: failed to compile filter regex " << filter_temp;
        reply.set_std_err(errStream.str());
        reply.set_retc(EINVAL);
        return reply;
      }

      // List the path and match against filter
      XrdMgmOfsDirectory dir;
      int listrc = dir.open(spath.c_str(), mVid, nullptr);

      if (!listrc) {
        const char* val;

        while ((val = dir.nextEntry())) {
          XrdOucString mpath = spath.c_str();
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
      recursive = false;
    } else {
      rmList.insert(spath);
    }

    // Find everything to be deleted
    if (recursive) {
      std::map<std::string, std::set<std::string>> found;
      std::map<std::string, std::set<std::string>>::const_reverse_iterator rfoundit;
      std::set<std::string>::const_iterator fileit;
      errInfo.clear();
      errInfo.setErrCode(E2BIG);// ask to fail E2BIG

      if (gOFS->_find(spath.c_str(), errInfo, m_err, mVid, found)) {
        if (errno == E2BIG) {
          errStream <<
                    "error: the directory tree exceeds the configured query limit for you - ask an administrator to increase your query limit or split the operation into several deletions";
        } else {
          errStream << "error: unable to list directory '" << spath << "'";
        }

        ret_c = errno;
      } else {
        XrdOucString recyclingAttribute = "";

        if (!force) {
          // only recycle if there is no '-f' flag
          unsigned long rpos;

          if ((rpos = spath.find("/.sys.v#.")) == std::string::npos) {
            // check if this path has a recycle attribute
            errInfo.clear();
            gOFS->_attr_get(spath.c_str(), errInfo, mVid, "",
                            Recycle::gRecyclingAttribute.c_str(), recyclingAttribute);
          } else {
            auto ppath = spath;
            ppath.erase(rpos);
            // get it from the parent directory for version directories
            errInfo.clear();
            gOFS->_attr_get(ppath.c_str(), errInfo, mVid, "",
                            Recycle::gRecyclingAttribute.c_str(), recyclingAttribute);
          }
        }

        // See if we have a recycle policy set
        if (recyclingAttribute.length() &&
            (spath.find(Recycle::gRecyclingPrefix) != 0)) {
          // Two step deletion via recycle bin
          // delete files in simulation mode
          std::map<uid_t, unsigned long long> user_deletion_size;
          std::map<gid_t, unsigned long long> group_deletion_size;

          for (rfoundit = found.rbegin(); rfoundit != found.rend(); rfoundit++) {
            int rpos = 0;

            if ((rpos = rfoundit->first.find("/.sys.v#.")) == STR_NPOS) {
              for (fileit = rfoundit->second.begin(); fileit != rfoundit->second.end();
                   fileit++) {
                std::string fspath = rfoundit->first;
                std::string entry = *fileit;
                size_t l_pos;

                if ((l_pos = entry.find(" ->")) != std::string::npos) {
                  entry.erase(l_pos);
                }

                fspath += entry;
                errInfo.clear();

                if (gOFS->_rem(fspath.c_str(), errInfo, mVid, nullptr, true)) {
                  errStream << "error: unable to remove file '" << fspath << "'"
                            << " - bulk deletion aborted" << std::endl;
                  reply.set_std_err(errStream.str());
                  reply.set_retc(errno);
                  return reply;
                }
              }
            }
          }

          // Delete directories in simulation mode
          for (rfoundit = found.rbegin(); rfoundit != found.rend(); rfoundit++) {
            // don't even try to delete the root directory
            std::string fspath = rfoundit->first;

            if (fspath == "/") {
              continue;
            }

            errInfo.clear();

            if (gOFS->_remdir(rfoundit->first.c_str(), errInfo, mVid, nullptr, true)
                && (errno != ENOENT)) {
              errStream << "error: unable to remove directory '" << rfoundit->first << "'"
                        << " - bulk deletion aborted" << std::endl;
              reply.set_std_err(errStream.str());
              reply.set_retc(errno);
              return reply;
            }
          }

          struct stat buf;

          errInfo.clear();

          if (gOFS->_stat(spath.c_str(), &buf, errInfo, mVid, "")) {
            errStream << "error: failed to stat bulk deletion directory '" << spath << "'";
            reply.set_std_err(errStream.str());
            reply.set_retc(errno);
            return reply;
          }

          spath += "/";
          eos::mgm::Recycle lRecycle(spath.c_str(), recyclingAttribute.c_str(),
                                     &mVid, buf.st_uid, buf.st_gid,
                                     (unsigned long long) buf.st_ino);
          errInfo.clear();

          if (lRecycle.ToGarbage("rm-r", errInfo)) {
            errStream << "error: failed to recycle path '" << spath << "'" << std::endl
                      << "reason: " << errInfo.getErrText() << std::endl;
            reply.set_std_err(errStream.str());
            reply.set_retc(errInfo.getErrInfo());
            return reply;
          } else {
            outStream << "success: you can recycle this deletion using 'recycle restore "
                      << "pxid:" << std::setw(16) << std::setfill('0') << std::hex
                      << buf.st_ino << "'" << std::endl;
            reply.set_std_out(outStream.str());
            reply.set_retc(SFS_OK);
            return reply;
          }
        } else {
          // Standard way to delete files recursively
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
              errInfo.clear();

              if (gOFS->_rem(fspath.c_str(), errInfo, mVid, nullptr, false, false,
                             force)) {
                errStream << "error: unable to remove file '" << fspath.c_str() << "'"
                          << std::endl;
                ret_c = errno;
              }
            }
          }

          // Delete directories starting at the deepest level
          for (rfoundit = found.rbegin(); rfoundit != found.rend(); rfoundit++) {
            // don't even try to delete the root directory
            std::string fspath = rfoundit->first;

            if (fspath == "/") {
              continue;
            }

            errInfo.clear();

            if (gOFS->_remdir(rfoundit->first.c_str(), errInfo, mVid, nullptr)) {
              if (errno != ENOENT) {
                errStream << "error: unable to remove directory "
                          << "'" << rfoundit->first.c_str() << "'" << std::endl
                          << "reason: " << errInfo.getErrText() << std::endl;
                ret_c = errno;
              }
            }
          }
        }
      }
    } else {
      for (const auto& it : rmList) {
        errInfo.clear();

        if (gOFS->_rem(it.c_str(), errInfo, mVid, nullptr, false, false,
                       force) && (errno != ENOENT)) {
          errStream << "error: unable to remove file/directory '" << it << "'"
                    << std::endl;
          ret_c |= errno;
        }
      }
    }
  }

  reply.set_retc(ret_c);
  reply.set_std_out(outStream.str());
  reply.set_std_err(errStream.str());
  return reply;
}

EOSMGMNAMESPACE_END
