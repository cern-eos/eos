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
#include "mgm/recycle/Recycle.hh"
#include "mgm/Macros.hh"
#include "mgm/recycle/RecycleEntry.hh"
#include "mgm/Access.hh"
#include "common/Path.hh"
#include "common/Glob.hh"

EOSMGMNAMESPACE_BEGIN

eos::console::ReplyProto
eos::mgm::RmCmd::ProcessRequest() noexcept
{
  ACCESSMODE_W;
  eos::console::ReplyProto reply;
  std::ostringstream outStream;
  std::ostringstream errStream;
  XrdOucString m_err {""};
  int ret_c = 0;
  eos::console::RmProto rm = mReqProto.rm();
  auto recursive = rm.recursive();
  auto noworkflow = rm.noworkflow();
  auto force = rm.bypassrecycle();
  auto noglobbing = rm.noglobbing();
  std::string spath;

  if (rm.path().empty()) {
    std::string full_path;
    std::string err_msg;

    if (rm.fileid()) {
      GetPathFromFid(full_path, rm.fileid(), err_msg);
    } else  if (rm.containerid()) {
      GetPathFromCid(full_path, rm.containerid(), err_msg);
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
                                 is_dir, force, err_msg)) {
          reply.set_std_out(err_msg);
          reply.set_retc(0);
        } else {
          reply.set_std_err(err_msg);
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
    if (!noglobbing) {
      // Globbing was not disabled by the user
      eos::common::Path objPath(spath.c_str());

      if (objPath.Globbing()) {
        spath = objPath.GetParentPath();
        filter = objPath.GetName();
      }
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
      // List the path and match against filter
      XrdMgmOfsDirectory dir;
      eos::common::Glob glob;
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

          if (glob.Match(filter.c_str(), entry.c_str())) {
            rmList.insert(mpath.c_str());
          }
        }

        // No file matched the globbing, don't silently fail nor succeed,
        // return ENOENT instead
        if (rmList.empty()) {
          errStream << "error: no such file or directory with path '" << spath << filter
                    << "', globbing did not match anything. You may disable the globbing by using --no-globbing";
          reply.set_std_err(errStream.str());
          reply.set_retc(ENOENT);
          return reply;
        }
      }

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
        std::string recycle_dir;
        std::string recycle_id;

        if (!force) {
          // Only recycle if there is no '-f' flag
          std::string lpath = spath.c_str();
          size_t pos = lpath.find("/.sys.v#.");

          if (pos != std::string::npos) {
            lpath.erase(pos);
          }

          // Get recycle directory and eventually the recycle id
          gOFS->_attr_get(lpath.c_str(), errInfo, mVid, "",
                          Recycle::gRecyclingAttribute.c_str(), recycle_dir);
          gOFS->_attr_get(lpath.c_str(), errInfo, mVid, "",
                          Recycle::gRecycleIdXattrKey.c_str(), recycle_id);
        }

        // See if we have a recycle policy set
        if (recycle_dir.length() &&
            (spath.find(Recycle::gRecyclingPrefix) != 0)) {
          // Two step deletion via recycle bin
          // delete files in simulation mode
          std::map<uid_t, unsigned long long> user_deletion_size;
          std::map<gid_t, unsigned long long> group_deletion_size;

          for (rfoundit = found.rbegin(); rfoundit != found.rend(); rfoundit++) {
            int rpos = 0;
            RECURSIVE_STALL("Rm", mVid);

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
            RECURSIVE_STALL("RmDir", mVid);
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
          RecycleEntry lRecycle(spath.c_str(), recycle_dir, recycle_id,
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
                             force, false, true, noworkflow)) {
                errStream << "error: unable to remove file '" << fspath.c_str() << "'"
                          << " reason: " << errInfo.getErrText() << std::endl;
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
                          << " reason: " << errInfo.getErrText() << std::endl;
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
                       force, false, true, noworkflow) && (errno != ENOENT)) {
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
