//------------------------------------------------------------------------------
// File: RmCmd.cc
// Author: Jozsef Makai - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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
#include "mgm/Quota.hh"
#include "mgm/Recycle.hh"
#include "mgm/Macros.hh"
#include "mgm/Access.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "common/Path.hh"

EOSMGMNAMESPACE_BEGIN

eos::console::ReplyProto
eos::mgm::RmCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  eos::console::RmProto rm = mReqProto.rm();
  std::ostringstream outStream;
  std::ostringstream errStream;
  XrdOucErrInfo errInfo;
  int errPos = 0;
  auto recursive = rm.recursive();
  auto force = rm.bypassrecycle();

  if (rm.identifier_size() == 0) {
    reply.set_std_err("error: No path identifier provided");
    reply.set_retc(ENOENT);
    return reply;
  }

  if (force && (vid.uid)) {
    errStream << "warning: removing the force flag "
              << "- this is only allowed for the 'root' role!"
              << std::endl;

    force = false;
    // Ignore this message when reporting error code
    errPos = errStream.tellp();
  }

  for (const auto& identifier: rm.identifier()) {
    XrdOucString path;
    std::string filter;
    stdErr = "";

    // Retrieve path from identifier
    switch  (identifier.Identifier_case()) {
      case eos::console::RmProto::IdentifierProto::kFid:
        GetPathFromFid(path, identifier.fid(), "error: ");

        if (!path.length()) {
          errStream << stdErr;
          continue;
        }

        break;

      case eos::console::RmProto::IdentifierProto::kCid:
        GetPathFromCid(path, identifier.cid(), "error: ");

        if (!path.length()) {
          errStream << stdErr;
          continue;
        }

        break;

      case eos::console::RmProto::IdentifierProto::kPath:
        path = identifier.path().c_str();

        if (!path.length()) {
          errStream << "warning: Empty path string provided" << std::endl;
          continue;
        }

        break;

      default:
        errStream << "error: No expected identifier provided" << std::endl;
        continue;
    }

    // Check if operation is allowed
    if (IsOperationForbidden(path.c_str()) == SFS_OK) {
      errStream << stdErr << "\n";
      continue;
    }

    // Check for wildcard deletion
    if (path.find('*') != STR_NPOS) {
      eos::common::Path cPath(path.c_str());
      path = cPath.GetParentPath();
      filter = cPath.GetName();
    }

    // Check for path existence
    XrdSfsFileExistence file_exists;
    errInfo.clear();

    if (gOFS->_exists(path.c_str(), file_exists, errInfo, mVid, nullptr)) {
      errStream << "error: unable to run exists on path '" << path << "'"
                << std::endl;
      continue;
    }

    if (file_exists == XrdSfsFileExistNo) {
      errStream << "error: no such file or directory with path '" << path << "'"
                << std::endl;
      continue;
    }

    // File deletion
    if (file_exists == XrdSfsFileExistIsFile) {
      if (RemoveFile(path.c_str(), force)) {
        errStream << "error: unable to remove file '" << path << "'"
                  << std::endl;
      }
    }

    // Wildcard file deletion
    if ((file_exists == XrdSfsFileExistIsDirectory) && filter.length()) {
      std::string errMsg;

      if (RemoveFilterMatch(path.c_str(), filter, force, errMsg)) {
        errStream << errMsg;
      }
    } else if (file_exists == XrdSfsFileExistIsDirectory) {
      // Directory deletion
      if (recursive) {
        std::string outMsg, errMsg;

        if (RemoveDirectory(path.c_str(), force, outMsg, errMsg)) {
          errStream << errMsg;
        }

        outStream << outMsg;
      } else {
        errStream << "warning: missing recursive flag for directory '"
                  << path << "'" << std::endl;
      }
    }
  }

  int rc = ((errPos == errStream.tellp()) ? 0 : 1);

  reply.set_retc(rc);
  reply.set_std_out(outStream.str());
  reply.set_std_err(errStream.str());
  return reply;
}


//----------------------------------------------------------------------------
// Attempts to remove file
//----------------------------------------------------------------------------
int
RmCmd::RemoveFile(const std::string path, bool force) {
  XrdOucErrInfo errInfo;

  if (gOFS->_rem(path.c_str(), errInfo, mVid, nullptr, false, false, force)) {
    return (errno != ENOENT) ? errno : ENOENT;
  }

  return SFS_OK;
}

//----------------------------------------------------------------------------
// Attempts to remove directory
//----------------------------------------------------------------------------
int
RmCmd::RemoveDirectory(const std::string path, bool force,
                       std::string& outMsg,
                       std::string& errMsg) {
  std::map<std::string, std::set<std::string>> found;
  ostringstream outStream;
  ostringstream errStream;
  XrdOucErrInfo errInfo;

  if (gOFS->_find(path.c_str(), errInfo, stdErr, mVid, found)) {
    errStream << "error: unable to search directory '" << path << "'"
              << " (bulk deletion aborted)" << std::endl;
    errMsg += errStream.str();
    return errno;
  }

  XrdOucString recyclingAttr = "";
  bool simulate = false;

  // Extract recycling flags for non-force delete
  if (!force) {
    std::string attrPath = path;
    size_t npos;

    // Extract recycling attribute from parent path
    if ((npos = attrPath.find("/.sys.v#.")) != std::string::npos) {
      attrPath.erase(npos);
    }

    gOFS->_attr_get(attrPath.c_str(), errInfo, mVid, "",
                    Recycle::gRecyclingAttribute.c_str(), recyclingAttr);

    // Check for simulation mode
    simulate = ((recyclingAttr.length()) &&
                (path.find(Recycle::gRecyclingPrefix) != 0));
  }

  //.....................................................................
  // Deletion via recycle bin requires two steps.
  // The files will be deleted in 'simulation' mode.
  // If the simulation succeeds, they are placed in the recycling bin.
  //
  // Deletion via recycle bin can happen only for non-force deletes.
  //.....................................................................

  // Deleting files starting at the deepest level
  for (auto rfoundIt = found.rbegin(); rfoundIt != found.rend(); rfoundIt ++) {
    for (const auto& entry: rfoundIt->second) {
      std::string fullPath = rfoundIt->first;
      std::string fileName = entry;
      size_t lpos;

      if ((lpos = fileName.find(" ->")) != std::string::npos) {
        fileName.erase(lpos);
      }

      fullPath += fileName;

      if (gOFS->_rem(fullPath.c_str(), errInfo, mVid, nullptr, simulate,
                     false, force)) {
        errStream << "error: unable to remove file '" << fullPath << "'"
                  << " (bulk deletion aborted) - reason: "
                  << errInfo.getErrText() << std::endl;
        errMsg += errStream.str();
        return errInfo.getErrInfo();
      }
    }
  }

  // Delete directories starting at the deepest level
  for (auto rfoundIt = found.rbegin(); rfoundIt != found.rend(); rfoundIt ++) {
    std::string dirPath = rfoundIt->first;

    // Avoid delete attempts on the root directory
    if (dirPath == "/") {
      continue;
    }

    if ((gOFS->_remdir(dirPath.c_str(), errInfo, mVid, nullptr)) &&
        (errno != ENOENT)) {
      errStream << "error: unable to remove directory '" << dirPath << "'"
                << " (bulk deletion aborted) - reason: "
                << errInfo.getErrText() << std::endl;
      errMsg += errStream.str();
      return errInfo.getErrInfo();
    }
  }

  // Place the files in recycle bin
  if (simulate) {
    struct stat buf;

    if (gOFS->_stat(path.c_str(), &buf, errInfo, mVid, "")) {
      errStream << "error: unable to stat directory '" << path << "'"
                << " (bulk deletion aborted)" << std::endl;
      errMsg += errStream.str();
      return errno;
    }

    std::string spath = path;
    spath += "/";
    eos::mgm::Recycle lRecycle(spath.c_str(), recyclingAttr.c_str(), &mVid,
                               buf.st_uid, buf.st_gid,
                               (unsigned long long) buf.st_ino);

    if (lRecycle.ToGarbage("rm-r", errInfo)) {
      errStream << "error: failed to recycle path '" << spath << "'"
                << " (bulk deletion aborted) - reason: "
                << errInfo.getErrText() << std::endl;
      errMsg += errStream.str();
      return errInfo.getErrInfo();
    } else {
      outStream << "success: you can recycle this deletion using "
                << "'recycle restore "
                << std::setw(16) << std::setfill('0') << std::hex
                << buf.st_ino << "'" << std::endl;
    }
  }

  outMsg += outStream.str();
  return SFS_OK;
}

//----------------------------------------------------------------------------
// Attempts to remove files matching a given filter
// Note: directories will not be removed
//----------------------------------------------------------------------------
int
RmCmd::RemoveFilterMatch(const std::string path,
                         const std::string filter,
                         bool force,
                         std::string& errMsg) {
  ostringstream errStream;
  XrdOucErrInfo errInfo;
  regex_t regex_filter;
  int rc = SFS_OK;

  // Adding beginning and ending regex anchors
  XrdOucString sfilter = "^";
  sfilter += filter.c_str();
  sfilter += "$";
  sfilter.replace("*", ".*");

  int regrc = regcomp(&(regex_filter), sfilter.c_str(),
                      REG_EXTENDED | REG_NEWLINE);

  if (regrc) {
    errStream << "error: failed to compile filter regex " << sfilter
              << std::endl;
    errMsg += errStream.str();
    return EINVAL;
  }

  XrdMgmOfsDirectory dir;
  // List the directory and match against filter
  int listrc = dir.open(path.c_str(), mVid, nullptr);

  if (!listrc) {
    const char* val;

    while ((val = dir.nextEntry())) {
      XrdOucString entry = val;

      if ((entry == ".") || (entry == "..")) {
        continue;
      }

      if (!regexec(&regex_filter, entry.c_str(), 0, nullptr, 0)) {
        entry.insert(path.c_str(), 0);

        // Delete only files
        XrdSfsFileExistence file_exists;

        if (gOFS->_exists(entry.c_str(), file_exists, errInfo, mVid, nullptr)) {
          errStream << "error: unable to run exists on path '" << entry << "'"
                    << std::endl;
          rc = EINVAL;
          continue;
        }

        if ((file_exists == XrdSfsFileExistIsFile) &&
            (RemoveFile(entry.c_str(), force))) {
          errStream << "error: unable to remove file '" << entry << "'"
                    << std::endl;
          rc = errno;
        }
      }
    }
  } else {
    errStream << "error: failed to list directory '" << path << "'"
              << std::endl;
    rc = errno;
  }

  errMsg += errStream.str();
  regfree(&regex_filter);
  return rc;
}

EOSMGMNAMESPACE_END
