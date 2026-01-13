//------------------------------------------------------------------------------
//! @file AccessChecker.cc
//! @author Fabio Luchetti, Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "mgm/auth/AccessChecker.hh"
#include "mgm/acl/Acl.hh"
#include "common/Definitions.hh"
#include "namespace/interface/IFileMD.hh"
#include <common/Path.hh>
#include <sys/stat.h>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Check access to the given container - linked attributes are necessary
// to construct the Acl object.
//
// All information required to make a decision are passed to this function.
//------------------------------------------------------------------------------
bool
AccessChecker::checkContainer(IContainerMD* cont,
                              const eos::IContainerMD::XAttrMap& linkedAttrs,
                              int mode, const eos::common::VirtualIdentity& vid)
{
  Acl acl(linkedAttrs, vid);
  // Delegate to method taking receiving acl object instead of linked xattrs
  return checkContainer(cont, acl, mode, vid);
}

//------------------------------------------------------------------------------
// Check access to the given container - all information required to make
// a decision are passed to this function, no external information should
// be needed.
//------------------------------------------------------------------------------
bool
AccessChecker::checkContainer(IContainerMD* cont, const Acl& acl,
                              int mode, const eos::common::VirtualIdentity& vid)
{
  // Allow root to do anything
  if (vid.uid == 0) {
    return true;
  }

  // Always allow daemon to read/browse
  if ((vid.uid == DAEMONUID) && !(mode & W_OK)) {
    return true;
  }

  // A non-root attempting to write an immutable directory?
  if (acl.HasAcl() && !acl.IsMutable() && (mode & W_OK)) {
    return false;
  }

  // A non-root attempting to prepare, but no explicit ACL allowing prepare?
  if ((mode & P_OK) && (!acl.HasAcl() || !acl.CanPrepare())) {
    return false;
  }

  // A non-root attempting to delete, we have two cases:
  // * container has S_ISVTX(sticky bit) then only the file owner can delete
  //   that file irrespective of the ACLs - this check is split between the
  //   current method and checkFile
  // * container does NOT have S_ISVTX set and NO ACL set, directory and file permission apply
  // * container does NOT have S_ISVTX set and ACL set with !d, the owner of the
  //   container can delete regardless of the !d ACL, directory and file permission apply
  if (mode & D_OK) {
    bool isvtx = cont->getMode() & S_ISVTX;

    if (isvtx) {
      if (cont->getCUid() != vid.uid) {
        // The second part of this check is done in checkFile
        return false;
      }
    } else {
      if (acl.HasAcl() && acl.CanNotDelete()) {
        // There's a !d ACL for that vid, we grant the deletion if the owner of the container is the vid provided
        // and has write permission on it
        if((cont->getCUid() == vid.uid) && (!vid.token?cont->access(vid.uid,vid.gid,W_OK):false)) {
          return true;
        }
        return false;
      }
    }
  }

  // Basic permission check
  bool basicCheck = !vid.token?cont->access(vid.uid, vid.gid, mode):false;

  // Access granted, or we have no Acls? We're done.
  if (basicCheck || !acl.HasAcl()) {
    return basicCheck;
  }

  // Basic check denied us access... let's see if we can recover through Acls
  if ((mode & W_OK) &&
      (acl.CanNotWrite() ||
       (!acl.CanWrite() && (!vid.token?!cont->access(vid.uid, vid.gid, W_OK):true)))) {
    // Asking for write permission, and neither basic check, nor Acls grant us
    // write. Deny.
    return false;
  }

  if ((mode & R_OK) &&
      (acl.CanNotRead() ||
       (!acl.CanRead() && (!vid.token?!cont->access(vid.uid, vid.gid, R_OK):true)))) {
    // Asking for read permission, and neither basic check, nor Acls grant us
    // read. Deny.
    return false;
  }

  if ((mode & X_OK) &&
      (acl.CanNotBrowse() ||
       (!acl.CanBrowse() && ( !vid.token?!cont->access(vid.uid, vid.gid, X_OK):true)))) {
    // Asking for browse permission, and neither basic check, nor Acls grant us
    // browse. Deny.
    return false;
  }

  // We survived Acl check, grant.
  return true;
}

//------------------------------------------------------------------------------
// Check access to the given file. The parent directory of the file
// needs to be checked separately!
//------------------------------------------------------------------------------
bool AccessChecker::checkFile(IFileMD* file, int mode, int dh_mode,
                              const eos::common::VirtualIdentity& vid)
{
  // root can do anything
  if (vid.uid == 0) {
    return true;
  }

  // Deletion when parent container has sticky bit is allowed only if
  // done by the owner of the file
  if (mode & D_OK) {
    if ((dh_mode & S_ISVTX) && (file->getCUid() != vid.uid)) {
      return false;
    }
  }

  // We only check browse permissions for files, for now.
  if (!(mode & X_OK)) {
    return true;
  }

  uint16_t flags = file->getFlags();
  uid_t uid = file->getCUid();
  gid_t gid = file->getCGid();

  // both uid and gid match? return OR-match
  if (vid.uid == uid && vid.gid == gid) {
    return (flags & S_IXUSR) || (flags & S_IXGRP);
  }

  // user check
  if (vid.uid == uid) {
    return (flags & S_IXUSR);
  }

  // group check
  if (vid.gid == gid) {
    return (flags & S_IXGRP);
  }

  // other check
  return (flags & S_IXOTH);
}

//---------------------------------------------------------------------------------------------------
// Test if public access is allowed for a given path
//---------------------------------------------------------------------------------------------------
bool
AccessChecker::checkPublicAccess(const std::string& fullpath,
                                 const common::VirtualIdentity& vid)
{
  int errc = 0;

  if ((eos::common::Mapping::UserNameToUid(std::string("eosnobody"), errc) == vid.uid) &&
      !errc && (strcmp(vid.prot.c_str(), "sss") == 0)) {
    // eosnobody can access all squash files
    eos::common::Path cPath(fullpath);

    if (!cPath.isSquashFile()) {
      errno = EACCES;
      return false;
    }

    return true;
  }

  // Check only for anonymous access
  // uid=99 for CentOS7 and uid=65534 for >= Alma9
  if ((vid.uid != 99) && (vid.uid != 65534)) {
    return true;
  } else {
    uint32_t level = eos::common::Mapping::GetPublicAccessLevel();

    if (level >= 1024) {
      return true;
    } // short cut

    eos::common::Path cPath{fullpath};
    return cPath.GetSubPathSize() < level ? true : false;
  }
}

EOSMGMNAMESPACE_END
