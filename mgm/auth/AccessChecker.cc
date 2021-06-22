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
#include "mgm/Acl.hh"
#include "namespace/interface/IFileMD.hh"
//#include <mgm/XrdMgmOfs.hh>
#include <common/Path.hh>
#include <sys/stat.h>

EOSMGMNAMESPACE_BEGIN

//----------------------------------------------------------------------------
// Check access to the given container - linked attributes are necessary
// to construct the Acl object.
//
// All information required to make a decision are passed to this function.
//----------------------------------------------------------------------------
bool AccessChecker::checkContainer(IContainerMD* cont,
                                   const eos::IContainerMD::XAttrMap& linkedAttrs, int mode,
                                   const eos::common::VirtualIdentity& vid)
{
  //----------------------------------------------------------------------------
  // Construct Acl object
  //----------------------------------------------------------------------------
  Acl acl(linkedAttrs, vid);
  //----------------------------------------------------------------------------
  // Delegate to method taking receiving acl object instead of linked xattrs
  //----------------------------------------------------------------------------
  return checkContainer(cont, acl, mode, vid);
}

//------------------------------------------------------------------------------
// Check access to the given container - all information required to make
// a decision are passed to this function, no external information should
// be needed.
//------------------------------------------------------------------------------
bool AccessChecker::checkContainer(IContainerMD* cont, const Acl& acl,
                                   int mode, const eos::common::VirtualIdentity& vid)
{
  //----------------------------------------------------------------------------
  // Allow root to do anything
  //----------------------------------------------------------------------------
  if (vid.uid == 0) {
    return true;
  }

  //----------------------------------------------------------------------------
  // Always allow daemon to read / browse
  //----------------------------------------------------------------------------
  if (vid.uid == DAEMONUID && (!(mode & W_OK))) {
    return true;
  }

  //----------------------------------------------------------------------------
  // A non-root attempting to write an immutable directory?
  //----------------------------------------------------------------------------
  if (acl.HasAcl() && (!acl.IsMutable() && (mode & W_OK))) {
    return false;
  }

  //----------------------------------------------------------------------------
  // A non-root attempting to prepare, but no explicit Acl allowing prepare?
  //----------------------------------------------------------------------------
  if ((mode & P_OK) && (!acl.HasAcl() || !acl.CanPrepare())) {
    return false;
  }

  //----------------------------------------------------------------------------
  // Basic permission check
  //----------------------------------------------------------------------------
  bool basicCheck = cont->access(vid.uid, vid.gid, mode);

  //----------------------------------------------------------------------------
  // Access granted, or we have no Acls? We're done.
  //----------------------------------------------------------------------------
  if (basicCheck || !acl.HasAcl()) {
    return basicCheck;
  }

  //----------------------------------------------------------------------------
  // Basic check denied us access... let's see if we can recover through Acls
  //----------------------------------------------------------------------------

  //if ((mode & W_OK) && (!acl.CanWrite() && !cont->access(vid.uid, vid.gid, W_OK) ))
  if ((mode & W_OK) &&
      (acl.CanNotWrite() ||
       (!acl.CanWrite() && !cont->access(vid.uid, vid.gid, W_OK))
      )
     ) {
    //--------------------------------------------------------------------------
    // Asking for write permission, and neither basic check, nor Acls grant us
    // write. Deny.
    //--------------------------------------------------------------------------
    return false;
  }

  // if ((mode & R_OK) && (!acl.CanRead() && !cont->access(vid.uid, vid.gid, R_OK) ))
  if ((mode & R_OK) &&
      (acl.CanNotRead() ||
       (!acl.CanRead() && !cont->access(vid.uid, vid.gid, R_OK))
      )
     ) {
    //--------------------------------------------------------------------------
    // Asking for read permission, and neither basic check, nor Acls grant us
    // read. Deny.
    //--------------------------------------------------------------------------
    return false;
  }

  // if ((mode & X_OK) && (!acl.CanBrowse() && !cont->access(vid.uid, vid.gid, X_OK) ))
  if ((mode & X_OK) &&
      (acl.CanNotBrowse() ||
       (!acl.CanBrowse() && !cont->access(vid.uid, vid.gid, X_OK))
      )
     ) {
    //--------------------------------------------------------------------------
    // Asking for browse permission, and neither basic check, nor Acls grant us
    // browse. Deny.
    //--------------------------------------------------------------------------
    return false;
  }

  //----------------------------------------------------------------------------
  // We survived Acl check, grant.
  //----------------------------------------------------------------------------
  return true;
}

//------------------------------------------------------------------------------
// Check access to the given file. The parent directory of the file
// needs to be checked separately!
//------------------------------------------------------------------------------
bool AccessChecker::checkFile(IFileMD* file, int mode,
                              const eos::common::VirtualIdentity& vid)
{
  //----------------------------------------------------------------------------
  // We only check browse permissions for files, for now.
  //----------------------------------------------------------------------------
  if (!(mode & X_OK)) {
    return true;
  }

  //----------------------------------------------------------------------------
  // root can do anything
  //----------------------------------------------------------------------------
  if (vid.uid == 0) {
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
//
//---------------------------------------------------------------------------------------------------
bool
AccessChecker::checkPublicAccess(const std::string& fullpath,
                                 const common::VirtualIdentity& vid)
{
  int errc = 0;

  if ((eos::common::Mapping::UserNameToUid(std::string("eosnobody"),
       errc) == vid.uid) && !errc && (strcmp(vid.prot.c_str(), "sss") == 0)) {
    // eosnobody can access all squash files
    eos::common::Path cPath(fullpath);

    if (!cPath.isSquashFile()) {
      errno = EACCES;
      return false;
    }

    return true;
  }

  /* check only for anonymous access */
  if (vid.uid != 99) {
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
