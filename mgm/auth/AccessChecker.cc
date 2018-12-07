//------------------------------------------------------------------------------
//! @file AccessChecker.cc
//! @author Georgios Bitzes - CERN
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

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Check access to the given container - all information required to make
// a decision are passed to this function, no external information should
// be needed.
//------------------------------------------------------------------------------
bool AccessChecker::checkContainer(IContainerMD *cont, const Acl &acl,
	int mode, const eos::common::Mapping::VirtualIdentity &vid)
{
  //----------------------------------------------------------------------------
  // Allow root to do anything
  //----------------------------------------------------------------------------
  if(vid.uid == 0) {
  	return true;
  }

  //----------------------------------------------------------------------------
  // Always allow daemon to read / browse
  //----------------------------------------------------------------------------
  if(vid.uid == DAEMONUID && (!(mode & W_OK)) ) {
  	return true;
  }

  //----------------------------------------------------------------------------
  // A non-root attempting to write an immutable directory?
  //----------------------------------------------------------------------------
  if(acl.HasAcl() && (!acl.IsMutable() && (mode & W_OK))) {
  	return false;
  }

  //----------------------------------------------------------------------------
  // A non-root attempting to prepare, but no explicit Acl allowing prepare?
  //----------------------------------------------------------------------------
  if( (mode & P_OK) && (!acl.HasAcl() || !acl.CanPrepare()) ) {
  	return false;
  }

  //----------------------------------------------------------------------------
  // Basic permission check
  //----------------------------------------------------------------------------
  bool basicCheck = cont->access(vid.uid, vid.gid, mode);

  //----------------------------------------------------------------------------
  // Access granted, or we have no Acls? We're done.
  //----------------------------------------------------------------------------
  if(basicCheck || !acl.HasAcl()) {
  	return basicCheck;
  }

  //----------------------------------------------------------------------------
  // Basic check denied us access... let's see if we can recover through Acls
  //----------------------------------------------------------------------------

  if ((mode & W_OK) && (!acl.CanWrite() && !cont->access(vid.uid, vid.gid, W_OK) )) {
    //--------------------------------------------------------------------------
    // Asking for write permission, and neither basic check, nor Acls grant us
    // write. Deny.
    //--------------------------------------------------------------------------
  	return false;
  }

  if ((mode & R_OK) && (!acl.CanRead() && !cont->access(vid.uid, vid.gid, R_OK) )) {
    //--------------------------------------------------------------------------
    // Asking for read permission, and neither basic check, nor Acls grant us
    // read. Deny.
    //--------------------------------------------------------------------------
  	return false;
  }

  if ((mode & X_OK) && (!acl.CanBrowse() && !cont->access(vid.uid, vid.gid, X_OK) )) {
    //--------------------------------------------------------------------------
    // Asking for browse permission, and neither basic check, nor Acls grant us
    // browse. Deny.
    //--------------------------------------------------------------------------
  	return false;
  }

  //--------------------------------------------------------------------------
  // We survived Acl check, grant.
  //--------------------------------------------------------------------------
  return true;
}

EOSMGMNAMESPACE_END
