// ----------------------------------------------------------------------
// File: Acl.hh
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

#ifndef __EOSMGM_ACL__HH__
#define __EOSMGM_ACL__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "common/Mapping.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
#include "XrdOuc/XrdOucErrInfo.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <string>
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
/**
 * @file   Acl.hh
 *
 * @brief  Class providing ACL interpretation and access control functions
 *
 */

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
//! Class implementing access control list interpretation.
//! ACL rules used in the constructor or set function are strings with
//! the following format:\n\n
//! rule=
//! 'u:<uid|username>|g:<gid|groupname>|egroup:<name>:{arw[o]ximc(!u)(+u)(!d)(+d)q}|z:xmc(!u)(+u)(!d)(+d)q}'
//!
/*----------------------------------------------------------------------------*/
class Acl {
  bool canRead; ///< acl allows read access
  bool canWrite; ///< acl allows write access
  bool canWriteOnce; ///< acl allows write-once access (creation, no delete)
  bool canUpdate; ///< acl allows update of files
  bool canBrowse; ///< acl allows browsing
  bool canChmod; ///< acl allows mode change
  bool canChown; ///< acl allows chown change
  bool canNotDelete; ///< acl forbids deletion
  bool canNotChmod; ///< acl forbids chmod
  bool canDelete; ///< acl allows deletion
  bool canSetQuota; ///< acl allows to set quota
  bool hasAcl; ///< acl is valid
  bool hasEgroup; ///< acl contains egroup rule
  bool isMutable; ///< acl does not contain the immutable flag
  bool canArchive; ///< acl which allows archiving
public:
  /*---------------------------------------------------------------------------*/
  //! Default Constructor

  /*---------------------------------------------------------------------------*/
  Acl ()
  {
    canRead = false;
    canWrite = false;
    canWriteOnce = false;
    canUpdate = false;
    canBrowse = false;
    canChmod = false;
    canNotChmod = false;
    canChown = false;
    canNotDelete = false;
    canDelete = false;
    canSetQuota = false;
    hasAcl = false;
    hasEgroup = false;
    isMutable = true;
    canArchive = false;
  }

  /*---------------------------------------------------------------------------*/
  //! Constructor
  /*---------------------------------------------------------------------------*/
  Acl (std::string sysacl,
       std::string useracl,
       eos::common::Mapping::VirtualIdentity &vid,
       bool allowUserAcl = false);

  /*--------------------------------------------------------------------------*/
  //! Destructor
  /*--------------------------------------------------------------------------*/
  ~Acl ()
  {
  };

  /*--------------------------------------------------------------------------*/
  //! Enter system and user definition + identity used for ACL interpretation
  /*--------------------------------------------------------------------------*/
  void Set (std::string sysacl,
            std::string useracl,
            eos::common::Mapping::VirtualIdentity &vid,
            bool allowUserAcl = false);

  /*--------------------------------------------------------------------------*/
  //! Use regex to check ACL format / syntax
  /*--------------------------------------------------------------------------*/
  static bool IsValid (const std::string value,
                       XrdOucErrInfo &error,
                       bool sysacl = false);

  /*--------------------------------------------------------------------------*/
  // Getter Functions for ACL booleans

  /*--------------------------------------------------------------------------*/

  bool
  CanRead ()
  /// allowed to read
  {
    return canRead;
  }

  bool
  CanWrite ()
  /// allowed to write
  {
    return canWrite;
  }

  bool
  CanWriteOnce ()
  /// allowed to write-once (no overwrite/update/delete)
  {
    return canWriteOnce;
  }

  bool
  CanUpdate ()
  /// allowed to update
  {
    return canUpdate;
  }

  bool
  CanBrowse ()
  /// allowed to list
  {
    return canBrowse;
  }

  bool
  CanChmod ()
  /// allowed to change mod
  {
    return canChmod;
  }

  bool
  CanNotChmod ()
  /// not allowed to change mod
  {
    return canNotChmod;
  }

  bool
  CanChown ()
  /// allowed to change owner
  {
    return canChown;
  }

  bool
  CanNotDelete ()
  /// not allowed to delete
  {
    return canNotDelete;
  }

  bool
  CanDelete ()
  /// allowed to delete
  {
    return canDelete;
  }

  bool
  CanSetQuota ()
  /// allowed to administer quota
  {
    return canSetQuota;
  }

  bool
  HasAcl ()
  /// has any acl defined
  {
    return hasAcl;
  }

  bool
  HasEgroup ()
  /// has any egroup defined
  {
    return hasEgroup;
  }

  bool
  IsMutable ()
  /// has not the 'i' flag
  {
    return isMutable;
  }

  //----------------------------------------------------------------------------
  //! Has the 'a' flag - archiving permission
  //----------------------------------------------------------------------------
  inline bool
  CanArchive() const
  {
    return canArchive;
  }
};

EOSMGMNAMESPACE_END

#endif
