//------------------------------------------------------------------------------
// File: Acl.hh
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

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

#include "mgm/Namespace.hh"
#include "common/Mapping.hh"
#include "namespace/interface/IContainerMD.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdOuc/XrdOucErrInfo.hh"
#include <sys/types.h>
#include <string>


EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class implementing access control list interpretation.
//! ACL rules used in the constructor or set function are strings with
//! the following format:\n\n
//! rule=
//! 'u:<uid|username>|g:<gid|groupname>|egroup:<name>:{arw[o]ximc(!u)(+u)(!d)(+d)q}|z:xmc(!u)(+u)(!d)(+d)q}'
//!
//------------------------------------------------------------------------------
class Acl
{
public:
  //----------------------------------------------------------------------------
  //! Default Constructor
  //----------------------------------------------------------------------------
  Acl():
    canRead(false), canWrite(false), canWriteOnce(false), canUpdate(false),
    canBrowse(false), canChmod(false), canChown(false), canNotDelete(false),
    canNotChmod(false), canDelete(false), canSetQuota(false), hasAcl(false),
    hasEgroup(false), isMutable(false), canArchive(false)
  {}

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param sysacl system acl definition string
  //! 'u:<uid|username>|g:<gid|groupname>|egroup:<name>:{rwxom(!d)(+d)(!u)(+u)}|z:{rw[o]xmc(!u)(+u)(!d)(+d)q}'
  //! @param useracl user acl definition string
  //! 'u:<uid|username>|g:<gid|groupname>|egroup:<name>:{rwxom(!d)(+d)(!u)(+u)}|z:{rw[o]xmc(!u)(+u)(!d)(+d)q}'
  //! @param vid virtual id to match ACL
  //! @param allowUserAcl if true evaluate also the user acl for the permissions
  //----------------------------------------------------------------------------
  Acl(std::string sysacl, std::string useracl,
      eos::common::Mapping::VirtualIdentity& vid, bool allowUserAcl = false);


  //----------------------------------------------------------------------------
  //! Constructor by path
  //!
  //! @param parent path where to read the acl attributes from
  //! @param error return error object
  //! @param vid virtual id to match ACL
  //! @param attr map returns all the attributes from path
  //! @param lockNs should we lock the namespace when retrieveng the attribute map
  //----------------------------------------------------------------------------
  Acl(const char* path, XrdOucErrInfo& error,
      eos::common::Mapping::VirtualIdentity& vid,
      eos::IContainerMD::XAttrMap& attrmap, bool lockNs);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~Acl() {}

  //----------------------------------------------------------------------------
  //! Enter system and user definition + identity used for ACL interpretation
  //!
  //! @param sysacl system acl definition string
  //! 'u:<uid|username>|g:<gid|groupname>|egroup:<name>:{arwxom(!d)(+d)(!u)}|z:{rw[o]xmc(!u)(+u)(!d)(+d)q}'
  //! @param useracl user acl definition string
  //! 'u:<uid|username>|g:<gid|groupname>|egroup:<name>:{rwxom(!d)(+d)(!u)}|z:{rw[o]xmc(!u)(+u)(!d)(+d)q}'
  //! @param vid virtual id to match ACL
  //! @param allowUserAcl if true evaluate the user acl for permissions
  //----------------------------------------------------------------------------
  void Set(std::string sysacl, std::string useracl,
           eos::common::Mapping::VirtualIdentity& vid,
           bool allowUserAcl = false);

  //----------------------------------------------------------------------------
  //! Use regex to check ACL format / syntax
  //!
  //! @param value value to check
  //! @param error error datastructure
  //! @param sysacl boolean indicating a sys acl entry which might have a z: rule
  //!
  //! return boolean indicating validity
  //----------------------------------------------------------------------------
  static bool IsValid(const std::string value, XrdOucErrInfo& error,
                      bool sysacl = false);

  //----------------------------------------------------------------------------
  // Getter Functions for ACL booleans
  //----------------------------------------------------------------------------
  inline bool CanRead() const
  {
    return canRead;
  }

  inline bool CanWrite() const
  {
    return canWrite;
  }

  inline bool CanWriteOnce() const
  {
    return canWriteOnce;
  }

  inline bool CanUpdate() const
  {
    return canUpdate;
  }

  inline bool CanBrowse() const
  {
    return canBrowse;
  }

  inline bool CanChmod() const
  {
    return canChmod;
  }

  inline bool CanNotChmod() const
  {
    return canNotChmod;
  }

  inline bool CanChown() const
  {
    return canChown;
  }

  inline bool CanNotDelete() const
  {
    return canNotDelete;
  }

  inline bool CanDelete() const
  {
    return canDelete;
  }

  inline bool CanSetQuota() const
  {
    return canSetQuota;
  }

  inline bool HasAcl() const
  {
    return hasAcl;
  }

  inline bool HasEgroup() const
  {
    return hasEgroup;
  }

  //----------------------------------------------------------------------------
  //! It should not have the 'i' flag to be mutable
  //----------------------------------------------------------------------------
  inline bool IsMutable() const
  {
    return isMutable;
  }

  //----------------------------------------------------------------------------
  //! Has the 'a' flag - archiving permission
  //----------------------------------------------------------------------------
  inline bool CanArchive() const
  {
    return canArchive;
  }

private:
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
};

EOSMGMNAMESPACE_END

#endif
