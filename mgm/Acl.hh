//------------------------------------------------------------------------------
//! @file Acl.hh
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

#pragma once
#include "mgm/Namespace.hh"
#include "mgm/proc/ProcCommand.hh"
#include "common/Mapping.hh"
#include "namespace/interface/IContainerMD.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdOuc/XrdOucErrInfo.hh"
#include <sys/types.h>
#include <string>

#define	P_OK	8		/* Test for workflow permission.  */

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class implementing access control list interpretation.
//! ACL rules used in the constructor or in the Set function are strings with
//! the following format:
//! rule= 'u:<uid|username>|g:<gid|groupname>|egroup:<name>:{arw[o]ximc(!u)
//!        (+u)(!d)(+d)q}|z:xmc(!u)(+u)(!d)(+d)q}'
//------------------------------------------------------------------------------
class Acl
{
public:
  static constexpr auto sRegexUsrGenericAcl =
    "^(((((u|g):(([0-9]+)|([\\.[:alnum:]_-]+)))|(egroup:([\\.[:alnum:]-]+))):"
    "(a|r|w|wo|x|i|m|!m|!d|[+]d|!u|[+]u|q|c)+)[,]?)*$";
  static constexpr auto sRegexSysGenericAcl =
    "^(((((u|g):(([0-9]+)|([\\.[:alnum:]_-]+)))|(egroup:([\\.[:alnum:]-]+))|(z)):"
    "(a|r|w|wo|x|i|m|!m|!d|[+]d|!u|[+]u|q|c|p)+)[,]?)*$";
  static constexpr auto sRegexUsrNumericAcl =
    "^(((((u|g):(([0-9]+)))|(egroup:([\\.[:alnum:]-]+))):"
    "(a|r|w|wo|x|i|m|!m|!d|[+]d|!u|[+]u|q|c)+)[,]?)*$";
  static constexpr auto sRegexSysNumericAcl =
    "^(((((u|g):(([0-9]+)))|(egroup:([\\.[:alnum:]-]+))|(z)):"
    "(a|r|w|wo|x|i|m|!m|!d|[+]d|!u|[+]u|q|c|p)+)[,]?)*$";

  //----------------------------------------------------------------------------
  //! Use regex to check ACL format / syntax
  //!
  //! @param value value to check
  //! @param error error datastructure
  //! @param is_sys_acl boolean indicating a sys acl entry which might have a
  //!        z: rule
  //! @param check_numeric if true use numeric format of the regex
  //!
  //! return boolean indicating validity
  //----------------------------------------------------------------------------
  static bool IsValid(const std::string& value, XrdOucErrInfo& error,
                      bool is_sys_acl = false, bool check_numeric = false);

  //----------------------------------------------------------------------------
  //! By default convert the uid/gid(s) to numeric representation. If to_string
  //! is set to true then convert from numeric to string representation.
  //!
  //! @param acl_val acl string which is modified in-place
  //! @param to_string by default false, if true convert uid/gids(s) from
  //!        numeric to string representation
  //----------------------------------------------------------------------------
  static void ConvertIds(std::string& acl_val, bool to_string = false);

  //----------------------------------------------------------------------------
  //! Default Constructor
  //----------------------------------------------------------------------------
  Acl():
    mCanRead(false), mCanWrite(false), mCanWriteOnce(false), mCanUpdate(false),
    mCanBrowse(false), mCanChmod(false), mCanChown(false), mCanNotDelete(false),
    mCanNotChmod(false), mCanDelete(false), mCanSetQuota(false), mHasAcl(false),
    mHasEgroup(false), mIsMutable(false), mCanArchive(false), mCanWorkflow(false)
  {}

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param sysacl system acl definition string
  //! 'u:<uid|username>|g:<gid|groupname>|egroup:<name>:{rwxom(!d)(+d)(!u)(+u)}
  //! |z:{rw[o]xmc(!u)(+u)(!d)(+d)q}'
  //! @param useracl user acl definition string
  //! 'u:<uid|username>|g:<gid|groupname>|egroup:<name>:{rwxom(!d)(+d)(!u)(+u)}
  //! |z:{rw[o]xmc(!u)(+u)(!d)(+d)q}'
  //! @param vid virtual id to match ACL
  //! @param allowUserAcl if true evaluate also the user acl for the permissions
  //----------------------------------------------------------------------------
  Acl(std::string sysacl, std::string useracl,
      eos::common::Mapping::VirtualIdentity& vid, bool allowUserAcl = false);

  /*---------------------------------------------------------------------------*/
  //! Constructor from XAttrMap
  /*---------------------------------------------------------------------------*/
  Acl (eos::IContainerMD::XAttrMap& xattrmap,
       eos::common::Mapping::VirtualIdentity &vid);

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
  virtual ~Acl() = default;

  //----------------------------------------------------------------------------
  //! Enter system and user definition + identity used for ACL interpretation
  //!
  //! @param sysacl system acl definition string
  //! 'u:<uid|username>|g:<gid|groupname>|egroup:<name>:{arwxom(!d)(+d)(!u)}
  //! |z:{rw[o]xmc(!u)(+u)(!d)(+d)q}'
  //! @param useracl user acl definition string
  //! 'u:<uid|username>|g:<gid|groupname>|egroup:<name>:{rwxom(!d)(+d)(!u)}
  //! |z:{rw[o]xmc(!u)(+u)(!d)(+d)q}'
  //! @param vid virtual id to match ACL
  //! @param allowUserAcl if true evaluate the user acl for permissions
  //----------------------------------------------------------------------------
  void Set(std::string sysacl, std::string useracl,
           eos::common::Mapping::VirtualIdentity& vid,
           bool allowUserAcl = false);

  //----------------------------------------------------------------------------
  // Getter Functions for ACL booleans
  //----------------------------------------------------------------------------
  inline bool CanRead() const
  {
    return mCanRead;
  }

  inline bool CanWrite() const
  {
    return mCanWrite;
  }

  inline bool CanWriteOnce() const
  {
    return mCanWriteOnce;
  }

  inline bool CanUpdate() const
  {
    return mCanUpdate;
  }

  inline bool CanBrowse() const
  {
    return mCanBrowse;
  }

  inline bool CanChmod() const
  {
    return mCanChmod;
  }

  inline bool CanNotChmod() const
  {
    return mCanNotChmod;
  }

  inline bool CanChown() const
  {
    return mCanChown;
  }

  inline bool CanNotDelete() const
  {
    return mCanNotDelete;
  }

  inline bool CanDelete() const
  {
    return mCanDelete;
  }

  inline bool CanSetQuota() const
  {
    return mCanSetQuota;
  }

  inline bool HasAcl() const
  {
    return mHasAcl;
  }

  inline bool HasEgroup() const
  {
    return mHasEgroup;
  }

  //----------------------------------------------------------------------------
  //! It should not have the 'i' flag to be mutable
  //----------------------------------------------------------------------------
  inline bool IsMutable() const
  {
    return mIsMutable;
  }

  //----------------------------------------------------------------------------
  //! Has the 'a' flag - archiving permission
  //----------------------------------------------------------------------------
  inline bool CanArchive() const
  {
    return mCanArchive;
  }

  //----------------------------------------------------------------------------
  //! Has the 'p' flag - archiving permission
  //----------------------------------------------------------------------------
  inline bool CanWorkflow() const
  {
    return mCanWorkflow;
  }

private:
  bool mCanRead; ///< acl allows read access
  bool mCanWrite; ///< acl allows write access
  bool mCanWriteOnce; ///< acl allows write-once access (creation, no delete)
  bool mCanUpdate; ///< acl allows update of files
  bool mCanBrowse; ///< acl allows browsing
  bool mCanChmod; ///< acl allows mode change
  bool mCanChown; ///< acl allows chown change
  bool mCanNotDelete; ///< acl forbids deletion
  bool mCanNotChmod; ///< acl forbids chmod
  bool mCanDelete; ///< acl allows deletion
  bool mCanSetQuota; ///< acl allows to set quota
  bool mHasAcl; ///< acl is valid
  bool mHasEgroup; ///< acl contains egroup rule
  bool mIsMutable; ///< acl does not contain the immutable flag
  bool mCanArchive; ///< acl which allows archiving
  bool mCanWorkflow; ///< acl which allows triggering workflows
};

EOSMGMNAMESPACE_END
