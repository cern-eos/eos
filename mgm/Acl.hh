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
/*----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <string>
/*----------------------------------------------------------------------------*/
/**
 * @file   Acl.hh
 * 
 * @brief  Class providing ACL interpretation and access control functions
 * 
 * ACL rules used in the constructor or set function are strings with the following format:
 * rule='u:<uid|username>|g:<gid|groupname>|egroup:<name>:{rwxom(!d)(+d)}'
 */

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
//! Class implementing access control list interpretation
/*----------------------------------------------------------------------------*/
class Acl {
  bool canRead;       //< acl allows read access
  bool canWrite;      //< acl allows write access
  bool canWriteOnce;  //< acl allows write-once access
  bool canBrowse;     //< acl allows browsing
  bool canChmod;      //< acl allows mode change
  bool canNotDelete;  //< acl forbids deletion
  bool canDelete;     //< acl allows deletion
  bool canSetQuota;   //< acl allows to set quota
  bool hasAcl;        //< acl is valid
  bool hasEgroup;     //< acl contains egroup rule
public:
  /*----------------------------------------------------------------------------*/
  //! Default Constructor
  /*----------------------------------------------------------------------------*/
  Acl() { canRead = false; canWrite = false; canWriteOnce = false; canBrowse = false; canChmod = false; canNotDelete = false; canDelete = false; canSetQuota = false; hasAcl = false; hasEgroup = false;}

  /*----------------------------------------------------------------------------*/
  //! Constructor
  /*----------------------------------------------------------------------------*/
  Acl(std::string sysacl, std::string useracl, eos::common::Mapping::VirtualIdentity &vid);

  /*----------------------------------------------------------------------------*/
  //! Destructor
  /*----------------------------------------------------------------------------*/
  ~Acl(){};
  
  /*----------------------------------------------------------------------------*/
  //! Enter system and user definition + identity used for ACL interpretation
  /*----------------------------------------------------------------------------*/
  void Set(std::string sysacl, std::string useracl, eos::common::Mapping::VirtualIdentity &vid);

  /*----------------------------------------------------------------------------*/
  //! Getter Functions for ACL booleans
  /*----------------------------------------------------------------------------*/
  bool CanRead()      {return canRead;}
  bool CanWrite()     {return canWrite;}
  bool CanWriteOnce() {return canWriteOnce;}
  bool CanBrowse()    {return canBrowse;}
  bool CanChmod()     {return canChmod;}
  bool CanNotDelete() {return canNotDelete;}
  bool CanSetQuota()  {return canSetQuota;}
  bool HasAcl()       {return hasAcl;}
  bool HasEgroup()    {return hasEgroup;}
};

EOSMGMNAMESPACE_END

#endif
