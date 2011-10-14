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

EOSMGMNAMESPACE_BEGIN

class Acl {
  bool canRead;
  bool canWrite;
  bool canWriteOnce;
  bool canBrowse;
  bool hasAcl;
  bool hasEgroup;

public:

  Acl() { canRead = false; canWrite = false; canWriteOnce = false; canBrowse = false; hasAcl = false; hasEgroup = false;}

  Acl(std::string sysacl, std::string useracl, eos::common::Mapping::VirtualIdentity &vid);
  ~Acl(){};
  
  void Set(std::string sysacl, std::string useracl, eos::common::Mapping::VirtualIdentity &vid);

  bool CanRead()      {return canRead;}
  bool CanWrite()     {return canWrite;}
  bool CanWriteOnce() {return canWriteOnce;}
  bool CanBrowse()   {return canBrowse;}
  bool HasAcl()       {return hasAcl;}
  bool HasEgroup()    {return hasEgroup;}
};

EOSMGMNAMESPACE_END

#endif
