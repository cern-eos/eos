// ----------------------------------------------------------------------
// File: ClientAdmin.hh
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

/**
 * @file   ClientAdmin.hh
 * 
 * @brief  Thread safe wrapper class to XrdClientAdmin.
 * 
 * 
 */

#ifndef __EOSCOMMON_CLIENTADMIN_HH__
#define __EOSCOMMON_CLIENTADMIN_HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdClient/XrdClientAdmin.hh"
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
//! Class adding locking to XrdClientAdmin objects
//! The usage of this class should become obsolete.
class ClientAdmin {
  XrdSysMutex clock;
  XrdClientAdmin* Admin;
  
public:
  // ---------------------------------------------------------------------------
  //! Lock a client admin object
  // ---------------------------------------------------------------------------
  void Lock() {clock.Lock();}

  // ---------------------------------------------------------------------------
  //! Unlock a client admin object
  // ---------------------------------------------------------------------------
  void UnLock() {clock.UnLock();}

  // ---------------------------------------------------------------------------
  //! Return a reference to an admin object
  // ---------------------------------------------------------------------------
  XrdClientAdmin* GetAdmin() { return Admin;}

  // ---------------------------------------------------------------------------
  //! Constructor accepting URL
  // ---------------------------------------------------------------------------
  ClientAdmin(const char* url) { Admin = new XrdClientAdmin(url);};

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~ClientAdmin() { if (Admin) delete Admin;}
};

/*----------------------------------------------------------------------------*/
//! Class handling ClientAdmin objects by URL
class ClientAdminManager {
private:
  XrdSysMutex Mutex;
  XrdOucHash<ClientAdmin> admins;
public:
  // ---------------------------------------------------------------------------
  //! Return an ClientAdmin object by <host:port> name
  //! If the corresponding admin does not exists, it is automatically added to the internal store
  // ---------------------------------------------------------------------------
  ClientAdmin* GetAdmin(const char* hostport) {
    ClientAdmin* myadmin=0;
    Mutex.Lock();
    if ((myadmin = admins.Find(hostport))) {
      Mutex.UnLock();
      return myadmin;
    } else {
      XrdOucString url = "root://"; url += hostport; url += "//dummy";
      myadmin = new ClientAdmin(url.c_str());
      admins.Add(hostport,myadmin);
      Mutex.UnLock();
      return myadmin;
    }
  }
  
  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  ClientAdminManager() {};

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~ClientAdminManager() {};
  
};

EOSCOMMONNAMESPACE_END
#endif
