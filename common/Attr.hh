// ----------------------------------------------------------------------
// File: Attr.hh
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
 * @file   Attr.hh
 * 
 * @brief  Interface to extended attributes on file level.
 * 
 * 
 */

#ifndef __EOSCOMMON_ATTR__HH__
#define __EOSCOMMON_ATTR__HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <string>
#include <sys/types.h>
#include <attr/xattr.h>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
//! Class to modify extended attributes on files.
//! Best is to use the factory function OpenAttr instead of the constructor.
/*----------------------------------------------------------------------------*/

class Attr {
private:
  std::string fName; //< File Name storing the attributes
  static char gBuffer[1024];
  static XrdSysMutex gBufferMutex; 

public:
  // ------------------------------------------------------------------------
  //! Set a binary attribute (name has to start with 'user.' !!!)
  // ------------------------------------------------------------------------
  bool Set(const char* name, const char* value, size_t len); 

  // ------------------------------------------------------------------------
  //! Set a string attribute (name has to start with 'user.' !!!)
  // ------------------------------------------------------------------------
  bool Set(std::string key, std::string value);          

  // ------------------------------------------------------------------------
  //! Get a binary attribute by name (name has to starte with 'user.' !!!)
  // ------------------------------------------------------------------------
  bool Get(const char* name, char* value, size_t &size); 

  // ------------------------------------------------------------------------
  //! Get a string attribute by name (name has to starte with 'user.' !!!)
  // ------------------------------------------------------------------------
  std::string Get(std::string name);                     
  
  // ------------------------------------------------------------------------
  //! Factory function to create an attribute object
  // ------------------------------------------------------------------------
  static Attr* OpenAttr(const char* file);               

  // ------------------------------------------------------------------------
  //! Constructor
  // ------------------------------------------------------------------------
  Attr(const char* file);                                
  
  // ------------------------------------------------------------------------
  //! Destructor
  // ------------------------------------------------------------------------
  ~Attr();                                               
};

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END
#endif

