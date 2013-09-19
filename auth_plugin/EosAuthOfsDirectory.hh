// ----------------------------------------------------------------------
// File: EosAuthOfsDirectory.hh
// Author: Elvin-Alin Sindrilau <esindril@cern.ch> CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

#ifndef __EOSAUTH_OFSDIRECTORY__HH__
#define __EOSAUTH_OFSDIRECTORY__HH__

/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "common/Mapping.hh"
#include "Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucErrInfo.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdSfs/XrdSfsInterface.hh"
/*----------------------------------------------------------------------------*/

EOSAUTHNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class implementing OFS directories
//------------------------------------------------------------------------------
class EosAuthOfsDirectory: public XrdSfsDirectory, public eos::common::LogId
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  EosAuthOfsDirectory(char *user = 0, int MonID = 0);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~EosAuthOfsDirectory();


  //----------------------------------------------------------------------------
  //! Open a directory
  //----------------------------------------------------------------------------
  int open(const char *name,
           const XrdSecClientName *client = 0,
           const char *opaque = 0);
  

  //----------------------------------------------------------------------------
  //! Get entry of an open directory
  //----------------------------------------------------------------------------
  const char *nextEntry();

  
  //----------------------------------------------------------------------------
  //! Close an open directory
  //----------------------------------------------------------------------------
  int close();

  
  //----------------------------------------------------------------------------
  //! Get name of an open directory
  //----------------------------------------------------------------------------
  const char* FName();

 private:

  std::string mName; ///< keep directory name just for debugging purposes
  std::string mNextEntry; ///< next entry value in directory
};

EOSAUTHNAMESPACE_END

#endif // __EOSAUTH_OFSDIRECTORY_HH__
