// ----------------------------------------------------------------------
// File: Exception.hh
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

#ifndef EOSCOMMON_EXCEPTION_HH
#define EOSCOMMON_EXCEPTION_HH

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include <stdexcept>
#include <sstream>
#include <cerrno>
#include <cstring>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

//----------------------------------------------------------------------------
//!  Exception handling errno + error text
//----------------------------------------------------------------------------
class Exception: public std::exception
{
public:
  //------------------------------------------------------------------------
  // Constructor
  //------------------------------------------------------------------------
  Exception( int errorNo = ENODATA ) throw():
    mErrorNo( errorNo ), mTmpMessage( 0 ) {}
  
  //------------------------------------------------------------------------
  //! Destructor
  //------------------------------------------------------------------------
  virtual ~Exception() throw()
  {
    delete [] mTmpMessage;
  }
  
  //------------------------------------------------------------------------
  //! Copy constructor - this is actually required because we cannot copy
  //! stringstreams
  //------------------------------------------------------------------------
  Exception( Exception &e )
  {
    mMessage << e.getMessage().str();
    mErrorNo = e.getErrno();
    mTmpMessage = 0;
  }
  
  //------------------------------------------------------------------------
  //! Get errno assosiated with the exception
  //------------------------------------------------------------------------
  int getErrno() const
  {
    return mErrorNo;
  }
  
  //------------------------------------------------------------------------
  //! Get the message stream
  //------------------------------------------------------------------------
  std::ostringstream &getMessage()
  {
    return mMessage;
  }
  
  //------------------------------------------------------------------------
  // Get the message
  //------------------------------------------------------------------------
  virtual const char *what() const throw()
  {
    // we could to that instead: return (mMessage.str()+" ").c_str();
    // but it's ugly and probably not portable
    
    if( mTmpMessage )
      delete [] mTmpMessage;
    
    std::string msg = mMessage.str();
    mTmpMessage = new char[msg.length()+1];
    mTmpMessage[msg.length()] = 0;
    strcpy( mTmpMessage, msg.c_str() );
    return mTmpMessage;
  }
  
private:
  //------------------------------------------------------------------------
  // Data members
  //------------------------------------------------------------------------
  std::ostringstream  mMessage;
  int                 mErrorNo;
  mutable char       *mTmpMessage;
};

EOSCOMMONNAMESPACE_END

#endif
