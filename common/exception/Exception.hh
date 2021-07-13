//------------------------------------------------------------------------------
//! @file Exception.hh
//! @author Cedric Caffy - CERN
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
#ifndef EOS_EXCEPTION_HH
#define EOS_EXCEPTION_HH

#include "common/Namespace.hh"
#include <exception>
#include <string>
#include <xrootd/XrdOuc/XrdOucErrInfo.hh>

EOSCOMMONNAMESPACE_BEGIN

class Exception : public std::exception {
public:
  /**
   * Constructor of the Exception
   * @param exceptionMsg the error message associated to this exception
   */
  Exception(const std::string & exceptionMsg);
  /**
   * Returns the message of this exception
   * @return the message of this exception
   */
  virtual const char*  what() const noexcept;

  /**
   * Assign the exception message to the Xrd error information passed in parameter
   * @param error the Xrd error info object to assign the exception message
   * @param errorCode the Xrd error code assocaited to the exception message
   * @return the error code linked to this exception
   */
  virtual int fillXrdErrInfo(XrdOucErrInfo & error,int errorCode) const;
private:
  std::string mErrorMsg;
};

EOSCOMMONNAMESPACE_END

#endif // EOS_EXCEPTION_HH
