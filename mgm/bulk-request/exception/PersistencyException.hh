//------------------------------------------------------------------------------
//! @file PersistenceException.hh
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

#ifndef EOS_PERSISTENCYEXCEPTION_HH
#define EOS_PERSISTENCYEXCEPTION_HH

#include "mgm/Namespace.hh"
#include <exception>
#include <string>

EOSMGMNAMESPACE_BEGIN

/**
 * Exception class for handling bulk-request persistency exceptions
 */
class PersistencyException : public std::exception {
public:
  /**
   * Constructor of the PersistencyException
   * @param exceptionMsg the error message associated to this exception
   */
  PersistencyException(const std::string & exceptionMsg);
  /**
   * Returns the message of this exception
   * @return the message of this exception
   */
  virtual const char*  what() const noexcept;
private:
  std::string mErrorMsg;
};

EOSMGMNAMESPACE_END

#endif // EOS_PERSISTENCYEXCEPTION_HH
