//------------------------------------------------------------------------------
//! @file PersistenceException.cc
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

#include "PersistencyException.hh"
#include <xrootd/XrdSfs/XrdSfsInterface.hh>

EOSBULKNAMESPACE_BEGIN

PersistencyException::PersistencyException(const std::string& exceptionMsg):std::exception(),mErrorMsg(exceptionMsg){
}

const char * PersistencyException::what() const noexcept {
  return mErrorMsg.c_str();
}

int PersistencyException::fillXrdErrInfo(XrdOucErrInfo & error,int errorCode) const {
  char buffer[4096];
  // Get the reason for the error
  if (errorCode < 0) {
    errorCode = -errorCode;
  }
  snprintf(buffer, sizeof(buffer), mErrorMsg.c_str());
  error.setErrInfo(errorCode,buffer);
  return SFS_ERROR;
}

EOSBULKNAMESPACE_END


