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

//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Metadata exception
//------------------------------------------------------------------------------

#ifndef EOS_NS_MD_EXCEPTION_HH
#define EOS_NS_MD_EXCEPTION_HH

#include <stdexcept>
#include <sstream>
#include <cerrno>
#include <cstring>
#include <folly/ExceptionWrapper.h>

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()
#define throw_mdexception(err, msg) { eos::MDException __md___exception____(err); __md___exception____.getMessage() << msg; throw __md___exception____; }
#define make_mdexception(err, msg) folly::make_exception_wrapper<eos::MDException>(err, std::move(SSTR(msg)))

namespace eos
{
//----------------------------------------------------------------------------
//! Metadata exception
//----------------------------------------------------------------------------
class MDException: public std::exception
{
public:
  //------------------------------------------------------------------------
  // Constructor taking an error code and string message
  //------------------------------------------------------------------------
  MDException(int errorNo = ENODATA, const std::string& msg = ""):
    pErrorNo(errorNo), pTmpMessage(0)
  {
    if (!msg.empty()) {
      getMessage() << msg;
    }
  }

  MDException(int errno, std::string&& msg):
    pErrorNo(errno), pTmpMessage(0)
  {
    getMessage() << msg;
  }
  //------------------------------------------------------------------------
  //! Destructor
  //------------------------------------------------------------------------
  virtual ~MDException() throw()
  {
    delete [] pTmpMessage;
  }

  //------------------------------------------------------------------------
  //! Copy constructor - this is actually required because we cannot copy
  //! stringstreams
  //------------------------------------------------------------------------
  MDException(const MDException& e)
  {
    pMessage << e.pMessage.str();
    pErrorNo = e.getErrno();
    pTmpMessage = 0;
  }

  //------------------------------------------------------------------------
  //! Get errno associated with the exception
  //------------------------------------------------------------------------
  int getErrno() const
  {
    return pErrorNo;
  }

  //------------------------------------------------------------------------
  //! Get the message stream
  //------------------------------------------------------------------------
  std::ostringstream& getMessage()
  {
    return pMessage;
  }

  //------------------------------------------------------------------------
  // Get the message
  //------------------------------------------------------------------------
  virtual const char* what() const noexcept override
  {
    // we could to that instead: return (pMessage.str()+" ").c_str();
    // but it's ugly and probably not portable
    if (pTmpMessage) {
      delete [] pTmpMessage;
    }

    std::string msg = pMessage.str();
    pTmpMessage = new char[msg.length() + 1];
    pTmpMessage[msg.length()] = 0;
    pTmpMessage = strcpy(pTmpMessage, msg.c_str());
    return pTmpMessage;
  }

  void wrapAndRethrow(const std::string& prefix) const
  {
    throw_mdexception(pErrorNo, SSTR(prefix << pMessage.str()));
  }

private:
  //------------------------------------------------------------------------
  // Data members
  //------------------------------------------------------------------------
  std::ostringstream  pMessage;
  int                 pErrorNo;
  mutable char*       pTmpMessage;
};

inline MDException makeMDException(int err, std::string&& msg)
{
  MDException exc(err);
  exc.getMessage() << msg;
  return exc;
}

//----------------------------------------------------------------------------
//! Metadata operation status
//----------------------------------------------------------------------------
class MDStatus
{
public:
  MDStatus() : localerrno(0) {}

  MDStatus(int localerrn, const std::string& error);
  bool ok() const
  {
    return err.empty();
  }
  std::string getError() const
  {
    return err;
  }
  int getErrno() const
  {
    return localerrno;
  }

  void throwIfNotOk(const std::string& prefix = {})
  {
    if (!ok()) {
      throw_mdexception(localerrno, SSTR(prefix << err));
    }
  }
private:
  int localerrno;
  std::string err;
};

}

#endif // EOS_NS_MD_EXCEPTION_HH
