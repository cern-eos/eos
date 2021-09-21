//------------------------------------------------------------------------------
//! @file EosFstHttpHandler.cc
//! @author Andreas-Joachim Peters & Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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

#pragma once
#include <map>
#include <string>
#include "XrdHttp/XrdHttpExtHandler.hh"
#include "XrdVersion.hh"

XrdVERSIONINFO(XrdHttpGetExtHandler, EOSFSTHTTP);

class XrdLink;
class XrdSecEntity;
class XrdHttpReq;
class XrdHttpProtocol;

//------------------------------------------------------------------------------
//! Class EosFstHttpHandler
//------------------------------------------------------------------------------
class EosFstHttpHandler : public XrdHttpExtHandler
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  EosFstHttpHandler() = default;

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~EosFstHttpHandler() = default;

  bool MatchesPath(const char* verb, const char* path);

  int ProcessReq(XrdHttpExtReq&);

  int Init(const char* cfgfile);

private:
  eos::fst::XrdFstOfs* OFS;

  //----------------------------------------------------------------------------
  //! Handle chunk upload operation
  //!
  //! @param req http external request object
  //! @param handler eos protocol handler object for file operations
  //! @param norm_hdrs normalized headers
  //! @param cookies cookies
  //! @param query query string
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool HandleChunkUpload(XrdHttpExtReq& req,
                         eos::common::ProtocolHandler* handler,
                         std::map<std::string, std::string>& norm_hdrs,
                         std::map<std::string, std::string>& cookies,
                         std::string& query);

  //----------------------------------------------------------------------------
  //! Handle chunk upload operation
  //!
  //! @param req http external request object
  //! @param handler eos protocol handler object for file operations
  //! @param norm_hdrs normalized headers
  //! @param cookies cookies
  //! @param query query string
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool HandleChunkUpload2(XrdHttpExtReq& req,
                          eos::common::ProtocolHandler* handler,
                          std::map<std::string, std::string>& norm_hdrs,
                          std::map<std::string, std::string>& cookies,
                          std::string& query);
};

/******************************************************************************/
/*                    X r d H t t p G e t E x t H a n d l e   r               */
/******************************************************************************/

//------------------------------------------------------------------------------
//! Obtain an instance of the XrdHttpExtHandler object.
//!
//! This extern "C" function is called when a shared library plug-in containing
//! implementation of this class is loaded. It must exist in the shared library
//! and must be thread-safe.
//!
//! @param  eDest -> The error object that must be used to print any errors or
//!                  other messages (see XrdSysError.hh).
//! @param  confg -> Name of the configuration file that was used. This pointer
//!                  may be null though that would be impossible.
//! @param  parms -> Argument string specified on the namelib directive. It may
//!                  be null or point to a null string if no parms exist.
//! @param  myEnv -> Environment variables for configuring the external handler;
//!                  it my be null.
//!
//! @return Success: A pointer to an instance of the XrdHttpSecXtractor object.
//!         Failure: A null pointer which causes initialization to fail.
//!

//------------------------------------------------------------------------------

class XrdSysError;
class XrdOucEnv;

#define XrdHttpExtHandlerArgs XrdSysError       *eDest, \
                              const char        *confg, \
                              const char        *parms, \
                              XrdOucEnv         *myEnv

extern "C" XrdHttpExtHandler* XrdHttpGetExtHandler(XrdHttpExtHandlerArgs)
{
  XrdHttpExtHandler* handler = new EosFstHttpHandler();
  handler->Init(confg);
  return handler;
}

//------------------------------------------------------------------------------
//! Declare compilation version.
//!
//! Additionally, you *should* declare the xrootd version you used to compile
//! your plug-in. While not currently required, it is highly recommended to
//! avoid execution issues should the class definition change. Declare it as:
//------------------------------------------------------------------------------
