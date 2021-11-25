//------------------------------------------------------------------------------
//! file EosMgmHttpHandler.hh
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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
#include "XrdHttp/XrdHttpExtHandler.hh"
#include "common/Logging.hh"
#include "XrdVersion.hh"

//! Forward declaration
class XrdMgmOfs;
class XrdAccAuthorize;
class XrdSfsFileSystem;

//------------------------------------------------------------------------------
//! Class OwningXrdSecEntity - this class is used to copy the contents of an
//! XrdSecEntity object which is not owned by us and provide an RAII mechanism
//! to manage the lifetime of the underlying object.
//------------------------------------------------------------------------------
class OwningXrdSecEntity
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param obj reference to XrdSecEntity object that we copy the info from
  //----------------------------------------------------------------------------
  explicit OwningXrdSecEntity(const XrdSecEntity& obj):
    mSecEntity(nullptr)
  {
    CreateFrom(obj);
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~OwningXrdSecEntity();

  //----------------------------------------------------------------------------
  //! Get underlying XrdSecEntity object
  //!
  //! @return pointer to owned XrdSecEntity object
  //----------------------------------------------------------------------------
  XrdSecEntity* GetObj()
  {
    return mSecEntity.get();
  }

private:
  std::unique_ptr<XrdSecEntity> mSecEntity;

  //----------------------------------------------------------------------------
  //! Populate internal XrdSecEntity with info from another object doing the
  //! necessary memory allocations
  //!
  //! @param other XrdSecEntity object used to copy info from
  //----------------------------------------------------------------------------
  void CreateFrom(const XrdSecEntity& other);
};


//------------------------------------------------------------------------------
//! Class EosMgmHttpHandler
//------------------------------------------------------------------------------
class EosMgmHttpHandler: public XrdHttpExtHandler,
  public eos::common::LogId
{

public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  EosMgmHttpHandler() :
    mRedirectToHttps(false), mTokenHttpHandler(nullptr),
    mTokenAuthzHandler(nullptr), mMgmOfsHandler(nullptr)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~EosMgmHttpHandler();

  //----------------------------------------------------------------------------
  //! Initialize the external request handler
  //!
  //! @param  confg  Name of the configuration file that was used
  //!
  //! @return 0 if successful, otherwise non-zero value
  //----------------------------------------------------------------------------
  int Init(const char* confg) override
  {
    // We do the work in the Config method as we have all the parameters needed
    // there.
    return 0;
  }

  //----------------------------------------------------------------------------
  //! Config the current request handler
  //!
  //! @param  eDest -> The error object that must be used to print any errors or
  //!                  other messages (see XrdSysError.hh).
  //! @param  confg -> Name of the configuration file that was used. This pointer
  //!                  may be null though that would be impossible.
  //! @param  parms -> Argument string specified on the namelib directive. It may
  //!                  be null or point to a null string if no parms exist.
  //! @param  myEnv -> Environment variables for configuring the external handler;
  //!                  it my be null.
  //! @return 0 if successful, otherwise non-zero value
  //----------------------------------------------------------------------------
  int Config(XrdSysError* eDest, const char* confg, const char* parms,
             XrdOucEnv* myEnv);

  //----------------------------------------------------------------------------
  //! Tells if the incoming path is recognizsed by the current plugin as one
  //! that needs to be processed.
  //!
  //! @param verb HTTP verb
  //! @param path request path
  //!
  //! @return true if current handler is to be invoked, otherwise false
  //----------------------------------------------------------------------------
  bool MatchesPath(const char* verb, const char* path) override;

  //----------------------------------------------------------------------------
  //! Process the HTTP request and send the response using by calling the
  //! XrdHttpProtocol directly
  //!
  //! @param req HTTP request
  //!
  //! @return 0 if successful, otherwise non-0
  //----------------------------------------------------------------------------
  int ProcessReq(XrdHttpExtReq& req) override;

private:
  bool mRedirectToHttps; ///< Flag if http traffic should be redirected to https
  XrdHttpExtHandler* mTokenHttpHandler; ///< Macaroons ext http handler
  //! Authz plugin from libMacaroons/libXrdSciTokens
  XrdAccAuthorize* mTokenAuthzHandler;
  XrdMgmOfs* mMgmOfsHandler; ///< Pointer to the MGM OFS plugin

  //----------------------------------------------------------------------------
  //! Copy XrdSecEntity info
  //!
  //! @param src source info
  //! @param dst newly populated objed
  //----------------------------------------------------------------------------
  void CopyXrdSecEntity(const XrdSecEntity& src, XrdSecEntity& dst) const;

  //----------------------------------------------------------------------------
  //! Get a pointer to the MGM OFS plugin
  //!
  //! @param eDest error object that must be used to print any errors or msgs
  //! @param confg name of the configuration file
  //! @param myEnv environment variables for configuring the external handler;
  //!              it may be null.
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool GetOfsPlugin(XrdSysError* eDest, const std::string& confg,
                    XrdOucEnv* myEnv);
  //----------------------------------------------------------------------------
  //! Reads the body of the XrdHttpExtReq object and put it in the
  //! body string
  //!
  //! @param req the request from which we will read the body content from
  //! @param body, the string where the body from the request will be put on
  //!
  //! @return a return code if there was an error during the reading. Nothing otherwise,
  //! hence the optional<int>
  //----------------------------------------------------------------------------
  std::optional<int> readBody(XrdHttpExtReq& req, std::string & body);
};
