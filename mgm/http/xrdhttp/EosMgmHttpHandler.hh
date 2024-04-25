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
#include <XrdHttp/XrdHttpExtHandler.hh>
#include "common/Logging.hh"
#include <XrdVersion.hh>
#include <optional>
#include <list>
#include <curl/curl.h>

//! Forward declaration
class XrdMgmOfs;
class XrdAccAuthorize;
class XrdSfsFileSystem;

//------------------------------------------------------------------------------
//! Class EosMgmHttpHandler
//------------------------------------------------------------------------------
class EosMgmHttpHandler: public XrdHttpExtHandler,
  public eos::common::LogId
{
public:
  using HdrsMapT = std::map<std::string, std::string>;

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
#ifdef IN_TEST_HARNESS
public:
#endif
  bool mRedirectToHttps; ///< Flag if http traffic should be redirected to https
  XrdHttpExtHandler* mTokenHttpHandler; ///< Macaroons ext http handler
  //! Authz plugin from libMacaroons/libXrdSciTokens
  XrdAccAuthorize* mTokenAuthzHandler;
  XrdMgmOfs* mMgmOfsHandler; ///< Pointer to the MGM OFS plugin
  //! Url to access rest api grpc-gateway
  const char* mRestApiGwUrl = "http://localhost:40054";
  //! Path to rest api grpc-gateway
  const char* mRestApiGwPath = "/v1/eos/rest/gateway/";

  //----------------------------------------------------------------------------
  //! Copy XrdSecEntity info
  //!
  //! @param src source info
  //! @param dst newly populated objed
  //----------------------------------------------------------------------------
  void CopyXrdSecEntity(const XrdSecEntity& src, XrdSecEntity& dst) const;

  //----------------------------------------------------------------------------
  //! Get OFS library path from the given configuration
  //!
  //! @param cfg_line relevant config line from file i.e xrd.cf.mgm
  //!
  //! @return string representing the OFS library
  //----------------------------------------------------------------------------
  std::string GetOfsLibPath(const std::string& cfg_line);

  //----------------------------------------------------------------------------
  //! Get XrdHttpExHandler library path from the given configuration
  //!
  //! @param cfg_line relevant config line from file i.e xrd.cf.mgm
  //!
  //! @return string representing the OFS library
  //----------------------------------------------------------------------------
  std::string GetHttpExtLibPath(const std::string& cfg_line);

  //----------------------------------------------------------------------------
  //! Get list of external authorization libraries present in the configuration.
  //! If multiple are present then the order is kept to properly apply chaining
  //! to these libraries.
  //!
  //! @param cfg_line relevant config line from file i.e xrd.cf.mgm
  //!
  //! @return list of external authorization libraries configured
  //----------------------------------------------------------------------------
  std::list<std::string> GetAuthzLibPaths(const std::string& cfg_line);

  //----------------------------------------------------------------------------
  //! Get a pointer to the MGM OFS plugin
  //!
  //! @param eDest error object that must be used to print any errors or msgs
  //! @param lib_path library path
  //! @param confg configuration file path
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  XrdMgmOfs* GetOfsPlugin(XrdSysError* eDest, const std::string& lib_path,
                          const char* confg);

  //----------------------------------------------------------------------------
  //! Get a pointer to the XrdHttpExtHandler plugin
  //!
  //! @param eDest error object that must be used to print any errors or msgs
  //! @param lib_path library path
  //! @param confg configuration file path
  //! @param myEnv environment variables for configuring the external handler;
  //!       it my be null.
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  XrdHttpExtHandler*
  GetHttpExtPlugin(XrdSysError* eDest, const std::string& lib_path,
                   const char* confg, XrdOucEnv* myEnv);

  //----------------------------------------------------------------------------
  //! Get a pointer to the XrdAccAuthorize plugin present in the given library
  //!
  //! @param eDest error object that must be used to print any errors or msgs
  //! @param lib_path library path
  //! @param confg configuration file path
  //! @param myEnv environment variables for configuring the external handler;
  //!              it may be null.
  //! @param to_chain XrdAccAuthorize plugin to chain to the newly loaded
  //!        authorization plugin
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  XrdAccAuthorize* GetAuthzPlugin(XrdSysError* eDest,
                                  const std::string& lib_path,
                                  const char* confg, XrdOucEnv* myEnv,
                                  XrdAccAuthorize* to_chain);

  //----------------------------------------------------------------------------
  //! Reads the body of the XrdHttpExtReq object and put it in the
  //! body string
  //!
  //! @param req the request from which we will read the body content from
  //! @param body, the string where the body from the request will be put on
  //!
  //! @return a return code if there was an error during the reading. Nothing
  //!         otherwise, hence the optional<int>.
  //----------------------------------------------------------------------------
  std::optional<int> readBody(XrdHttpExtReq& req, std::string& body);

  //----------------------------------------------------------------------------
  //! Returns true if the request is a macaroon token request
  //! false otherwise
  //! @param req the request from which we will read the header and the HTTP verb
  //!
  //! @return true if the request is a macaroon token request, false otherwise
  //----------------------------------------------------------------------------
  bool IsMacaroonRequest(const XrdHttpExtReq& req) const;

  //----------------------------------------------------------------------------
  //! Process macaroon POST request
  //!
  //! @param req XrdHttp request object
  //!
  //! @return 0 if successful, otherwise non-0
  //----------------------------------------------------------------------------
  int ProcessMacaroonPOST(XrdHttpExtReq& req);

  //----------------------------------------------------------------------------
  //! Returns true if the request is a rest api gateway token request
  //! false otherwise
  //! @param req the request from which we will read the header and the HTTP verb
  //!
  //! @return true if the request is a macaroon token request, false otherwise
  //----------------------------------------------------------------------------
  bool IsRestApiRequest(const XrdHttpExtReq& req) const;

  //----------------------------------------------------------------------------
  //! Process REST API gateway POST request
  //!
  //! @param req XrdHttp request object
  //! @param norm_hdrs normalized headers from the HTTP request
  //!
  //! @return 0 if successful, otherwise non-0
  //----------------------------------------------------------------------------
  int ProcessRestApiPost(XrdHttpExtReq& req,
                         const HdrsMapT& norm_hdrs);

  //----------------------------------------------------------------------------
  //! Forward the authentication relevant info as custom headers to the
  //! GRPC-gateway that will then send them further down to the GRPC server
  //!
  //! @note All appened headers need to start with the Grpc-Metadata- prefix
  //! so that they are forwarded by the GRPC gateway otherwise they will just
  //! be discarded!
  //!
  //! @param curl CURL handler where headers are appended
  //! @param client XrdSecEntity object from which information is extracted
  //! @param norm_hdrs normalized headers from the HTTP request
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool RestApiGwFrwAuthHeaders(CURL* curl, const XrdSecEntity& client,
                               const HdrsMapT& norm_hdrs);

  //----------------------------------------------------------------------------
  //! Function used to handle responses from grpc server
  //!
  //! @param contents pointer to the data received from the grpc server
  //! @param size the size of each data element received
  //! @param nmemb the number of data elements received
  //! @param output buffer where the received data from an HTTP response is
  //!               stored
  //!
  //! @return number of bytes received if successful, otherwise non-0
  //----------------------------------------------------------------------------
  static size_t WriteCallback(void* contents, size_t size, size_t nmemb,
                              std::string* output);
};
