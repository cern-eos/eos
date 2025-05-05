// ----------------------------------------------------------------------
// File: TapeRestApiConfig.hh
// Author: Cedric Caffy - CERN
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
#ifndef EOS_TAPERESTAPICONFIG_HH
#define EOS_TAPERESTAPICONFIG_HH

#include "mgm/Namespace.hh"
#include <string>
#include "common/RWMutex.hh"
#include <atomic>

EOSMGMRESTNAMESPACE_BEGIN

/**
 * This class holds all the configuration related
 * to the Tape REST API
 */
class TapeRestApiConfig
{
public:
  /**
   * Default constructor: the accessURL is "api"
   */
  TapeRestApiConfig();
  /**
   * Constructor with the accessURL
   * @param accessURL the accessURL that will allow
   * the user to access the REST API
   */
  TapeRestApiConfig(const std::string& accessURL);
  /**
   * Sets the siteName that will be used for
   * targetedMetadata
   */
  void setSiteName(const std::string& siteName);
  /**
   * Returns true if the tape REST API has been
   * activated, false otherwise
   */
  bool isActivated() const;
  /**
   * Enables/disables the tape REST API
   * @param activated is set to true if the tape REST API
   * should be activated, false otherwise
   */
  void setActivated(const bool activated);

  /**
   * Sets the tape enabled flag
   * @param tapeEnabled this value should come from the MGM configuration
   */
  void setTapeEnabled(const bool tapeEnabled);

  /**
   * Sets the DNS alias of the server where the REST API
   * is running
   * @param mgmOfsAlias the DNS alias of the server where the REST API is running
   */
  void setHostAlias(const std::string& mgmOfsAlias);

  /**
   * @return Gets the DNS alias of the server where the REST API
   * is running
   */
  std::string getHostAlias() const;

  /**
   * Sets (thus overriding the default values) the mapping between the REST API versions and URIs in the .well-known
   * is running
   * @param tapeRestApiEndpointUriMap the mapping between the REST API versions and URIs
   */
  void setEndpointToUrlMapping(const std::map<std::string, std::string>& tapeRestApiEndpointUriMap);

  /**
   * @return Gets the mapping between the REST API versions and URIs
   */
  std::map<std::string, std::string> getEndpointToUriMapping() const;

  /**
   * Sets the port of the XrdHttp server where the tape REST API is running
   * @param xrdHttpPort
   */
  void setXrdHttpPort(const uint16_t xrdHttpPort);

  /**
   * @return the port of the XrdHttp server where the tape REST API is running
   */
  uint16_t getXrdHttpPort() const;

  /**
   * Returns the value of the tape enabled flag
   */
  bool isTapeEnabled() const;

  std::string getSiteName() const;
  const std::string& getAccessURL() const;

  void setStageEnabled(const bool isStageEnabled);
  bool isStageEnabled() const;

private:
  /**
   * This parameter represents the STAGE targeted
   * metadata identifier that allows the user to pass any
   * extra information for this API endpoint
   */
  std::string mSiteName;
  //Access URL of the tape REST API (without https://fqdn)
  std::string mAccessURL;
  //The mgmofs.alias value coming from the MGM configuration file
  std::string mHostAlias;
  // The mapping between the REST API versions and URIs
  std::map<std::string, std::string> mTapeRestApiEndpointUrlMap;
  //By default, the tape REST API is not activated
  std::atomic<bool> mIsActivated = false;
  //The tape enabled flag of the EOS instance where the tape REST API is running
  std::atomic<bool> mTapeEnabled = false;
  //The port of the XrdHttp server where the tape REST API is running
  std::atomic<uint16_t> mXrdHttpPort;
  //Mutex protecting all variables of this configuration
  mutable common::RWMutex mConfigMutex;
  //This flag allows to activate or deactivate the staging of the tape REST API
  std::atomic<bool> mStageEnabled = false;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_TAPERESTAPICONFIG_HH
