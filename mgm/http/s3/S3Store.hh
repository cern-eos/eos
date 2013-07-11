// ----------------------------------------------------------------------
// File: S3Store.hh
// Author: Andreas-Joachim Peters & Justin Lewis Salmon - CERN
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

/**
 * @file  S3Store.hh
 *
 * @brief creates the S3 store object knowing ids, keys and containers
 *        and their mapping to the real namespace
 */

#ifndef __EOSMGM_S3Store__HH__
#define __EOSMGM_S3Store__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/http/s3/S3Handler.hh"
#include "mgm/Namespace.hh"
#include "common/RWMutex.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <map>
#include <set>
#include <string>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class S3Handler;

class S3Store
{

private:
  eos::common::RWMutex                         mStoreMutex;            //< mutex protecting all mS3xx variables
  time_t                                       mStoreModificationTime; //< last modification time of the loaded store
  time_t                                       mStoreReloadTime;       //< last time the store was refreshed
  std::map<std::string, std::set<std::string>> mS3ContainerSet;        //< map pointing from user name to list of container
  std::map<std::string, std::string>           mS3Keys;                //< map pointing from user name to user secret key
  std::map<std::string, std::string>           mS3ContainerPath;       //< map pointing from container name to path
  std::string                                  mS3DefContainer;        //< path where all s3 objects are defined

public:

  /**
   * Constructor
   *
   * @param s3defpath  path where all s3 objects will be defined
   */
  S3Store (const char* s3defpath);

  /**
   * Destructor
   */
  virtual ~S3Store () {};

  /**
   * Refresh function to reload keys from the namespace definition
   */
  void Refresh ();


  /**
   * Verify S3 request
   *
   * @param S3 object containing S3 id & key
   *
   * @return true if signature ok, false otherwise
   */
  bool VerifySignature (S3Handler &s3);

  /**
   * Get a list of all buckets for a given S3 requester
   *
   * @param response_code  return of HTTP response code (only modified in case
   *                       of errors)
   * @param s3             object containing s3 id & key
   * @param header         http response headers
   *
   * @return XML bucket list
   */
  std::string ListBuckets (int                                &response_code,
                           S3Handler                          &s3,
                           std::map<std::string, std::string> &header);

  /**
   * Get a bucket listing for a given S3 bucket
   *
   * @param response_code  return of HTTP response code (only modified in case
   *                       of errors)
   * @param s3             object containing s3 id & key
   * @param header         http response headers
   *
   * @return XML bucket list
   */
  std::string ListBucket (int                                &response_code,
                          S3Handler                          &s3,
                          std::map<std::string, std::string> &header);

  /**
   * Head a bucket (acts like stat on a bucket)
   *
   * @param response_code  return of HTTP response code (only modified in case
   *                       of errors)
   * @param s3             object containing s3 id & key
   * @param header         http response headers
   *
   * @return XML bucket list
   */
  std::string HeadBucket (int                                &response_code,
                          S3Handler                          &s3,
                          std::map<std::string, std::string> &header);

  /**
   * Get metadata for an object
   *
   * @param response_code  return of HTTP response code (only modified in case
   *                       of errors)
   * @param s3             object containing s3 id & key
   * @param header         http response headers
   *
   * @return XML bucket list
   */
  std::string HeadObject (int                                &response_code,
                          S3Handler                          &s3,
                          std::map<std::string, std::string> &header);

  /**
   * Get an object (redirection)
   *
   * @param response_code  return of HTTP response code (only modified in case
   *                       of errors)
   * @param s3             object containing s3 id & key
   * @param header         http response headers
   *
   * @return XML bucket list
   */
  std::string GetObject (int                                &response_code,
                         S3Handler                          &s3,
                         std::map<std::string, std::string> &header);

  /**
   * Create a new object (redirection)
   *
   * @param response_code  return of HTTP response code (only modified in case
   *                       of errors)
   * @param s3             object containing s3 id & key
   * @param header         http response headers
   *
   * @return XML bucket list
   */
  std::string PutObject (int                                &response_code,
                         S3Handler                          &s3,
                         std::map<std::string, std::string> &header);

};

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END

#endif

