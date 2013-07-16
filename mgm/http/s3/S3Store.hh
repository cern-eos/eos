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

#ifndef __EOSMGM_S3STORE__HH__
#define __EOSMGM_S3STORE__HH__

/*----------------------------------------------------------------------------*/
#include "common/http/HttpResponse.hh"
#include "common/RWMutex.hh"
#include "mgm/Namespace.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <map>
#include <set>
#include <string>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

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
  void
  Refresh ();

  /**
   * @return map pointing from user name to user secret key
   */
  inline std::map<std::string, std::string>&
  GetKeys () { return mS3Keys; };

  /**
   * Get a list of all buckets for a given S3 request
   *
   * @param id  the S3 id of the client
   *
   * @return S3 HTTP response object
   */
  eos::common::HttpResponse*
  ListBuckets (const std::string &id);

  /**
   * Get a bucket listing for a given S3 bucket
   *
   * @param bucket  the name of the bucket to list
   * @param query   the client request query string
   *
   * @return S3 HTTP response object
   */
  eos::common::HttpResponse*
  ListBucket (const std::string &bucket, const std::string &query);

  /**
   * Head a bucket (acts like stat on a bucket)
   *
   * @param id      the S3 id of the client
   * @param bucket  the name of the bucket to head
   * @param date    the request x-amz-date header
   *
   * @return S3 HTTP response object
   */
  eos::common::HttpResponse*
  HeadBucket (const std::string &id,
              const std::string &bucket,
              const std::string &date);

  /**
   * Get metadata for an object
   *
   * @param id      the S3 id of the client
   * @param bucket  the name of the bucket to head
   * @param path    the request path
   * @param date    the request x-amz-date header
   *
   * @return S3 HTTP response object
   */
  eos::common::HttpResponse*
  HeadObject (const std::string &id,
              const std::string &bucket,
              const std::string &path,
              const std::string &date);

  /**
   * Get an object (redirection)
   *
   * @param request  the client request object
   * @param id       the S3 id of the client
   * @param bucket   the name of the bucket to head
   * @param path     the request path
   * @param date     the request x-amz-date header
   *
   * @return S3 HTTP response object
   */
  eos::common::HttpResponse*
  GetObject (eos::common::HttpRequest *request,
             const std::string        &id,
             const std::string        &bucket,
             const std::string        &path,
             const std::string        &query);

  /**
   * Create a new object (redirection)
   *
   * @param request  the client request object
   * @param id       the S3 id of the client
   * @param bucket   the name of the bucket to head
   * @param path     the request path
   * @param date     the request x-amz-date header
   *
   * @return S3 HTTP response object
   */
  eos::common::HttpResponse*
  PutObject (eos::common::HttpRequest *request,
             const std::string        &id,
             const std::string        &bucket,
             const std::string        &path,
             const std::string        &query);

  /**
   * Delete an object from a bucket
   *
   * @param id      the S3 id of the client
   * @param bucket  the name of the bucket to head
   * @param path    the request path
   *
   * @return S3 HTTP response object
   */
  eos::common::HttpResponse*
  DeleteObject (const std::string &id,
                const std::string &bucket,
                const std::string &path);

};

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END

#endif

