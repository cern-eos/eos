// ----------------------------------------------------------------------
// File: S3Store.hh
// Author: Andreas-Joachim Peters - CERN
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

#ifndef __EOSMGM_S3Store__HH__
#define __EOSMGM_S3Store__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "common/RWMutex.hh"
#include "common/S3.hh"
/*----------------------------------------------------------------------------*/
#include <map>
#include <set>
#include <string>
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class S3Store
{
  // -------------------------------------------------------------
  // ! creates the S3 store object knowing ids, keys and container
  // ! and their mapping to the real namespace
  // -------------------------------------------------------------

public:

  class BucketCache
  {
  public:

    /**
     * Class implementing a bucket listing for caching
     */
    class BucketListing
    {
    public:
      std::map < std::string, std::set < std::string >> mFind;
      time_t mExpires;
      std::string mBucketName;

      BucketListing ()
      {
        mExpires = 0;
      }

      BucketListing (std::string& lName, std::map < std::string, std::set < std::string >> &lFind, time_t lLifetime)
      {
        mBucketName = lName;
        mExpires = time(NULL) + lLifetime;
        mFind = lFind;
      }

      virtual
      ~BucketListing () { }
    };

    std::map<std::string, BucketListing* > mBucketCache; //< cache storing long bucket listings

    BucketCache () { }

    virtual
    ~BucketCache ()
    {
      eos::common::RWMutexWriteLock mLock(mMutex);
      for (auto it = mBucketCache.begin(); it != mBucketCache.end(); it++)
      {
        if (it->second)
        {
          delete it->second;
          it->second = 0;
        }
      }
    }

    void
    Expire ()
    {
      time_t now = time(NULL);
      eos::common::RWMutexWriteLock mLock(mMutex);
      for (auto it = mBucketCache.begin(); it != mBucketCache.end(); it++)
      {
        if ((it->second) && (it->second->mExpires < now))
        {
          delete it->second;
          it->second = 0;
        }
      }
    }

    BucketListing*
    getListing (std::string lContainerName)
    {
      if (mBucketCache.count(lContainerName))
      {
        return mBucketCache[lContainerName];
      }
      else
      {
        return 0;
      }
    }

    void
    uncacheListing (std::string lContainerName)
    {
      eos::common::RWMutexWriteLock mLock(mMutex);
      delete (mBucketCache[lContainerName]);
      mBucketCache[lContainerName] = 0;
    }

    void
    addListing (std::string lContainerName, std::map < std::string, std::set < std::string >> &lFind, time_t lLifetime)
    {
      eos::common::RWMutexWriteLock mLock(mMutex);
      delete (mBucketCache[lContainerName]);
      mBucketCache[lContainerName] = new BucketListing(lContainerName, lFind, lLifetime);
    }

    void
    LockRead ()
    {
      mMutex.LockRead();
    }

    void
    UnLockRead ()
    {
      mMutex.UnLockRead();
    }

  private:
    eos::common::RWMutex mMutex; //< mutex protecting the bucket cache
  };

  /**
   * Constructor
   */
  S3Store (const char* s3defpath);

  /**
   * Destructor
   */
  virtual ~S3Store ();

  /**
   * Refresh function to reload keys from the namespace definition
   */
  void Refresh ();


  /**
   * verify s3 request
   * @param s3 object containing s3 id & key
   * @return true if signature ok
   */
  bool VerifySignature (eos::common::S3 &s3);

  /**
   * return bucket list for a given S3 requestor
   * @param return of HTTP response code (only modified in case of errors)
   * @param s3 object containing s3 id & key
   * @param http response header
   * @return XML bucket list
   */
  std::string ListBuckets (int& response_code, eos::common::S3 &s3, std::map<std::string, std::string> &header);

  /**
   * return bucket listing for a given S3 requestor
   * @param return of HTTP response code (only modified in case of errors)
   * @param s3 object containing s3 id & key
   * @param http response header
   * @return XML bucket listing
   */
  std::string ListBucket (int& response_code, eos::common::S3 &s3, std::map<std::string, std::string> &header);

  /**
   * return act's like stat on a bucket
   * @param return of HTTP response code (only modified in case of errors)
   * @param s3 object containing s3 id & key
   * @param http response header
   * @return http response body
   */
  std::string HeadBucket (int& response_code, eos::common::S3 &s3, std::map<std::string, std::string> &header);

  /**
   * return meta data for an object
   * @param return of HTTP response code (only modified in case of errors)
   * @param s3 object containing s3 id & key
   * @param http response header
   * @return http response body
   */
  std::string HeadObject (int& response_code, eos::common::S3 &s3, std::map<std::string, std::string> &header);

  /**
   * return object e.g. redirection
   * @param return of HTTP response code (only modified in case of errors)
   * @param s3 object containing s3 id & key
   * @param http response header
   * @return http response body
   */
  std::string GetObject (int& response_code, eos::common::S3 &s3, std::map<std::string, std::string> &header);

  /**
   * create new object e.g. redirection
   * @param return of HTTP response code (only modified in case of errors)
   * @param s3 object containing s3 id & key
   * @param http response header
   * @return http response body
   */
  std::string PutObject (int& response_code, eos::common::S3 &s3, std::map<std::string, std::string> &header);

private:
  eos::common::RWMutex mStoreMutex; //< mutex protecting all mS3xx variables
  time_t mStoreModificationTime; //< last modification time of the loaded store
  time_t mStoreReloadTime; //< last time the store was refreshed
  std::map<std::string, std::set<std::string >> mS3ContainerSet; //< map pointing from user name to list of container
  std::map<std::string, std::string> mS3Keys; //< map pointing from user name to user secret key
  std::map<std::string, std::string> mS3ContainerPath; //< map pointing from container name to path
  std::string mS3DefContainer; //< path where all s3 objects are defined

  BucketCache mBucketCache; //< cache for bucket listings
};

EOSMGMNAMESPACE_END

#endif

