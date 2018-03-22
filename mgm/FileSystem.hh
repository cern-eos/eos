// ----------------------------------------------------------------------
// File: FileSystem.hh
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

#ifndef __EOSMGM_FILESYSTEM_HH__
#define __EOSMGM_FILESYSTEM_HH__

#include "common/FileSystem.hh"
#include "mgm/DrainJob.hh"

/*----------------------------------------------------------------------------*/
/**
 * @file FileSystem.hh
 *
 * @brief Class implementing egroup support via LDAP queries
 *
 */
/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_BEGIN

class DrainJob;

/*----------------------------------------------------------------------------*/
/**
 *
 * @brief Class representing a filesystem on the MGM
 */
/*----------------------------------------------------------------------------*/
class FileSystem : public eos::common::FileSystem
{
public:
  //----------------------------------------------------------------------------
  //! @brief Constructor
  //!
  //! @param queuepath describing a filesystem like /eos/<host:port>/fst/data01/
  //! @param queue associated to a filesystem like /eos/<host:port>/fst
  //! @param som external shared object manager object
  //----------------------------------------------------------------------------
  FileSystem(const char* queuepath, const char* queue,
             XrdMqSharedObjectManager* som) :
    eos::common::FileSystem(queuepath, queue, som), mDrainJob(0) {}

  //----------------------------------------------------------------------------
  //! Destructor - needs to kill any on-going drain jobs
  //----------------------------------------------------------------------------
  virtual ~FileSystem()
  {
    mDrainJobMutex.Lock();

    if (mDrainJob) {
      delete mDrainJob;
      mDrainJob = 0;
    }

    mDrainJobMutex.UnLock();
  }

  //----------------------------------------------------------------------------
  //! @brief Return the current broadcasting setting
  //!
  //! @return true if broadcasting otherwise false
  //----------------------------------------------------------------------------
  bool ShouldBroadCast()
  {
    if (mSom) {
      return mSom->ShouldBroadCast();
    } else {
      return false;
    }
  }

  //----------------------------------------------------------------------------
  //! Set the configuration status of a file system. This can be used to trigger
  //! the draining.
  //! @note Must be called wiht a lock on FsView::ViewMutex
  //!
  //! @param status file system status
  //! @param bool centraldrain - to be dropped
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetConfigStatus(eos::common::FileSystem::fsstatus_t status,
                       bool centraldrain = false);

  //----------------------------------------------------------------------------
  //! Set a 'key' describing the filesystem
  //! @note Must be called wiht a lock on FsView::ViewMutex
  //!
  //! @param key key to set
  //! @param str value of the key
  //! @param broadcast if true broadcast the change around
  //!
  //! @return true if successfull otherwise false
  //----------------------------------------------------------------------------
  bool SetString(const char* key, const char* str, bool broadcast = true);

  //----------------------------------------------------------------------------
  // starts a drain job with the opserror flag -
  // this is triggered by stat.errc!= 0 via the FsListener Thread
  //----------------------------------------------------------------------------
  bool StartDrainJob();

  //----------------------------------------------------------------------------
  // stops  a drain job with the opserror flag -
  // this is triggered by stat.errc = 0 via the FsListener Thread
  //----------------------------------------------------------------------------
  bool StopDrainJob();

private:
  /// Mutex protecting the DrainJob object in a filesystem
  XrdSysMutex mDrainJobMutex;

  /// Drainjob object associated with a filesystem
  DrainJob* mDrainJob;
};

EOSMGMNAMESPACE_END

#endif
