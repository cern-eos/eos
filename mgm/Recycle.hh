// ----------------------------------------------------------------------
// File: Recycle.hh
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

#ifndef __EOSMGM_RECYCLE__HH__
#define __EOSMGM_RECYCLE__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucErrInfo.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/**
 * @file   Recycle.hh
 *
 * @brief  This class implements the thread cleaning up a recycling bin and the movement of a file or bulk directory to the recycling bin
 * 
 * If the class is called with an empty constructor the Start function starts a
 * which is cleaning up under gRecyclingPrefix according to the attribute 
 * "sys.recycle.keep" which defines the time in second how long files are kept
 * in the recycling bin.
 * If the class is called with the complex constructor it is used with the ToGarbage
 * method to move a deleted file or a bulk deletion into the recycling bin.
 * The Recycling bin has the substructure /recycle/<gid>/<uid>/<constracted-path>.<08x:inode>
 * The constrcated path is the full path of the file where all '/' are replaced
 * with a '#:#'
 */

class Recycle
{
private:
  //............................................................................
  // variables for the recyling thread
  //............................................................................
  pthread_t mThread; //< thread id of the recyling thread

  //............................................................................
  // variables for an recyling action (e.g. deletion movement into bin)
  //............................................................................

  eos::common::Mapping::VirtualIdentity_t *pVid;
  std::string mPath;
  std::string mRecycleDir;
  std::string mRecyclePath;
  uid_t mOwnerUid;
  gid_t mOwnerGid;
  unsigned long long mId;

public:

  /* Default Constructor - use it to run the Recycle thread by callign Start afterwards
   */
  Recycle ()
  {
    mThread = 0;
  }

  /* Start the recycle thread cleaning up the recycle bin
   */
  bool Start ();

  /* Stop the recycle thread
   */
  void Stop ();

  /* Thread start function for recycle thread
   */
  static void* StartRecycleThread (void*);

  /* Recycle method doing the actual clean-up
   */
  void* Recycler ();

  /**
   * Constructor
   * @param path path to recycle
   * @param recycle bin directory
   * @param uid user id 
   * @param gid group id
   * @param id of the container or file
   */

  Recycle (const char* path, const char* recycledir, eos::common::Mapping::VirtualIdentity_t* vid, uid_t ownerUid, gid_t ownerGid, unsigned long long id)
  {
    mPath = path;
    mRecycleDir = recycledir;
    pVid = vid;
    mOwnerUid = ownerUid;
    mOwnerGid = ownerGid;
    mId = id;
    mThread = 0;
  }

  ~Recycle ()
  {
    if (mThread) Stop();
  };

  /**
   * do the recycling of the recycle object (file or subtree)
   * @param epname error tag
   * @param error object
   * @return SFS_OK if ok, otherwise SFS_ERR + errno + error object set
   */

  int ToGarbage (const char* epname, XrdOucErrInfo& error);

  /**
   * return the path from where the action can be recycled ( this is filled after ToGarbage has been called
   * @return string with the path name in the recycle bin
   */

  std::string
  getRecyclePath ()
  {
    return mRecyclePath;
  }

  /**
   * print the recycle bin contents
   * @param stdOut where to print
   * @param stdErr where to print
   * @param vid of the client
   * @param monitoring selects monitoring key-value output format
   * @param translateids selects to display uid/gid as number or string
   */
  static void Print (XrdOucString &stdOut, XrdOucString &stdErr, eos::common::Mapping::VirtualIdentity_t &vid, bool monitoring, bool transalteids, bool details);

  /**
   * undo a deletion
   * @param stdOut where to print
   * @param stdErr where to print
   * @param vid of the client
   * @param key (==inode) to undelete
   * @param option can be --force-original-name or -f
   * @return 0 if done, otherwise errno
   */
  static int Restore (XrdOucString &stdOut, XrdOucString &stdErr, eos::common::Mapping::VirtualIdentity_t &vid, const char* key, XrdOucString &options);

  /**
   * purge all files in the recycle bin
   * @param stdOut where to print
   * @param stdErr where to print
   * @param vid of the client
   * @return 0 if done, otherwise errno
   */
  static int Purge (XrdOucString &stdOut, XrdOucString &stdErr, eos::common::Mapping::VirtualIdentity_t &vid);

  /**
   * configure the recycle bin
   * @param stdOut where to print
   * @param stdErr where to print
   * @param vid of the client
   * @arg configuration value
   * @option configuration type
   * @return 0 if done, otherwise errno
   */
  static int Config (XrdOucString &stdOut, XrdOucString &stdErr, eos::common::Mapping::VirtualIdentity_t &vid, const char* arg, XrdOucString &options);

  static std::string gRecyclingPrefix; //< prefix for all recycle bins
  static std::string gRecyclingAttribute; //< attribute key defining a recycling location
  static std::string gRecyclingTimeAttribute; //< attribute key defining the max. time a file stays in the garbage directory
  static std::string gRecyclingPostFix; //<  postfix which identifies a name in the garbage bin as a bulk deletion of a directory
  static int gRecyclingPollTime; //< poll interval inside the garbage bin
};

EOSMGMNAMESPACE_END

#endif
