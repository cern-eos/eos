// ----------------------------------------------------------------------
// File: Recycle.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/****************************A********************************************
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

#include "mgm/Namespace.hh"
#include "common/AssistedThread.hh"
#include <XrdOuc/XrdOucString.hh>
#include <sys/types.h>

class XrdOucErrInfo;

EOSMGMNAMESPACE_BEGIN

/**
 * @file   Recycle.hh
 *
 * @brief  This class implements the thread cleaning up a recycling bin and the
 * movement of a file or bulk directory to the recycling bin
 *
 * If the class is called with an empty constructor the Start function starts a
 * which is cleaning up under gRecyclingPrefix according to the attribute
 * "sys.recycle.keep" which defines the time in second how long files are kept
 * in the recycling bin.
 * If the class is called with the complex constructor it is used with the ToGarbage
 * method to move a deleted file or a bulk deletion into the recycling bin.
 * The Recycling bin has the substructure <instance-proc>/recycle/<gid>/<uid>/
 * <constracted-path>.<08x:inode>
 * The constrcated path is the full path of the file where all '/' are replaced
 * with a '#:#'
 */

class Recycle
{
public:
  //----------------------------------------------------------------------------
  //! Default Constructor - use it to run the Recycle thread by callign Start
  //! afterwards.
  //----------------------------------------------------------------------------

  Recycle() :
    mPath(""), mRecycleDir(""), mRecyclePath(""),
    mOwnerUid(99), mOwnerGid(99), mId(0), mWakeUp(false) { }

  //----------------------------------------------------------------------------
  //! Constructor
  //! @param path path to recycle
  //! @param recycle bin directory
  //! @param uid user id
  //! @param gid group id
  //! @param id of the container or file
  //----------------------------------------------------------------------------

  Recycle(const char* path, const char* recycledir,
          eos::common::VirtualIdentity* vid, uid_t ownerUid,
          gid_t ownerGid, unsigned long long id) :
    mPath(path), mRecycleDir(recycledir),
    mRecyclePath(""), mOwnerUid(ownerUid), mOwnerGid(ownerGid), mId(id),
    mWakeUp(false) { }

  ~Recycle()
  {
    Stop();
  }


  /* Start the recycle thread cleaning up the recycle bin
   */
  bool Start();

  /* Stop the recycle thread
   */
  void Stop();

  /* Recycle method doing the actual clean-up
   */
  void Recycler(ThreadAssistant& assistant) noexcept;

  /**
   * do the recycling of the recycle object (file or subtree)
   * @param epname error tag
   * @param error object
   * @param fusexcast indicate if this requires an external fusex cast call
   * @return SFS_OK if ok, otherwise SFS_ERR + errno + error object set
   */
  int ToGarbage(const char* epname, XrdOucErrInfo& error, bool fusexcast = true);


  /**
   * @parame epname error printing name
   * @param error error object
   * @param recyclepath computed by this function
   * @param index if -1, a new index directory will be created, otherwise the given one will be returned
   * @return SFS_OK if ok, otherwise SFS_ERR + errno + error object set
   */
  int GetRecyclePrefix(const char* epname, XrdOucErrInfo& error,
                       std::string& recyclepath, int index = -1);

  /**
   * return the path from where the action can be recycled ( this is filled after ToGarbage has been called
   * @return string with the path name in the recycle bin
   */

  std::string
  getRecyclePath()
  {
    return mRecyclePath;
  }

  typedef std::vector<std::map<std::string, std::string>> RecycleListing ;

  /**
   * print the recycle bin contents
   * @param std_out where to print
   * @param std_err where to print
   * @param vid of the client
   * @param monitoring selects monitoring key-value output format
   * @param translateids selects to display uid/gid as number or string
   * @param global show files of all users as root
   * @param date filter recycle bin for given date <year> or <year>/<month> or <year>/<month>/<day>
   * @param rvec a vector of maps with all recycle informations requested
   * @param whodeleted - show who exectued a deletion
   * @param maxentries - maximum number of entries to report
   * @return 0 if success, E2BIG if return list is limited
   */
  static int Print(std::string& std_out, std::string& std_err,
                    eos::common::VirtualIdentity& vid, bool monitoring,
                    bool transalteids, bool details,
                    std::string date = "",
                    bool global = false,
                    RecycleListing* rvec = 0,
		    bool whodeleted = true,
		    int32_t maxentries = 0
                   );

  /**
   * print the recycle bin contents
   * @param std_out where to print
   * @param std_err where to print
   * @param vid of the client
   * @param monitoring selects monitoring key-value output format
   * @param translateids selects to display uid/gid as number or string
   */
  static void PrintOld(std::string& std_out, std::string& std_err,
                       eos::common::VirtualIdentity& vid, bool monitoring,
                       bool transalteids, bool details);

  /**
   * undo a deletion
   * @param std_out where to print
   * @param std_err where to print
   * @param vid of the client
   * @param key (==inode) to undelete
   * @param force_orig_name flag to force restore to the original name
   * @param restore_versions flag to restore all versions
   * @param make_path flag to recreate all missing parent directories
   * @return 0 if done, otherwise errno
   */
  static int Restore(std::string& std_out, std::string& std_err,
                     eos::common::VirtualIdentity& vid, const char* key,
                     bool force_orig_name, bool restore_versions, bool make_path = false);

  static int PurgeOld(std::string& std_out, std::string& std_err,
                      eos::common::VirtualIdentity& vid);

  /**
   * purge all files in the recycle bin with new uid:<uid>/<date> structure
   * @param std_out where to print
   * @param std_err where to print
   * @param vid of the client
   * @PARAM date can be empty, <year> or <year>/<month> or <year>/<month>/<day>
   * @return 0 if done, otherwise errno
   */
  static int Purge(std::string& std_out, std::string& std_err,
                   eos::common::VirtualIdentity& vid,
                   std::string date = "",
                   bool global = false,
                   std::string pattern = ""
                  );

  /**
   * configure the recycle bin
   * @param std_out where to print
   * @param std_err where to print
   * @param vid of the client
   * @param arg configuration key/op
   * @param option configuration value
   * @return 0 if done, otherwise errno
   */
  static int Config(std::string& std_out, std::string& std_err,
                    eos::common::VirtualIdentity& vid,
                    const std::string& key, const std::string& value);

  /**
   * set the wake-up flag in the recycle thread to look at modified recycle bin settings
   */
  void WakeUp()
  {
    mWakeUp = true;
  }

  static bool InRecycleBin(std::string& path)
  {
    return (path.substr(0, Recycle::gRecyclingPrefix.length()) ==
            Recycle::gRecyclingPrefix);
  }

  static std::string gRecyclingPrefix; //< prefix for all recycle bins
  static std::string
  gRecyclingAttribute; //< attribute key defining a recycling location
  static std::string
  gRecyclingTimeAttribute; //< attribute key defining the max. time a file stays in the garbage directory
  static std::string
  gRecyclingKeepRatio; //<  ratio from 0 ..1.0 defining a threshold when the recycle bin is not yet cleaned even if files have expired their lifetime attribute
  static std::string
  gRecyclingPostFix; //<  postfix which identifies a name in the garbage bin as a bulk deletion of a directory
  static std::string
  gRecyclingVersionKey; //<  attribute key storing the recycling key of the version directory belonging to a given file
  static int gRecyclingPollTime; //< poll interval inside the garbage bin

private:
#ifdef IN_TEST_HARNESS
public:
#endif

  AssistedThread mThread; ///< Thread doing the recycling
  std::string mPath;
  std::string mRecycleDir;
  std::string mRecyclePath;
  uid_t mOwnerUid;
  gid_t mOwnerGid;
  unsigned long long mId;
  std::atomic<bool> mWakeUp;

  //----------------------------------------------------------------------------
  //! Handle symlink or symlink like file names. Three scenarios:
  //! - file does not contain the ' -> ' string so it's returned as it is
  //! - file is not a symlink but contains the ' -> ' string in its name then
  //!   we return it as it is
  //! - file is a symlink so the name contains the ' -> ' string and we need
  //!   to remove everything after this occurent so that we can properly
  //!   target the symlink file and not the target of the symlink
  //!
  //! @param ppath full parent directory path
  //! @param fn file name
  //!
  //! @return output file name that we can act on
  //----------------------------------------------------------------------------
  static std::string
  HandlePotentialSymlink(const std::string& ppath, const std::string& fn);
};

EOSMGMNAMESPACE_END

#endif
