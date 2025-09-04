// ----------------------------------------------------------------------
// File: Recycle.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/****************************(********************************************
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

#pragma once
#include "mgm/Namespace.hh"
#include "mgm/recycle/RecyclePolicy.hh"
#include "common/AssistedThread.hh"
#include <XrdOuc/XrdOucString.hh>
#include <sys/types.h>

class XrdOucErrInfo;

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! @brief  This class implements the thread cleaning up a recycling bin and the
//! movement of a file or bulk directory to the recycling bin
//!
//! If the class is called with an empty constructor the Start function starts a
//! which is cleaning up under gRecyclingPrefix according to the attribute
//! "sys.recycle.keep" which defines the time in second how long files are kept
//! in the recycling bin.
//! If the class is called with the complex constructor it is used with the ToGarbage
//! method to move a deleted file or a bulk deletion into the recycling bin.
//! The Recycling bin has the substructure <instance-proc>/recycle/<gid>/<uid>/
//! <constracted-path>.<08x:inode>
//! The constrcated path is the full path of the file where all '/' are replaced
//! with a '#:#'
//------------------------------------------------------------------------------
class Recycle
{
public:
  typedef std::vector<std::map<std::string, std::string>> RecycleListing;

  //----------------------------------------------------------------------------
  //! Default constructor
  //----------------------------------------------------------------------------
  Recycle() :
    mPath(""), mRecycleDir(""), mRecyclePath(""),
    mOwnerUid(DAEMONUID), mOwnerGid(DAEMONGID), mId(0), mWakeUp(false),
    mSnooze(gRecyclingPollTime)
  {}

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
    mPath(path), mRecycleDir(recycledir), mRecyclePath(""),
    mOwnerUid(ownerUid), mOwnerGid(ownerGid), mId(id), mWakeUp(false),
    mSnooze(gRecyclingPollTime)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~Recycle()
  {
    Stop();
  }

  //----------------------------------------------------------------------------
  //! Start the recycle thread cleaning up the recycle bin
  //----------------------------------------------------------------------------
  bool Start()
  {
    mThread.reset(&Recycle::Recycler, this);
    return true;
  }

  //----------------------------------------------------------------------------
  //! Stop the recycle thread
  //----------------------------------------------------------------------------
  void Stop()
  {
    mThread.join();
  }

  //----------------------------------------------------------------------------
  //! Recycle method doing the clean-up. One should define the
  //! 'sys.recycle.keeptime' on the recycle directory which is the
  //! time in seconds of how long files stay in the recycle bin.
  //!
  //! @param assistant thread information
  //----------------------------------------------------------------------------
  void Recycler(ThreadAssistant& assistant) noexcept;

  //----------------------------------------------------------------------------
  //! Configure the recycle bin
  //!
  //! @param std_out where to print
  //! @param std_err where to print
  //! @param vid of the client
  //! @param arg configuration key/op
  //! @param option configuration value
  //!
  //! @return 0 if successful, otherwise errno
  //----------------------------------------------------------------------------
  static int Config(std::string& std_out, std::string& std_err,
                    eos::common::VirtualIdentity& vid,
                    const std::string& key, const std::string& value);

  //----------------------------------------------------------------------------
  //! Recycle the given object (file or subtree)
  //!
  //! @param epname error tag
  //! @param error object
  //! @param fusexcast indicate if this requires an external fusex cast call
  //!
  //! @return SFS_OK if ok, otherwise SFS_ERR + errno + error object set
  //----------------------------------------------------------------------------
  int ToGarbage(const char* epname, XrdOucErrInfo& error, bool fusexcast = true);

  //----------------------------------------------------------------------------
  //! Set the wake-up flag in the recycle thread to look at modified recycle
  //! bin settings.
  //----------------------------------------------------------------------------
  inline void WakeUp()
  {
    mWakeUp = true;
  }

  //----------------------------------------------------------------------------
  //! Print the recycle bin contents
  //!
  //! @param std_out where to print
  //! @param std_err where to print
  //! @param vid of the client
  //! @param monitoring selects monitoring key-value output format
  //! @param translateids selects to display uid/gid as number or string
  //! @param global show files of all users as root
  //! @param date filter recycle bin for given date <year> or <year>/<month> or <year>/<month>/<day>
  //! @param rvec a vector of maps with all recycle informations requested
  //! @param whodeleted - show who exectued a deletion
  //! @param maxentries - maximum number of entries to report
  //!
  //! @return 0 if success, E2BIG if return list is limited
  //----------------------------------------------------------------------------
  static int Print(std::string& std_out, std::string& std_err,
                   eos::common::VirtualIdentity& vid, bool monitoring,
                   bool transalteids, bool details,
                   std::string date = "",
                   bool global = false,
                   RecycleListing* rvec = 0,
                   bool whodeleted = true,
                   int32_t maxentries = 0);

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

  //----------------------------------------------------------------------------
  //! Check if given path is inside the recycle bin
  //!
  //! @param path searched path
  //!
  //! @return true if path inside the recycle bin, otherwise false
  //----------------------------------------------------------------------------
  static bool InRecycleBin(const std::string& path)
  {
    return (path.substr(0, Recycle::gRecyclingPrefix.length()) ==
            Recycle::gRecyclingPrefix);
  }

  //----------------------------------------------------------------------------
  //! Check if given path matches the top recycle bin directory
  //!
  //! @param path searched path
  //!
  //! @return true if path inside the recycle bin, otherwise false
  //----------------------------------------------------------------------------
  static bool IsTopRecycleBin(std::string path)
  {
    if (*(path.rbegin()) != '/') {
      path += '/';
    }

    return (path == Recycle::gRecyclingPrefix);
  }

  //! Prefix for all recycle bins
  static std::string gRecyclingPrefix;
  //! Attribute key defining a recycling location
  static std::string gRecyclingAttribute;
  //! Attribute key defining the max. time a file stays in the garbage directory
  static std::string gRecyclingTimeAttribute;
  //! Ratio from 0 ..1.0 defining a threshold when the recycle bin is not yet
  //! cleaned even if files have expired their lifetime attributel.
  static std::string gRecyclingKeepRatio;
  //! Postfix which identifies a name in the garbage bin as a bulk deletion of a directory
  static std::string gRecyclingPostFix;
  //! Attribute key storing the recycling key of the version directory
  //! belonging to a given file
  static std::string gRecyclingVersionKey;
  static int gRecyclingPollTime; ///< Poll interval inside the garbage bin
  static eos::common::VirtualIdentity mRootVid;

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
  time_t mSnooze;
  RecyclePolicy mPolicy;
  std::multimap<time_t, std::string> mPendingDeletions;

  //----------------------------------------------------------------------------
  //! Handle symlink or symlink like file names. Three scenarios:
  //! - file does not contain the ' -> ' string so it's returned as it is
  //! - file is not a symlink but contains the ' -> ' string in its name then
  //!   we return it as it is
  //! - file is a symlink so the name contains the ' -> ' string and we need
  //!   to remove everything after this occurence so that we can properly
  //!   target the symlink file and not the target of the symlink
  //!
  //! @param ppath full parent directory path
  //! @param fn file name
  //!
  //! @return output file name that we can act on
  //----------------------------------------------------------------------------
  static std::string
  HandlePotentialSymlink(const std::string& ppath, const std::string& fn);

  //----------------------------------------------------------------------------
  //! Compute recycle path directory for given user and timestamp
  //!
  //! epname error printing name
  //! error error object
  //! recyclepath computed by this function
  //!
  //! SFS_OK if ok, otherwise SFS_ERR + errno + error object set
  //----------------------------------------------------------------------------
  int GetRecyclePrefix(const char* epname, XrdOucErrInfo& error,
                       std::string& recyclepath);

  //----------------------------------------------------------------------------
  //! Collect entries to recycle based on current policy
  //!
  //! @param assistant thread assistant
  //----------------------------------------------------------------------------
  void CollectEntries(ThreadAssistant& assistant);

  //----------------------------------------------------------------------------
  //! Remove the pending deletions
  //----------------------------------------------------------------------------
  void RemoveEntries();

  //----------------------------------------------------------------------------
  //! Remove given file entry
  //!
  //! @param fullpath full path to file
  //!
  //! return 0 (SFS_OK) if successful, otherwise SFS_ERROR
  //----------------------------------------------------------------------------
  int RemoveFile(std::string_view fullpath);

  //----------------------------------------------------------------------------
  //! Remove all the entries in the given subtree
  //!
  //! @param fullpath full path to directory
  //----------------------------------------------------------------------------
  void RemoveSubtree(std::string_view fullpath);

  //----------------------------------------------------------------------------
  //! Check if keep ratio enabled and the current quota accounting is below
  //! the specified threshold.
  //!
  //! @return true if keep ratio enabled and b
  //----------------------------------------------------------------------------
  bool IsWithinLimits();

  //----------------------------------------------------------------------------
  //! Update snooz time for the recycler thread. This should match the time
  //! between now and the expiration date of the oldest entry
  //!
  //! @param oldest_ts timestamp of the oldest entry in the recycle bin
  //----------------------------------------------------------------------------
  void UpdateSnooze(time_t oldest_ts = 0);

};

EOSMGMNAMESPACE_END
