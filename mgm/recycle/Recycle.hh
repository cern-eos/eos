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
#include "common/SystemClock.hh"
#include "proto/Recycle.pb.h"
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
  //! Attribute defining how often the collection of entries is attempted
  static std::string gRecyclingCollectInterval;
  //! Attribute defining how often the removeal of collected entries is attempted
  static std::string gRecyclingRemoveInterval;
  //! Attribute key defining whether the recycler runs in dry-run mode or not
  static std::string gRecyclingDryRunAttribute;
  //! Attribute key storing the recycling key of the version directory
  //! belonging to a given file
  static std::string gRecyclingVersionKey;
  //! Recycle id extended attribute value used to store the container id
  //! for which the corresponding recycle directory belong to
  static std::string gRecycleIdXattrKey;
  //! Root virtual identity used internally for unrestricted operations
  static eos::common::VirtualIdentity mRootVid;
  //! Timestamp of the last remove operation done by the recycler
  static std::chrono::seconds mLastRemoveTs;

  //----------------------------------------------------------------------------
  //! Configure the recycle bin
  //!
  //! @param std_out where to print
  //! @param std_err where to print
  //! @param vid of the client
  //! @param op operation type according to Recycle.proto
  //! @param value configuration value for the given operation type
  //!
  //! @return 0 if successful, otherwise errno
  //----------------------------------------------------------------------------
  static int Config(std::string& std_out, std::string& std_err,
                    eos::common::VirtualIdentity& vid,
                    eos::console::RecycleProto_ConfigProto_OpType op,
                    const std::string& value);

  //----------------------------------------------------------------------------
  //! Configure a recycle id for the given path and optionally set the given
  //! ACL on the computed top level recycle directory to control access with
  //! respect to restoring recycled entries
  //!
  //! @param path path to top level directory that is to be labeled with a
  //!             recycle id. The recycle id will match the top container id.
  //! @param acl string representation of ACLs to be appended to the top
  //!             recycle directory
  //! @param std_err output string holding any potential error message
  //!
  //! @return 0 if succsessful, otherwise errno
  //----------------------------------------------------------------------------
  static int RecycleIdSetup(std::string_view path, std::string_view acl,
                            std::string& std_err);

  //----------------------------------------------------------------------------
  //! Print the recycle bin contents
  //!
  //! @param std_out where to print
  //! @param std_err where to print
  //! @param vid of the client
  //! @param monitoring selects monitoring key-value output format
  //! @param translateids selects to display uid/gid as number or string
  //! @param details display detailed info
  //! @param display_type e.g. all, by uid, by recycle id
  //! @param display_value
  //! @param date filter recycle bin for given date <year> or <year>/<month>
  //!        or <year>/<month>/<day>
  //! @param rvec a vector of maps with all recycle informations requested
  //! @param whodeleted - show who exectued a deletion
  //! @param maxentries - maximum number of entries to report
  //!
  //! @return 0 if success, E2BIG if return list is limited
  //----------------------------------------------------------------------------
  static int Print(std::string& std_out, std::string& std_err,
                   eos::common::VirtualIdentity& vid, bool monitoring,
                   bool transalteids, bool details,
                   std::string_view display_type,
                   std::string_view display_val,
                   std::string_view date = "", RecycleListing* rvec = 0,
                   bool whodeleted = true, int32_t maxentries = 0);

  //----------------------------------------------------------------------------
  //! Restore an entry from the recycle bin to the original location
  //!
  //! @param std_out stdout message
  //! @param std_err stderr error message
  //! @param vid client virtual identity
  //! @param key (==inode) identifier to restore (undelete)
  //! @param force_orig_name flag to force restore to the original name
  //! @param restore_versions flag to restore all versions
  //! @param make_path flag to recreate all missing parent directories
  //!
  //! @return 0 if successful, otherwise errno
  //----------------------------------------------------------------------------
  static int Restore(std::string& std_out, std::string& std_err,
                     eos::common::VirtualIdentity& vid, std::string_view key,
                     bool force_orig_name, bool restore_versions,
                     bool make_path = false);

  //----------------------------------------------------------------------------
  //! Get recycle bin path from the given restore key information
  //!
  //! @param key restore key fxid:<val> or pxid:<val>
  //! @param vid client virtual identity
  //! @param std_err error message
  //! @param recycle_path computed recycle path
  //!
  //! @return 0 if successful, otherwise errno
  //----------------------------------------------------------------------------
  static int
  GetPathFromRestoreKey(std::string_view key,
                        const eos::common::VirtualIdentity& vid,
                        std::string& std_err, std::string& recycle_path);

  //----------------------------------------------------------------------------
  //! Demangle path from recycle bin to obtain the original path
  //!
  //! @param recyle_path recycle path using the #.# encoding. This only
  //!        contains the flattened structure without the path location
  //!        inside the recycle bin.
  //!
  //! @return original path or empty string if there was an error
  //----------------------------------------------------------------------------
  static std::string
  DemanglePath(std::string_view recycle_path);

  //----------------------------------------------------------------------------
  //! Purge files in the recycle bin
  //!
  //! @param std_out where to print
  //! @param std_err where to print
  //! @param vid client virtual identity
  //! @param key (==inode) identifier to purge (undelete)
  //! @param date can be empty, <year>, <year>/<month> or <year>/<month>/<day>
  //! @param type glocal, user or request id type of operation
  //! @param recycle_id recycle project identifier for scoping this request
  //!
  //! @return 0 if successful, otherwise errno
  //----------------------------------------------------------------------------
  static int Purge(std::string& std_out, std::string& std_err,
                   eos::common::VirtualIdentity& vid,
                   std::string key, std::string_view date,
                   std::string_view type = "",
                   std::string_view recycle_id = "");

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


  //----------------------------------------------------------------------------
  //! Default constructor
  //----------------------------------------------------------------------------
  Recycle(bool fake_clock = false);

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
  void Start()
  {
    mThread.reset(&Recycle::Recycler, this);
  }

  //----------------------------------------------------------------------------
  //! Stop the recycle thread
  //----------------------------------------------------------------------------
  void Stop()
  {
    // Unblock thread waiting for timeout of config update
    NotifyConfigUpdate();
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
  //! Notify the recycle that the configuration was updated
  //----------------------------------------------------------------------------
  inline void NotifyConfigUpdate()
  {
    {
      // Set the refresh flag and notify
      std::unique_lock<std::mutex> lock(mCvMutex);
      mTriggerRefresh = true;
    }
    mCvCfgUpdate.notify_all();
  }

  //----------------------------------------------------------------------------
  //! Get collect interval value
  //----------------------------------------------------------------------------
  inline uint64_t GetCollectInterval() const
  {
    return mPolicy.mCollectInterval.load().count();
  }

  //----------------------------------------------------------------------------
  //! Dump recycler configutation
  //!
  //! @return string representation of the recycler configuration
  //----------------------------------------------------------------------------
  inline std::string Dump() const
  {
    return mPolicy.Dump();
  }

private:
#ifdef IN_TEST_HARNESS
public:
#endif
  AssistedThread mThread; ///< Thread doing the recycling
  std::string mPath; ///< Path of file to be recycled
  std::string mRecycleDir;
  std::string mRecyclePath; ///< Final path of file in the recycle bin
  uid_t mOwnerUid;
  gid_t mOwnerGid;
  unsigned long long mId;
  RecyclePolicy mPolicy;
  //! Map holding the container identifier and the full path of the directories
  //! to be deleted.
  std::map<eos::IContainerMD::id_t, std::string> mPendingDeletions;
  eos::common::SystemClock mClock;
  //! Condition variable to notify a configuration update is needed
  std::mutex mCvMutex;
  std::condition_variable mCvCfgUpdate;
  bool mTriggerRefresh {false};

  //----------------------------------------------------------------------------
  //! Check if client is allowed to restore the given recyle path. There are
  //! two situations when restore is allowed:
  //! * client is the owner of the entry
  //! * directory ACLs allow the client to read the entry i.e. restore
  //!
  //! @param recycle_path path in the recycle bin to restore
  //! @param vid client virtual identity
  //!
  //! @return 0 if allowe, otherwise errno
  //----------------------------------------------------------------------------
  static int
  IsAllowedToRestore(std::string_view recycle_path,
                     const eos::common::VirtualIdentity& vid);

  //----------------------------------------------------------------------------
  //! Remove all the entries in the given subtree
  //!
  //! @param fullpath full path to directory
  //----------------------------------------------------------------------------
  static void RemoveSubtree(std::string_view fullpath);

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
  //! Get cut-off date based on the configured retention policy with respect
  //! to the current timestamp.
  //!
  //! @return a string representing a date <year>/<month>/day numeric format
  //----------------------------------------------------------------------------
  std::string GetCutOffDate();
};

EOSMGMNAMESPACE_END
