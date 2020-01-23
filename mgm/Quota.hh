// ----------------------------------------------------------------------
// File: Quota.hh
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

#ifndef __EOSMGM_QUOTA__HH__
#define __EOSMGM_QUOTA__HH__

// this is needed because of some openssl definition conflict!
#undef des_set_key
#include <google/dense_hash_map>
#include <google/sparsehash/densehashtable.h>
#include "mgm/Scheduler.hh"
#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "common/Mapping.hh"
#include "common/RWMutex.hh"
#include "namespace/interface/IQuota.hh"
#include "XrdOuc/XrdOucString.hh"

EOSMGMNAMESPACE_BEGIN

class Quota;

//------------------------------------------------------------------------------
//! Class SpaceQuota
//------------------------------------------------------------------------------
class SpaceQuota : public eos::common::LogId
{
  friend class Quota;

public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  SpaceQuota(const char* path);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~SpaceQuota() = default;

  //----------------------------------------------------------------------------
  //! Get space name
  //----------------------------------------------------------------------------
  inline const char* GetSpaceName() const
  {
    return pPath.c_str();
  }

  //----------------------------------------------------------------------------
  //! Get namespace quota node
  //----------------------------------------------------------------------------
  inline eos::IQuotaNode* GetQuotaNode()
  {
    return mQuotaNode;
  }

  //----------------------------------------------------------------------------
  //! Get space layout size factor
  //----------------------------------------------------------------------------
  double GetLayoutSizeFactor()
  {
    return mLayoutSizeFactor;
  }

  //----------------------------------------------------------------------------
  //! Check user and/or group quota. If both are present, they both have to be
  //! fullfilled.
  //!
  //! @param uid user id
  //! @param gid group id
  //! @param desired_vol desired volume (size)
  //! @param desired_inodes desired number of inodes
  //!
  //! @return true if user has enough quota, otherwise false
  //----------------------------------------------------------------------------
  bool CheckWriteQuota(uid_t uid, gid_t gid, long long desired_vol,
                       unsigned int desired_inodes);

  //----------------------------------------------------------------------------
  //! Print quota information
  //----------------------------------------------------------------------------
  void PrintOut(XrdOucString& output, long long int uid_sel = -1,
                long long int gid_sel = -1, bool monitoring = false,
                bool translate_ids = false);

  //----------------------------------------------------------------------------
  //! Quota type tags
  //----------------------------------------------------------------------------
  enum eQuotaTag {
    kUserBytesIs = 1,                 kUserLogicalBytesIs = 2,
    kUserLogicalBytesTarget = 3,      kUserBytesTarget = 4,
    kUserFilesIs = 5,                 kUserFilesTarget = 6,
    kGroupBytesIs = 7,                kGroupLogicalBytesIs = 8,
    kGroupLogicalBytesTarget = 9,     kGroupBytesTarget = 10,
    kGroupFilesIs = 11,               kGroupFilesTarget = 12,
    kAllUserBytesIs = 13,             kAllUserLogicalBytesIs = 14,
    kAllUserLogicalBytesTarget = 15,  kAllUserBytesTarget = 16,
    kAllGroupBytesIs = 17,            kAllGroupLogicalBytesIs = 18,
    kAllGroupLogicalBytesTarget = 19, kAllGroupBytesTarget = 20,
    kAllUserFilesIs = 21,             kAllUserFilesTarget = 22,
    kAllGroupFilesIs = 23,            kAllGroupFilesTarget = 24
  };

private:

  //----------------------------------------------------------------------------
  //! Get quota
  //!
  //! @param tag quota type tag (eQuotaTag)
  //! @param id uid/gid/project id
  //!
  //! @return requested quota value
  //----------------------------------------------------------------------------
  long long GetQuota(unsigned long tag, unsigned long id);

  //----------------------------------------------------------------------------
  //! Set quota
  //!
  //! @param tag quota type tag (eQuotaTag)
  //! @param id user/group id
  //! @param value quota value to set
  //----------------------------------------------------------------------------
  void SetQuota(unsigned long tag, unsigned long id, unsigned long long value);

  //----------------------------------------------------------------------------
  //! Reset quota
  //!
  //! @param tag quota type tag (eQuotaTag)
  //! @param uid uid/gid/project id
  //!
  //! @warning Caller needs to hold a lock on mMutex
  //----------------------------------------------------------------------------
  void ResetQuota(unsigned long tag, unsigned long id);

  //----------------------------------------------------------------------------
  //! Add quota
  //!
  //! @param tag quota type tag (eQuotaTag)
  //! @param id user/group id
  //! @param value quota value to be added
  //!
  //! @warning Caller needs to hold a lock on mMutex.
  //----------------------------------------------------------------------------
  void AddQuota(unsigned long tag, unsigned long id, long long value);

  //----------------------------------------------------------------------------
  //! Account ns quota values into the space quota view.
  //----------------------------------------------------------------------------
  void AccountNsToSpace();

  //----------------------------------------------------------------------------
  //! Update quota from the ns quota node for the given identity only if the
  //! requested path is actually a ns quota node.
  //!
  //! @param uid user id
  //! @param gid group id
  //! @param upd_proj_quota if true then update also the project quota
  //----------------------------------------------------------------------------
  void UpdateFromQuotaNode(uid_t uid, gid_t, bool upd_proj_quota);

  //----------------------------------------------------------------------------
  //! Refresh quota all quota values for current space
  //!
  //! @warning Caller needs to hold a read-lock on both eosViewRWMutex and
  //!          pMapMutex
  //----------------------------------------------------------------------------
  void Refresh();

  //----------------------------------------------------------------------------
  //! Calculate the size factor used to estimate the logical available bytes
  //----------------------------------------------------------------------------
  void UpdateLogicalSizeFactor();

  //----------------------------------------------------------------------------
  //! Update target quota values
  //----------------------------------------------------------------------------
  void UpdateTargetSums();

  //----------------------------------------------------------------------------
  //! Update current quota values
  //----------------------------------------------------------------------------
  void UpdateIsSums();

  //----------------------------------------------------------------------------
  //! Update ns quota node address referred to by current space quota
  //!
  //! @return true if update successful, otherwise false
  //! @warning Caller needs to hold a read-lock on eosViewRWMutex
  //----------------------------------------------------------------------------
  bool UpdateQuotaNodeAddress();

  //----------------------------------------------------------------------------
  //! Serialize index
  //----------------------------------------------------------------------------
  inline unsigned long long Index(unsigned long tag, unsigned long id)
  {
    return ((tag << 32) | id);
  }

  //----------------------------------------------------------------------------
  //! Deserialize index
  //----------------------------------------------------------------------------
  inline unsigned long UnIndex(unsigned long long reindex)
  {
    return (reindex >> 32) & 0xffffffff;
  }

  //----------------------------------------------------------------------------
  //! Check if quota is enabled - needs a lock on the FsView
  //!
  //! @return true if quota enabled, otherwise false
  //! @warning Caller needs to hold a read-lock on gFsView::ViewMutex
  //----------------------------------------------------------------------------
  bool IsEnabled();

  //----------------------------------------------------------------------------
  //! Remove quota
  //!
  //! @param tag quota type tag
  //! @param uid uid/gid/project id
  //!
  //! @return true if quota deleted, false if quota not found
  //----------------------------------------------------------------------------
  bool RmQuota(unsigned long tag, unsigned long id);

  //----------------------------------------------------------------------------
  //! Get current quota value as percentage of the available one
  //!
  //! @param is current quota value
  //! @param avail available quota value
  //!
  //! @return string representation of the percentage value
  //----------------------------------------------------------------------------
  const float
  GetQuotaPercentage(unsigned long long is, unsigned long long avail);

  //----------------------------------------------------------------------------
  //! Get quota status
  //!
  //! @para is current quota value
  //! @param avail available quota value
  //!
  //! @return string representing the status i.e. ignored/ok/warning/exceeded
  //----------------------------------------------------------------------------
  const char* GetQuotaStatus(unsigned long long is, unsigned long long avail);

  //----------------------------------------------------------------------------
  //! Convert int tag to string representation
  //!
  //! @param tag int tag value
  //!
  //! @return string representation of the tag
  //----------------------------------------------------------------------------
  static const char* GetTagAsString(int tag);

  //----------------------------------------------------------------------------
  //! Convert int tag to string description
  //!
  //! @param tag int tag value
  //!
  //! @return string tag description
  //----------------------------------------------------------------------------
  static const char* GetTagName(int tag);

  //----------------------------------------------------------------------------
  //! Convert string tag to int representation
  //!
  //! @param tag string tag
  //!
  //! @return int representation of the tag
  //----------------------------------------------------------------------------
  static unsigned long GetTagFromString(const std::string& tag);

  //----------------------------------------------------------------------------
  //! Convert int tag to user or group category
  //!
  //! @param tag int tag value
  //!
  //! @return user/group category
  //----------------------------------------------------------------------------
  static const char* GetTagCategory(int tag);

  std::string pPath; ///< quota node path
  eos::IQuotaNode* mQuotaNode; ///< corresponding ns quota node
  XrdSysMutex mMutex; ///< mutex to protect access to mMapIdQuota
  time_t mLastEnableCheck; ///< timestamp of the last check
  double mLayoutSizeFactor; ///< layout dependent size factor
  bool mDirtyTarget; ///< mark to recompute target values

  //! Map for user view, depending on eQuota and uid/gid
  std::map<long long, unsigned long long> mMapIdQuota;
};


//------------------------------------------------------------------------------
//! Class Quota
//------------------------------------------------------------------------------
class Quota: eos::common::LogId
{
public:
  enum IdT { kUid, kGid }; ///< Id type enum
  enum Type { kUnknown, kVolume, kInode, kAll }; ///< Quota types

  //----------------------------------------------------------------------------
  //! Create space quota
  //!
  //! @param path quota node path which needs to be '/' terminated
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  static bool Create(const std::string& path);

  //----------------------------------------------------------------------------
  //! Check if quota node for path exists
  //!
  //! @param qpath path to search for
  //!
  //! @return true if quota node exists, otherwise false
  //----------------------------------------------------------------------------
  static bool Exists(const std::string& qpath);

  //----------------------------------------------------------------------------
  //! Check if there is a quota node responsible for the given path
  //!
  //! @param path path for which a quota node is searched, which needs to be
  //!             '/' terminated
  //!
  //! @return true if quota node exists, otherwise false
  //----------------------------------------------------------------------------
  static bool ExistsResponsible(const std::string& path);

  //----------------------------------------------------------------------------
  //! Get individual quota values
  //!
  //! @param vid client virtual identity
  //! @param path path
  //! @param max_bytes max bytes value
  //! @param free_bytes free bytes value
  //! @param logical request to return the layout corrected quota values
  //!
  //----------------------------------------------------------------------------
  static void GetIndividualQuota(eos::common::VirtualIdentity& vid,
                                 const std::string& path,
                                 long long& max_bytes,
                                 long long& free_bytes,
                                 long long& max_files,
                                 long long& free_files,
                                 bool logical = false);


  //----------------------------------------------------------------------------
  //! Set quota type of id (uid/gid)
  //!
  //! @param qpath quota path
  //! @param id uid or gid value depending on the id_type
  //! @param id_type type of id, can be uid or gid
  //! @param quota_type type of quota to remove, can be inode or volume
  //! @param value quota value to be set
  //! @param msg message returned to the client
  //! @param retc error number returned to the client
  //!
  //! @return true if quota set successful, otherwise false
  //----------------------------------------------------------------------------
  static bool SetQuotaTypeForId(const std::string& qpath, long id,
                                Quota::IdT id_type, Quota::Type quota_type,
                                unsigned long long value, std::string& msg,
                                int& retc);


  //----------------------------------------------------------------------------
  //! Set quota specified by the quota tag.
  //!
  //! @param qpath quota path
  //! @param quota_tag string representation of the SpaceQuota::eQuotaTag. From
  //!                  this we can deduce the quota type and the id type.
  //! @param id uid or gid value depending on the space_tag
  //! @param value quota value to be set
  //!
  //! @return true if quota set successful, otherwise false
  //----------------------------------------------------------------------------
  static bool SetQuotaForTag(const std::string& qpath,
                             const std::string& quota_tag,
                             long id, unsigned long long value);

  //----------------------------------------------------------------------------
  //! Remove all quota types for an id
  //!
  //! @param path quota node path
  //! @param id uid or gid value depending on the id_type
  //! @param id_type type of id, can be uid or gid
  //! @param msg message returned to the client
  //! @param retc error number returned to the client
  //!
  //! @return true if operation successful, otherwise false
  //----------------------------------------------------------------------------
  static bool RmQuotaForId(const std::string& path, long id,
                           Quota::IdT id_type, std::string& msg, int& retc);

  //----------------------------------------------------------------------------
  //! Remove quota type for id
  //!
  //! @param qpath quota node path
  //! @param id uid or gid value depending on the id_type
  //! @param id_type type of id, can be uid or gid
  //! @param quota_type type of quota to remove, can be inode or volume
  //! @param msg message returned to the client
  //! @param retc error number returned to the client
  //!
  //! @return true if operation successful, otherwise false
  //----------------------------------------------------------------------------
  static bool RmQuotaTypeForId(const std::string& qpath, long id,
                               Quota::IdT id_type, Quota::Type quota_type,
                               std::string& msg, int& retc);

  //------------------------------------------------------------------------------
  //! Remove quota specified by the quota tag
  //!
  //! @param qpath quota node path
  //! @param quota_tag string representation of the SpaceQuota::eQuotaTag. From
  //!                  this we can deduce the quota type and the id type.
  //! @param id uid or gid value depending on the space_tag
  //!
  //! @return true if quota set successful, otherwise false
  //------------------------------------------------------------------------------
  static bool RmQuotaForTag(const std::string& space,
                            const std::string& quota_stag,
                            long id);

  //----------------------------------------------------------------------------
  //! Removes a quota node
  //!
  //! @param qpath quota node path to be removed
  //! @param msg message returned to the client
  //! @param retc error number returned to the client
  //!
  //! @return true if operation successful, otherwise false
  //----------------------------------------------------------------------------
  static bool RmSpaceQuota(const std::string& qpath, std::string& msg, int& retc);

  //----------------------------------------------------------------------------
  //! Get group quota values for a particular path and id
  //!
  //! @param qpath quota node path
  //! @param id uid/gid/projectid
  //!
  //! @return map between quota types and values. The map contains 4 entries
  //!         corresponding to the following keys: kGroupBytesIs,
  //!         kGroupBytesTarget, kGroupFilesIs and kGroupFilesTarget
  //----------------------------------------------------------------------------
  static std::map<int, unsigned long long>
  GetGroupStatistics(const std::string& qpath, long id);

  //----------------------------------------------------------------------------
  //! Update SpaceQuota from the namespace quota only if the requested path is
  //! actually a ns quota node. This also performs an update for the project
  //! quota.
  //!
  //! @param path path
  //! @param uid user id
  //! @param gid group id
  //!
  //! @return true if update successful, otherwise it means that current path
  //!         doesn't point to a ns quota node and return false.
  //----------------------------------------------------------------------------
  static bool UpdateFromNsQuota(const std::string& path, uid_t uid, gid_t gid);

  //----------------------------------------------------------------------------
  //! Check if the requested volume and inode values respect the quota
  //!
  //! @param path path
  //! @param uid user id
  //! @param gid group id
  //! @param desired_vol desired space
  //! @param desired_inodes desired number of inondes
  //!
  //! @return true if quota is respected, otherwise false
  //----------------------------------------------------------------------------
  static bool Check(const std::string& path, uid_t uid, gid_t gid,
                    long long desired_vol, unsigned int desired_inodes);

  //----------------------------------------------------------------------------
  //! Callback function to calculate how much pyhisical space a file occupies
  //!
  //! @param file file MD object
  //!
  //! @return physical size depending on file layout type
  //----------------------------------------------------------------------------
  static uint64_t MapSizeCB(const eos::IFileMD* file);

  //----------------------------------------------------------------------------
  //! Load function to initialize all SpaceQuota's with the quota node
  //! definition from the namespace
  //----------------------------------------------------------------------------
  static void LoadNodes();

  //----------------------------------------------------------------------------
  //! Clean-up all space quotas by deleting them and clearing the map
  //----------------------------------------------------------------------------
  static void CleanUp();

  //----------------------------------------------------------------------------
  //! Print out quota information
  //!
  //! @return true if operation successful, otherwise false and populate the
  //!         output string with the error messsage
  //----------------------------------------------------------------------------
  static bool PrintOut(const std::string& path, XrdOucString& output,
                       long long int uid_sel = -1, long long int gid_sel = -1,
                       bool monitoring = false, bool translate_ids = false);

  //----------------------------------------------------------------------------
  //! Take the decision where to place a new file in the system. The core of the
  //! implementation is in the Scheduler and GeoTreeEngine.
  //!
  //! @param space quota space name
  //! @param path file path
  //! @param vid virtual id of client
  //! @param grouptag group tag for placement
  //! @param lid layout to be placed
  //! @param alreadyused_filsystems filesystems to avoid
  //! @param selected_filesystems filesystems selected by scheduler
  //! @param dataproxys if non null, schedule dataproxys for each fs
  //! @param firewallentpts if non null, schedule firewall entry points for each fs
  //! @param plctpolicy indicates if placement should be local/spread/hybrid
  //! @param plctTrgGeotag indicates close to which Geotag collocated stripes
  //!                      should be placed
  //! @param truncate indicates placement with truncation
  //! @param forched_scheduling_group_index forced index for the scheduling
  //!                      subgroup to be used
  //! @param bookingsize size to book for the placement
  //!
  //! @return 0 if placement successful, otherwise a non-zero value
  //!         ENOSPC - no space quota defined for current space
  //!         EDQUOT - no quota node found or not enough quota to place
  //! @warning Must be called with a lock on the FsView::gFsView::ViewMutex
  //----------------------------------------------------------------------------
  static
  int FilePlacement(Scheduler::PlacementArguments* args);

  //----------------------------------------------------------------------------
  //! @brief Retrieve the kAllGroupLogicalBytesIs and kAllGroupLogicalBytesTarget
  //! values for the quota nodes.
  //!
  //! @return a map with the paths of the quota nodes and the corresponding
  //! values
  //----------------------------------------------------------------------------
  static std::map<std::string, std::tuple<unsigned long long,
         unsigned long long,
         unsigned long long>>
         GetAllGroupsLogicalQuotaValues();

  //----------------------------------------------------------------------------
  //! Get quota for requested user and group by path
  //!
  //! @param path path for which to search for a quota node
  //! @param uid user id
  //! @param gid group id
  //! @param avail_files inode quota left
  //! @param avail_bytes size quota left
  //! @param quota_inode inode of the quota node
  //!
  //! @return 0 if successful
  //----------------------------------------------------------------------------
  static int QuotaByPath(const char* path, uid_t uid, gid_t gid,
                         long long& avail_files, long long& avail_bytes,
                         eos::IContainerMD::id_t& quota_inode);


  //----------------------------------------------------------------------------
  //! Get quota for requested user and group by quota inode
  //!
  //! @param qino inode of quota node
  //! @param uid user id
  //! @param gid group id
  //! @param avail_files inode quota left
  //! @param avail_bytes size quota left
  //!
  //! @return 0 if successful
  //----------------------------------------------------------------------------
  static int QuotaBySpace(const eos::IContainerMD::id_t, uid_t uid, gid_t gid,
                          long long& avail_files, long long& avail_bytes);

  //----------------------------------------------------------------------------
  //! Get space quota node path looking for the most specific
  //! match.
  //!
  //! @param path path for which we search for a responsible space quota
  //!
  //! @return path name to space quota
  //----------------------------------------------------------------------------
  static std::string GetResponsibleSpaceQuotaPath(const std::string& path);


  //----------------------------------------------------------------------------
  //! Get logical free und max bytes for this space
  //----------------------------------------------------------------------------
  static void GetStatfs(const std::string& path, unsigned long long& maxbytes, unsigned long long& freebytes);




  static gid_t gProjectId; ///< gid indicating project quota
  static eos::common::RWMutex pMapMutex; ///< Protect access to pMapQuota

private:

  //----------------------------------------------------------------------------
  //! Private method to collect desired info from a quota node
  //!
  //! @param squota quota object
  //! @param uid user id
  //! @param gid group id
  //! @param avail_files inode quota left
  //! @param avail_bytes size quota left
  //!
  //! @return 0 if successful
  //! @note locks should be taken outside this method
  //----------------------------------------------------------------------------
  static int GetQuotaInfo(SpaceQuota* squota, uid_t uid, gid_t gid,
                          long long& avail_files, long long& avail_bytes);

  //----------------------------------------------------------------------------
  //! Get space quota object for exact path
  //!
  //! @param qpath path of the quota node
  //!
  //! @return SpaceQuota object
  //----------------------------------------------------------------------------
  static SpaceQuota* GetSpaceQuota(const std::string& qpath);

  //----------------------------------------------------------------------------
  //! Get space quota node responsible for path looking for the most specific
  //! match.
  //!
  //! @param path path for which we search for a responsible space quotap
  //!
  //! @return SpaceQuota object
  //----------------------------------------------------------------------------
  static SpaceQuota* GetResponsibleSpaceQuota(const std::string& path);


  //----------------------------------------------------------------------------
  //! Make sure the path ends with a /
  //!
  //! @param path input path
  //!
  //! @return / terminated path
  //----------------------------------------------------------------------------
  static inline std::string NormalizePath(const std::string& ipath)
  {
    std::string path = ipath;

    if (!path.empty() && (path.back() != '/')) {
      path += '/';
    }

    return path;
  }

  //! Map from path to SpaceQuota object
  static std::map<std::string, SpaceQuota*> pMapQuota;
  //! Map from container id to SpaceQuota object
  static std::map<eos::IContainerMD::id_t, SpaceQuota*> pMapInodeQuota;
};

EOSMGMNAMESPACE_END

#endif
