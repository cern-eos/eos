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

/*----------------------------------------------------------------------------*/
// this is needed because of some openssl definition conflict!
#undef des_set_key
/*----------------------------------------------------------------------------*/
#include <vector>
#include <set>
#include <stdint.h>
#include "mgm/Namespace.hh"
#include "mgm/FsView.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Scheduler.hh"
#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "common/Mapping.hh"
#include "common/GlobalConfig.hh"
#include "common/RWMutex.hh"
#include "namespace/interface/IQuota.hh"
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

#define EOSMGMQUOTA_DISKHEADROOM 1024ll*1024ll*1024l*25

class Quota;

//------------------------------------------------------------------------------
//! Class SpaceQuota
//------------------------------------------------------------------------------
class SpaceQuota : public Scheduler
{
  friend class Quota;

public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  SpaceQuota(const char* name);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~SpaceQuota();

  //----------------------------------------------------------------------------
  //! Get space name
  //----------------------------------------------------------------------------
  inline const char* GetSpaceName()
  {
    return SpaceName.c_str();
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
  //!
  //----------------------------------------------------------------------------
  void PrintOut(XrdOucString& output, long uid_sel = -1,
		long gid_sel = -1, bool monitoring = false,
		bool translate_ids = false);

  //----------------------------------------------------------------------------
  //! Quota type tags
  //----------------------------------------------------------------------------
  enum eQuotaTag
  {
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
  //! @param id uid/gid/projec it
  //!
  //! @return reuqested quota value
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
  void  ResetQuota(unsigned long tag, unsigned long id);

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
  //! Import ns quota values into current space quota
  //----------------------------------------------------------------------------
  void NsQuotaToSpaceQuota();

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
  //! @warning Caller needs to hold a read-lock on eosViewRWMutexo
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
  //! @param spercentage (out) string representation of the percentage
  //!
  //! @return string representation of the percentage value
  //----------------------------------------------------------------------------
  const char*
  GetQuotaPercentage(unsigned long long is, unsigned long long avail,
		     XrdOucString& spercentage);

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

  bool mEnabled; ///< true if space quota enabled, otherwise false
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
  //! Constructor
  //----------------------------------------------------------------------------
  Quota() { }

  //----------------------------------------------------------------------------
  //! Desstructor
  //----------------------------------------------------------------------------
  virtual ~Quota() { }

  //----------------------------------------------------------------------------
  //! Check if space quota exists
  //!
  //! @param space quota space to search for
  //!
  //! @return true if space quota exists, otherwise false
  //----------------------------------------------------------------------------
  static bool ExistsSpace(const std::string& space);

  //----------------------------------------------------------------------------
  //! Check if there is a quota node responsible for the given path
  //!
  //! @param path path for which a quota node is searched
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
  //!
  //----------------------------------------------------------------------------
  static void GetIndividualQuota(eos::common::Mapping::VirtualIdentity_t& vid,
				 const std::string& path, long long& max_bytes,
				 long long& free_bytes);


  //----------------------------------------------------------------------------
  //! Set quota type of id (uid/gid)
  //!
  //! @param space quota path
  //! @param id uid or gid value depending on the id_type
  //! @param id_type type of id, can be uid or gid
  //! @param quota_type type of quota to remove, can be inode or volume
  //! @param value quota value to be set
  //! @param msg message returned to the client
  //! @param retc error number returned to the client
  //!
  //! @return true if quota set successful, otherwise false
  //----------------------------------------------------------------------------
  static bool SetQuotaTypeForId(const std::string& space, long id,
				Quota::IdT id_type, Quota::Type quota_type,
				unsigned long long value, std::string& msg,
				int& retc);


  //----------------------------------------------------------------------------
  //! Set quota specified by the quota tag.
  //!
  //! @param space quota path
  //! @param quota_tag string representation of the SpaceQuota::eQuotaTag. From
  //!                  this we can deduce the quota type and the id type.
  //! @param id uid or gid value depending on the space_tag
  //! @param value quota value to be set
  //!
  //! @return true if quota set successful, otherwise false
  //----------------------------------------------------------------------------
  static bool SetQuotaForTag(const std::string& space,
			     const std::string& quota_tag,
			     long id, unsigned long long value);

  //----------------------------------------------------------------------------
  //! Remove all quota types for an id
  //!
  //! @param space path to node
  //! @param id uid or gid value depending on the id_type
  //! @param id_type type of id, can be uid or gid
  //! @param msg message returned to the client
  //! @param retc error number returned to the client
  //!
  //! @return true if operation successful, otherwise false
  //----------------------------------------------------------------------------
  static bool RmQuotaForId(const std::string& space, long id,
			   Quota::IdT id_type, std::string& msg, int& retc);

  //----------------------------------------------------------------------------
  //! Remove quota type for id
  //!
  //! @param space path to node
  //! @param id uid or gid value depending on the id_type
  //! @param id_type type of id, can be uid or gid
  //! @param quota_type type of quota to remove, can be inode or volume
  //! @param msg message returned to the client
  //! @param retc error number returned to the client
  //!
  //! @return true if operation successful, otherwise false
  //----------------------------------------------------------------------------
  static bool RmQuotaTypeForId(const std::string& space, long id,
			       Quota::IdT id_type, Quota::Type quota_type,
			       std::string& msg, int& retc);

  //------------------------------------------------------------------------------
  //! Remove quota specified by the quota tag
  //!
  //! @param space quota path
  //! @param quota_tag string representation of the SpaceQuota::eQuotaTag. From
  //!                  this we can deduce the quota type and the id type.
  //! @param id uid or gid value depending on the space_tag
  //!
  //! @return true if quota set successful, otherwise false
  //------------------------------------------------------------------------------
  static bool RmQuotaForTag(const std::string& space, const std::string& quota_stag,
			    long id);

  //----------------------------------------------------------------------------
  //! Removes a quota node
  //!
  //! @param space path for quota node to be removed
  //! @param msg message returned to the client
  //! @param retc error number returned to the client
  //!
  //! @return true if operation successful, otherwise false
  //----------------------------------------------------------------------------
  static bool RmSpaceQuota(std::string& space, std::string& msg, int& retc);

  //----------------------------------------------------------------------------
  //! Get group quota values for a particular space and id
  //!
  //! @param space space name
  //! @param id uid/gid/projectid
  //!
  //! @return map between quota types and values. The map contains 4 entries
  //!         corresponding to the following keys: kGroupBytesIs,
  //!         kGroupBytesTarget, kGroupFilesIs and kGroupFilesTarget
  //----------------------------------------------------------------------------
  static std::map<int, unsigned long long>
  GetGroupStatistics(const std::string& space, long id);

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
  //----------------------------------------------------------------------------
  static void PrintOut(const std::string& space, XrdOucString& output,
		       long uid_sel = -1, long gid_sel = -1,
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
  int FilePlacement(const std::string& space,
		    const char* path,
		    eos::common::Mapping::VirtualIdentity_t& vid,
		    const char* grouptag,
		    unsigned long lid,
		    std::vector<unsigned int>& alreadyused_filesystems,
		    std::vector<unsigned int>& selected_filesystems,
		    Scheduler::tPlctPolicy plctpolicy,
		    const std::string& plctTrgGeotag,
		    bool truncate = false,
		    int forced_scheduling_group_index = -1,
		    unsigned long long bookingsize = 1024 * 1024 * 1024ll);

  //----------------------------------------------------------------------------
  //! Take the decision from where to access a file. The core of the
  //! implementation is in the Scheduler and GeoTreeEngine.
  //!
  //! @param space quota space name
  //! @param vid virutal id of the client
  //! @param focedfsid forced filesystem for access
  //! @param forcedspace forced space for access
  //! @param tried_cgi cgi containing already tried hosts
  //! @param lid layout fo the file
  //! @param locationsfs filesystem ids where layout is stored
  //! @param fsindex return index pointing to layout entry filesystem
  //! @param isRW indicate pure read or rd/wr access
  //! @param bookingsize size to book additionally for rd/wr access
  //! @param unavailfs return filesystems currently unavailable
  //! @param min_fsstatus define minimum filesystem state to allow fs selection
  //! @param overridegeoloc override geolocation defined in the virtual id
  //! @param noIO don't apply the penalty as this file access won't result in
  //!             any IO
  //!
  //! @return 0 if successful, otherwise a non-zero value
  //! @warning Must be called with a lock on the FsView::gFsView::ViewMutex
  //----------------------------------------------------------------------------
  static int FileAccess(const std::string& space,
			eos::common::Mapping::VirtualIdentity_t& vid,
			unsigned long forcedfsid,
			const char* forcedspace,
			std::string tried_cgi,
			unsigned long lid,
			std::vector<unsigned int>& locationsfs,
			unsigned long& fsindex,
			 bool isRW,
			unsigned long long bookingsize,
			std::vector<unsigned int>& unavailfs,
			eos::common::FileSystem::fsstatus_t min_fsstatus =
			eos::common::FileSystem::kDrain,
			std::string overridegeoloc = "",
			bool noIO = false);

  static gid_t gProjectId; ///< gid indicating project quota
  static eos::common::RWMutex pMapMutex; ///< mutex to protect access to pMapQuota

private:

  //----------------------------------------------------------------------------
  //! Get space quota object for exact path or create if doesn't exist
  //!
  //! @param path path of the space quota
  //!
  //! @return SpaceQuota object
  //----------------------------------------------------------------------------
  static SpaceQuota* GetSpaceQuota(const std::string& path);

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
  //! Create space quota
  //!
  //! @param path quota node path which needs to be '/' terminated
  //----------------------------------------------------------------------------
  static void CreateSpaceQuota(const std::string& path);

  //! Map from path to SpaceQuota object
  static std::map<std::string, SpaceQuota*> pMapQuota;

};

EOSMGMNAMESPACE_END

#endif
