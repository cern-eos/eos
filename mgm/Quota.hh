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
#include <google/dense_hash_map>
#include <google/sparsehash/densehashtable.h>
/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "mgm/FsView.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Scheduler.hh"
#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "common/Mapping.hh"
#include "common/GlobalConfig.hh"
#include "common/RWMutex.hh"
#include "mq/XrdMqMessage.hh"
#include "namespace/interface/IQuota.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <vector>
#include <set>
#include <stdint.h>
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
  //! Get QuotaNode
  //!
  //! @return QuotaNode object
  //----------------------------------------------------------------------------
  inline eos::IQuotaNode* GetQuotaNode()
  {
    return mQuotaNode;
  }

  //----------------------------------------------------------------------------
  //! Get space name
  //----------------------------------------------------------------------------
  const char* GetSpaceName()
  {
    return SpaceName.c_str();
  }

  //----------------------------------------------------------------------------
  //! Print quota information
  //----------------------------------------------------------------------------
  void PrintOut(XrdOucString& output, long uid_sel = -1,
		long gid_sel = -1, bool monitoring = false,
		bool translate_ids = false);

  //----------------------------------------------------------------------------
  //! Check user and/or group quota. If both are present, they both have to be
  //! fullfilled.
  //!
  //! @param uid user id
  //! @param gid group id
  //! @param desired_space space
  //! @param inodes
  //!
  //! @return true if user has enough quota, otherwise false
  //----------------------------------------------------------------------------
  bool CheckWriteQuota(uid_t uid, gid_t gid, long long desiredspace,
		       unsigned int inodes);

  //----------------------------------------------------------------------------
  //! Write placement routine. We implement the quota check while the scheduling
  //! is in the Scheduler base class.
  //!
  //! @param path path to place
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
  //! @return
  //----------------------------------------------------------------------------
  int FilePlacement(const char* path,
		    eos::common::Mapping::VirtualIdentity_t& vid,
		    const char* grouptag,
		    unsigned long lid,
		    std::vector<unsigned int>& alreadyused_filesystems,
		    std::vector<unsigned int>& selected_filesystems,
		    tPlctPolicy plctpolicy,
		    const std::string& plctTrgGeotag,
		    bool truncate = false,
		    int forced_scheduling_group_index = -1,
		    unsigned long long bookingsize = 1024 * 1024 * 1024ll);

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
  //! @param tag quota tag
  //! @param id uid/gid/projec it
  //!
  //! @return reuqest quota value
  //----------------------------------------------------------------------------
  long long GetQuota(unsigned long tag, unsigned long id, bool lock = true);

  //----------------------------------------------------------------------------
  //! Set quota
  //!
  //! @param tag quota type tag (eQuotaTag)
  //! @param id user/group id
  //! @param value quota value to set
  //----------------------------------------------------------------------------
  void SetQuota(unsigned long tag, unsigned long id, unsigned long long value);

  //----------------------------------------------------------------------------
  //! Add quota
  //----------------------------------------------------------------------------
  void AddQuota(unsigned long tag, unsigned long id, long long value,
		bool lock = true);

  //----------------------------------------------------------------------------
  //! Update SpaceQuota from the ns quota node for the given identity
  //!
  //! @param uid user id
  //! @param gid group id
  //! @param upd_proj_quota if true then update also the project quota
  //!
  //----------------------------------------------------------------------------
  void UpdateFromQuotaNode(uid_t uid, gid_t, bool upd_proj_quota);

  //----------------------------------------------------------------------------
  //! Refresh quota
  //----------------------------------------------------------------------------
  void Refresh();

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  void UpdateLogicalSizeFactor();

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  void UpdateTargetSums();

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  void UpdateIsSums();

  //----------------------------------------------------------------------------
  //! Updates the valid address of a quota node from the filesystem view
  //----------------------------------------------------------------------------
  bool UpdateQuotaNodeAddress();

  //----------------------------------------------------------------------------
  //! Serialize index
  //----------------------------------------------------------------------------
  unsigned long long Index(unsigned long tag, unsigned long id)
  {
    unsigned long long fulltag = tag;
    fulltag <<= 32;
    fulltag |= id;
    return fulltag;
  }

  //----------------------------------------------------------------------------
  //! Deserialize index
  //----------------------------------------------------------------------------
  unsigned long UnIndex(unsigned long long reindex)
  {
    return (reindex >> 32) & 0xffffffff;
  }

  //----------------------------------------------------------------------------
  //! Remove quota node
  //----------------------------------------------------------------------------
  void RemoveQuotaNode(std::string& msg, int& retc);

  //----------------------------------------------------------------------------
  //! Check if quota is enabled
  //!
  //! @return true if quota enabled, otherwise false
  //----------------------------------------------------------------------------
  bool IsEnabled();

  //----------------------------------------------------------------------------
  //! Remove quota
  //----------------------------------------------------------------------------
  bool RmQuota(unsigned long tag, unsigned long id, bool lock = true);

  //----------------------------------------------------------------------------
  //! Reset quota
  //----------------------------------------------------------------------------
  void
  ResetQuota(unsigned long tag, unsigned long id)
  {
    return SetQuota(tag, id, 0);
  }

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
  eos::IQuotaNode* mQuotaNode;
  XrdSysMutex mMutex; ///< mutex to protect access to mMapIdQuota
  time_t mLastEnableCheck; ///< timestamp of the last check
  double mLayoutSizeFactor; ///< layout dependent size factor
  bool mDirtyTarget; ///< mark to recompute target values

  //! One hash map for user view! depending on eQuota Tag id is either uid or gid!
  //! The key is (eQuotaTag<<32) | id
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
  ~Quota() { };

  //----------------------------------------------------------------------------
  //! Get space quota object for exact path or create if doesn't exist
  //!
  //! @param cpath path
  //! @param nocreate if true, don't try to create it
  //!
  //! @return SpaceQuota object
  //----------------------------------------------------------------------------
  static SpaceQuota* GetSpaceQuota(const char* cpath, bool nocreate = false);

  //----------------------------------------------------------------------------
  //! Get space quota node responsible for path looking for the most specific
  //! match.
  //!
  //! @param cpath path
  //!
  //! @return SpaceQuota object
  //----------------------------------------------------------------------------
  static SpaceQuota* GetResponsibleSpaceQuota(const char* cpath);

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
				 const char* path, long long& max_bytes,
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
  static bool RmSpaceQuota(const std::string& space, std::string& msg, int& retc);

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
  //! Callback function for the namespace implementation to calculate the size
  //! a file occupies
  //----------------------------------------------------------------------------
  static uint64_t MapSizeCB(const eos::IFileMD* file);

  //----------------------------------------------------------------------------
  //! Load function to initialize all SpaceQuota's with the quota node
  //! definition from the namespace
  //----------------------------------------------------------------------------
  static void LoadNodes();

  //----------------------------------------------------------------------------
  //! Inserts the current state of the quota nodes into SpaceQuota's
  //----------------------------------------------------------------------------
  static void NodesToSpaceQuota();

  //----------------------------------------------------------------------------
  //! Insert current state of a single quota node into a SpaceQuota
  //!
  //! @param path space quota path
  //----------------------------------------------------------------------------
  static void NodeToSpaceQuota(const char* name);

  //----------------------------------------------------------------------------
  //! Clean-up all space quotas by deleting them and clearing the map
  //----------------------------------------------------------------------------
  static void CleanUp();

  //----------------------------------------------------------------------------
  //! Print out quota information
  //----------------------------------------------------------------------------
  static void PrintOut(const char* space, XrdOucString& output,
		       long uid_sel = -1, long gid_sel = -1,
		       bool monitoring = false, bool translate_ids = false);

  static gid_t gProjectId; ///< gid indicating project quota
  static eos::common::RWMutex gQuotaMutex; ///< mutex to protect access to gQuota

private:



  //! Map from path to SpaceQuota object
  static std::map<std::string, SpaceQuota*> pMapQuota;
};

EOSMGMNAMESPACE_END

#endif
