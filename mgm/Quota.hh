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
#include "namespace/accounting/QuotaStats.hh"
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

class SpaceQuota : public Scheduler
{
private:
  XrdSysMutex Mutex;
  time_t LastCalculationTime;
  time_t LastEnableCheck;
  bool On;
  eos::QuotaNode* QuotaNode;
  double LayoutSizeFactor; // this is layout dependent!

  bool
  Enabled ()
  {
    time_t now = time (NULL);
    if (now > (LastEnableCheck + 5))
    {
      std::string spacename = SpaceName.c_str ();
      std::string key = "quota";
      if (FsView::gFsView.mSpaceView.count (spacename))
      {
        std::string ison = FsView::gFsView.mSpaceView[spacename]->GetConfigMember (key);
        if (ison == "on")
        {
          On = true;
        }
        else
        {
          On = false;
        }
      }
      else
      {
        On = false;
      }
      LastEnableCheck = now;
    }
    return On;
  }

  // one hash map for user view! depending on eQuota Tag id is either uid or gid!

  std::map<long long, unsigned long long> Quota; // the key is (eQuotaTag<<32) | id

  unsigned long long PhysicalFreeBytes; // this is coming from the statfs calls on all file systems
  unsigned long long PhysicalFreeFiles; // this is coming from the statfs calls on all file systems
  unsigned long long PhysicalMaxBytes; // this is coming from the statfs calls on all file systems
  unsigned long long PhysicalMaxFiles; // this is coming from the statfs calls on all file systems

  // this is used to recalculate the values without invalidating the old one
  unsigned long long PhysicalTmpFreeBytes; // this is coming from the statfs calls on all file systems
  unsigned long long PhysicalTmpFreeFiles; // this is coming from the statfs calls on all file systems
  unsigned long long PhysicalTmpMaxBytes; // this is coming from the statfs calls on all file systems
  unsigned long long PhysicalTmpMaxFiles; // this is coming from the statfs calls on all file systems

public:
  XrdSysMutex OpMutex;

  enum eQuotaTag
  {
    kUserBytesIs = 1, kUserLogicalBytesIs = 2, kUserLogicalBytesTarget = 3, kUserBytesTarget = 4,
    kUserFilesIs = 5, kUserFilesTarget = 6,
    kGroupBytesIs = 7, kGroupLogicalBytesIs = 8, kGroupLogicalBytesTarget = 9, kGroupBytesTarget = 10,
    kGroupFilesIs = 11, kGroupFilesTarget = 12,
    kAllUserBytesIs = 13, kAllUserLogicalBytesIs = 14, kAllUserLogicalBytesTarget = 15, kAllUserBytesTarget = 16,
    kAllGroupBytesIs = 17, kAllGroupLogicalBytesIs = 18, kAllGroupLogicalBytesTarget = 19, kAllGroupBytesTarget = 20,
    kAllUserFilesIs = 21, kAllUserFilesTarget = 22,
    kAllGroupFilesIs = 23, kAllGroupFilesTarget = 24
  };

  static const char*
  GetTagAsString (int tag)
  {
    if (tag == kUserBytesTarget)
    {
      return "userbytes";
    }
    if (tag == kUserFilesTarget)
    {
      return "userfiles";
    }
    if (tag == kGroupBytesTarget)
    {
      return "groupbytes";
    }
    if (tag == kGroupFilesTarget)
    {
      return "groupfiles";
    }
    if (tag == kAllUserBytesTarget)
    {
      return "alluserbytes";
    }
    if (tag == kAllUserFilesTarget)
    {
      return "alluserfiles";
    }
    if (tag == kAllGroupBytesTarget)
    {
      return "allgroupbytes";
    }
    if (tag == kAllGroupFilesTarget)
    {
      return "allgroupfiles";
    }
    return 0;
  }

  static unsigned long
  GetTagFromString (XrdOucString &tag)
  {
    if (tag == "userbytes") return kUserBytesTarget;
    if (tag == "userfiles") return kUserFilesTarget;
    if (tag == "groupbytes") return kGroupBytesTarget;
    if (tag == "groupfiles") return kGroupFilesTarget;
    if (tag == "alluserbytes") return kAllUserBytesTarget;
    if (tag == "alluserfiles") return kAllUserFilesTarget;
    if (tag == "allgroupbytes") return kAllGroupBytesTarget;
    if (tag == "allgroupfiles") return kAllGroupFilesTarget;
    return 0;
  }

  const char*
  GetTagCategory (int tag)
  {

    if ((tag == kUserBytesIs) || (tag == kUserBytesTarget) || (tag == kUserLogicalBytesIs) || (tag == kUserLogicalBytesTarget) ||
        (tag == kUserFilesIs) || (tag == kUserFilesTarget)) return "user ";
    if ((tag == kGroupBytesIs) || (tag == kGroupBytesTarget) || (tag == kGroupLogicalBytesIs) || (tag == kGroupLogicalBytesTarget) ||
        (tag == kGroupFilesIs) || (tag == kGroupFilesTarget)) return "group";
    if ((tag == kAllUserBytesIs) || (tag == kAllUserBytesTarget) ||
        (tag == kAllUserFilesIs) || (tag == kAllUserFilesTarget)) return "user ";
    if ((tag == kAllGroupBytesIs) || (tag == kAllGroupBytesTarget) ||
        (tag == kAllGroupFilesIs) || (tag == kAllGroupFilesTarget)) return "group";
    return "-----";
  }

  const char*
  GetTagName (int tag)
  {
    if (tag == kUserBytesIs)
    {
      return "used bytes";
    }
    if (tag == kUserLogicalBytesIs)
    {
      return "logi bytes";
    }
    if (tag == kUserBytesTarget)
    {
      return "aval bytes";
    }
    if (tag == kUserFilesIs)
    {
      return "used files";
    }
    if (tag == kUserFilesTarget)
    {
      return "aval files";
    }
    if (tag == kUserLogicalBytesTarget)
    {
      return "aval logib";
    }

    if (tag == kGroupBytesIs)
    {
      return "used bytes";
    }
    if (tag == kGroupLogicalBytesIs)
    {
      return "logi bytes";
    }
    if (tag == kGroupBytesTarget)
    {
      return "aval bytes";
    }
    if (tag == kGroupFilesIs)
    {
      return "used files";
    }
    if (tag == kGroupFilesTarget)
    {
      return "aval files";
    }
    if (tag == kGroupLogicalBytesTarget)
    {
      return "aval logib";
    }

    if (tag == kAllUserBytesIs)
    {
      return "used bytes";
    }
    if (tag == kAllUserLogicalBytesIs)
    {
      return "logi bytes";
    }
    if (tag == kAllUserBytesTarget)
    {
      return "aval bytes";
    }
    if (tag == kAllUserFilesIs)
    {
      return "used files";
    }
    if (tag == kAllUserFilesTarget)
    {
      return "aval files";
    }
    if (tag == kAllUserLogicalBytesTarget)
    {
      return "aval logib";
    }
    if (tag == kAllGroupBytesIs)
    {
      return "used bytes";
    }
    if (tag == kAllGroupLogicalBytesIs)
    {
      return "logi bytes";
    }
    if (tag == kAllGroupBytesTarget)
    {
      return "aval bytes";
    }
    if (tag == kAllGroupFilesIs)
    {
      return "used files";
    }
    if (tag == kAllGroupFilesTarget)
    {
      return "aval files";
    }
    if (tag == kAllGroupLogicalBytesTarget)
    {
      return "aval logib";
    }
    return "---- -----";
  }

  bool UpdateQuotaNodeAddress (); // updates the valid address of a quota node from the filesystem view

  eos::QuotaNode*
  GetQuotaNode ()
  {
    return QuotaNode;
  }

  const char*
  GetQuotaStatus (unsigned long long is, unsigned long long avail)
  {
    if (!avail)
    {
      return "ignored";
    }
    double p = (100.0 * is / avail);
    if (p < 90)
    {
      return "ok";
    }
    if (p < 99)
    {
      return "warning";
    }
    return "exceeded";
  }

  const char*
  GetQuotaPercentage (unsigned long long is, unsigned long long avail, XrdOucString &spercentage)
  {
    char percentage[1024];
    float fp = avail ? (100.0 * is / avail) : 100.0;
    if (fp > 100.0)
    {
      fp = 100.0;
    }
    if (fp < 0)
    {
      fp = 0;
    }
    sprintf (percentage, "%.02f", fp);
    spercentage = percentage;
    return spercentage.c_str ();
  }

  bool
  NeedsRecalculation ()
  {
    if ((time (NULL) - LastCalculationTime) > 10) return true;
    else return false;
  }

  void UpdateLogicalSizeFactor ();
  void UpdateTargetSums ();
  void UpdateIsSums ();
  void UpdateFromQuotaNode (uid_t uid, gid_t gid);

  SpaceQuota (const char* name);
  ~SpaceQuota ();

  void RemoveQuotaNode (XrdOucString &msg, int &retc);

  std::map<long long, unsigned long long>::const_iterator
  Begin ()
  {
    return Quota.begin ();
  }

  std::map<long long, unsigned long long>::const_iterator
  End ()
  {
    return Quota.end ();
  }

  const char*
  GetSpaceName ()
  {
    return SpaceName.c_str ();
  }

  long long GetQuota (unsigned long tag, unsigned long id, bool lock = true);

  void SetQuota (unsigned long tag, unsigned long id, unsigned long long value, bool lock = true);
  bool RmQuota (unsigned long tag, unsigned long id, bool lock = true);
  void AddQuota (unsigned long tag, unsigned long id, long long value, bool lock = true);

  void
  SetPhysicalTmpFreeBytes (unsigned long long bytes)
  {
    PhysicalTmpFreeBytes = bytes;
  }

  void
  SetPhysicalTmpFreeFiles (unsigned long long files)
  {
    PhysicalTmpFreeFiles = files;
  }

  void
  SetPhysicalFreeBytes (unsigned long long bytes)
  {
    PhysicalFreeBytes = bytes;
  }

  void
  SetPhysicalFreeFiles (unsigned long long files)
  {
    PhysicalFreeFiles = files;
  }

  void
  SetPhysicalTmpMaxBytes (unsigned long long bytes)
  {
    PhysicalTmpMaxBytes = bytes;
  }

  void
  SetPhysicalTmpMaxFiles (unsigned long long files)
  {
    PhysicalTmpMaxFiles = files;
  }

  void
  SetPhysicalMaxBytes (unsigned long long bytes)
  {
    PhysicalMaxBytes = bytes;
  }

  void
  SetPhysicalMaxFiles (unsigned long long files)
  {
    PhysicalMaxFiles = files;
  }

  void
  ResetPhysicalFreeBytes ()
  {
    SetPhysicalFreeBytes (0);
  }

  void
  ResetPhysicalFreeFiles ()
  {
    SetPhysicalFreeFiles (0);
  }

  void
  ResetPhysicalMaxBytes ()
  {
    SetPhysicalMaxBytes (0);
  }

  void
  ResetPhysicalMaxFiles ()
  {
    SetPhysicalMaxFiles (0);
  }

  void
  ResetPhysicalTmpFreeBytes ()
  {
    SetPhysicalTmpFreeBytes (0);
  }

  void
  ResetPhysicalTmpFreeFiles ()
  {
    SetPhysicalTmpFreeFiles (0);
  }

  void
  ResetPhysicalTmpMaxBytes ()
  {
    SetPhysicalTmpMaxBytes (0);
  }

  void
  ResetPhysicalTmpMaxFiles ()
  {
    SetPhysicalTmpMaxFiles (0);
  }

  void
  AddPhysicalFreeBytes (unsigned long long bytes)
  {
    PhysicalFreeBytes += bytes;
  }

  void
  AddPhysicalFreeFiles (unsigned long long files)
  {
    PhysicalFreeFiles += files;
  }

  void
  AddPhysicalMaxBytes (unsigned long long bytes)
  {
    PhysicalMaxBytes += bytes;
  }

  void
  AddPhysicalMaxFiles (unsigned long long files)
  {
    PhysicalMaxFiles += files;
  }

  void
  AddPhysicalTmpFreeBytes (unsigned long long bytes)
  {
    PhysicalTmpFreeBytes += bytes;
  }

  void
  AddPhysicalTmpFreeFiles (unsigned long long files)
  {
    PhysicalTmpFreeFiles += files;
  }

  void
  AddPhysicalTmpMaxBytes (unsigned long long bytes)
  {
    PhysicalTmpMaxBytes += bytes;
  }

  void
  AddPhysicalTmpMaxFiles (unsigned long long files)
  {
    PhysicalTmpMaxFiles += files;
  }

  void
  PhysicalTmpToFreeBytes ()
  {
    PhysicalFreeBytes = PhysicalTmpFreeBytes;
  }

  void
  PhysicalTmpToFreeFiles ()
  {
    PhysicalFreeFiles = PhysicalTmpFreeFiles;
  }

  void
  PhysicalTmpToMaxBytes ()
  {
    PhysicalMaxBytes = PhysicalTmpMaxBytes;
  }

  void
  PhysicalTmpToMaxFiles ()
  {
    PhysicalMaxFiles = PhysicalTmpMaxFiles;
  }

  unsigned long long
  GetPhysicalFreeBytes ()
  {
    return PhysicalFreeBytes;
  }

  unsigned long long
  GetPhysicalFreeFiles ()
  {
    return PhysicalFreeFiles;
  }

  unsigned long long
  GetPhysicalMaxBytes ()
  {
    return PhysicalMaxBytes;
  }

  unsigned long long
  GetPhysicalMaxFiles ()
  {
    return PhysicalMaxFiles;
  }

  void
  ResetQuota (unsigned long tag, unsigned long id, bool lock = true)
  {
    return SetQuota (tag, id, 0, lock);
  }
  void PrintOut (XrdOucString& output, long uid_sel = -1, long gid_sel = -1, bool monitoring = false, bool translateids = false);

  unsigned long long
  Index (unsigned long tag, unsigned long id)
  {
    unsigned long long fulltag = tag;
    fulltag <<= 32;
    fulltag |= id;
    return fulltag;
  }

  unsigned long
  UnIndex (unsigned long long reindex)
  {
    return (reindex >> 32) & 0xffff;
  }

  bool CheckWriteQuota (uid_t, gid_t, long long desiredspace, unsigned int inodes);

  //! -------------------------------------------------------------
  //! the write placement routine - here we implement the quota check while the scheduling is in the Scheduler base calss
  //! -------------------------------------------------------------
  int FilePlacement (const char* path, //< path to place
                     eos::common::Mapping::VirtualIdentity_t &vid, //< virtual id of client
                     const char* grouptag, //< group tag for placement
                     unsigned long lid, //< layout to be placed
                     std::vector<unsigned int> &avoid_filesystems, //< filesystems to avoid
                     std::vector<unsigned int> &selected_filesystems, //< return filesystems selected by scheduler
                     bool truncate = false, //< indicates placement with truncation
                     int forced_scheduling_group_index = -1, //< forced index for the scheduling subgroup to be used 
                     unsigned long long bookingsize = 1024 * 1024 * 1024ll //< size to book for the placement
                     );

};

class Quota : eos::common::LogId
{
private:

public:
  static std::map<std::string, SpaceQuota*> gQuota;
  static eos::common::RWMutex gQuotaMutex;

  static SpaceQuota* GetSpaceQuota (const char* name, bool nocreate = false);
  static SpaceQuota* GetResponsibleSpaceQuota (const char*path); // returns a space (+quota node), which is responsible for <path>

  Quota () { }

  ~Quota () { };

  void Recalculate ();

  // builds a list with the names of all spaces
  static int GetSpaceNameList (const char* key, SpaceQuota* spacequota, void *Arg);

  static void PrintOut (const char* space, XrdOucString &output, long uid_sel = -1, long gid_sel = -1, bool monitoring = false, bool translateids = false);

  static bool SetQuota (XrdOucString space, long uid_sel, long gid_sel, long long bytes, long long files, XrdOucString &msg, int &retc); // -1 means it is not set for all long/long long values

  static bool RmQuota (XrdOucString space, long uid_sel, long gid_sel, XrdOucString &msg, int &retc); // -1 means it is not set for all long/long long values

  static bool RmSpaceQuota (XrdOucString space, XrdOucString &msg, int &retc); // removes a quota space/quota node

  // callback function for the namespace implementation to calculate the size a file occupies
  static uint64_t MapSizeCB (const eos::FileMD *file);

  // load function to initialize all SpaceQuota's with the quota node definition from the namespace
  static void LoadNodes ();

  // inserts the current state of the quota nodes into SpaceQuota's
  static void NodesToSpaceQuota ();

  // insert current state of a single quota node into a SpaceQuota
  static void NodeToSpaceQuota (const char* name, bool lock = true);

  static gid_t gProjectId; //< gid indicating project quota
};

EOSMGMNAMESPACE_END

#endif
