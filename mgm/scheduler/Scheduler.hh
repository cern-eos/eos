// ----------------------------------------------------------------------
// File: Scheduler.hh
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

#pragma once
#include "common/Logging.hh"
#include "common/FileSystem.hh"
#include "common/LayoutId.hh"
#include "mgm/Namespace.hh"
#include "mgm/fsview/FsView.hh"

EOSMGMNAMESPACE_BEGIN

namespace placement {
class FsScheduler;
}

//------------------------------------------------------------------------------
//! Class implementing file scheduling e.g. access and placement
//------------------------------------------------------------------------------
class Scheduler
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Scheduler() = default;

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~Scheduler() = default;

  //! Types of placement policy
  enum tPlctPolicy
  { kScattered, kHybrid, kGathered };

  //----------------------------------------------------------------------------
  //! File placement structs and methods
  //----------------------------------------------------------------------------
  struct PlacementArguments {
    /// INPUT
    //! space name
    const std::string* spacename;
    //! file path
    const char* path;
    //! group tag for placement
    const char* grouptag;
    //! layout to be placed
    unsigned long lid;
    //! file inode
    ino64_t inode;
    //! indicates if placement should be local/spread/hybrid
    tPlctPolicy plctpolicy;
    //! indicates close to which Geotag collocated stripes should be placed
    const std::string* plctTrgGeotag;
    //! indicates placement with truncation
    bool truncate;
    //! forced index for the scheduling group to be used
    int forced_scheduling_group_index;
    //! size to book for the placement
    unsigned long long bookingsize;
    //! traffic class this placement belongs to - client traffic unless an
    //! internal engine (drain, balance, conversion, fsck) asked for it
    eos::common::SchedActivity activity;
    //! scheduling strategy to be used (if not set, use space default)
    //! scheduling strategy if set from opaque string
    const char* sched_strategy_cstr;
    //! virtual identity of the client
    const eos::common::VirtualIdentity* vid;
    /// INPUT/OUTPUT
    //! filesystems to avoid
    std::vector<unsigned int>* alreadyused_filesystems;
    //! selected_filesystems filesystems selected by the scheduler
    std::vector<unsigned int>* selected_filesystems;
    //! file systems not to be used by the scheduler
    std::vector<unsigned int>* exclude_filesystems;

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    PlacementArguments()
        : spacename(0)
        , path(0)
        , grouptag(0)
        , lid(0)
        , inode(0)
        , plctpolicy(kScattered)
        , plctTrgGeotag()
        , truncate(false)
        , forced_scheduling_group_index(-1)
        , bookingsize(1024 * 1024 * 1024ll)
        , activity(eos::common::SchedActivity::kClient)
        , sched_strategy_cstr(nullptr)
        , vid(0)
        , alreadyused_filesystems(0)
        , selected_filesystems(0)
        , exclude_filesystems(0)
    {}

    //--------------------------------------------------------------------------
    //! Check if all placement argumets are valid
    //--------------------------------------------------------------------------
    bool isValid() const
    {
      return spacename && spacename->size() && path && lid && IsValidReplicas(lid) &&
             vid && alreadyused_filesystems && exclude_filesystems &&
             selected_filesystems;
    }

    //--------------------------------------------------------------------------
    //! Check if number of replicas/stripes is a valid value depending on the
    //! input layout value
    //!
    //! @param lid layout id
    //!
    //! @return true if valid, otherwise false
    //--------------------------------------------------------------------------
    bool
    IsValidReplicas(unsigned long lid) const
    {
      return common::LayoutId::GetStripeNumber(lid) + 1 < std::numeric_limits<uint8_t>::max();
    }

    // Strong Types to avoid misplaced function calls
    struct Path { const char* value; };
    struct GroupTag { const char* value; };
    struct Lid { unsigned long value; }; // LayoutId would conflict with eos::common
    struct BookingSize { unsigned long long value; };

    PlacementArguments& setFileParams(const std::string& p_space,
                                      Path p_path,
                                      GroupTag p_grouptag,
                                      Lid p_lid,
                                      ino64_t p_inode,
                                      BookingSize p_bookingsize,
                                      bool p_truncate,
                                      const common::VirtualIdentity& p_vid) {
      spacename = &p_space;
      path = p_path.value;
      grouptag = p_grouptag.value;
      lid = p_lid.value;
      inode = p_inode;
      vid = &p_vid;
      bookingsize = p_bookingsize.value;
      truncate = p_truncate;
      return *this;
    }

    PlacementArguments& setFsParams(std::vector<unsigned int>* p_alreadyused_filesystems,
                                    std::vector<unsigned int>* p_exclude_filesystems,
                                    std::vector<unsigned int>* p_selected_filesystems) {
      alreadyused_filesystems = p_alreadyused_filesystems;
      exclude_filesystems = p_exclude_filesystems;
      selected_filesystems = p_selected_filesystems;
      return *this;
    }

    PlacementArguments& setPlctParams(tPlctPolicy p_plctpolicy,
                                     const std::string* p_plctTrgGeotag,
                                     int p_forced_group_index,
                                     const char* p_sched_strategy_cstr) {
      plctpolicy = p_plctpolicy;
      plctTrgGeotag = p_plctTrgGeotag;
      forced_scheduling_group_index = p_forced_group_index;
      sched_strategy_cstr = p_sched_strategy_cstr;
      return *this;
    }

    PlacementArguments&
    setSchedActivity(eos::common::SchedActivity p_activity)
    {
      activity = p_activity;
      return *this;
    }
  };

  //----------------------------------------------------------------------------
  //! Take the decision where to place a new file in the system.
  //!
  //! @param args the structure holding all the input and output arguments
  //!
  //! @return 0 if placement successful, otherwise a non-zero value
  //!         ENOSPC - no space quota defined for current space
  //!
  //! NOTE: Has to be called with a lock on the FsView::gFsView::ViewMutex
  //----------------------------------------------------------------------------
  static int Placement(PlacementArguments* args);

  //----------------------------------------------------------------------------
  //! Resolve the traffic class of a request from the eos.schedclass opaque
  //! value it carries. The key is only honoured on a trusted identity - an
  //! sss-authenticated connection mapped to the daemon account, which is what
  //! every internal engine already uses - so a client cannot buy itself
  //! internal scheduling by simply typing the key. Absent, unrecognised or
  //! untrusted all resolve to client traffic; there is no way to *ask* for
  //! client class because it is the default.
  //!
  //! @param schedclass value of the eos.schedclass key, nullptr if absent
  //! @param vid virtual identity of the requester
  //!
  //! @return traffic class to schedule this request under
  //----------------------------------------------------------------------------
  static eos::common::SchedActivity
  SchedActivityFromRequest(const char* schedclass,
                           const eos::common::VirtualIdentity& vid);

  //----------------------------------------------------------------------------
  //! Schedule file placement using the given flat scheduler. This is the
  //! whole implementation, the overload above only supplies the MGM's
  //! scheduler instance - the seam is what makes the bridge unit-testable.
  //!
  //! @param args the structure holding all the input and output arguments
  //! @param fs_sched flat scheduler to place with
  //!
  //! @return 0 if placement successful, otherwise a non-zero value
  //----------------------------------------------------------------------------
  static int FlatSchedulerPlacement(PlacementArguments* args,
                                    placement::FsScheduler& fs_sched);

  //----------------------------------------------------------------------------
  //! Get the number of replicas that should stay in the client's geolocation,
  //! the rest are spread over the rest of the topology
  //!
  //! @param plctpolicy placement policy asked for
  //! @param lid layout identifier of the file
  //! @param has_geolocation whether the client has a geotag at all
  //!
  //! @return number of replicas to collocate, 0 if there is no preference
  //----------------------------------------------------------------------------
  static unsigned int GetCollocatedReplicas(tPlctPolicy plctpolicy, unsigned long lid,
                                            bool has_geolocation);

  //----------------------------------------------------------------------------
  //! File access structs and methods
  //----------------------------------------------------------------------------
  struct AccessArguments {
    /// INPUT
    //! forced filesystem for access
    unsigned long forcedfsid;
    //! forced space for access
    const char* forcedspace;
    //! cgi containing already tried hosts
    const std::string* tried_cgi;
    //! layout of the file
    unsigned long lid;
    //! file inode
    ino64_t inode;
    //! indicate pure read or rd/wr access
    bool isRW;
    //! size to book additionally for rd/wr access
    unsigned long long bookingsize;
    //! traffic class this access belongs to - client traffic unless an
    //! internal engine (drain, balance, conversion, fsck) asked for it
    eos::common::SchedActivity activity;
    //! virtual identity of the client
    const eos::common::VirtualIdentity* vid;
    //!filesystem ids where layout is stored
    const std::vector<unsigned int>* locationsfs;
    /// INPUT/OUTPUT
    //! return index pointing to layout entry filesystem
    unsigned long* fsindex;
    //! return filesystems currently unavailable
    std::vector<unsigned int>* unavailfs;
    //! filesystem ids to exclude from access scheduling (eos.excludefsid)
    const std::vector<uint32_t>* exclude_filesystems;

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    AccessArguments()
        : forcedfsid(0)
        , forcedspace(0)
        , tried_cgi()
        , lid(0)
        , inode(0)
        , isRW(false)
        , bookingsize(0)
        , activity(eos::common::SchedActivity::kClient)
        , vid(nullptr)
        , locationsfs(nullptr)
        , fsindex(nullptr)
        , unavailfs(nullptr)
        , exclude_filesystems(nullptr)
    {}

    //--------------------------------------------------------------------------
    //! Check if all access arguments are valid
    //--------------------------------------------------------------------------
    bool isValid() const
    {
      return lid && vid && locationsfs && fsindex && unavailfs;
    }

  };

  //----------------------------------------------------------------------------
  //! Take the decision from where to access a file.
  //!
  //! @param args the structure holding all the input and output arguments
  //!
  //! @return 0 if successful, otherwise a non-zero value
  //!
  //! NOTE: Has to be called with a lock on the FsView::gFsView::ViewMutex
  //----------------------------------------------------------------------------
  static int Access(AccessArguments* args);

  //----------------------------------------------------------------------------
  //! Schedule file access using the given flat scheduler. This is the whole
  //! implementation, the overload above only supplies the MGM's scheduler
  //! instance - the seam is what makes the bridge unit-testable.
  //!
  //! @param args the structure holding all the input and output arguments
  //! @param fs_sched flat scheduler to select with
  //!
  //! @return 0 if access successful, otherwise a non-zero value
  //----------------------------------------------------------------------------
  static int FlatSchedulerAccess(AccessArguments* args, placement::FsScheduler& fs_sched);

  //----------------------------------------------------------------------------
  //! Honour the eos.excludefsid exclusion list on an already selected access
  //! location: if the chosen file system is excluded, re-point fsindex to a
  //! location that is neither excluded nor unavailable
  //!
  //! @param args the structure holding all the input and output arguments,
  //!        fsindex is expected to hold the engine's selection
  //!
  //! @return 0 if a usable location is selected, ENODATA if the exclusion
  //!         list leaves none
  //----------------------------------------------------------------------------
  static int ApplyAccessExclusionFilter(AccessArguments* args);

  //----------------------------------------------------------------------------
  //! Get the writable placement capacity of a space in bytes, from whichever
  //! engine actually schedules it: the flat scheduler when the space is opted
  //! in, the geotree engine otherwise - the same routing the placement bridge
  //! applies. Replaces the direct GeoTreeEngine::placementSpace calls; the
  //! geotree branch goes when the engine does.
  //!
  //! @param spacename name of the space
  //!
  //! @return capacity in bytes, 0 when the space has none
  //----------------------------------------------------------------------------
  static uint64_t GetPlacementCapacity(const std::string& spacename);

  //----------------------------------------------------------------------------
  //! Select a drain destination file system for a single replica, from
  //! whichever engine schedules the space: the flat scheduler when the space is
  //! opted in, the geotree engine (draining policy) otherwise - the same
  //! routing Placement applies. A drain replica must stay in its source
  //! group and avoid both the file systems already holding the file and the
  //! ones the caller has already tried.
  //!
  //! @param spacename name of the space the file lives in
  //! @param group source scheduling group the replica has to stay in
  //! @param fid file identifier
  //! @param bookingsize size to book for the placement
  //! @param existing_repl file systems already holding a replica
  //! @param exclude_dsts destinations already tried by the caller
  //!
  //! @return selected file system id, or 0 if none could be scheduled
  //!
  //! NOTE: Has to be called with a lock on the FsView::gFsView::ViewMutex
  //----------------------------------------------------------------------------
  static eos::common::FileSystem::fsid_t
  PlaceDrainReplica(const std::string& spacename, FsGroup* group, unsigned long long fid,
                    unsigned long long bookingsize,
                    const std::vector<eos::common::FileSystem::fsid_t>& existing_repl,
                    const std::vector<eos::common::FileSystem::fsid_t>& exclude_dsts);

  //----------------------------------------------------------------------------
  //! Get placement policy from string representation
  //----------------------------------------------------------------------------
  static int PlctPolicyFromString(const std::string& placement)
  {
    if (placement == "scattered") {
      return kScattered;
    } else if (placement == "hybrid") {
      return kHybrid;
    } else if (placement == "gathered") {
      return kGathered;
    }

    return -1;
  }

  //----------------------------------------------------------------------------
  //! Reshuffle the selected file system ids by returning as first entry the
  //! lowest fsid if the sum of the fsids is odd, and the highest if the sum
  //! is even.
  //!
  //! @param selectedfs vector modified in place
  //----------------------------------------------------------------------------
  static void ReshuffleFs(std::vector<unsigned int>& selectedfs);

private:
  //----------------------------------------------------------------------------
  //! Schedule file placement using the MGM's flat scheduler instance. Internal
  //! convenience wrapper over the injectable overload above - it reaches into
  //! the global gOFS, which is why it is private while the injectable seam
  //! stays public for the unit tests.
  //!
  //! @param args the structure holding all the input and output arguments
  //!
  //! @return 0 if placement successful, otherwise a non-zero value
  //----------------------------------------------------------------------------
  static int FlatSchedulerPlacement(PlacementArguments* args);

  //----------------------------------------------------------------------------
  //! Schedule file access using the MGM's flat scheduler instance. Private for
  //! the same reason as FlatSchedulerPlacement above.
  //!
  //! @param args the structure holding all the input and output arguments
  //!
  //! @return 0 if access successful, otherwise a non-zero value
  //----------------------------------------------------------------------------
  static int FlatSchedulerAccess(AccessArguments* args);

  //----------------------------------------------------------------------------
  //! Schedule file placement using the legacy geotree engine. The fallback
  //! branch of Placement, kept behind the facade so no MGM code outside
  //! this class names an engine for a placement decision.
  //!
  //! @param args the structure holding all the input and output arguments
  //!
  //! @return 0 if placement successful, otherwise a non-zero value
  //----------------------------------------------------------------------------
  static int GeoTreePlacement(PlacementArguments* args);

  //----------------------------------------------------------------------------
  //! Schedule file access using the legacy geotree engine. The fallback branch
  //! of Access, kept behind the facade for the same reason.
  //!
  //! @param args the structure holding all the input and output arguments
  //!
  //! @return 0 if access successful, otherwise a non-zero value
  //----------------------------------------------------------------------------
  static int GeoTreeAccess(AccessArguments* args);

protected:
  static std::mutex sMapMutex; //< Mutex to protect the map below
  static std::map<std::string, FsGroup*> schedulingGroup;
};

namespace scheduler {

inline Scheduler::PlacementArguments::Path Path(const char* v) {
  return Scheduler::PlacementArguments::Path{v};
}

inline Scheduler::PlacementArguments::GroupTag GroupTag(const char* v) {
  return Scheduler::PlacementArguments::GroupTag{v};
}

inline Scheduler::PlacementArguments::Lid Lid(unsigned long v) {
  return Scheduler::PlacementArguments::Lid{v};
}

inline Scheduler::PlacementArguments::BookingSize BookingSize(unsigned long long v) {
  return Scheduler::PlacementArguments::BookingSize{v};
}


} // namespace scheduler

EOSMGMNAMESPACE_END
