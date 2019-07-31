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

#ifndef __EOSMGM_SCHEDULER__HH__
#define __EOSMGM_SCHEDULER__HH__

/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "common/FileSystem.hh"
#include "common/LayoutId.hh"
#include "mgm/Namespace.hh"
#include "mgm/FsView.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class implementing file scheduling e.g. access and placement
//------------------------------------------------------------------------------
class Scheduler
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Scheduler();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~Scheduler();

  //! Types of placement policy
  enum tPlctPolicy
  { kScattered, kHybrid, kGathered };

  //! Types of scheduling
  enum tSchedType
  { regular, draining };

  //! Arguments to place a file placement
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
    //! indicate if this is a request for regular, draining or balancing placement
    tSchedType schedtype;
    //! virtual identity of the client
    const eos::common::VirtualIdentity* vid;
    /// INPUT/OUTPUT
    //! filesystems to avoid
    std::vector<unsigned int>* alreadyused_filesystems;
    //! selected_filesystems filesystems selected by the scheduler
    std::vector<unsigned int>* selected_filesystems;
    //! file systems not to be used by the scheduler
    std::vector<unsigned int>* exclude_filesystems;
    //! if non NULL, schedule dataproxys for each fs if proxygroups are defined (empty string if not defined)
    std::vector<std::string>* dataproxys;
    //! if non NULL, schedule a firewall entry point for each fs
    std::vector<std::string>* firewallentpts;

    PlacementArguments() :
      spacename(0),
      path(0),
      grouptag(0),
      lid(0),
      inode(0),
      plctpolicy(kScattered),
      plctTrgGeotag(),
      truncate(false),
      forced_scheduling_group_index(-1),
      bookingsize(1024 * 1024 * 1024ll),
      schedtype(regular),
      vid(0),
      alreadyused_filesystems(0),
      selected_filesystems(0),
      exclude_filesystems(0),
      dataproxys(0),
      firewallentpts(0)
    {}

    bool isValid() const
    {
      return
        spacename && spacename->size()
        && path
        && lid
        && vid
        && alreadyused_filesystems
        && exclude_filesystems
        && selected_filesystems;
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
  static int FilePlacement(PlacementArguments* args);

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
    //! indicate if this is a request for regular, draining or balancing access
    tSchedType schedtype;
    //! virtual identity of the client
    const eos::common::VirtualIdentity* vid;
    /// INPUT/OUTPUT
    //!filesystem ids where layout is stored
    std::vector<unsigned int>* locationsfs;
    //! if non NULL, schedule dataproxys for each fs if proxygroups are defined (empty string if not defined)
    std::vector<std::string>* dataproxys;
    //! firewallentpts if non NULL, schedule a firewall entry point for each fs
    std::vector<std::string>* firewallentpts;
    //! return index pointing to layout entry filesystem
    unsigned long* fsindex;
    //! return filesystems currently unavailable
    std::vector<unsigned int>* unavailfs;

    AccessArguments() :
      forcedfsid(0),
      forcedspace(0),
      tried_cgi(),
      lid(0),
      inode(0),
      isRW(false),
      bookingsize(0),
      schedtype(regular),
      vid(NULL),
      locationsfs(NULL),
      dataproxys(NULL),
      firewallentpts(NULL),
      fsindex(NULL),
      unavailfs(NULL)
    {}

    bool isValid() const
    {
      return
        lid
        && vid
        && locationsfs
        && fsindex
        && unavailfs;
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
  static int FileAccess(AccessArguments* args);

  //----------------------------------------------------------------------------
  //! Translate placement policy type to string
  //----------------------------------------------------------------------------
  static const char* PlctPolicyString(tPlctPolicy plctPolicy)
  {
    if (plctPolicy == kScattered) {
      return "scattered";
    } else if (plctPolicy == kHybrid) {
      return "hybrid";
    } else if (plctPolicy == kGathered) {
      return "gathered";
    } else {
      return "none";
    }
  }

  //----------------------------------------------------------------------------
  //! Return placement policy from string representation
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

protected:

  static XrdSysMutex pMapMutex; //< protect the following scheduling state maps

  //! Points to the current scheduling group where to start scheduling =>
  //! std::string = <grouptag>|<uid>:<gid>
  static std::map<std::string, FsGroup*> schedulingGroup;
};

EOSMGMNAMESPACE_END

#endif
