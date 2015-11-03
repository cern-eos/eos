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
  enum tPlctPolicy {kScattered, kHybrid, kGathered};

  //----------------------------------------------------------------------------
  //! Write placement routine
  //!
  //! @param path path to place
  //! @param vid virtual id of the client
  //! @param grouptag group tag for placement
  //! @param lid layout to be placed
  //! @param alreadyused_filesystems filesystems to avoid
  //! @param selected_filesystems return filesystems selected by the scheduler
  //! @param plctpolicy type of placement local/spread/hybrid
  //! @param plctTrgGeotag geotag where collocated stripes should be placed
  //! @param truncate indicates placement with truncation
  //! @param forced_scheduling_group_index forced index for the scheduling
  //!               group to be used
  //! @param bookingsize size to book for the placement
  //!
  //! @return
  //----------------------------------------------------------------------------
  virtual int FilePlacement(const char* path,
			    eos::common::Mapping::VirtualIdentity_t& vid,
			    const char* grouptag,
			    unsigned long lid,
			    std::vector<unsigned int>& alreadyused_filesystems,
			    std::vector<unsigned int>& selected_filesystems,
			    tPlctPolicy plctpolicy,
			    const std::string& plctTrgGeotag = "",
			    bool truncate = false,
			    int forced_scheduling_group_index = -1,
			    unsigned long long bookingsize = 1024 * 1024 * 1024ll);

  //----------------------------------------------------------------------------
  //! Read(/write) access routine
  //!
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
  //! @return
  //----------------------------------------------------------------------------
  virtual int FileAccess(eos::common::Mapping::VirtualIdentity_t& vid,
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

protected:

  XrdOucString SpaceName; //< space name where the scheduling object is attached
  XrdSysMutex schedulingMutex; //< protect the following scheduling state maps

  //! Points to the current scheduling group where to start scheduling =>
  //! std::string = <grouptag>|<uid>:<gid>
  std::map<std::string, FsGroup*> schedulingGroup;
  //! Points to the current feilsystem where to start scheduling
  std::map<std::string, eos::common::FileSystem::fsid_t> schedulingFileSystem;
};

EOSMGMNAMESPACE_END

#endif
