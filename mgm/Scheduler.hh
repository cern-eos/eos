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

/**
 * @file   Scheduler.hh
 * 
 * @brief  Class implementing file scheduling e.g. access and placement
 * 
 * 
 */


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

class Scheduler
{
public:
  Scheduler ();
  virtual ~Scheduler ();

  enum tPlctPolicy {kScattered,kHybrid,kGathered};
  //! -------------------------------------------------------------
  //! the write placement routine
  //! -------------------------------------------------------------
  virtual int FilePlacement (const char* path, //< path to place
                             eos::common::Mapping::VirtualIdentity_t &vid, //< virtual id of client
                             const char* grouptag, //< group tag for placement
                             unsigned long lid, //< layout to be placed
                             std::vector<unsigned int> &avoid_filesystems, //< filesystems to avoid
                             std::vector<unsigned int> &selected_filesystems, //< return filesystems selected by scheduler
                             tPlctPolicy plctpolicy, //< indicates if the placement should be local or spread or hybrid
                             const std::string &plctTrgGeotag="", //< indicates close to which Geotag collocated stripes should be placed
                             bool truncate = false, //< indicates placement with truncation
                             int forced_scheduling_group_index = -1, //< forced index for the scheduling subgroup to be used 
                             unsigned long long bookingsize = 1024 * 1024 * 1024ll //< size to book for the placement
                             );

  //! -------------------------------------------------------------
  //! the read(/write) access routine
  //! -------------------------------------------------------------
  virtual int FileAccess (
                          eos::common::Mapping::VirtualIdentity_t &vid, //< virtual id of client
                          unsigned long forcedfsid, //< forced file system for access
                          const char* forcedspace, //< forced space for access
                          unsigned long lid, //< layout of the file
                          std::vector<unsigned int> &locationsfs, //< filesystem id's where layout is stored
                          unsigned long &fsindex, //< return index pointing to layout entry filesystem
                          bool isRW, //< indicating if pure read or read/write access
                          unsigned long long bookingsize, //< size to book additionally for read/write access
                          std::vector<unsigned int> &unavailfs, //< return filesystems currently unavailable
                          eos::common::FileSystem::fsstatus_t min_fsstatus = eos::common::FileSystem::kDrain //< defines minimum filesystem state to allow filesystem selection
                          );

protected:
  XrdSysMutex schedulingMutex; //< protect the following scheduling state maps

  std::map<std::string, FsGroup*> schedulingGroup; //< points to the current scheduling group where to start scheduling =>  std::string = <grouptag>|<uid>:<gid> 
  std::map<std::string, eos::common::FileSystem::fsid_t > schedulingFileSystem; //< points to the current feilsystem where to start scheduling

  XrdOucString SpaceName; //< the name of the space where the scheduling object is attached

};

EOSMGMNAMESPACE_END

#endif