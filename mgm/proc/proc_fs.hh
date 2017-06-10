//------------------------------------------------------------------------------
//! @file proc_fs.hh
//! @author Andreas-Joachim Peters - CERN & Ivan Arizanovic - Comtrade
//------------------------------------------------------------------------------

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

#ifndef __EOSMGM_PROC_FS__HH__
#define __EOSMGM_PROC_FS__HH__

#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/Mapping.hh"
#include "mgm/FileSystem.hh"
#include "mgm/FsView.hh"
#include "XrdSec/XrdSecEntity.hh"

EOSMGMNAMESPACE_BEGIN

//! Type of entity we are dealing with during an fs operation
enum EntityType {
  UNKNOWN = 0x00,  // unknown entity
  FS      = 0x01, // file system
  GROUP   = 0x10, // eos space
  SPACE   = 0x11  // eos group
};

//! Type of accepted operations for fs mv command
enum class MvOpType {
  UNKNOWN     = 0x0000,
  FS_2_GROUP  = (EntityType::FS << 2) | EntityType::GROUP,
  FS_2_SPACE  = (EntityType::FS << 2) | EntityType::SPACE,
  GRP_2_SPACE = (EntityType::GROUP << 2) | EntityType::SPACE,
  SPC_2_SPACE = (EntityType::SPACE << 2) | EntityType::SPACE
};

//------------------------------------------------------------------------------
//! Dump metadata held on filesystem
//------------------------------------------------------------------------------
int proc_fs_dumpmd(std::string& fsidst, XrdOucString& option, XrdOucString& dp,
                   XrdOucString& df, XrdOucString& ds, XrdOucString& stdOut,
                   XrdOucString& stdErr, std::string& tident,
                   eos::common::Mapping::VirtualIdentity& vid_in,
                   size_t& entries);

//------------------------------------------------------------------------------
//! Dump metada held on filesystem
//------------------------------------------------------------------------------
int proc_fs_config(std::string& identifier, std::string& key,
                   std::string& value,
                   XrdOucString& stdOut, XrdOucString& stdErr, std::string& tident,
                   eos::common::Mapping::VirtualIdentity& vid_in);

//------------------------------------------------------------------------------
//! Dump metada held on filesystem
//------------------------------------------------------------------------------
int proc_fs_add(std::string& sfsid, std::string& uuid, std::string& nodename,
                std::string& mountpoint, std::string& space,
                std::string& configstatus, XrdOucString& stdOut,
                XrdOucString& stdErr, std::string& tident,
                eos::common::Mapping::VirtualIdentity& vid_in);

//------------------------------------------------------------------------------
//! Find a filesystem in the source_group which in not part of the target_group
//!
//! @param source_group
//! @param target_group
//!
//! @return file system object if found, otherwise null
//------------------------------------------------------------------------------
FileSystem* proc_fs_source(std::string source_group, std::string target_group);

//------------------------------------------------------------------------------
//! Find best suited scheduling group for a new filesystem
//!
//! @param target_group can be a space or a space and group specification
//!
//! @return space and group where filesystem can be added. If none found then
//!         a random one is returned
//------------------------------------------------------------------------------
std::string proc_fs_target(std::string target_group);

//------------------------------------------------------------------------------
//! Move filesystem to the best group possible in the required space.
//!
//! @param fs filesystem object
//! @param space space name
//!
//! @return group where the filesystem can be moved
//------------------------------------------------------------------------------
std::string proc_fs_mv_bestgroup(FileSystem* fs, std::string space);

//------------------------------------------------------------------------------
//! Move a filesystem
//------------------------------------------------------------------------------
int proc_fs_mv(std::string& src, std::string& dst, XrdOucString& stdOut,
               XrdOucString& stdErr, std::string& tident,
               eos::common::Mapping::VirtualIdentity& vid_in);

//------------------------------------------------------------------------------
//! Remove a filesystem
//!
//! @param nodename
//! @param mountpont
//! @param id
//! @param stdOut normal output string
//! @param stdErr error output string
//! @param tident identity of the client
//! @param vid_in virtual identify of the client
//!
//! @return 0 if successful, otherwise error code value
//------------------------------------------------------------------------------
int proc_fs_rm(std::string& nodename, std::string& mountpoint, std::string& id,
               XrdOucString& stdOut, XrdOucString& stdErr, std::string& tident,
               eos::common::Mapping::VirtualIdentity& vid_in);

//------------------------------------------------------------------------------
//! Clear unlinked files from the filesystem
//!
//! @param id id of the filesystem
//! @param stdOut normal output string
//! @param stdErr error output string
//! @param tident identity of the client
//! @param vid_in virtual identify of the client
//!
//! @return 0 if successful, otherwise error code value
//------------------------------------------------------------------------------
int proc_fs_dropdeletion(std::string& id, XrdOucString& stdOut,
                         XrdOucString& stdErr, std::string& tident,
                         eos::common::Mapping::VirtualIdentity& vid_in);


//------------------------------------------------------------------------------
//! Get type of entity. It can either be a filesyste, an eos group or an eos
//! space.
//!
//! @param input string given as input
//! @param stdOut normal output string
//! @param stdErr error output string
//!
//! @return type of entity
//------------------------------------------------------------------------------
EntityType get_entity_type(const std::string& input, XrdOucString& stdOut,
                           XrdOucString& stdErr);

//------------------------------------------------------------------------------
//! Get operation type based on input entity types
//!
//! @param input1 first input
//! @param input2 second input
//! @param stdOut normal output string
//! @param stdErr error output string
//!
//! @return operation type
//------------------------------------------------------------------------------
MvOpType get_operation_type(const std::string& in1, const std::string& in2,
                            XrdOucString& stdOut, XrdOucString& stdErr);

//------------------------------------------------------------------------------
//! Move a filesystem to a group
//! @note needs to be called with FsView::ViewMutex locked
//!
//! @param fs_view file system view handler
//! @param src file system to move
//! @param dst destination group
//! @param stdOut output string
//! @param stdErr error output string
//!
//! @return 0 if successful, otherwise error code
//------------------------------------------------------------------------------
int proc_mv_fs_group(FsView& fs_view, const std::string& src,
                     const std::string& dst, XrdOucString& stdOut,
                     XrdOucString& stdErr);

//------------------------------------------------------------------------------
//! Move a filesystem to a space
//! @note needs to be called with FsView::ViewMutex locked
//!
//! @param fs_view file system view handler
//! @param src file system to move
//! @param dst destination space
//! @param stdOut output string
//! @param stdErr error output string
//!
//! @return 0 if successful, otherwise error code
//------------------------------------------------------------------------------
int proc_mv_fs_space(FsView& fs_view, const std::string& src,
                     const std::string& dst, XrdOucString& stdOut,
                     XrdOucString& stdErr);

//------------------------------------------------------------------------------
//! Move a group to a space
//! @note needs to be called with FsView::ViewMutex locked
//
//! @param fs_view file system view handler
//! @param src group to move
//! @param dst destination space
//! @param stdOut output string
//! @param stdErr error output string
//!
//! @return 0 if successful, otherwise error code
//------------------------------------------------------------------------------
int proc_mv_grp_space(FsView& fs_view, const std::string& src,
                      const std::string& dst, XrdOucString& stdOut,
                      XrdOucString& stdErr);

//------------------------------------------------------------------------------
//! Move a space to a space
//! @note needs to be called with FsView::ViewMutex locked
//!
//! @param fs_view file system view handler
//! @param src group to move
//! @param dst destination space
//! @param stdOut output string
//! @param stdErr error output string
//!
//! @return 0 if successful, otherwise error code
//------------------------------------------------------------------------------
int proc_mv_space_space(FsView& fs_view, const std::string& src,
                        const std::string& dst, XrdOucString& stdOut,
                        XrdOucString& stdErr);

//------------------------------------------------------------------------------
//! Sort the groups in a space by priority - the first ones are the ones that
//! are most suitable to add a new file system to them.
//!
//! @param fs_view file system view handler
//! @param space space from which to sort the groups
//!
//! @return sorted list of groups with the most desirable one in the beginning
//------------------------------------------------------------------------------
std::list<std::string>
proc_sort_groups_by_priority(FsView& fs_view, const std::string& space);

EOSMGMNAMESPACE_END
#endif
