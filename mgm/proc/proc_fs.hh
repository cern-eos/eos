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
#include "namespace/interface/IFileMD.hh"
#include "XrdSec/XrdSecEntity.hh"
#include <set>
#include <list>

namespace eos::mq
{
class MessagingRealm;
}


EOSMGMNAMESPACE_BEGIN

//! Type of entity we are dealing with during an fs operation
enum EntityType {
  UNKNOWN = 0x00, // unknown entity
  FS      = 0x01, // file system
  GROUP   = 0x10, // eos space
  SPACE   = 0x11, // eos group
  NODE    = 0x1000 // node
};

//! Type of accepted operations for fs mv command
enum class MvOpType {
  UNKNOWN     = 0x0000,
  FS_2_GROUP  = (EntityType::FS << 2) | EntityType::GROUP,
  FS_2_SPACE  = (EntityType::FS << 2) | EntityType::SPACE,
  GRP_2_SPACE = (EntityType::GROUP << 2) | EntityType::SPACE,
  SPC_2_SPACE = (EntityType::SPACE << 2) | EntityType::SPACE,
  FS_2_NODE   = (EntityType::FS << 2) | EntityType::NODE
};

//------------------------------------------------------------------------------
//! Dump metadata held on filesystem
//!
//! @param sfsid id of the filesystem
//! @param option output format option (can be default or monitor)
//! @param dp display path flag
//! @param df display fid flag
//! @param ds display size flag
//! @param stdOut normal output string
//! @param stdErr error output string
//! @param vid_in virtual identity of the client
//! @param entries counts the number of entries
//!
//! @return 0 if successful, otherwise error code value
//------------------------------------------------------------------------------
int proc_fs_dumpmd(std::string& sfsid, XrdOucString& option, XrdOucString& dp,
                   XrdOucString& df, XrdOucString& ds, XrdOucString& stdOut,
                   XrdOucString& stdErr, eos::common::VirtualIdentity& vid_in,
                   size_t& entries);

//------------------------------------------------------------------------------
//! Set filesystem configuration parameter
//!
//! @param identifier filesystem identifier (can be id, uuid)
//! @param key parameter identifier
//! @param value parameter value
//! @param stdOut normal output string
//! @param stdErr error output string
//! @param vid_in virtual identity of the client
//! @param statusComment comment detailing last config status change
//!
//! @return 0 if successful, otherwise error code value
//------------------------------------------------------------------------------
int proc_fs_config(std::string& identifier, std::string& key,
                   std::string& value,
                   XrdOucString& stdOut, XrdOucString& stdErr,
                   eos::common::VirtualIdentity& vid_in,
                   const std::string& statusComment = "");

//------------------------------------------------------------------------------
//! Add a new filesystem
//!
//! @param realm messaging realm
//! @param sfsid id of the filesystem
//! @param uuid uuid of the filesystem
//! @param nodename node identifier in the node queue
//! @param mountpoint location where to mount the filesystem
//! @param space assigned scheduling space
//! @param configstatus assigned config status
//! @param stdOut normal output string
//! @param stdErr error output string
//! @param vid_in virtual identity of the client
//!
//! @return 0 if successful, otherwise error code value
//------------------------------------------------------------------------------
int proc_fs_add(mq::MessagingRealm* realm, std::string& sfsid,
                std::string& uuid,
                std::string& nodename, std::string& mountpoint, std::string& space,
                std::string& configstatus, XrdOucString& stdOut,
                XrdOucString& stdErr,
                eos::common::VirtualIdentity& vid_in);

//------------------------------------------------------------------------------
//! Remove a filesystem
//!
//! @param nodename node identifier in the node queue
//! @param mountpoint location where to mount the filesystem
//! @param id id of the filesystem
//! @param stdOut normal output string
//! @param stdErr error output string
//! @param vid_in virtual identify of the client
//!
//! @return 0 if successful, otherwise error code value
//------------------------------------------------------------------------------
int proc_fs_rm(std::string& nodename, std::string& mountpoint, std::string& id,
               XrdOucString& stdOut, XrdOucString& stdErr,
               eos::common::VirtualIdentity& vid_in);

//------------------------------------------------------------------------------
//! Clear unlinked files from the filesystem
//!
//! @param fsid filesystem id
//! @param out normal output string
//! @param err error output string
//! @param vid_in virtual identify of the client
//!
//! @return 0 if successful, otherwise error code value
//------------------------------------------------------------------------------
int proc_fs_dropdeletion(const eos::common::FileSystem::fsid_t& id,
                         const eos::common::VirtualIdentity& vid_in,
                         std::string& out, std::string& err);

//------------------------------------------------------------------------------
//! Drop ghost entries from a filesystem view (file ids without meta data objects)
//!
//! @param fsid filesystem id
//! @param fids explicit set of fids to be checked and dropped if they are
//!        ghosts entries
//! @param out normal output string
//! @param err error output string
//! @param vid_in virtual identify of the client
//!
//! @return 0 if successful, otherwise error code value
//------------------------------------------------------------------------------
int proc_fs_dropghosts(const eos::common::FileSystem::fsid_t& fsid,
                       const std::set<eos::IFileMD::id_t>& fids,
                       const eos::common::VirtualIdentity& vid_in,
                       std::string& out, std::string& err);


//------------------------------------------------------------------------------
//! Get type of entity. It can either be a filesystem, an eos group or an eos
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
//! Move a filesystem operation
//!
//! @param src file system/group/space to move
//! @param dst destination
//! @param stdOut output string
//! @param stdErr error output string
//! @param force allows to move non-empty filesystems
//! @param realm messaging realm
//!
//! @return 0 if successful, otherwise error code
//------------------------------------------------------------------------------
int proc_fs_mv(std::string& src, std::string& dst, XrdOucString& stdOut,
               XrdOucString& stdErr,
               eos::common::VirtualIdentity& vid_in,
               bool force,
	       mq::MessagingRealm* realm = 0);


//------------------------------------------------------------------------------
//! Check if a file system can be moved. It needs to be active and in RW mode.
//! @note needs to be called with FsView::ViewMutex locked
//!
//! @param fs file system object
//! @param dst destination
//! @param stdOut output string
//! @param stdErr error output string
//! @param force allows to move non-empty filesystems
//!
//! @return true if can be moved, otherwise false
//------------------------------------------------------------------------------
bool proc_fs_can_mv(eos::mgm::FileSystem* fs, const std::string& dst,
                    XrdOucString& stdOut, XrdOucString& stdErr, bool force);

//------------------------------------------------------------------------------
//! Move a filesystem to a group
//! @note needs to be called with FsView::ViewMutex locked
//!
//! @param fs_view file system view handler
//! @param src file system to move
//! @param dst destination group
//! @param stdOut output string
//! @param stdErr error output string
//! @param force allows to move non-empty filesystems
//!
//! @return 0 if successful, otherwise error code
//------------------------------------------------------------------------------
int proc_mv_fs_group(FsView& fs_view, const std::string& src,
                     const std::string& dst, XrdOucString& stdOut,
                     XrdOucString& stdErr, bool force);

//------------------------------------------------------------------------------
//! Move a filesystem to a space
//! @note needs to be called with FsView::ViewMutex locked
//!
//! @param fs_view file system view handler
//! @param src file system to move
//! @param dst destination space
//! @param stdOut output string
//! @param stdErr error output string
//! @param force allows to move non-empty filesystems
//!
//! @return 0 if successful, otherwise error code
//------------------------------------------------------------------------------
int proc_mv_fs_space(FsView& fs_view, const std::string& src,
                     const std::string& dst, XrdOucString& stdOut,
                     XrdOucString& stdErr, bool force);

//------------------------------------------------------------------------------
//! Move a group to a space
//! @note needs to be called with FsView::ViewMutex locked
//
//! @param fs_view file system view handler
//! @param src group to move
//! @param dst destination space
//! @param stdOut output string
//! @param stdErr error output string
//! @param force allows to move non-empty filesystems
//!
//! @return 0 if successful, otherwise error code
//------------------------------------------------------------------------------
int proc_mv_grp_space(FsView& fs_view, const std::string& src,
                      const std::string& dst, XrdOucString& stdOut,
                      XrdOucString& stdErr, bool force);

//------------------------------------------------------------------------------
//! Move a space to a space
//! @note needs to be called with FsView::ViewMutex locked
//!
//! @param fs_view file system view handler
//! @param src group to move
//! @param dst destination space
//! @param stdOut output string
//! @param stdErr error output string
//! @param force allows to move non-empty filesystems
//!
//! @return 0 if successful, otherwise error code
//------------------------------------------------------------------------------
int proc_mv_space_space(FsView& fs_view, const std::string& src,
                        const std::string& dst, XrdOucString& stdOut,
                        XrdOucString& stdErr, bool force);


//------------------------------------------------------------------------------
//! Move a filesystem between nodes
//! @note needs to be called with FsView::ViewMutex locked
//!
//! @param fs_view file system view handler
//! @param src filesyste to move
//! @param dst destination node
//! @param stdOut output string
//! @param stdErr error output string
//! @param force currently unused
//!
//! @return 0 if successful, otherwise error code
//------------------------------------------------------------------------------
int proc_mv_fs_node(FsView& fs_view, const std::string& src,
		    const std::string& dst, XrdOucString& stdOut,
		    XrdOucString& stdErr, bool force,
		    eos::common::VirtualIdentity& vid_in,
		    mq::MessagingRealm* realm);

//------------------------------------------------------------------------------
//! Sort the groups in a space by priority - the first ones are the ones that
//! are most suitable to add a new file system to them.
//!
//! @param fs_view file system view handler
//! @param space space from which to sort the groups
//! @param grp_size maximum number of file systems per group
//! @param grp_mod maximum number of groups in current space
//!
//! @return sorted list of groups with the most desirable one in the beginning
//------------------------------------------------------------------------------
std::list<std::string>
proc_sort_groups_by_priority(FsView& fs_view, const std::string& space,
                             size_t grp_size, size_t grp_mod);

EOSMGMNAMESPACE_END
#endif
