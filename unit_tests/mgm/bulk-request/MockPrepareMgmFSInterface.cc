//------------------------------------------------------------------------------
//! @file MockPrepareMgmFSInterface.cc
//! @author Cedric Caffy - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "MockPrepareMgmFSInterface.hh"
#include <XrdSfs/XrdSfsFlags.hh>
#include "common/Constants.hh"

EOSBULKNAMESPACE_BEGIN

std::function<int(const char* path, XrdSfsFileExistence& file_exists, XrdOucErrInfo& error, const XrdSecEntity* client, const char* ininfo)>
MockPrepareMgmFSInterface::_EXISTS_FILE_EXISTS_LAMBDA =
  [](const char* path, XrdSfsFileExistence& file_exists, XrdOucErrInfo& error,
     const XrdSecEntity* client, const char* ininfo)
{
  file_exists = XrdSfsFileExistIsFile;
  return SFS_OK;
};

std::function<int(const char* path, XrdSfsFileExistence& file_exists, XrdOucErrInfo& error, const XrdSecEntity* client, const char* ininfo)>
MockPrepareMgmFSInterface::_EXISTS_FILE_DOES_NOT_EXIST_LAMBDA =
  [](const char* path, XrdSfsFileExistence& file_exists, XrdOucErrInfo& error,
     const XrdSecEntity* client, const char* ininfo)
{
  file_exists = XrdSfsFileExistNo;
  return SFS_ERROR;
};

std::function<int(const char* path, XrdSfsFileExistence& file_exists, XrdOucErrInfo& error, eos::common::VirtualIdentity& vid, const char* opaque, bool take_lock)>
MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA =
  [](const char* path, XrdSfsFileExistence& file_exists, XrdOucErrInfo& error,
     eos::common::VirtualIdentity& vid, const char* opaque, bool take_lock)
{
  file_exists = XrdSfsFileExistIsFile;
  return SFS_OK;
};

std::function<int(const char* path, XrdSfsFileExistence& file_exists, XrdOucErrInfo& error, eos::common::VirtualIdentity& vid, const char* opaque, bool take_lock)>
MockPrepareMgmFSInterface::_EXISTS_VID_FILE_DOES_NOT_EXIST_LAMBDA =
  [](const char* path, XrdSfsFileExistence& file_exists, XrdOucErrInfo& error,
     eos::common::VirtualIdentity& vid, const char* opaque, bool take_lock)
{
  file_exists = XrdSfsFileExistNo;
  return SFS_ERROR;
};

std::function<int(const char* path, XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid, const char* opaque, eos::IContainerMD::XAttrMap& map, bool links)>
MockPrepareMgmFSInterface::_ATTR_LS_STAGE_PREPARE_LAMBDA =
  [](const char* path, XrdOucErrInfo& out_error,
     const eos::common::VirtualIdentity& vid, const char* opaque,
     eos::IContainerMD::XAttrMap& map, bool links)
{
  map["sys.workflow.sync::prepare"] = "";
  return SFS_OK;
};

std::function<int(const char* path, XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid, const char* opaque, eos::IContainerMD::XAttrMap& map, bool links)>
MockPrepareMgmFSInterface::_ATTR_LS_ABORT_PREPARE_LAMBDA =
  [](const char* path, XrdOucErrInfo& out_error,
     const eos::common::VirtualIdentity& vid, const char* opaque,
     eos::IContainerMD::XAttrMap& map, bool links)
{
  map["sys.workflow.sync::abort_prepare"] = "";
  return SFS_OK;
};

std::function<int(const char* path, XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid, const char* opaque, eos::IContainerMD::XAttrMap& map, bool links)>
MockPrepareMgmFSInterface::_ATTR_LS_EVICT_PREPARE_LAMBDA =
  [](const char* path, XrdOucErrInfo& out_error,
     const eos::common::VirtualIdentity& vid, const char* opaque,
     eos::IContainerMD::XAttrMap& map, bool links)
{
  map["sys.workflow.sync::evict_prepare"] = "";
  return SFS_OK;
};

std::function<int(const char* path, XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid, const char* opaque, eos::IContainerMD::XAttrMap& map, bool links)>
MockPrepareMgmFSInterface::_ATTR_LS_QUERY_PREPARE_NO_ERROR_LAMBDA =
  [](const char* path, XrdOucErrInfo& out_error,
     const eos::common::VirtualIdentity& vid, const char* opaque,
     eos::IContainerMD::XAttrMap& map, bool links)
{
  map[common::RETRIEVE_ERROR_ATTR_NAME] = "";
  map[common::ARCHIVE_ERROR_ATTR_NAME] = "";
  return SFS_OK;
};

std::function<int(const char* path, XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid, const char* opaque, eos::IContainerMD::XAttrMap& map, bool links)>
MockPrepareMgmFSInterface::_ATTR_LS_RETRIEVE_ERROR_LAMBDA =
  [](const char* path, XrdOucErrInfo& out_error,
     const eos::common::VirtualIdentity& vid, const char* opaque,
     eos::IContainerMD::XAttrMap& map, bool links)
{
  map[common::RETRIEVE_ERROR_ATTR_NAME] = ERROR_RETRIEVE_STR;
  map[common::ARCHIVE_ERROR_ATTR_NAME] = "";
  map[common::RETRIEVE_REQID_ATTR_NAME] = RETRIEVE_REQ_ID;
  map[common::RETRIEVE_REQTIME_ATTR_NAME] = RETRIEVE_REQ_TIME;
  return SFS_OK;
};

std::function<int(const char* path, XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid, const char* opaque, eos::IContainerMD::XAttrMap& map, bool links)>
MockPrepareMgmFSInterface::_ATTR_LS_ARCHIVE_ERROR_LAMBDA =
  [](const char* path, XrdOucErrInfo& out_error,
     const eos::common::VirtualIdentity& vid, const char* opaque,
     eos::IContainerMD::XAttrMap& map, bool links)
{
  //No retrieve error if archive error
  map[common::ARCHIVE_ERROR_ATTR_NAME] = ERROR_ARCHIVE_STR;
  return SFS_OK;
};

std::function<int(const char* path, XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid, const char* opaque, eos::IContainerMD::XAttrMap& map, bool links)>
MockPrepareMgmFSInterface::_ATTR_LS_ARCHIVE_RETRIEVE_ERROR_LAMBDA =
  [](const char* path, XrdOucErrInfo& out_error,
     const eos::common::VirtualIdentity& vid, const char* opaque,
     eos::IContainerMD::XAttrMap& map, bool links)
{
  map[common::RETRIEVE_ERROR_ATTR_NAME] = ERROR_RETRIEVE_STR;
  map[common::ARCHIVE_ERROR_ATTR_NAME] = ERROR_ARCHIVE_STR;
  return SFS_OK;
};
std::function<int(const char* Name, struct stat* buf, XrdOucErrInfo& out_error, eos::common::VirtualIdentity& vid, const char* opaque, std::string* etag, bool follow, std::string* uri)>
MockPrepareMgmFSInterface::_STAT_FILE_ON_TAPE_ONLY =
  [](const char* Name, struct stat* buf, XrdOucErrInfo& out_error,
     eos::common::VirtualIdentity& vid, const char* opaque, std::string* etag,
     bool follow, std::string* uri)
{
  //File is on tape
  buf->st_rdev |= XRDSFS_HASBKUP;
  //File is not on disk
  buf->st_rdev |= XRDSFS_OFFLINE;
  return SFS_OK;
};

std::function<int(const char* Name, struct stat* buf, XrdOucErrInfo& out_error, eos::common::VirtualIdentity& vid, const char* opaque, std::string* etag, bool follow, std::string* uri)>
MockPrepareMgmFSInterface::_STAT_FILE_ON_DISK_ONLY =
  [](const char* Name, struct stat* buf, XrdOucErrInfo& out_error,
     eos::common::VirtualIdentity& vid, const char* opaque, std::string* etag,
     bool follow, std::string* uri)
{
  //File is on disk
  buf->st_rdev &= ~XRDSFS_OFFLINE;
  //File is not on tape
  buf->st_rdev &= ~XRDSFS_HASBKUP;
  return SFS_OK;
};

std::function<int(const char* Name, struct stat* buf, XrdOucErrInfo& out_error, eos::common::VirtualIdentity& vid, const char* opaque, std::string* etag, bool follow, std::string* uri)>
MockPrepareMgmFSInterface::_STAT_FILE_ON_DISK_AND_TAPE =
  [](const char* Name, struct stat* buf, XrdOucErrInfo& out_error,
     eos::common::VirtualIdentity& vid, const char* opaque, std::string* etag,
     bool follow, std::string* uri)
{
  //File is on tape
  buf->st_rdev |= XRDSFS_HASBKUP;
  //File is on disk
  buf->st_rdev &= ~XRDSFS_OFFLINE;
  return SFS_OK;
};

std::function<int(const char* Name, struct stat* buf, XrdOucErrInfo& out_error, eos::common::VirtualIdentity& vid, const char* opaque, std::string* etag, bool follow, std::string* uri)>
MockPrepareMgmFSInterface::_STAT_ERROR =
  [](const char* Name, struct stat* buf, XrdOucErrInfo& out_error,
     eos::common::VirtualIdentity& vid, const char* opaque, std::string* etag,
     bool follow, std::string* uri)
{
  out_error.setErrInfo(666, ERROR_STAT_STR.c_str());
  return SFS_ERROR;
};

std::function<int(const char* path, int mode, XrdOucErrInfo& error, eos::common::VirtualIdentity& vid, const char* info)>
MockPrepareMgmFSInterface::_ACCESS_FILE_NO_PREPARE_PERMISSION_LAMBDA =
  [](const char* path, int mode, XrdOucErrInfo& error,
     eos::common::VirtualIdentity& vid, const char* info)
{
  return SFS_ERROR;
};

std::function<int(const char* path, int mode, XrdOucErrInfo& error, eos::common::VirtualIdentity& vid, const char* info)>
MockPrepareMgmFSInterface::_ACCESS_FILE_PREPARE_PERMISSION_LAMBDA =
  [](const char* path, int mode, XrdOucErrInfo& error,
     eos::common::VirtualIdentity& vid, const char* info)
{
  return SFS_OK;
};
EOSBULKNAMESPACE_END