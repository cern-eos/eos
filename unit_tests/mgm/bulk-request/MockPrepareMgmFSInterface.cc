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

EOSBULKNAMESPACE_BEGIN

std::function<int(const char* path, XrdSfsFileExistence& file_exists, XrdOucErrInfo& error, const XrdSecEntity* client, const char* ininfo)>
    MockPrepareMgmFSInterface::_EXISTS_FILE_EXISTS_LAMBDA =
      [](const char* path, XrdSfsFileExistence& file_exists, XrdOucErrInfo& error, const XrdSecEntity* client, const char* ininfo){
        file_exists = XrdSfsFileExistIsFile;
        return SFS_OK;
      };

std::function<int(const char* path, XrdSfsFileExistence& file_exists, XrdOucErrInfo& error, const XrdSecEntity* client, const char* ininfo)>
    MockPrepareMgmFSInterface::_EXISTS_FILE_DOES_NOT_EXIST_LAMBDA =
    [](const char* path, XrdSfsFileExistence& file_exists, XrdOucErrInfo& error, const XrdSecEntity* client, const char* ininfo){
      file_exists = XrdSfsFileExistNo;
      return SFS_ERROR;
    };

std::function<int(const char* path, XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid, const char* opaque, eos::IContainerMD::XAttrMap& map, bool take_lock, bool links)>
    MockPrepareMgmFSInterface::_ATTR_LS_STAGE_PREPARE_LAMBDA =
    [](const char* path, XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid, const char* opaque, eos::IContainerMD::XAttrMap& map, bool take_lock, bool links){
      map["sys.workflow.sync::prepare"] = "";
      return SFS_OK;
    };

std::function<int(const char* path, XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid, const char* opaque, eos::IContainerMD::XAttrMap& map, bool take_lock, bool links)>
    MockPrepareMgmFSInterface::_ATTR_LS_ABORT_PREPARE_LAMBDA =
    [](const char* path, XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid, const char* opaque, eos::IContainerMD::XAttrMap& map, bool take_lock, bool links){
      map["sys.workflow.sync::abort_prepare"] = "";
      return SFS_OK;
    };

std::function<int(const char* path, XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid, const char* opaque, eos::IContainerMD::XAttrMap& map, bool take_lock, bool links)>
    MockPrepareMgmFSInterface::_ATTR_LS_EVICT_PREPARE_LAMBDA =
    [](const char* path, XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid, const char* opaque, eos::IContainerMD::XAttrMap& map, bool take_lock, bool links){
      map["sys.workflow.sync::evict_prepare"] = "";
      return SFS_OK;
    };

std::function<int(const char* Name, struct stat* buf, XrdOucErrInfo& out_error, eos::common::VirtualIdentity& vid, const char* opaque, std::string* etag, bool follow, std::string* uri)>
    MockPrepareMgmFSInterface::_STAT_FILE_ON_TAPE_ONLY =
    [](const char* Name, struct stat* buf, XrdOucErrInfo& out_error, eos::common::VirtualIdentity& vid, const char* opaque, std::string* etag, bool follow, std::string* uri){
      return SFS_OK;
    };

std::function<int(const char* Name, struct stat* buf, XrdOucErrInfo& out_error, eos::common::VirtualIdentity& vid, const char* opaque, std::string* etag, bool follow, std::string* uri)>
    MockPrepareMgmFSInterface::_STAT_FILE_ON_DISK_ONLY =
    [](const char* Name, struct stat* buf, XrdOucErrInfo& out_error, eos::common::VirtualIdentity& vid, const char* opaque, std::string* etag, bool follow, std::string* uri){
      return SFS_OK;
    };

std::function<int(const char* Name, struct stat* buf, XrdOucErrInfo& out_error, eos::common::VirtualIdentity& vid, const char* opaque, std::string* etag, bool follow, std::string* uri)>
    MockPrepareMgmFSInterface::_STAT_FILE_ON_DISK_AND_TAPE =
    [](const char* Name, struct stat* buf, XrdOucErrInfo& out_error, eos::common::VirtualIdentity& vid, const char* opaque, std::string* etag, bool follow, std::string* uri){
      return SFS_OK;
    };
EOSBULKNAMESPACE_END