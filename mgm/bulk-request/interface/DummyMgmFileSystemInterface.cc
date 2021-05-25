//------------------------------------------------------------------------------
//! @file DummyMgmFileSystemInterface.cc
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

#include "DummyMgmFileSystemInterface.hh"

EOSMGMNAMESPACE_BEGIN

void DummyMgmFileSystemInterface::addStats(const char* tag, uid_t uid, gid_t gid, unsigned long val){

}

bool DummyMgmFileSystemInterface::isTapeEnabled()
{
  return true;
}

int DummyMgmFileSystemInterface::Emsg(const char* pfx, XrdOucErrInfo& einfo, int ecode, const char* op, const char* target){
  return 0;
}
int DummyMgmFileSystemInterface::_exists(const char* path, XrdSfsFileExistence& file_exists, XrdOucErrInfo& error, const XrdSecEntity* client, const char* ininfo){
  return 0;
}

int DummyMgmFileSystemInterface::_attr_ls(const char* path, XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid, const char* opaque, eos::IContainerMD::XAttrMap& map, bool take_lock, bool links){
  return 0;
}

int DummyMgmFileSystemInterface::_access(const char* path, int mode,XrdOucErrInfo& error, eos::common::VirtualIdentity& vid, const char* info, bool lock) {
  return 0;
}

int DummyMgmFileSystemInterface::FSctl(const int cmd, XrdSfsFSctl& args, XrdOucErrInfo& error, const XrdSecEntity* client){
  return 0;
}

EOSMGMNAMESPACE_END