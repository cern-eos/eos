//------------------------------------------------------------------------------
//! @file RealMgmFileSystemInterface.cc
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

#include "RealMgmFileSystemInterface.hh"
#include "mgm/Stat.hh"
#include "mgm/Iostat.hh"

EOSBULKNAMESPACE_BEGIN

RealMgmFileSystemInterface::RealMgmFileSystemInterface(XrdMgmOfs* mgmOfs):
  mMgmOfs(mgmOfs) {}

void RealMgmFileSystemInterface::addStats(const char* tag, uid_t uid, gid_t gid,
    unsigned long val)
{
  mMgmOfs->MgmStats.Add(tag, uid, gid, val);
}

bool RealMgmFileSystemInterface::isTapeEnabled()
{
  return mMgmOfs->mTapeEnabled;
}

int RealMgmFileSystemInterface::Emsg(const char* pfx, XrdOucErrInfo& einfo,
                                     int ecode, const char* op, const char* target)
{
  return mMgmOfs->Emsg(pfx, einfo, ecode, op, target);
}
int RealMgmFileSystemInterface::_exists(const char* path,
                                        XrdSfsFileExistence& file_exists, XrdOucErrInfo& error,
                                        const XrdSecEntity* client, const char* ininfo)
{
  return mMgmOfs->_exists(path, file_exists, error, client, ininfo);
}
int RealMgmFileSystemInterface::_exists(const char* path,
                                        XrdSfsFileExistence& file_exists, XrdOucErrInfo& error,
                                        eos::common::VirtualIdentity& vid, const char* opaque, bool take_lock)
{
  return mMgmOfs->_exists(path, file_exists, error, vid, opaque, take_lock);
}

int RealMgmFileSystemInterface::_attr_ls(const char* path,
    XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid,
    const char* opaque, eos::IContainerMD::XAttrMap& map,
    bool links)
{
  return mMgmOfs->_attr_ls(path, out_error, vid, opaque, map, links);
}

int RealMgmFileSystemInterface::_access(const char* path, int mode,
                                        XrdOucErrInfo& error, eos::common::VirtualIdentity& vid, const char* info)
{
  return mMgmOfs->_access(path, mode, error, vid, info);
}

int RealMgmFileSystemInterface::FSctl(const int cmd, XrdSfsFSctl& args,
                                      XrdOucErrInfo& error, const XrdSecEntity* client)
{
  return mMgmOfs->FSctl(cmd, args, error, client);
}

int RealMgmFileSystemInterface::_stat(const char* path, struct stat* buf,
                                      XrdOucErrInfo& error, eos::common::VirtualIdentity& vid, const char* ininfo,
                                      std::string* etag, bool follow, std::string* uri)
{
  return mMgmOfs->_stat(path, buf, error, vid, ininfo, etag, follow, uri);
}

void RealMgmFileSystemInterface::_stat_set_flags(struct stat* buf)
{
  mMgmOfs->_stat_set_flags(buf);
}

std::string RealMgmFileSystemInterface::get_logId()
{
  return std::string(mMgmOfs->logId);
}

std::string RealMgmFileSystemInterface::get_host()
{
  if (mMgmOfs->MgmOfsAlias.length()) {
    return std::string(mMgmOfs->MgmOfsAlias.c_str());
  } else if (mMgmOfs->HostName != nullptr) {
    return std::string(mMgmOfs->HostName);
  } else {
    return "unknown";
  }
}

void RealMgmFileSystemInterface::writeEosReportRecord(const std::string&
    record)
{
  if (mMgmOfs->IoStats) {
    mMgmOfs->IoStats->WriteRecord(record);
  }
}

EOSBULKNAMESPACE_END