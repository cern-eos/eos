//------------------------------------------------------------------------------
//! @file DummyMgmFileSystemInterface.hh
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

#ifndef EOS_DUMMYMGMFILESYSTEMINTERFACE_HH
#define EOS_DUMMYMGMFILESYSTEMINTERFACE_HH

#include "mgm/Namespace.hh"
#include "mgm/bulk-request/interface/IMgmFileSystemInterface.hh"

EOSMGMNAMESPACE_BEGIN

/**
 * Implementation of the MGM File system interface
 */
class DummyMgmFileSystemInterface : public IMgmFileSystemInterface {
public:
  void addStats(const char* tag, uid_t uid, gid_t gid, unsigned long val) override;
  bool isTapeEnabled() override;
  int Emsg(const char* pfx, XrdOucErrInfo& einfo, int ecode, const char* op, const char* target = "") override;
  int _exists(const char* path, XrdSfsFileExistence& file_exists, XrdOucErrInfo& error, const XrdSecEntity* client = 0, const char* ininfo = 0) override;
  int _attr_ls(const char* path, XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid, const char* opaque, eos::IContainerMD::XAttrMap& map, bool take_lock = true, bool links = false) override;
  int _access(const char* path, int mode,XrdOucErrInfo& error, eos::common::VirtualIdentity& vid, const char* info, bool lock = true) override;
  int FSctl(const int cmd, XrdSfsFSctl& args, XrdOucErrInfo& error, const XrdSecEntity* client) override;
};

EOSMGMNAMESPACE_END

#endif // EOS_DUMMYMGMFILESYSTEMINTERFACE_HH
