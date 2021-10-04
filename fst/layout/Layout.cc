//------------------------------------------------------------------------------
// File: Layout.cc
// Author: Elvin-Alin Sindrilaru / Andreas-Joachim Peters - CERN
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

#include "fst/layout/Layout.hh"
#include "fst/XrdFstOfsFile.hh"
#include "common/Strerror_r_wrapper.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Layout::Layout(XrdFstOfsFile* file) :
  mIsEntryServer(false), mLayoutId(0), mName(""),
  mLastErrCode(0), mLastErrNo(0), mOfsFile(file), mError(0), mSecEntity(0),
  mIoType(eos::common::LayoutId::kLocal), mTimeout(0), mFileIO(nullptr)
{}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Layout::Layout(XrdFstOfsFile* file,
               unsigned long lid,
               const XrdSecEntity* client,
               XrdOucErrInfo* outError,
               const char* path,
               uint16_t timeout) :
  eos::common::LogId(), mIsEntryServer(false), mLayoutId(lid),
  mLastErrCode(0), mLastErrNo(0), mOfsFile(file), mError(outError),
  mTimeout(timeout)
{
  mSecEntity = const_cast<XrdSecEntity*>(client);
  mIoType = eos::common::LayoutId::GetIoType(path);
  mName = eos::common::LayoutId::GetLayoutTypeString(mLayoutId);
  mLocalPath = (path ? path : "");
  mFileIO.reset(FileIoPlugin::GetIoObject((path ? path : ""), mOfsFile,
                                          mSecEntity));
}

//------------------------------------------------------------------------------
// Return error message
//------------------------------------------------------------------------------
int
Layout::Emsg(const char* pfx, XrdOucErrInfo& einfo,
             int ecode, const char* op, const char* target)
{
  char etext[128], buffer[4096];

  // Get the reason for the error
  if (ecode < 0) {
    ecode = -ecode;
  }

  if (eos::common::strerror_r(ecode, etext, sizeof(etext))) {
    sprintf(etext, "reason unknown (%d)", ecode);
  }

  // Format the error message
  snprintf(buffer, sizeof(buffer), "Unable to %s %s; %s", op, target, etext);

  if ((ecode == EIDRM) || (ecode == ENODATA)) {
    eos_static_debug("Unable to %s %s; %s", op, target, etext);
  } else {
    if ((!strcmp(op, "stat")) || (((!strcmp(pfx, "attr_get")) ||
                                   (!strcmp(pfx, "attr_ls")) ||
                                   (!strcmp(pfx, "FuseX"))) && (ecode == ENOENT))) {
      eos_static_debug("Unable to %s %s; %s", op, target, etext);
    } else {
      eos_static_err("Unable to %s %s; %s", op, target, etext);
    }
  }

  // Place the error message in the error object and return
  einfo.setErrInfo(ecode, buffer);
  return SFS_ERROR;
}

EOSFSTNAMESPACE_END
