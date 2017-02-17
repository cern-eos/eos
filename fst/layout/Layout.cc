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

/*----------------------------------------------------------------------------*/
#include "fst/layout/Layout.hh"
#include "fst/XrdFstOfsFile.hh"

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Layout::Layout(XrdFstOfsFile* file) :
  mIsEntryServer(false), mLayoutId(0), mLastErrCode(0),
  mLastErrNo(0), mOfsFile(file), mError(0), mSecEntity(0),
  mIoType(eos::common::LayoutId::kLocal), mTimeout(0), mFileIO(0)
{
  mName = "";
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Layout::Layout(XrdFstOfsFile* file,
               unsigned long lid,
               const XrdSecEntity* client,
               XrdOucErrInfo* outError,
               const char* path,
               uint16_t timeout) :
  eos::common::LogId(),
  mLayoutId(lid),
  mLastErrCode(0),
  mLastErrNo(0),
  mOfsFile(file),
  mError(outError),
  mTimeout(timeout)

{
  mSecEntity = const_cast<XrdSecEntity*>(client);
  mIoType = eos::common::LayoutId::GetIoType(path);
  mName = eos::common::LayoutId::GetLayoutTypeString(mLayoutId);
  mIsEntryServer = false;
  mLocalPath = (path ? path : "");
  mFileIO = FileIoPlugin::GetIoObject((path ? path : ""), mOfsFile, mSecEntity);
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
Layout::~Layout()
{
  if (mFileIO) {
    delete mFileIO;
  }
}

EOSFSTNAMESPACE_END
