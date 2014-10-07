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
Layout::Layout (XrdFstOfsFile* file) :
mOfsFile (file)
{
  mName = "";
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Layout::Layout (XrdFstOfsFile* file,
                unsigned long lid,
                const XrdSecEntity* client,
                XrdOucErrInfo* outError,
                eos::common::LayoutId::eIoType io,
                uint16_t timeout) :
eos::common::LogId(),
mLayoutId(lid),
mOfsFile(file),
mError(outError),
mIoType(io),
mTimeout(timeout)
{
  mSecEntity = const_cast<XrdSecEntity*> (client);
  mName = eos::common::LayoutId::GetLayoutTypeString(mLayoutId);
  mIsEntryServer = false;
  mLocalPath = "";
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
Layout::~Layout () {
  // empty
}

EOSFSTNAMESPACE_END
