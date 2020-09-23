// ----------------------------------------------------------------------
// File: Deletion.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

#pragma once
#include "fst/Namespace.hh"
#include "common/FileId.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
#include <vector>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class Deletion
//------------------------------------------------------------------------------
class Deletion
{
public:
  std::vector<unsigned long long> mFidVect;
  unsigned long mFsid;
  XrdOucString mLocalPrefix;

  //------------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param id_vect file ids to delete
  //! @param fsid filesystem id
  //! @param local_prerfix filesystem local prefix path
  //------------------------------------------------------------------------------
  Deletion(std::vector<unsigned long long> id_vect, unsigned long fsid,
           const char* local_prefix):
    mFidVect(id_vect), mFsid(fsid), mLocalPrefix(local_prefix)
  {}

  //------------------------------------------------------------------------------
  //! Destructor
  //------------------------------------------------------------------------------
  ~Deletion() = default;

  //------------------------------------------------------------------------------
  //! Create deletion object from the provided opaque information
  //!
  //! @param opaque opaque info
  //!
  //! @return deletion object
  //------------------------------------------------------------------------------
  static std::unique_ptr<Deletion>
  Create(XrdOucEnv* capOpaque)
  {
    const char* localprefix = 0;
    XrdOucString hexfids = "";
    XrdOucString hexfid = "";
    XrdOucString access = "";
    const char* sfsid = 0;
    std::vector <unsigned long long> idvector;
    localprefix = capOpaque->Get("mgm.localprefix");
    hexfids = capOpaque->Get("mgm.fids");
    sfsid = capOpaque->Get("mgm.fsid");
    access = capOpaque->Get("mgm.access");

    if ((access != "delete") || !localprefix || !hexfids.length() || !sfsid) {
      return nullptr;
    }

    while (hexfids.replace(",", " ")) {
    };

    XrdOucTokenizer subtokenizer((char*) hexfids.c_str());

    subtokenizer.GetLine();

    while (true) {
      hexfid = subtokenizer.GetToken();

      if (hexfid.length()) {
        idvector.push_back(eos::common::FileId::Hex2Fid(hexfid.c_str()));
      } else {
        break;
      }
    }

    unsigned long fsid = atoi(sfsid);
    return std::make_unique<Deletion>(std::move(idvector), fsid, localprefix);
  }
};

EOSFSTNAMESPACE_END
