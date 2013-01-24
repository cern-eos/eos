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

#ifndef __EOSFST_DELETION_HH__
#define __EOSFST_DELETION_HH__
/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "common/FileId.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
/*----------------------------------------------------------------------------*/
#include <vector>

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
class Deletion
{
public:
  std::vector<unsigned long long> fIdVector;
  unsigned long fsId;
  XrdOucString localPrefix;
  XrdOucString managerId;
  XrdOucString opaque;

  Deletion (std::vector<unsigned long long> &idvector, unsigned long fsid, const char* localprefix, const char* managerid, const char* inopaque)
  {
    fIdVector = idvector;
    fsId = fsid;
    localPrefix = localprefix;
    managerId = managerid;
    opaque = inopaque;
  }

  static Deletion*
  Create (XrdOucEnv* capOpaque)
  {
    // decode the opaque tags
    const char* localprefix = 0;
    XrdOucString hexfids = "";
    XrdOucString hexfid = "";
    XrdOucString access = "";
    const char* sfsid = 0;
    const char* smanager = 0;
    std::vector <unsigned long long> idvector;

    unsigned long long fileid = 0;
    unsigned long fsid = 0;

    localprefix = capOpaque->Get("mgm.localprefix");
    hexfids = capOpaque->Get("mgm.fids");
    sfsid = capOpaque->Get("mgm.fsid");
    smanager = capOpaque->Get("mgm.manager");
    access = capOpaque->Get("mgm.access");

    // permission check
    if (access != "delete")
      return 0;

    if (!localprefix || !hexfids.length() || !sfsid || !smanager)
    {
      return 0;
    }

    int envlen;
    while (hexfids.replace(",", " "))
    {
    };
    XrdOucTokenizer subtokenizer((char*) hexfids.c_str());
    subtokenizer.GetLine();
    while (1)
    {
      hexfid = subtokenizer.GetToken();
      if (hexfid.length())
      {
        fileid = eos::common::FileId::Hex2Fid(hexfid.c_str());
        idvector.push_back(fileid);
      }
      else
      {
        break;
      }
    }

    fsid = atoi(sfsid);
    return new Deletion(idvector, fsid, localprefix, smanager, capOpaque->Env(envlen));
  };

  ~Deletion () { };
};

EOSFSTNAMESPACE_END

#endif
